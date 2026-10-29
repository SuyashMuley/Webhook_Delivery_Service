// Package secretbox encrypts endpoint signing secrets at rest using envelope
// encryption.
//
// Each secret is encrypted with its own random data key (DEK) using AES-256-GCM;
// the DEK is in turn encrypted ("wrapped") by a key-encryption key (KEK) from a
// Keyring. Only the wrapped DEK is stored next to the ciphertext. Rotating the
// KEK therefore only needs the small DEKs re-wrapped, and the Sealer interface
// is the seam where a cloud KMS can replace the local Keyring in production.
//
// The additional authenticated data (AAD) binds a ciphertext to the record it
// belongs to, so a sealed secret copied onto another endpoint fails to open.
package secretbox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Sealer encrypts and decrypts secrets. Keyring is the local implementation.
type Sealer interface {
	Seal(plaintext, aad string) (string, error)
	Open(sealed, aad string) (string, error)
}

const version = "v1"

var keyIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

// Keyring holds named 32-byte KEKs. New secrets are sealed with the active
// key; any key in the ring can open secrets sealed with it.
type Keyring struct {
	active string
	keys   map[string][]byte
}

// NewKeyring returns a ring with a single key.
func NewKeyring(id string, key []byte) (*Keyring, error) {
	if !keyIDPattern.MatchString(id) {
		return nil, fmt.Errorf("secretbox: invalid key id %q", id)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("secretbox: key %q must be 32 bytes, got %d", id, len(key))
	}
	return &Keyring{active: id, keys: map[string][]byte{id: key}}, nil
}

// ParseKeyring reads "id1:base64key,id2:base64key". The first key is active;
// the rest are kept so secrets sealed under older keys can still be opened
// during a key rotation.
func ParseKeyring(spec string) (*Keyring, error) {
	var ring *Keyring
	for _, part := range strings.Split(spec, ",") {
		id, b64, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, errors.New("secretbox: keys must look like id:base64key")
		}
		key, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("secretbox: key %q is not valid base64: %w", id, err)
		}
		if ring == nil {
			if ring, err = NewKeyring(id, key); err != nil {
				return nil, err
			}
			continue
		}
		if !keyIDPattern.MatchString(id) || len(key) != 32 {
			return nil, fmt.Errorf("secretbox: key %q must have a valid id and 32 bytes", id)
		}
		if _, dup := ring.keys[id]; dup {
			return nil, fmt.Errorf("secretbox: duplicate key id %q", id)
		}
		ring.keys[id] = key
	}
	if ring == nil {
		return nil, errors.New("secretbox: no keys configured")
	}
	return ring, nil
}

// DevKeyring returns a fixed, public key for local development only. Secrets
// sealed with it are NOT protected; production config rejects it.
func DevKeyring() *Keyring {
	k := sha256.Sum256([]byte("webhookdelivery-insecure-dev-key"))
	ring, _ := NewKeyring("dev", k[:])
	return ring
}

// GenerateKey returns a new random KEK in the form accepted by ParseKeyring ("id:base64").
func GenerateKey(id string) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return id + ":" + base64.StdEncoding.EncodeToString(b), nil
}

// ActiveKeyID is the id of the key new secrets are sealed with.
func (k *Keyring) ActiveKeyID() string { return k.active }

var b64 = base64.RawURLEncoding

// Seal encrypts plaintext and returns "v1.<key id>.<wrapped DEK>.<ciphertext>".
func (k *Keyring) Seal(plaintext, aad string) (string, error) {
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		return "", err
	}
	ct, err := encrypt(dek, []byte(plaintext), []byte(aad))
	if err != nil {
		return "", err
	}
	wrapped, err := encrypt(k.keys[k.active], dek, wrapAAD(k.active, aad))
	if err != nil {
		return "", err
	}
	return strings.Join([]string{version, k.active, b64.EncodeToString(wrapped), b64.EncodeToString(ct)}, "."), nil
}

// Open decrypts a value produced by Seal. It fails if the value was modified,
// was sealed for a different aad, or was sealed with a key not in the ring.
func (k *Keyring) Open(sealed, aad string) (string, error) {
	parts := strings.Split(sealed, ".")
	if len(parts) != 4 || parts[0] != version {
		return "", errors.New("secretbox: unrecognized sealed format")
	}
	kek, ok := k.keys[parts[1]]
	if !ok {
		return "", fmt.Errorf("secretbox: sealed with unknown key id %q", parts[1])
	}
	wrapped, err1 := b64.DecodeString(parts[2])
	ct, err2 := b64.DecodeString(parts[3])
	if err1 != nil || err2 != nil {
		return "", errors.New("secretbox: sealed value is not valid base64")
	}
	dek, err := decrypt(kek, wrapped, wrapAAD(parts[1], aad))
	if err != nil {
		return "", errors.New("secretbox: cannot unwrap data key (wrong key or tampered value)")
	}
	pt, err := decrypt(dek, ct, []byte(aad))
	if err != nil {
		return "", errors.New("secretbox: cannot decrypt (wrong context or tampered value)")
	}
	return string(pt), nil
}

// KeyID returns the id of the key a sealed value was sealed with, so callers
// can find values that need re-sealing after a key rotation.
func KeyID(sealed string) string {
	parts := strings.Split(sealed, ".")
	if len(parts) != 4 {
		return ""
	}
	return parts[1]
}

func wrapAAD(keyID, aad string) []byte { return []byte("dek:" + keyID + ":" + aad) }

func encrypt(key, plaintext, aad []byte) ([]byte, error) {
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, plaintext, aad), nil // nonce || ciphertext
}

func decrypt(key, blob, aad []byte) ([]byte, error) {
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	if len(blob) < aead.NonceSize() {
		return nil, errors.New("short ciphertext")
	}
	return aead.Open(nil, blob[:aead.NonceSize()], blob[aead.NonceSize():], aad)
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
