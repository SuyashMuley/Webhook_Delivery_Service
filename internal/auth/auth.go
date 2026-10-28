// Package auth generates and hashes tenant API keys.
//
// Keys are 256 bits of randomness, so a plain SHA-256 is sufficient for
// storage (no need for a slow password hash). Only the hash is persisted.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
)

// GenerateKey returns a new API key and its non-secret display prefix.
// Format: whk_<8 hex>_<43 base64url>.
func GenerateKey() (key, prefix string, err error) {
	id := make([]byte, 4)
	secret := make([]byte, 32)
	if _, err = rand.Read(id); err != nil {
		return "", "", err
	}
	if _, err = rand.Read(secret); err != nil {
		return "", "", err
	}
	prefix = "whk_" + hex.EncodeToString(id)
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(secret), prefix, nil
}

// HashKey returns the value stored in api_keys.key_hash.
func HashKey(key string) []byte {
	h := sha256.Sum256([]byte(key))
	return h[:]
}
