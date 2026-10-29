package secretbox

import (
	"crypto/rand"
	"strings"
	"testing"
)

func ring(t *testing.T, id string) *Keyring {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	r, err := NewKeyring(id, key)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRoundTrip(t *testing.T) {
	r := ring(t, "k1")
	sealed, err := r.Seal("whsec_supersecret", "ep1/sec1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sealed, "supersecret") {
		t.Fatal("sealed value leaks the plaintext")
	}
	got, err := r.Open(sealed, "ep1/sec1")
	if err != nil || got != "whsec_supersecret" {
		t.Fatalf("Open = %q, %v", got, err)
	}
}

func TestSealIsRandomized(t *testing.T) {
	r := ring(t, "k1")
	a, _ := r.Seal("same", "ctx")
	b, _ := r.Seal("same", "ctx")
	if a == b {
		t.Fatal("two seals of the same value are identical: nonce/DEK reuse")
	}
}

func TestOpenRejectsWrongContextAndTampering(t *testing.T) {
	r := ring(t, "k1")
	sealed, _ := r.Seal("secret", "ep1/sec1")

	if _, err := r.Open(sealed, "ep2/sec1"); err == nil {
		t.Error("a sealed secret opened under a different endpoint's context")
	}

	parts := strings.Split(sealed, ".")
	flipped := []byte(parts[3])
	if flipped[len(flipped)-1] == 'A' {
		flipped[len(flipped)-1] = 'B'
	} else {
		flipped[len(flipped)-1] = 'A'
	}
	parts[3] = string(flipped)
	if _, err := r.Open(strings.Join(parts, "."), "ep1/sec1"); err == nil {
		t.Error("tampered ciphertext was accepted")
	}

	for _, bad := range []string{"", "plaintext", "v2.k1.a.b", "v1.k1.a"} {
		if _, err := r.Open(bad, "x"); err == nil {
			t.Errorf("Open(%q) succeeded", bad)
		}
	}
}

func TestWrongKeyCannotOpen(t *testing.T) {
	sealed, _ := ring(t, "k1").Seal("secret", "ctx")
	if _, err := ring(t, "k1").Open(sealed, "ctx"); err == nil {
		t.Error("a different key with the same id opened the secret")
	}
	if _, err := ring(t, "other").Open(sealed, "ctx"); err == nil {
		t.Error("a ring without the key id opened the secret")
	}
}

func TestKeyRotation(t *testing.T) {
	oldKey, _ := GenerateKey("old")
	newKey, _ := GenerateKey("new")

	oldRing, _ := ParseKeyring(oldKey)
	sealed, _ := oldRing.Seal("secret", "ctx")
	if KeyID(sealed) != "old" {
		t.Fatalf("KeyID = %q", KeyID(sealed))
	}

	// After rotation "new" is active but "old" is kept for reading.
	rotated, err := ParseKeyring(newKey + "," + oldKey)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := rotated.Open(sealed, "ctx"); err != nil || got != "secret" {
		t.Fatalf("old secret unreadable after rotation: %q %v", got, err)
	}
	resealed, _ := rotated.Seal("secret", "ctx")
	if KeyID(resealed) != "new" || rotated.ActiveKeyID() != "new" {
		t.Fatalf("new seals must use the active key, got %q", KeyID(resealed))
	}
}

func TestParseKeyringErrors(t *testing.T) {
	good, _ := GenerateKey("k1")
	for name, spec := range map[string]string{
		"empty":      "",
		"no colon":   "justabase64",
		"bad base64": "k1:!!!",
		"short key":  "k1:AAAA",
		"bad id":     "bad id:" + strings.SplitN(good, ":", 2)[1],
		"dup id":     good + "," + good,
	} {
		if _, err := ParseKeyring(spec); err == nil {
			t.Errorf("%s: ParseKeyring(%q) succeeded", name, spec)
		}
	}
}
