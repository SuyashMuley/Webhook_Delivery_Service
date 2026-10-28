package sign

import (
	"errors"
	"testing"
	"time"
)

// Test vector from the Standard Webhooks specification repository.
func TestSignKnownVector(t *testing.T) {
	secret, err := ParseSecret("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")
	if err != nil {
		t.Fatal(err)
	}
	got := Sign(secret, "msg_p5jXN8AQM9LWM0D4loKWxJek", "1614265330", []byte(`{"test": 2432232314}`))
	want := "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
	if got != want {
		t.Fatalf("signature = %q, want %q", got, want)
	}
}

func TestVerify(t *testing.T) {
	s1, _ := GenerateSecret()
	s2, _ := GenerateSecret()
	k1, _ := ParseSecret(s1)
	k2, _ := ParseSecret(s2)
	now := time.Unix(1_700_000_000, 0)
	ts := "1700000000"
	body := []byte(`{"a":1}`)

	// Header signed with both secrets (rotation): either one verifies.
	header := Header([][]byte{k1, k2}, "id1", ts, body)
	for name, key := range map[string][]byte{"old": k1, "new": k2} {
		if err := Verify(header, [][]byte{key}, "id1", ts, body, 5*time.Minute, now); err != nil {
			t.Errorf("%s secret: %v", name, err)
		}
	}

	if err := Verify(header, [][]byte{k1}, "id1", ts, []byte(`{"a":2}`), 5*time.Minute, now); !errors.Is(err, ErrInvalidSignature) {
		t.Errorf("tampered body: got %v", err)
	}
	if err := Verify(header, [][]byte{k1}, "other", ts, body, 5*time.Minute, now); !errors.Is(err, ErrInvalidSignature) {
		t.Errorf("wrong id: got %v", err)
	}
	if err := Verify(header, [][]byte{k1}, "id1", ts, body, 5*time.Minute, now.Add(time.Hour)); !errors.Is(err, ErrTimestamp) {
		t.Errorf("stale timestamp: got %v", err)
	}
}

func TestParseSecretRejectsBadInput(t *testing.T) {
	for _, s := range []string{"", "nope", "whsec_!!!", "whsec_YQ=="} {
		if _, err := ParseSecret(s); err == nil {
			t.Errorf("ParseSecret(%q) succeeded, want error", s)
		}
	}
}
