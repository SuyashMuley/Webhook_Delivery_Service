// Package sign implements Standard Webhooks (v1, HMAC-SHA256) signing and verification.
//
// The signed content is "{id}.{timestamp}.{body}", the signature header is a
// space-separated list of "v1,<base64>" entries (one per active secret), which
// is what makes zero-downtime secret rotation possible.
package sign

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const secretPrefix = "whsec_"

var (
	ErrInvalidSignature = errors.New("sign: no matching signature")
	ErrTimestamp        = errors.New("sign: timestamp outside tolerance")
)

// GenerateSecret returns a new random signing secret ("whsec_" + base64 of 32 bytes).
func GenerateSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return secretPrefix + base64.StdEncoding.EncodeToString(b), nil
}

// ParseSecret decodes a "whsec_..." secret into its raw key bytes.
func ParseSecret(s string) ([]byte, error) {
	raw, ok := strings.CutPrefix(s, secretPrefix)
	if !ok {
		return nil, fmt.Errorf("secret must start with %q", secretPrefix)
	}
	b, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("secret is not valid base64: %w", err)
	}
	if len(b) < 16 || len(b) > 64 {
		return nil, errors.New("secret must decode to 16-64 bytes")
	}
	return b, nil
}

// Sign returns "v1,<base64 HMAC-SHA256>" for one secret.
func Sign(secret []byte, id, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(id))
	mac.Write([]byte{'.'})
	mac.Write([]byte(timestamp))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// Header returns the webhook-signature header value covering all secrets.
func Header(secrets [][]byte, id, timestamp string, body []byte) string {
	sigs := make([]string, len(secrets))
	for i, s := range secrets {
		sigs[i] = Sign(s, id, timestamp, body)
	}
	return strings.Join(sigs, " ")
}

// Verify checks header against any of secrets and rejects timestamps further
// than tolerance from now (replay protection).
func Verify(header string, secrets [][]byte, id, timestamp string, body []byte, tolerance time.Duration, now time.Time) error {
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrTimestamp
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return ErrTimestamp
	}
	for _, secret := range secrets {
		want := []byte(Sign(secret, id, timestamp, body))
		for _, got := range strings.Fields(header) {
			if hmac.Equal([]byte(got), want) {
				return nil
			}
		}
	}
	return ErrInvalidSignature
}
