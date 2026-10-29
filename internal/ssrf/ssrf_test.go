package ssrf

import (
	"errors"
	"net/netip"
	"testing"
)

func TestBlocked(t *testing.T) {
	cases := map[string]bool{
		// internal and special-purpose: blocked
		"127.0.0.1":              true,
		"127.255.255.254":        true,
		"10.1.2.3":               true,
		"172.16.0.1":             true,
		"172.31.255.255":         true,
		"192.168.1.1":            true,
		"169.254.169.254":        true, // cloud metadata
		"100.64.0.1":             true,
		"0.0.0.0":                true,
		"255.255.255.255":        true,
		"224.0.0.1":              true,
		"::1":                    true,
		"::":                     true,
		"fe80::1":                true,
		"fc00::1":                true,
		"fd12:3456::1":           true,
		"::ffff:127.0.0.1":       true, // IPv4-mapped loopback must not slip through
		"::ffff:10.0.0.1":        true,
		"::ffff:169.254.169.254": true,
		"64:ff9b::a00:1":         true,
		"2002:7f00:1::1":         true,
		// public: allowed
		"8.8.8.8":              false,
		"1.1.1.1":              false,
		"172.15.255.255":       false, // just outside 172.16/12
		"172.32.0.1":           false,
		"100.63.255.255":       false, // just outside 100.64/10
		"93.184.216.34":        false,
		"2606:4700:4700::1111": false,
		"::ffff:8.8.8.8":       false,
	}
	for s, want := range cases {
		if got := Blocked(netip.MustParseAddr(s)); got != want {
			t.Errorf("Blocked(%s) = %v, want %v", s, got, want)
		}
	}
	if !Blocked(netip.Addr{}) {
		t.Error("the zero address must be blocked")
	}
}

func TestControl(t *testing.T) {
	var be *BlockedError
	if err := Control("tcp", "127.0.0.1:443", nil); !errors.As(err, &be) {
		t.Errorf("loopback: got %v, want BlockedError", err)
	}
	if err := Control("tcp6", "[::1]:443", nil); !errors.As(err, &be) {
		t.Errorf("ipv6 loopback: got %v, want BlockedError", err)
	}
	if err := Control("tcp", "8.8.8.8:443", nil); err != nil {
		t.Errorf("public address: %v", err)
	}
	if err := Control("tcp", "not-an-address", nil); err == nil {
		t.Error("malformed address must be rejected")
	}
}

func TestValidateHost(t *testing.T) {
	bad := []string{"localhost", "LOCALHOST.", "app.localhost", "db.internal", "printer.local", "127.0.0.1", "10.0.0.5", "::1", "169.254.169.254"}
	for _, h := range bad {
		if ValidateHost(h) == nil {
			t.Errorf("ValidateHost(%q) accepted an internal host", h)
		}
	}
	for _, h := range []string{"example.com", "hooks.customer.io", "8.8.8.8"} {
		if err := ValidateHost(h); err != nil {
			t.Errorf("ValidateHost(%q) = %v", h, err)
		}
	}
}
