// Package ssrf stops webhook delivery from being used to reach internal
// services (cloud metadata endpoints, databases, admin interfaces).
//
// The check that matters runs at connect time on the IP address actually being
// dialed (see Control). Validating a hostname when an endpoint is registered is
// not enough: DNS can answer differently later (DNS rebinding), so only the
// resolved address of the live connection can be trusted.
package ssrf

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
	"syscall"
)

// blockedPrefixes lists address ranges that must never be a delivery target:
// everything that is not a routable public unicast address.
var blockedPrefixes = mustParse(
	// IPv4
	"0.0.0.0/8",       // "this network"
	"10.0.0.0/8",      // private
	"100.64.0.0/10",   // carrier-grade NAT
	"127.0.0.0/8",     // loopback
	"169.254.0.0/16",  // link-local, includes the 169.254.169.254 cloud metadata service
	"172.16.0.0/12",   // private
	"192.0.0.0/24",    // IETF protocol assignments
	"192.0.2.0/24",    // documentation
	"192.88.99.0/24",  // 6to4 relay (deprecated)
	"192.168.0.0/16",  // private
	"198.18.0.0/15",   // benchmarking
	"198.51.100.0/24", // documentation
	"203.0.113.0/24",  // documentation
	"224.0.0.0/4",     // multicast
	"240.0.0.0/4",     // reserved, includes broadcast
	// IPv6 (IPv4-mapped addresses are unmapped and checked as IPv4)
	"::/96",         // unspecified, loopback and deprecated IPv4-compatible
	"64:ff9b::/96",  // NAT64: would embed an IPv4 address we cannot vet
	"100::/64",      // discard-only
	"2001::/32",     // Teredo
	"2001:db8::/32", // documentation
	"2002::/16",     // 6to4: embeds an IPv4 address
	"fc00::/7",      // unique local
	"fe80::/10",     // link-local
	"ff00::/8",      // multicast
)

func mustParse(cidrs ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(cidrs))
	for i, c := range cidrs {
		out[i] = netip.MustParsePrefix(c)
	}
	return out
}

// Blocked reports whether ip is not a public address that deliveries may reach.
func Blocked(ip netip.Addr) bool {
	ip = ip.Unmap().WithZone("")
	if !ip.IsValid() {
		return true
	}
	for _, p := range blockedPrefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// BlockedError is returned when a connection to a non-public address is refused.
type BlockedError struct{ IP netip.Addr }

func (e *BlockedError) Error() string {
	return fmt.Sprintf("blocked by SSRF policy: %s is not a public address", e.IP)
}

// Control is a net.Dialer.Control function. It runs after DNS resolution with
// the concrete address about to be connected to, so it cannot be bypassed by a
// hostname that resolves to (or later rebinds to) an internal IP.
func Control(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("ssrf: bad dial address %q: %w", address, err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("ssrf: bad dial address %q: %w", address, err)
	}
	if Blocked(ip) {
		return &BlockedError{IP: ip}
	}
	return nil
}

// ValidateHost rejects hosts that are obviously internal. It gives users fast
// feedback when registering an endpoint, but it is advisory: Control is the
// actual enforcement.
func ValidateHost(host string) error {
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	if h == "localhost" || strings.HasSuffix(h, ".localhost") ||
		strings.HasSuffix(h, ".local") || strings.HasSuffix(h, ".internal") {
		return fmt.Errorf("host %q is not a public address", host)
	}
	if ip, err := netip.ParseAddr(h); err == nil && Blocked(ip) {
		return fmt.Errorf("host %q is not a public address", host)
	}
	return nil
}
