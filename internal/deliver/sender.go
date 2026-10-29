// Package deliver performs a single signed webhook HTTP request.
package deliver

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"webhookdelivery/internal/sign"
	"webhookdelivery/internal/ssrf"
)

const (
	maxResponseBytes = 64 << 10
	snippetBytes     = 1024
)

type Request struct {
	URL       string
	ID        string // webhook-id: stable across retries so receivers can dedupe
	Timestamp time.Time
	Body      []byte
	Secrets   [][]byte
}

type Result struct {
	StatusCode  int // 0 if no response was received
	Err         error
	Duration    time.Duration
	BodySnippet string
	RetryAfter  time.Duration // from the Retry-After response header; 0 if absent
}

// Success reports whether the receiver acknowledged with a 2xx.
func (r Result) Success() bool {
	return r.Err == nil && r.StatusCode >= 200 && r.StatusCode < 300
}

type Sender struct {
	client       *http.Client
	allowPrivate bool
}

// ErrInsecureURL is returned for a non-https target when private targets are not allowed.
var ErrInsecureURL = errors.New("refusing to deliver over plain http")

// NewSender builds a sender whose requests are bounded by timeout in total.
// Redirects are never followed and environment proxies are ignored.
//
// Unless allowPrivateTargets is set (development and tests only), deliveries
// are https-only and connections to non-public addresses (loopback, private
// ranges, link-local/cloud metadata, ...) are refused at connect time; see
// package ssrf.
func NewSender(timeout time.Duration, allowPrivateTargets bool) *Sender {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	if !allowPrivateTargets {
		dialer.Control = ssrf.Control
	}
	transport := &http.Transport{
		DialContext:         dialer.DialContext,
		TLSHandshakeTimeout: 5 * time.Second,
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	return &Sender{allowPrivate: allowPrivateTargets, client: &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

func (s *Sender) Send(ctx context.Context, r Request) Result {
	start := time.Now()
	// Defense in depth: the API only accepts https URLs in production, but an
	// endpoint stored while running in development must not be reachable over
	// plain http after switching to production.
	if u, err := url.Parse(r.URL); err != nil || (!s.allowPrivate && u.Scheme != "https") {
		if err == nil {
			err = ErrInsecureURL
		}
		return Result{Err: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(r.Body))
	if err != nil {
		return Result{Err: err}
	}
	ts := strconv.FormatInt(r.Timestamp.Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "webhookdelivery/1")
	req.Header.Set("webhook-id", r.ID)
	req.Header.Set("webhook-timestamp", ts)
	req.Header.Set("webhook-signature", sign.Header(r.Secrets, r.ID, ts, r.Body))

	resp, err := s.client.Do(req)
	if err != nil {
		return Result{Err: err, Duration: time.Since(start)}
	}
	defer func() { _ = resp.Body.Close() }()

	buf := make([]byte, maxResponseBytes)
	n := readFull(resp, buf)
	return Result{
		StatusCode:  resp.StatusCode,
		Duration:    time.Since(start),
		BodySnippet: snippet(buf[:n]),
		RetryAfter:  parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
	}
}

// readFull reads up to len(buf) bytes of the body, ignoring read errors: the
// status code is what matters, the body is only kept for debugging.
func readFull(resp *http.Response, buf []byte) int {
	n := 0
	for n < len(buf) {
		m, err := resp.Body.Read(buf[n:])
		n += m
		if err != nil {
			break
		}
	}
	return n
}

// snippet returns the first KB of b as text that Postgres will accept
// (valid UTF-8, no NUL bytes).
func snippet(b []byte) string {
	if len(b) > snippetBytes {
		b = b[:snippetBytes]
	}
	s := strings.ToValidUTF8(string(b), "�")
	return strings.ReplaceAll(s, "\x00", "")
}

// maxRetryAfter keeps absurd header values from overflowing time.Duration.
const maxRetryAfter = 30 * 24 * time.Hour

// parseRetryAfter understands both forms allowed by RFC 9110: a number of
// seconds, or an HTTP date. Anything unparseable or in the past yields 0.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		if secs >= int64(maxRetryAfter/time.Second) { // also guards the multiply below against overflow
			return maxRetryAfter
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return min(max(t.Sub(now), 0), maxRetryAfter)
	}
	return 0
}
