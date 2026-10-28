// Package deliver performs a single signed webhook HTTP request.
package deliver

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"webhookdelivery/internal/sign"
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
}

// Success reports whether the receiver acknowledged with a 2xx.
func (r Result) Success() bool {
	return r.Err == nil && r.StatusCode >= 200 && r.StatusCode < 300
}

type Sender struct {
	client *http.Client
}

// NewSender builds a sender whose requests are bounded by timeout in total.
// Redirects are never followed and environment proxies are ignored.
func NewSender(timeout time.Duration) *Sender {
	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		TLSHandshakeTimeout: 5 * time.Second,
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	return &Sender{client: &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

func (s *Sender) Send(ctx context.Context, r Request) Result {
	start := time.Now()
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
