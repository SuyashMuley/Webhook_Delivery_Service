package deliver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"webhookdelivery/internal/sign"
	"webhookdelivery/internal/ssrf"
)

func testRequest(url string) (Request, []byte) {
	s, _ := sign.GenerateSecret()
	key, _ := sign.ParseSecret(s)
	return Request{
		URL:       url,
		ID:        "evt_1",
		Timestamp: time.Now(),
		Body:      []byte(`{"hello":"world"}`),
		Secrets:   [][]byte{key},
	}, key
}

func TestSendSignsRequest(t *testing.T) {
	var gotBody []byte
	var gotHeader http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotHeader = r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	req, key := testRequest(srv.URL)
	res := NewSender(5*time.Second, true).Send(context.Background(), req)
	if !res.Success() || res.StatusCode != http.StatusNoContent {
		t.Fatalf("result = %+v", res)
	}
	err := sign.Verify(gotHeader.Get("webhook-signature"), [][]byte{key},
		gotHeader.Get("webhook-id"), gotHeader.Get("webhook-timestamp"), gotBody, time.Minute, time.Now())
	if err != nil {
		t.Fatalf("receiver could not verify signature: %v", err)
	}
	if gotHeader.Get("webhook-id") != "evt_1" {
		t.Errorf("webhook-id = %q", gotHeader.Get("webhook-id"))
	}
}

func TestSendClassifiesResponses(t *testing.T) {
	for _, code := range []int{http.StatusInternalServerError, http.StatusBadRequest, http.StatusFound} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if code == http.StatusFound {
				w.Header().Set("Location", "/elsewhere")
			}
			w.WriteHeader(code)
		}))
		req, _ := testRequest(srv.URL)
		res := NewSender(5*time.Second, true).Send(context.Background(), req)
		srv.Close()
		if res.Success() || res.StatusCode == 0 {
			t.Errorf("code %d: result = %+v, want a non-success response", code, res)
		}
	}
}

func TestSendDoesNotFollowRedirects(t *testing.T) {
	hit := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hit = true }))
	defer target.Close()
	srv := httptest.NewServer(http.RedirectHandler(target.URL, http.StatusTemporaryRedirect))
	defer srv.Close()

	req, _ := testRequest(srv.URL)
	res := NewSender(5*time.Second, true).Send(context.Background(), req)
	if hit || res.StatusCode != http.StatusTemporaryRedirect || res.Success() {
		t.Fatalf("redirect followed or treated as success: hit=%v res=%+v", hit, res)
	}
}

func TestSendTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer srv.Close()

	req, _ := testRequest(srv.URL)
	res := NewSender(100*time.Millisecond, true).Send(context.Background(), req)
	if res.Err == nil || res.Success() {
		t.Fatalf("expected timeout error, got %+v", res)
	}
}

func TestSnippetSanitizes(t *testing.T) {
	got := snippet([]byte("ok\x00\xff end"))
	if got != "ok� end" {
		t.Fatalf("snippet = %q", got)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	cases := map[string]time.Duration{
		"":                              0,
		"30":                            30 * time.Second,
		" 120 ":                         2 * time.Minute,
		"0":                             0,
		"-5":                            0,
		"soon":                          0,
		"Wed, 28 Oct 2026 12:05:00 GMT": 5 * time.Minute,
		"Wed, 28 Oct 2026 11:00:00 GMT": 0, // in the past
		"99999999999999":                maxRetryAfter,
	}
	for in, want := range cases {
		if got := parseRetryAfter(in, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestProductionSenderRefusesInternalAndInsecureTargets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("request reached an internal server")
	}))
	defer srv.Close()
	prod := NewSender(5*time.Second, false)

	// plain http is refused before any connection is made
	req, _ := testRequest(srv.URL)
	if res := prod.Send(context.Background(), req); !errors.Is(res.Err, ErrInsecureURL) {
		t.Errorf("http target: got %+v, want ErrInsecureURL", res)
	}

	// https to a loopback address is refused at connect time
	tlsSrv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("request reached an internal TLS server")
	}))
	defer tlsSrv.Close()
	req, _ = testRequest(tlsSrv.URL)
	res := prod.Send(context.Background(), req)
	var blocked *ssrf.BlockedError
	if !errors.As(res.Err, &blocked) {
		t.Errorf("loopback https target: got %+v, want BlockedError", res)
	}

	// a hostname that resolves to loopback (what DNS rebinding relies on) is refused too
	req, _ = testRequest(strings.Replace(tlsSrv.URL, "127.0.0.1", "localhost", 1))
	if res := prod.Send(context.Background(), req); !errors.As(res.Err, &blocked) {
		t.Errorf("hostname resolving to loopback: got %+v, want BlockedError", res)
	}
}

// Many concurrent deliveries to one host must share connections. Opening a new
// TCP (and TLS) connection per delivery exhausts ephemeral ports under load and
// pays a handshake each time; found by the load test, which saw
// "can't assign requested address" at ~1,000 deliveries/s.
func TestConcurrentDeliveriesToOneHostReuseConnections(t *testing.T) {
	var conns atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Millisecond) // keep requests overlapping, like real receivers
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	sender := NewSender(5*time.Second, true)
	const workers, perWorker = 40, 25 // 1,000 deliveries, 40 at a time
	var wg sync.WaitGroup
	var failed atomic.Int64
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				req, _ := testRequest(srv.URL)
				if !sender.Send(context.Background(), req).Success() {
					failed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if failed.Load() != 0 {
		t.Fatalf("%d deliveries failed", failed.Load())
	}
	// With reuse, connections track concurrency (~40). Without it, nearly every one of the 1,000 dials anew.
	if got := conns.Load(); got > 120 {
		t.Fatalf("1,000 deliveries at concurrency %d opened %d connections: connections are not being reused", workers, got)
	}
}
