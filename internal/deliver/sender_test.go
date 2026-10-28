package deliver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"webhookdelivery/internal/sign"
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
	res := NewSender(5*time.Second).Send(context.Background(), req)
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
		res := NewSender(5*time.Second).Send(context.Background(), req)
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
	res := NewSender(5*time.Second).Send(context.Background(), req)
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
	res := NewSender(100*time.Millisecond).Send(context.Background(), req)
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
