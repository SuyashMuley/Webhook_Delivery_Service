package loadtest

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"webhookdelivery/internal/sign"
)

// reply is the part of a response the tests inspect; the body is already closed.
type reply struct{ StatusCode int }

func send(t *testing.T, r *Receiver, endpoint, id string, secret string, timeout time.Duration) (*reply, error) {
	t.Helper()
	key, _ := sign.ParseSecret(secret)
	body := []byte(`{"n":1}`)
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", r.URL(endpoint), bytes.NewReader(body))
	req.Header.Set("webhook-id", id)
	req.Header.Set("webhook-timestamp", ts)
	req.Header.Set("webhook-signature", sign.Header([][]byte{key}, id, ts, body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return &reply{StatusCode: resp.StatusCode}, nil
}

func TestReceiverRecordsOnlyWhatItAcknowledged(t *testing.T) {
	r, err := NewReceiver()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	secret, _ := sign.GenerateSecret()
	if err := r.Register("ep1", secret); err != nil {
		t.Fatal(err)
	}

	// healthy: recorded
	if resp, err := send(t, r, "ep1", "ev1", secret, time.Second); err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthy send: %v %v", resp, err)
	}
	// 5xx: the endpoint refused it, so it must NOT count as received
	r.SetFault(Fault{Kind: FaultStatus})
	if resp, _ := send(t, r, "ep1", "ev2", secret, time.Second); resp == nil || resp.StatusCode != 500 {
		t.Fatalf("injected 500 not returned: %v", resp)
	}
	// reset / garbage: no usable response, not recorded
	for _, k := range []FaultKind{FaultReset, FaultGarbage} {
		r.SetFault(Fault{Kind: k})
		if _, err := send(t, r, "ep1", "ev-"+k.String(), secret, time.Second); err == nil {
			t.Fatalf("%v: the client got a valid response", k)
		}
	}
	// slow: the request WAS processed even though the client gives up: it counts, so the retry is a duplicate
	r.SetFault(Fault{Kind: FaultSlow, Delay: 2 * time.Second})
	if _, err := send(t, r, "ep1", "ev-slow", secret, 100*time.Millisecond); err == nil {
		t.Fatal("slow request did not time out")
	}

	if r.Count("ev1", "ep1") != 1 {
		t.Error("healthy delivery not recorded")
	}
	for _, id := range []string{"ev2", "ev-reset", "ev-garbage"} {
		if r.Count(id, "ep1") != 0 {
			t.Errorf("%s was recorded although the endpoint did not acknowledge it", id)
		}
	}
	if r.Count("ev-slow", "ep1") != 1 {
		t.Error("a processed-but-slow request must be recorded")
	}
	if _, ok := r.FirstSeen("ev1", "ep1"); !ok {
		t.Error("no first-seen time")
	}
}

func TestReceiverRejectsBadSignaturesAndCountsThemAsBugs(t *testing.T) {
	r, _ := NewReceiver()
	defer r.Close()
	good, _ := sign.GenerateSecret()
	other, _ := sign.GenerateSecret()
	_ = r.Register("ep1", good)
	if resp, _ := send(t, r, "ep1", "ev1", other, time.Second); resp == nil || resp.StatusCode != 401 {
		t.Fatalf("a request signed with the wrong secret was accepted: %v", resp)
	}
	if r.BadSignature.Load() != 1 || r.Count("ev1", "ep1") != 0 {
		t.Fatalf("bad signature bookkeeping: bad=%d recorded=%d", r.BadSignature.Load(), r.Count("ev1", "ep1"))
	}
}

func TestFaultProbabilityMixesHealthyAndFaulty(t *testing.T) {
	r, _ := NewReceiver()
	defer r.Close()
	secret, _ := sign.GenerateSecret()
	_ = r.Register("ep1", secret)
	r.SetFault(Fault{Kind: FaultStatus, Prob: 0.5})
	ok, bad := 0, 0
	for i := range 200 {
		resp, _ := send(t, r, "ep1", "ev"+strconv.Itoa(i), secret, time.Second)
		if resp != nil && resp.StatusCode == 200 {
			ok++
		} else {
			bad++
		}
	}
	if ok < 60 || bad < 60 {
		t.Fatalf("a 50%% fault gave %d ok / %d failed out of 200", ok, bad)
	}
}
