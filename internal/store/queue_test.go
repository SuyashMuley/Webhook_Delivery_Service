package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"webhookdelivery/internal/store"
	"webhookdelivery/internal/testutil"
)

func seed(t *testing.T, st *store.Store, events int) string {
	t.Helper()
	ctx := context.Background()
	tenant, _ := testutil.NewTenant(t, st, "acme")
	if _, err := st.CreateEndpoint(ctx, store.Endpoint{
		TenantID: tenant, URL: "http://127.0.0.1:1/", EventTypes: []string{"*"},
	}); err != nil {
		t.Fatal(err)
	}
	for i := range events {
		if _, err := st.IngestEvent(ctx, tenant, "t.e", json.RawMessage(`{}`), fmt.Sprint("k", i)); err != nil {
			t.Fatal(err)
		}
	}
	return tenant
}

// The core distributed guarantee: however many workers poll concurrently,
// each delivery is leased to exactly one of them.
func TestClaimIsExclusiveAcrossConcurrentWorkers(t *testing.T) {
	st := testutil.NewStore(t)
	const total = 300
	seed(t, st, total)

	var mu sync.Mutex
	owner := map[string]string{} // delivery id -> worker that claimed it
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := fmt.Sprint("worker-", w)
			for {
				got, err := st.ClaimDeliveries(context.Background(), worker, 7, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, d := range got {
					if prev, dup := owner[d.ID]; dup {
						t.Errorf("delivery %s claimed by both %s and %s", d.ID, prev, worker)
					}
					owner[d.ID] = worker
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(owner) != total {
		t.Fatalf("claimed %d distinct deliveries, want %d", len(owner), total)
	}
	var inFlight, badAttempts int
	_ = st.Pool.QueryRow(context.Background(), `SELECT count(*) FROM deliveries WHERE status = 'in_flight'`).Scan(&inFlight)
	_ = st.Pool.QueryRow(context.Background(), `SELECT count(*) FROM deliveries WHERE attempt_count <> 1`).Scan(&badAttempts)
	if inFlight != total || badAttempts != 0 {
		t.Fatalf("in_flight=%d (want %d), rows with attempt_count<>1: %d", inFlight, total, badAttempts)
	}
}

func TestClaimSkipsDeliveriesNotYetDue(t *testing.T) {
	st := testutil.NewStore(t)
	seed(t, st, 2)
	if _, err := st.Pool.Exec(context.Background(),
		`UPDATE deliveries SET next_attempt_at = now() + interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	got, err := st.ClaimDeliveries(context.Background(), "w", 10, time.Minute)
	if err != nil || len(got) != 0 {
		t.Fatalf("claimed %d not-yet-due deliveries (err %v)", len(got), err)
	}
}

func TestFinishRequiresLease(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	seed(t, st, 1)

	got, err := st.ClaimDeliveries(ctx, "worker-a", 1, time.Minute)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	attempt := store.Attempt{StartedAt: time.Now(), Duration: time.Millisecond, StatusCode: 200}

	if err := st.CompleteDelivery(ctx, "worker-b", got[0], attempt); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("complete by non-owner: got %v, want ErrLeaseLost", err)
	}
	if err := st.CompleteDelivery(ctx, "worker-a", got[0], attempt); err != nil {
		t.Fatalf("complete by owner: %v", err)
	}
	// A second completion (e.g. a duplicate/stale worker) must not apply.
	if err := st.CompleteDelivery(ctx, "worker-a", got[0], attempt); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("double complete: got %v, want ErrLeaseLost", err)
	}
}
