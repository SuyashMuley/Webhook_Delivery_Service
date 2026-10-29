package store_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"webhookdelivery/internal/testutil"
)

// BenchmarkClaimParallel measures ClaimDeliveries with many workers polling at
// once against a skewed backlog: 50 tenants x 5 endpoints, where tenant t1
// owns about 2/3 of the 150k due deliveries. Nothing is completed, so each
// claim takes fresh rows. Besides time per claim it reports how many rows a
// claim actually returned (the maximum is 20).
//
//	DATABASE_URL=... go test ./internal/store -run xxx -bench ClaimParallel -benchtime 2000x
func BenchmarkClaimParallel(b *testing.B) {
	st := testutil.NewStore(b)
	ctx := context.Background()
	conn, err := st.Pool.Acquire(ctx)
	if err != nil {
		b.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO tenants (name, max_inflight) SELECT 't' || g, 1000000 FROM generate_series(1, 50) g`,
		`INSERT INTO endpoints (tenant_id, url, max_concurrency)
		   SELECT t.id, 'http://x.test/' || g, 1000000 FROM tenants t, generate_series(1, 5) g`,
		`CREATE TEMP TABLE ep AS SELECT id, tenant_id, row_number() OVER (PARTITION BY tenant_id ORDER BY id) AS k FROM endpoints`,
		`CREATE TEMP TABLE plan AS
		   SELECT t.id AS tenant_id, g, gen_random_uuid() AS event_id
		     FROM tenants t JOIN LATERAL generate_series(1, CASE WHEN t.name = 't1' THEN 100000 ELSE 1000 END) g ON true`,
		`INSERT INTO events (id, tenant_id, type, payload) SELECT event_id, tenant_id, 't', '{}' FROM plan`,
		`INSERT INTO deliveries (tenant_id, event_id, endpoint_id, next_attempt_at)
		   SELECT p.tenant_id, p.event_id, e.id, now() - (p.g || ' ms')::interval
		     FROM plan p JOIN ep e ON e.tenant_id = p.tenant_id AND e.k = 1 + (p.g % 5)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			b.Fatalf("%s: %v", q, err)
		}
	}
	conn.Release()
	if _, err := st.Pool.Exec(ctx, `ANALYZE`); err != nil {
		b.Fatal(err)
	}

	var rows, calls atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			got, err := st.ClaimDeliveries(ctx, "bench", 20, 5, time.Hour)
			if err != nil {
				b.Error(err)
				return
			}
			rows.Add(int64(len(got)))
			calls.Add(1)
		}
	})
	b.ReportMetric(float64(rows.Load())/float64(calls.Load()), "rows/claim")
}
