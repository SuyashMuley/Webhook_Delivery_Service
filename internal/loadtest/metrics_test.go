package loadtest

import (
	"context"
	"math"
	"net/http/httptest"
	"testing"
	"time"

	"webhookdelivery/internal/metrics"
)

func TestBucketQuantileInterpolatesLikePrometheus(t *testing.T) {
	// 100 observations: 50 up to 0.1s, 40 more up to 0.5s, 10 more up to 1s.
	cum := map[float64]float64{0.1: 50, 0.5: 90, 1: 100, math.Inf(1): 100}
	cases := map[float64]time.Duration{
		0.5:  100 * time.Millisecond, // exactly the first bucket's edge
		0.95: 750 * time.Millisecond, // halfway through the last bucket
	}
	for q, want := range cases {
		got := bucketQuantile(cum, q)
		if d := got - want; d < -5*time.Millisecond || d > 5*time.Millisecond {
			t.Errorf("q%.2f = %v, want %v", q, got, want)
		}
	}
	if bucketQuantile(nil, 0.99) != 0 {
		t.Error("empty histogram should give 0")
	}
}

func TestSamplesPercentiles(t *testing.T) {
	var s Samples
	for i := 1; i <= 1000; i++ {
		s.Add(time.Duration(i) * time.Millisecond)
	}
	got := s.Summary()
	if got.P50 != 500*time.Millisecond || got.P99 != 990*time.Millisecond || got.Max != time.Second || got.N != 1000 {
		t.Fatalf("summary = %+v", got)
	}
}

// The scraper must cope with real exposition from the services (a zero-value
// parser panics in current prometheus/common, which this guards against).
func TestScrapeWorkersParsesRealExposition(t *testing.T) {
	m := metrics.New()
	m.FirstAttemptDelay.Observe(0.05)
	m.FirstAttemptDelay.Observe(0.4)
	m.Deliveries.WithLabelValues("succeeded").Add(7)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	got := ScrapeWorkers(context.Background(), []string{srv.URL, "http://127.0.0.1:1/unreachable"})
	if got.Scraped != 1 || got.FirstAttempt.Count != 2 || got.Outcomes["succeeded"] != 7 {
		t.Fatalf("scrape = %+v", got)
	}
	if got.FirstAttempt.P99 <= 0 {
		t.Fatalf("no p99 computed: %+v", got.FirstAttempt)
	}
}
