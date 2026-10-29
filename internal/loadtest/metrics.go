package loadtest

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// ServerMetrics is what the worker processes themselves reported.
type ServerMetrics struct {
	Scraped         int                // processes that answered
	FirstAttempt    HistogramQuantiles // enqueue -> first attempt, as the workers measured it
	Outcomes        map[string]float64 // webhook_deliveries_total by outcome
	LeaseReclaims   float64
	ClaimErrors     float64
	QuotaCheckFails float64
}

type HistogramQuantiles struct {
	Count         float64
	P50, P95, P99 time.Duration
}

// ScrapeWorkers reads /metrics from each URL and merges the results. A process
// that was killed during the run loses its counters, so numbers cover the
// surviving incarnations only.
func ScrapeWorkers(ctx context.Context, urls []string) ServerMetrics {
	sm := ServerMetrics{Outcomes: map[string]float64{}}
	buckets := map[float64]float64{}
	for _, u := range urls {
		req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			continue
		}
		parser := expfmt.NewTextParser(model.UTF8Validation)
		fams, err := parser.TextToMetricFamilies(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			continue
		}
		sm.Scraped++
		for name, f := range fams {
			for _, m := range f.GetMetric() {
				switch name {
				case "webhook_first_attempt_delay_seconds":
					h := m.GetHistogram()
					sm.FirstAttempt.Count += float64(h.GetSampleCount())
					for _, b := range h.GetBucket() {
						buckets[b.GetUpperBound()] += float64(b.GetCumulativeCount())
					}
				case "webhook_deliveries_total":
					for _, l := range m.GetLabel() {
						if l.GetName() == "outcome" {
							sm.Outcomes[l.GetValue()] += m.GetCounter().GetValue()
						}
					}
				case "webhook_lease_reclaims_total":
					sm.LeaseReclaims += m.GetCounter().GetValue()
				case "webhook_claim_errors_total":
					sm.ClaimErrors += m.GetCounter().GetValue()
				case "webhook_quota_check_errors_total":
					sm.QuotaCheckFails += m.GetCounter().GetValue()
				}
			}
		}
	}
	sm.FirstAttempt.P50 = bucketQuantile(buckets, 0.50)
	sm.FirstAttempt.P95 = bucketQuantile(buckets, 0.95)
	sm.FirstAttempt.P99 = bucketQuantile(buckets, 0.99)
	return sm
}

// bucketQuantile estimates a quantile from cumulative histogram buckets the way
// Prometheus's histogram_quantile does (linear interpolation inside a bucket).
func bucketQuantile(cum map[float64]float64, q float64) time.Duration {
	if len(cum) == 0 {
		return 0
	}
	bounds := make([]float64, 0, len(cum))
	for b := range cum {
		bounds = append(bounds, b)
	}
	sort.Float64s(bounds)
	total := cum[bounds[len(bounds)-1]]
	if total == 0 {
		return 0
	}
	rank := q * total
	prevBound, prevCount := 0.0, 0.0
	for _, b := range bounds {
		c := cum[b]
		if c >= rank {
			if math.IsInf(b, 1) {
				return time.Duration(prevBound * float64(time.Second))
			}
			frac := 0.0
			if c > prevCount {
				frac = (rank - prevCount) / (c - prevCount)
			}
			return time.Duration((prevBound + (b-prevBound)*frac) * float64(time.Second))
		}
		prevBound, prevCount = b, c
	}
	return time.Duration(prevBound * float64(time.Second))
}

func (h HistogramQuantiles) String() string {
	if h.Count == 0 {
		return "no samples"
	}
	return fmt.Sprintf("n=%.0f p50=%v p95=%v p99=%v (bucket-interpolated)", h.Count, round(h.P50), round(h.P95), round(h.P99))
}
