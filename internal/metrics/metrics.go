// Package metrics defines every Prometheus metric the services export.
//
// Cardinality is deliberately bounded: labels are small fixed sets (outcome,
// result, route pattern, ...) and there is no per-tenant or per-endpoint label,
// because those grow with customers. Tenant-level detail belongs in logs and
// traces.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ns = "webhook"

// Outcomes recorded on webhook_deliveries_total.
const (
	OutcomeSucceeded        = "succeeded"
	OutcomeRetry            = "retry"
	OutcomeDeadMaxAttempts  = "dead_max_attempts"
	OutcomeDeadEndpointGone = "dead_endpoint_gone"
	OutcomeReleased         = "released" // handed back during shutdown, not an attempt outcome
	OutcomeLeaseLost        = "lease_lost"
)

type Metrics struct {
	Registry *prometheus.Registry

	// HTTP (API and health servers)
	HTTPRequests *prometheus.CounterVec   // method, route, code
	HTTPDuration *prometheus.HistogramVec // method, route

	// Ingest
	EventsIngested   *prometheus.CounterVec // result: accepted|duplicate
	IngestRejected   *prometheus.CounterVec // reason: rate_limited|backlog_full
	QuotaCheckErrors prometheus.Counter     // limiter/backlog check failed and was skipped (fail open)

	// Delivery (worker)
	Deliveries        *prometheus.CounterVec   // outcome
	AttemptDuration   *prometheus.HistogramVec // result
	FirstAttemptDelay prometheus.Histogram     // enqueue -> first attempt started
	Inflight          prometheus.Gauge
	ClaimBatchSize    prometheus.Histogram
	ClaimErrors       prometheus.Counter
	LeaseReclaims     *prometheus.CounterVec // result: requeued|dead
	Purged            *prometheus.CounterVec // table

	// Queue state, sampled from Postgres by every worker (aggregate with max()).
	QueueDepth       *prometheus.GaugeVec // state: due|scheduled|in_flight
	OldestPendingAge prometheus.Gauge     // age of the oldest due delivery that is claimable
	DLQDepth         *prometheus.GaugeVec // reason
	Endpoints        *prometheus.GaugeVec // state: circuit_open|paused|disabled
	StatsErrors      prometheus.Counter
	StatsLastSuccess prometheus.Gauge // unix time of the last successful sample
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(o prometheus.Opts) prometheus.Opts { o.Namespace = ns; return o }

	m := &Metrics{Registry: reg}
	m.HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts(f(prometheus.Opts{
		Name: "http_requests_total", Help: "HTTP requests handled, by method, route pattern and status code."})),
		[]string{"method", "route", "code"})
	m.HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "http_request_duration_seconds", Help: "HTTP request latency by method and route pattern.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}},
		[]string{"method", "route"})

	m.EventsIngested = prometheus.NewCounterVec(prometheus.CounterOpts(f(prometheus.Opts{
		Name: "events_ingested_total", Help: "Events accepted at ingest (duplicate = idempotent replay)."})), []string{"result"})
	m.IngestRejected = prometheus.NewCounterVec(prometheus.CounterOpts(f(prometheus.Opts{
		Name: "ingest_rejected_total", Help: "Events rejected by tenant quotas."})), []string{"reason"})
	m.QuotaCheckErrors = prometheus.NewCounter(prometheus.CounterOpts(f(prometheus.Opts{
		Name: "quota_check_errors_total", Help: "Quota checks that failed and were skipped (fail open)."})))

	m.Deliveries = prometheus.NewCounterVec(prometheus.CounterOpts(f(prometheus.Opts{
		Name: "deliveries_total", Help: "Delivery attempt outcomes: succeeded, retry, dead_*, released, lease_lost."})), []string{"outcome"})
	m.AttemptDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "attempt_duration_seconds", Help: "Duration of one HTTP delivery attempt, by result class.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}}, []string{"result"})
	m.FirstAttemptDelay = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "first_attempt_delay_seconds",
		Help:    "Time from a delivery being enqueued to its first attempt starting (the platform's own latency).",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900}})
	m.Inflight = prometheus.NewGauge(prometheus.GaugeOpts(f(prometheus.Opts{
		Name: "inflight_deliveries", Help: "Deliveries currently being sent by this worker."})))
	m.ClaimBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "claim_batch_size", Help: "Deliveries returned per claim.", Buckets: []float64{0, 1, 2, 5, 10, 20, 50}})
	m.ClaimErrors = prometheus.NewCounter(prometheus.CounterOpts(f(prometheus.Opts{
		Name: "claim_errors_total", Help: "Failed claim queries."})))
	m.LeaseReclaims = prometheus.NewCounterVec(prometheus.CounterOpts(f(prometheus.Opts{
		Name: "lease_reclaims_total", Help: "Deliveries recovered from workers that crashed or stalled."})), []string{"result"})
	m.Purged = prometheus.NewCounterVec(prometheus.CounterOpts(f(prometheus.Opts{
		Name: "purged_rows_total", Help: "Rows removed by the retention job."})), []string{"table"})

	m.QueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts(f(prometheus.Opts{
		Name: "queue_depth", Help: "Deliveries by state: due (all due work), due_claimable (due work extra workers could take now: the autoscaling signal), scheduled (waiting for a retry time), in_flight."})), []string{"state"})
	m.OldestPendingAge = prometheus.NewGauge(prometheus.GaugeOpts(f(prometheus.Opts{
		Name: "oldest_pending_age_seconds",
		Help: "Age of the oldest due delivery whose endpoint is claimable. The primary latency SLO signal; excludes work held for paused endpoints and open circuits."})))
	m.DLQDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts(f(prometheus.Opts{
		Name: "dlq_depth", Help: "Dead-lettered deliveries by reason."})), []string{"reason"})
	m.Endpoints = prometheus.NewGaugeVec(prometheus.GaugeOpts(f(prometheus.Opts{
		Name: "endpoints", Help: "Endpoints that are not fully healthy: circuit_open, paused, disabled."})), []string{"state"})
	m.StatsErrors = prometheus.NewCounter(prometheus.CounterOpts(f(prometheus.Opts{
		Name: "queue_stats_errors_total", Help: "Failed samples of queue state."})))
	m.StatsLastSuccess = prometheus.NewGauge(prometheus.GaugeOpts(f(prometheus.Opts{
		Name: "queue_stats_last_success_timestamp_seconds", Help: "When queue state was last sampled successfully."})))

	reg.MustRegister(m.HTTPRequests, m.HTTPDuration, m.EventsIngested, m.IngestRejected, m.QuotaCheckErrors,
		m.Deliveries, m.AttemptDuration, m.FirstAttemptDelay, m.Inflight, m.ClaimBatchSize, m.ClaimErrors,
		m.LeaseReclaims, m.Purged, m.QueueDepth, m.OldestPendingAge, m.DLQDepth, m.Endpoints,
		m.StatsErrors, m.StatsLastSuccess)

	// Make the common series exist at zero so rate()/absent() behave and
	// dashboards show 0 rather than "no data" before the first event.
	for _, o := range []string{OutcomeSucceeded, OutcomeRetry, OutcomeDeadMaxAttempts, OutcomeDeadEndpointGone, OutcomeReleased, OutcomeLeaseLost} {
		m.Deliveries.WithLabelValues(o)
	}
	for _, r := range []string{"requeued", "dead"} {
		m.LeaseReclaims.WithLabelValues(r)
	}
	for _, r := range []string{"accepted", "duplicate"} {
		m.EventsIngested.WithLabelValues(r)
	}
	for _, r := range []string{"rate_limited", "backlog_full"} {
		m.IngestRejected.WithLabelValues(r)
	}
	for _, t := range []string{"deliveries_succeeded", "deliveries_dead", "events"} {
		m.Purged.WithLabelValues(t)
	}
	return m
}

// Handler serves the metrics in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}
