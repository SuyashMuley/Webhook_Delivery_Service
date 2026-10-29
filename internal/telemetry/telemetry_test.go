package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func newRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagator)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return rec
}

// The whole point: a trace started in one process continues in another after
// the context has been stored as a string and loaded again.
func TestTraceContextSurvivesTheQueue(t *testing.T) {
	rec := newRecorder(t)

	ingestCtx, ingest := Tracer().Start(context.Background(), "ingest")
	stored := Inject(ingestCtx) // what the API writes to deliveries.trace_context
	ingest.End()
	if stored == "" {
		t.Fatal("no traceparent produced for a valid span")
	}

	// Later, in the worker.
	workerCtx, attempt := Tracer().Start(Extract(context.Background(), stored), "delivery.attempt")
	attempt.End()

	spans := rec.Ended()
	if len(spans) != 2 {
		t.Fatalf("recorded %d spans, want 2", len(spans))
	}
	parent, child := spans[0], spans[1]
	if child.SpanContext().TraceID() != parent.SpanContext().TraceID() {
		t.Error("worker span is in a different trace than the ingest span")
	}
	if child.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Error("worker span is not a child of the ingest span")
	}
	if tid, sid := IDs(workerCtx); tid != child.SpanContext().TraceID().String() || sid == "" {
		t.Errorf("IDs() = %q, %q", tid, sid)
	}
}

func TestInjectExtractEdgeCases(t *testing.T) {
	newRecorder(t)
	if got := Inject(context.Background()); got != "" {
		t.Errorf("Inject without a span = %q, want empty", got)
	}
	ctx := context.Background()
	for _, bad := range []string{"", "garbage", "00-zzzz-yyyy-01"} {
		if got := Extract(ctx, bad); got != ctx {
			if tid, _ := IDs(got); tid != "" {
				t.Errorf("Extract(%q) produced a trace id %q", bad, tid)
			}
		}
	}
	if tid, sid := IDs(ctx); tid != "" || sid != "" {
		t.Errorf("IDs on an empty context = %q %q", tid, sid)
	}
}
