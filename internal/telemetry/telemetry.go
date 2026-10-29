// Package telemetry sets up distributed tracing and helps carry trace context
// across the queue.
//
// A trace starts at the API request that ingests an event. The W3C trace
// context of the ingest span is stored on each delivery row, and the worker
// resumes it when it sends the delivery, so one trace shows the request, the
// enqueue and every delivery attempt even though they run in different
// processes at different times.
package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	Service     string
	Endpoint    string  // OTLP/HTTP collector, e.g. http://jaeger:4318; empty = do not export
	SampleRatio float64 // fraction of new traces to sample, 0..1
}

var propagator = propagation.TraceContext{}

// Setup installs a global tracer provider and propagator and returns a function
// that flushes and stops it. With no Endpoint nothing is exported, but spans
// still get real IDs so logs can be correlated and trace context can be stored.
func Setup(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", cfg.Service)))
	if err != nil {
		return nil, fmt.Errorf("telemetry resource: %w", err)
	}
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	}
	if cfg.Endpoint != "" {
		exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(cfg.Endpoint+"/v1/traces"))
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagator)
	return tp.Shutdown, nil
}

// Tracer returns the tracer used by this module.
func Tracer() trace.Tracer { return otel.Tracer("webhookdelivery") }

// Inject returns the W3C traceparent for the span in ctx, or "" if there is none.
func Inject(ctx context.Context) string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ""
	}
	c := propagation.MapCarrier{}
	propagator.Inject(ctx, c)
	return c.Get("traceparent")
}

// Extract resumes the trace described by a stored traceparent. An empty or
// invalid value returns ctx unchanged.
func Extract(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}

// ExtractHeader resumes the trace carried by incoming request headers.
func ExtractHeader(ctx context.Context, h propagation.TextMapCarrier) context.Context {
	return propagator.Extract(ctx, h)
}

// IDs returns the trace and span IDs in ctx for log correlation ("" if none).
func IDs(ctx context.Context) (traceID, spanID string) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return "", ""
	}
	return sc.TraceID().String(), sc.SpanID().String()
}
