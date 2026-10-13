// Package tracing wires OpenTelemetry once per service: W3C TraceContext
// propagation always, OTLP export only when an address is set. Empty
// address means local runs stay silent instead of failing.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Init installs the propagator and, if addr is set, a batching OTLP
// exporter. It returns a shutdown function the caller defers in main.
func Init(ctx context.Context, addr string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if addr == "" {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(addr),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))

	otel.SetTracerProvider(provider)

	return provider.Shutdown, nil
}
