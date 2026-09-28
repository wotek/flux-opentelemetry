package fluxotel

import (
	"go.opentelemetry.io/otel/trace"
)

type config struct {
	spanKind trace.SpanKind
}

func newConfig(opts []Option, defaultKind trace.SpanKind) config {
	cfg := config{
		spanKind: defaultKind,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// Option configures OpenTelemetry instrumentation options.
type Option func(*config)

// WithSpanKind overrides the default span kind for middleware spans.
func WithSpanKind(kind trace.SpanKind) Option {
	return func(c *config) {
		c.spanKind = kind
	}
}
