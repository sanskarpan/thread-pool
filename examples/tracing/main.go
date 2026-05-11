// This example demonstrates how to use the thread pool with OpenTelemetry tracing.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/sanskarpan/thread-pool/pool"
	"github.com/sanskarpan/thread-pool/task"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
)

// newExporter creates a new trace exporter that writes to stdout.
func newExporter() (trace.SpanExporter, error) {
	return stdouttrace.New(
		stdouttrace.WithWriter(os.Stdout),
		stdouttrace.WithPrettyPrint(),
		stdouttrace.WithoutTimestamps(),
	)
}

// newTraceProvider creates a new trace provider with the given exporter.
func newTraceProvider(exp trace.SpanExporter) *trace.TracerProvider {
	return trace.NewTracerProvider(
		trace.WithBatcher(exp),
	)
}

func main() {
	// 1. Set up the OpenTelemetry pipeline.
	exp, err := newExporter()
	if err != nil {
		log.Fatalf("failed to initialize stdout exporter: %v", err)
	}
	tp := newTraceProvider(exp)
	otel.SetTracerProvider(tp)
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	// 2. Create a parent span for the main application flow.
	tracer := otel.Tracer("main-app")
	mainCtx, parentSpan := tracer.Start(context.Background(), "main-operation")

	// 3. Create a new pool with the TracerProvider configured.
	config := &pool.Config{
		WorkerCount:    2,
		TracerProvider: tp,
	}
	p := pool.NewFixedPool(config)
	defer p.ShutdownNow()

	// 4. Submit a task using the context that contains the parent span.
	// This will link the task's trace to the main application's trace.
	log.Println("Submitting a task with a parent trace context...")
	future, err := p.SubmitFunc(
		func(ctx context.Context) (interface{}, error) {
			// This task is now part of the trace.
			_, childSpan := tracer.Start(ctx, "sub-task-work")
			defer childSpan.End()
			time.Sleep(100 * time.Millisecond)
			return "success", nil
		},
		task.WithContext(mainCtx), // Pass the context with the parent span
	)
	if err != nil {
		log.Fatalf("Failed to submit task: %v", err)
	}

	// Wait for the task to complete.
	future.Get()
	log.Println("Task finished. Check the console output for the trace.")

	// End the parent span.
	parentSpan.End()

	// Give the exporter time to flush.
	time.Sleep(500 * time.Millisecond)
}
