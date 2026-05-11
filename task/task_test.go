package task

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestNewTask(t *testing.T) {
	fn := func(ctx context.Context) (interface{}, error) {
		return "test", nil
	}
	task := NewTask(fn)
	if task == nil {
		t.Fatal("NewTask returned nil")
	}
	if task.State() != StatePending {
		t.Errorf("Expected state to be StatePending, got %v", task.State())
	}
}

func TestTaskWithPriority(t *testing.T) {
	fn := func(ctx context.Context) (interface{}, error) { return nil, nil }
	task := NewTask(fn, WithPriority(PriorityHigh))
	if task.Priority() != PriorityHigh {
		t.Errorf("Expected priority to be High, got %v", task.Priority())
	}
}

func TestTaskExecute(t *testing.T) {
	var executed atomic.Bool
	fn := func(ctx context.Context) (interface{}, error) {
		executed.Store(true)
		return "result", nil
	}
	task := NewTask(fn)
	task.Execute()
	future := task.Future()
	result, err := future.Get()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result.(string) != "result" {
		t.Errorf("Expected result 'result', got %v", result)
	}
	if !executed.Load() {
		t.Error("Task function was not executed")
	}
	if task.State() != StateCompleted {
		t.Errorf("Expected state to be Completed, got %v", task.State())
	}
}

func TestTaskExecuteWithError(t *testing.T) {
	fn := func(ctx context.Context) (interface{}, error) {
		return nil, errors.New("task error")
	}
	task := NewTask(fn)
	task.Execute()
	future := task.Future()
	_, err := future.Get()
	if err == nil {
		t.Fatal("Expected an error, got nil")
	}
	if err.Error() != "task error" {
		t.Errorf("Expected error 'task error', got %v", err)
	}
	if task.State() != StateFailed {
		t.Errorf("Expected state to be Failed, got %v", task.State())
	}
}

func TestTaskWithTimeout(t *testing.T) {
	fn := func(ctx context.Context) (interface{}, error) {
		time.Sleep(500 * time.Millisecond)
		return nil, nil
	}
	task := NewTask(fn, WithTimeout(100*time.Millisecond))
	task.Execute()
	future := task.Future()
	_, err := future.Get()
	if err != context.DeadlineExceeded {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
	if task.State() != StateTimeout {
		t.Errorf("Expected state to be Timeout, got %v", task.State())
	}
}

func TestTaskCancel(t *testing.T) {
	var executed atomic.Bool
	fn := func(ctx context.Context) (interface{}, error) {
		time.Sleep(200 * time.Millisecond)
		executed.Store(true)
		return nil, nil
	}
	task := NewTask(fn)
	go task.Execute()
	time.Sleep(50 * time.Millisecond) // Give it time to start
	task.Cancel()
	future := task.Future()
	_, err := future.Get()
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
	if executed.Load() {
		t.Error("Task executed even though it was canceled")
	}
	if task.State() != StateCancelled {
		t.Errorf("Expected state to be Cancelled, got %v", task.State())
	}
}

func TestTaskWithRetry(t *testing.T) {
	var attempts atomic.Int32
	maxRetries := 3
	fn := func(ctx context.Context) (interface{}, error) {
		attempts.Add(1)
		if attempts.Load() <= int32(maxRetries) {
			return nil, errors.New("temporary error")
		}
		return "success", nil
	}
	task := NewTask(fn, WithRetry(maxRetries), WithRetryPolicy(RetryPolicyFunc(func(int) time.Duration { return 0 })))

	task.Execute()

	result, err := task.Future().Get()
	if err != nil {
		t.Fatalf("Expected no error on final attempt, got %v", err)
	}
	if result != "success" {
		t.Errorf("Expected result 'success', got %v", result)
	}
	if attempts.Load() != int32(maxRetries+1) {
		t.Errorf("Expected %d attempts, got %d", maxRetries+1, attempts.Load())
	}
	if task.RetryCount() != maxRetries {
		t.Errorf("Expected retry count %d, got %d", maxRetries, task.RetryCount())
	}
}

func TestTaskWithCustomRetryPolicy(t *testing.T) {
	var attempts atomic.Int32
	var seen []time.Duration
	task := NewTask(func(ctx context.Context) (interface{}, error) {
		attempts.Add(1)
		if attempts.Load() < 2 {
			return nil, errors.New("temporary error")
		}
		return "ok", nil
	}, WithRetry(1), WithRetryPolicy(RetryPolicyFunc(func(attempt int) time.Duration {
		seen = append(seen, time.Duration(attempt)*time.Millisecond)
		return 0
	})))

	task.Execute()
	if _, err := task.Future().Get(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts.Load())
	}
	if len(seen) != 1 || seen[0] != time.Millisecond {
		t.Fatalf("unexpected retry policy observations: %#v", seen)
	}
}

func TestTaskMetadata(t *testing.T) {
	fn := func(ctx context.Context) (interface{}, error) { return nil, nil }
	task := NewTask(fn, WithMetadata("key", "value"))
	val, ok := task.GetMetadata("key")
	if !ok || val != "value" {
		t.Errorf("Expected metadata 'value' for key 'key', got %v", val)
	}
}

func TestTaskDuration(t *testing.T) {
	fn := func(ctx context.Context) (interface{}, error) {
		time.Sleep(50 * time.Millisecond)
		return nil, nil
	}
	task := NewTask(fn)
	task.Execute()
	task.Future().Get()
	if task.Duration() < 50*time.Millisecond {
		t.Errorf("Expected duration to be at least 50ms, got %v", task.Duration())
	}
}

func TestTaskPanicRecovery(t *testing.T) {
	fn := func(ctx context.Context) (interface{}, error) {
		panic("something went wrong")
	}
	task := NewTask(fn)
	task.Execute()
	_, err := task.Future().Get()
	if err == nil {
		t.Fatal("Expected a panic error, got nil")
	}
	if _, ok := err.(*PanicError); !ok {
		t.Errorf("Expected error to be of type PanicError, got %T", err)
	}
	if task.State() != StateFailed {
		t.Errorf("Expected state to be Failed, got %v", task.State())
	}
}

func TestTask_Tracing(t *testing.T) {
	// 1. Setup a memory-based exporter for testing
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	tracer := tp.Tracer("test-tracer")

	// 2. Create a parent span
	parentCtx, parentSpan := tracer.Start(context.Background(), "parent-op")

	// 3. Create and execute a task with the parent context and tracer
	var executed atomic.Bool
	fn := func(ctx context.Context) (interface{}, error) {
		// Verify the context inside the task carries the correct span
		span := oteltrace.SpanFromContext(ctx)
		if !span.SpanContext().IsValid() {
			t.Error("SpanContext in task is not valid")
		}
		if span.SpanContext().TraceID() != parentSpan.SpanContext().TraceID() {
			t.Error("Mismatched TraceIDs between parent and task span")
		}
		executed.Store(true)
		return nil, nil
	}

	task := NewTask(fn, WithContext(parentCtx), WithTracer(tracer))
	task.Execute()
	parentSpan.End() // End the parent span

	// 4. Verify the exported spans
	if !executed.Load() {
		t.Fatal("Task function was not executed")
	}

	// Force flush to get spans
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("Failed to flush spans: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("Expected 2 spans (parent and child task), got %d", len(spans))
	}

	// The order can vary, so we identify them
	var childSpan, pSpan tracetest.SpanStub
	for _, s := range spans {
		if s.Parent.HasTraceID() {
			childSpan = s
		} else {
			pSpan = s
		}
	}

	if childSpan.Name == "" || pSpan.Name == "" {
		t.Fatal("Could not identify parent and child spans")
	}

	// 5. Check the parent-child relationship
	if childSpan.Parent.TraceID() != pSpan.SpanContext.TraceID() {
		t.Errorf("Child span's parent TraceID does not match parent span's TraceID")
	}
	if childSpan.Parent.SpanID() != pSpan.SpanContext.SpanID() {
		t.Errorf("Child span's parent SpanID does not match parent span's SpanID")
	}
}
