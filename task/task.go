// Package task provides task abstractions for the thread pool
package task

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TaskFunc is the function signature for tasks
type TaskFunc func(ctx context.Context) (interface{}, error)

// Priority defines task execution priority
type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
	PriorityUrgent
)

// State represents the current state of a task
type State int32

const (
	StatePending State = iota
	StateRunning
	StateCompleted
	StateFailed
	StateCancelled
	StateTimeout
)

func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateRunning:
		return "running"
	case StateCompleted:
		return "completed"
	case StateFailed:
		return "failed"
	case StateCancelled:
		return "cancelled"
	case StateTimeout:
		return "timeout"
	default:
		return "unknown"
	}
}

// Task represents a unit of work to be executed
type Task struct {
	id          string
	fn          TaskFunc
	opts        []TaskOption // Store original options
	priority    Priority
	timeout     time.Duration
	retryCount  int
	maxRetries  int
	retryPolicy RetryPolicy
	submittedAt time.Time
	startedAt   time.Time
	completedAt time.Time
	state       atomic.Int32
	ctx         context.Context
	cancel      context.CancelFunc
	future      *Future
	metadata    map[string]interface{}
	mu          sync.RWMutex

	// OpenTelemetry
	tracer trace.Tracer
	span   trace.Span

	// Dependencies
	dependencies    []*Future
	DependencyCount atomic.Int32
	poolSubmitter   func(*Task) error
}

// TaskOption is a function that configures a Task
type TaskOption func(*Task)

// RetryPolicy determines how long to wait before retrying a task.
type RetryPolicy interface {
	Delay(attempt int) time.Duration
}

// RetryPolicyFunc adapts a function to RetryPolicy.
type RetryPolicyFunc func(attempt int) time.Duration

// Delay implements RetryPolicy.
func (f RetryPolicyFunc) Delay(attempt int) time.Duration {
	return f(attempt)
}

// NewTask creates a new task with the given function and options
func NewTask(fn TaskFunc, opts ...TaskOption) *Task {
	ctx, cancel := context.WithCancel(context.Background())

	t := &Task{
		id:          generateID(),
		fn:          fn,
		opts:        opts, // Store for recreating recurring tasks
		priority:    PriorityNormal,
		timeout:     0,
		retryCount:  0,
		maxRetries:  0,
		retryPolicy: defaultRetryPolicy(),
		submittedAt: time.Now(),
		ctx:         ctx,
		cancel:      cancel,
		future:      NewFuture(),
		metadata:    make(map[string]interface{}),
	}

	t.state.Store(int32(StatePending))

	for _, opt := range opts {
		opt(t)
	}

	// Start OpenTelemetry span if a tracer is provided
	if t.tracer != nil {
		// The parent span (if any) is extracted from the context
		t.ctx, t.span = t.tracer.Start(t.ctx, "task_lifecycle")
		t.span.SetAttributes(
			attribute.String("task.id", t.id),
			attribute.String("task.priority", strconv.Itoa(int(t.priority))),
			attribute.Int("task.max_retries", t.maxRetries),
		)
	}

	// Register dependencies if any
	if len(t.dependencies) > 0 {
		for _, dep := range t.dependencies {
			dep.OnComplete(t)
		}
	}

	return t
}

// RetryCount returns the number of retry attempts performed so far.
func (t *Task) RetryCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.retryCount
}

// WithPriority sets the task priority
func WithPriority(p Priority) TaskOption {
	return func(t *Task) {
		t.priority = p
	}
}

// WithTimeout sets a timeout for task execution
func WithTimeout(d time.Duration) TaskOption {
	return func(t *Task) {
		t.timeout = d
	}
}

// WithRetry sets the maximum number of retries
func WithRetry(maxRetries int) TaskOption {
	return func(t *Task) {
		t.maxRetries = maxRetries
	}
}

// WithRetryPolicy sets the retry backoff policy.
func WithRetryPolicy(policy RetryPolicy) TaskOption {
	return func(t *Task) {
		t.retryPolicy = policy
	}
}

// WithContext sets a custom context
func WithContext(ctx context.Context) TaskOption {
	return func(t *Task) {
		t.cancel() // Cancel the default context
		t.ctx, t.cancel = context.WithCancel(ctx)
	}
}

// WithMetadata sets metadata for the task
func WithMetadata(key string, value interface{}) TaskOption {
	return func(t *Task) {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.metadata[key] = value
	}
}

// WithTracer provides a tracer to the task for OpenTelemetry.
func WithTracer(tracer trace.Tracer) TaskOption {
	return func(t *Task) {
		t.tracer = tracer
	}
}

// After sets a list of antecedent tasks that must complete before this one can run.
func After(dependencies ...*Future) TaskOption {
	return func(t *Task) {
		t.dependencies = dependencies
		t.DependencyCount.Store(int32(len(dependencies)))
	}
}

// HasDependencies returns true if the task has dependencies.
func (t *Task) HasDependencies() bool {
	return len(t.dependencies) > 0
}

// SetSubmitter is used by the pool to give the task a callback to enqueue itself.
func (t *Task) SetSubmitter(submitter func(*Task) error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.poolSubmitter = submitter
}

// resolveDependency is called by a future this task depends on.
// When all dependencies are resolved, it submits the task to the pool.
func (t *Task) resolveDependency() {
	if t.DependencyCount.Add(-1) == 0 {
		t.mu.RLock()
		defer t.mu.RUnlock()
		if t.poolSubmitter != nil {
			t.poolSubmitter(t)
		}
	}
}

// ID returns the task ID
func (t *Task) ID() string {
	return t.id
}

// Priority returns the task priority
func (t *Task) Priority() Priority {
	return t.priority
}

// State returns the current state
func (t *Task) State() State {
	return State(t.state.Load())
}

// Execute runs the task
func (t *Task) Execute() {
	// Check if already executed
	if !t.state.CompareAndSwap(int32(StatePending), int32(StateRunning)) {
		return
	}

	if t.span != nil {
		t.span.AddEvent("Execution Started")
	}

	t.mu.Lock()
	t.startedAt = time.Now()
	t.mu.Unlock()

	// Create execution context with timeout if specified
	execCtx := t.ctx
	var execCancel context.CancelFunc

	if t.timeout > 0 {
		execCtx, execCancel = context.WithTimeout(t.ctx, t.timeout)
		defer execCancel()
	}

	for {
		if err := execCtx.Err(); err != nil {
			if err == context.DeadlineExceeded {
				t.state.Store(int32(StateTimeout))
				t.future.SetError(context.DeadlineExceeded)
				if t.span != nil {
					t.span.SetStatus(codes.Error, "Timeout")
					t.span.End()
				}
			} else {
				t.state.Store(int32(StateCancelled))
				t.future.SetError(context.Canceled)
				if t.span != nil {
					t.span.SetStatus(codes.Error, "Cancelled")
					t.span.End()
				}
			}
			t.mu.Lock()
			t.completedAt = time.Now()
			t.mu.Unlock()
			return
		}

		// Execute with panic recovery
		result, err := t.executeWithRecovery(execCtx)

		// Handle timeout
		if execCtx.Err() == context.DeadlineExceeded {
			t.state.Store(int32(StateTimeout))
			t.future.SetError(context.DeadlineExceeded)
			t.mu.Lock()
			t.completedAt = time.Now()
			t.mu.Unlock()
			if t.span != nil {
				t.span.SetStatus(codes.Error, "Timeout")
				t.span.End()
			}
			return
		}

		// Handle cancellation
		if execCtx.Err() == context.Canceled {
			t.state.Store(int32(StateCancelled))
			t.future.SetError(context.Canceled)
			t.mu.Lock()
			t.completedAt = time.Now()
			t.mu.Unlock()
			if t.span != nil {
				t.span.SetStatus(codes.Error, "Cancelled")
				t.span.End()
			}
			return
		}

		if err != nil {
			t.mu.Lock()
			canRetry := t.retryCount < t.maxRetries
			if canRetry {
				t.retryCount++
			}
			retryAttempt := t.retryCount
			t.mu.Unlock()

			if canRetry {
				if t.span != nil {
					t.span.AddEvent("Retryable Error", trace.WithAttributes(attribute.String("error", err.Error())))
				}
				t.state.Store(int32(StatePending))
				if !waitForRetryBackoff(execCtx, t.retryDelay(retryAttempt)) {
					continue
				}
				t.state.Store(int32(StateRunning))
				continue
			}

			t.state.Store(int32(StateFailed))
			t.future.SetError(err)
			if t.span != nil {
				t.span.SetStatus(codes.Error, "Execution Failed")
				t.span.RecordError(err)
				t.span.End()
			}
		} else {
			t.state.Store(int32(StateCompleted))
			t.future.SetResult(result)
			if t.span != nil {
				t.span.SetStatus(codes.Ok, "Completed")
				t.span.End()
			}
		}

		break
	}

	t.mu.Lock()
	t.completedAt = time.Now()
	t.mu.Unlock()
}

func waitForRetryBackoff(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func defaultRetryPolicy() RetryPolicy {
	return RetryPolicyFunc(func(attempt int) time.Duration {
		if attempt <= 0 {
			return 0
		}

		delay := 50 * time.Millisecond
		for i := 1; i < attempt; i++ {
			delay *= 2
			if delay >= 2*time.Second {
				return 2 * time.Second
			}
		}
		return delay
	})
}

func (t *Task) retryDelay(attempt int) time.Duration {
	if t.retryPolicy == nil {
		return defaultRetryPolicy().Delay(attempt)
	}
	return t.retryPolicy.Delay(attempt)
}

// executeWithRecovery executes the task with panic recovery
func (t *Task) executeWithRecovery(ctx context.Context) (result interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Recovered: r}
		}
	}()

	return t.fn(ctx)
}

// Cancel cancels the task
func (t *Task) Cancel() {
	if t.state.CompareAndSwap(int32(StatePending), int32(StateCancelled)) ||
		t.state.CompareAndSwap(int32(StateRunning), int32(StateCancelled)) {
		t.cancel()
		t.future.SetError(context.Canceled)
		t.mu.Lock()
		t.completedAt = time.Now()
		t.mu.Unlock()
		if t.span != nil {
			t.span.SetStatus(codes.Error, "Cancelled by user")
			t.span.End()
		}
	}
}

// Future returns the task's future
func (t *Task) Future() *Future {
	return t.future
}

// Duration returns the execution duration
func (t *Task) Duration() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.startedAt.IsZero() {
		return 0
	}
	if t.completedAt.IsZero() {
		return time.Since(t.startedAt)
	}
	return t.completedAt.Sub(t.startedAt)
}

// WaitTime returns the time spent waiting in queue
func (t *Task) WaitTime() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.startedAt.IsZero() {
		return time.Since(t.submittedAt)
	}
	return t.startedAt.Sub(t.submittedAt)
}

// GetMetadata retrieves metadata by key
func (t *Task) GetMetadata(key string) (interface{}, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	val, ok := t.metadata[key]
	return val, ok
}

// GetFn returns the task's function
func (t *Task) GetFn() TaskFunc {
	return t.fn
}

// GetOpts returns the task's options
func (t *Task) GetOpts() []TaskOption {
	return t.opts
}

// PanicError represents a panic that occurred during task execution
type PanicError struct {
	Recovered interface{}
}

func (e *PanicError) Error() string {
	return "panic during task execution"
}

// ID generation
var taskCounter atomic.Uint64

func generateID() string {
	return "task-" + time.Now().Format("20060102150405") + "-" + strconv.FormatUint(taskCounter.Add(1), 10)
}
