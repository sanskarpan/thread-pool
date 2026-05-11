// Package pool provides various thread pool implementations
package pool

import (
	"context"
	"errors"

	"github.com/sanskarpan/thread-pool/metrics"
	"github.com/sanskarpan/thread-pool/task"
	"github.com/sanskarpan/thread-pool/worker"
	"go.opentelemetry.io/otel/trace"
)

var (
	// ErrPoolClosed is returned when submitting to a closed pool
	ErrPoolClosed = errors.New("pool is closed")
	// ErrPoolFull is returned when the pool queue is full
	ErrPoolFull = errors.New("pool queue is full")
)

// Pool is the interface for all thread pool implementations
type Pool interface {
	Submit(t *task.Task) error
	SubmitFunc(fn task.TaskFunc, opts ...task.TaskOption) (*task.Future, error)
	Shutdown(ctx context.Context) error
	ShutdownNow() error
	IsShutdown() bool
	AwaitTermination(ctx context.Context) error
	Metrics() *metrics.Metrics
	Workers() []*worker.Worker
}

// TaskEvent is a struct used to send task lifecycle events from the pool.
type TaskEvent struct {
	EventType string
	TaskID    string
	Priority  int
	State     string
	Duration  int64
}


// Config holds pool configuration
type Config struct {
	// WorkerCount is the number of worker goroutines
	WorkerCount int
	// QueueSize is the size of the task queue (0 = unbounded)
	QueueSize int
	// EnableMetrics enables metrics collection
	EnableMetrics bool
	// TracerProvider is an OpenTelemetry TracerProvider
	TracerProvider trace.TracerProvider
	// TaskEventChan is a channel to send task lifecycle events to.
	TaskEventChan chan<- TaskEvent
}

// DefaultConfig returns the default pool configuration
func DefaultConfig() *Config {
	return &Config{
		WorkerCount:   4,
		QueueSize:     100,
		EnableMetrics: true,
	}
}
