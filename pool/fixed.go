// Fixed thread pool implementation
package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/thread-pool/metrics"
	"github.com/sanskarpan/thread-pool/queue"
	"github.com/sanskarpan/thread-pool/task"
	"github.com/sanskarpan/thread-pool/worker"
	"go.opentelemetry.io/otel/trace"
)

// FixedPool is a thread pool with a fixed number of workers
type FixedPool struct {
	config     *Config
	queue      queue.Queue
	workers    *worker.WorkerPool
	metrics    *metrics.Metrics
	shutdown   atomic.Bool
	shutdownCh chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
	tracer     trace.Tracer
}

// NewFixedPool creates a new fixed-size thread pool
func NewFixedPool(config *Config) *FixedPool {
	if config == nil {
		config = DefaultConfig()
	}

	var q queue.Queue
	if config.QueueSize > 0 {
		q = queue.NewBoundedQueue(config.QueueSize)
	} else {
		q = queue.NewUnboundedQueue()
	}

	var m *metrics.Metrics
	if config.EnableMetrics {
		m = metrics.NewMetrics(config.QueueSize)
		m.SetWorkerCount(config.WorkerCount)
	}

	pool := &FixedPool{
		config:     config,
		queue:      q,
		workers:    worker.NewWorkerPool(config.WorkerCount, q),
		metrics:    m,
		shutdownCh: make(chan struct{}),
	}

	// Setup tracer if provider is configured
	if config.TracerProvider != nil {
		pool.tracer = config.TracerProvider.Tracer("threadpool")
	}

	// Start workers
	pool.workers.Start()

	// Start metrics updater if metrics enabled
	if config.EnableMetrics {
		go pool.updateMetrics()
	}

	return pool
}

// Submit submits a task to the pool, handling dependencies.
func (p *FixedPool) Submit(t *task.Task) error {
	if p.IsShutdown() {
		return ErrPoolClosed
	}

	// Give the task a way to re-submit itself once dependencies are met.
	// This MUST be set before checking dependencies.
	t.SetSubmitter(p.submit)

	// If the task has dependencies, don't enqueue it yet.
	// It will be submitted later by the final dependency.
	if t.HasDependencies() && t.DependencyCount.Load() > 0 {
		// Record metrics for dependent tasks right away
		if p.metrics != nil {
			p.metrics.IncrementSubmitted()
		}
		return nil
	}

	return p.submit(t)
}

// submit is the internal method that enqueues a task for execution.
func (p *FixedPool) submit(t *task.Task) error {
	// Record metrics
	if p.metrics != nil && !t.HasDependencies() { // Avoid double-counting for dependent tasks
		p.metrics.IncrementSubmitted()
	}

	// Enqueue task
	if err := p.queue.Enqueue(t); err != nil {
		if err == queue.ErrQueueFull {
			return ErrPoolFull
		}
		return err
	}

	// Track task completion
	if p.metrics != nil {
		p.wg.Add(1)
		go p.trackTask(t)
	}

	return nil
}

// SubmitFunc submits a function as a task
func (p *FixedPool) SubmitFunc(fn task.TaskFunc, opts ...task.TaskOption) (*task.Future, error) {
	if p.tracer != nil {
		opts = append(opts, task.WithTracer(p.tracer))
	}
	t := task.NewTask(fn, opts...)
	if err := p.Submit(t); err != nil {
		return nil, err
	}
	return t.Future(), nil
}

// trackTask tracks task completion for metrics and events
func (p *FixedPool) trackTask(t *task.Task) {
	defer p.wg.Done()

	// Send "submitted" event
	p.sendTaskEvent("submitted", t)

	future := t.Future()
	_, err := future.Get()

	if p.metrics != nil {
		recordTaskRetries(p.metrics, t.RetryCount())

		// Record completion
		state := t.State()
		switch state {
		case task.StateCompleted:
			p.metrics.IncrementCompleted()
			p.sendTaskEvent("completed", t)
		case task.StateFailed:
			p.metrics.IncrementFailed()
			p.sendTaskEvent("failed", t)
		case task.StateCancelled:
			p.metrics.IncrementCancelled()
			p.sendTaskEvent("cancelled", t)
		case task.StateTimeout:
			p.metrics.IncrementTimedOut()
			p.sendTaskEvent("timeout", t)
		}

		// Record timing
		if err == nil {
			p.metrics.RecordWaitTime(t.WaitTime())
			p.metrics.RecordExecTime(t.Duration())
		}
	}
}

// sendTaskEvent sends a task event if the channel is configured.
func (p *FixedPool) sendTaskEvent(eventType string, t *task.Task) {
	if p.config.TaskEventChan != nil {
		event := TaskEvent{
			EventType: eventType,
			TaskID:    t.ID(),
			Priority:  int(t.Priority()),
			State:     t.State().String(),
			Duration:  t.Duration().Milliseconds(),
		}
		// Non-blocking send
		select {
		case p.config.TaskEventChan <- event:
		default:
		}
	}
}

// Shutdown gracefully shuts down the pool
func (p *FixedPool) Shutdown(ctx context.Context) error {
	if !p.shutdown.CompareAndSwap(false, true) {
		return nil // Already shut down
	}

	close(p.shutdownCh)

	// Close queue (no new tasks)
	p.queue.Close()

	// Wait for all tasks to complete or context to be cancelled
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All tasks completed
	case <-ctx.Done():
		// Context cancelled, stop workers immediately
		p.workers.Stop()
		return ctx.Err()
	}

	// Stop workers
	p.workers.StopAndWait()

	return nil
}

// ShutdownNow immediately shuts down the pool
func (p *FixedPool) ShutdownNow() error {
	if !p.shutdown.CompareAndSwap(false, true) {
		return nil
	}

	close(p.shutdownCh)

	// Close queue
	p.queue.Close()

	// Stop workers immediately
	p.workers.Stop()

	return nil
}

// IsShutdown returns true if the pool has been shut down
func (p *FixedPool) IsShutdown() bool {
	return p.shutdown.Load()
}

// AwaitTermination waits for all tasks to complete
func (p *FixedPool) AwaitTermination(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		p.workers.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Metrics returns the pool metrics
func (p *FixedPool) Metrics() *metrics.Metrics {
	return p.metrics
}

// updateMetrics periodically updates metrics
func (p *FixedPool) updateMetrics() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.shutdownCh:
			return
		case <-ticker.C:
			if p.metrics != nil {
				p.metrics.SetQueueSize(p.queue.Size())
				p.metrics.SetActiveWorkers(p.workers.ActiveWorkers())
				p.metrics.RecordSnapshot()
			}
		}
	}
}

// WorkerCount returns the number of workers
func (p *FixedPool) WorkerCount() int {
	return p.workers.Size()
}

// QueueSize returns the current queue size
func (p *FixedPool) QueueSize() int {
	return p.queue.Size()
}

// ActiveWorkers returns the number of active workers
func (p *FixedPool) ActiveWorkers() int {
	return p.workers.ActiveWorkers()
}

// IdleWorkers returns the number of idle workers
func (p *FixedPool) IdleWorkers() int {
	return p.workers.IdleWorkers()
}

// Workers returns the list of workers in the pool
func (p *FixedPool) Workers() []*worker.Worker {
	return p.workers.Workers()
}

// Resize changes the number of workers in the pool.
func (p *FixedPool) Resize(newSize int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	currentSize := p.workers.Size()
	if newSize > currentSize {
		// Scale up
		for i := 0; i < newSize-currentSize; i++ {
			p.workers.AddWorker()
		}
	} else if newSize < currentSize {
		// Scale down
		for i := 0; i < currentSize-newSize; i++ {
			p.workers.RemoveWorker()
		}
	}

	p.config.WorkerCount = newSize
	if p.metrics != nil {
		p.metrics.SetWorkerCount(newSize)
	}
}
