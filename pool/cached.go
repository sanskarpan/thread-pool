// Cached thread pool implementation
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

// CachedPool creates new workers as needed and removes idle workers
type CachedPool struct {
	config       *Config
	maxWorkers   int
	idleTimeout  time.Duration
	queue        queue.Queue
	workers      *worker.WorkerPool
	metrics      *metrics.Metrics
	shutdown     atomic.Bool
	shutdownCh   chan struct{}
	wg           sync.WaitGroup
	mu           sync.RWMutex
	lastActivity time.Time
	tracer       trace.Tracer
}

// NewCachedPool creates a new cached thread pool
func NewCachedPool(config *Config, maxWorkers int, idleTimeout time.Duration) *CachedPool {
	if config == nil {
		config = DefaultConfig()
	}

	q := queue.NewUnboundedQueue()

	pool := &CachedPool{
		config:       config,
		maxWorkers:   maxWorkers,
		idleTimeout:  idleTimeout,
		queue:        q,
		workers:      worker.NewWorkerPool(0, q),
		metrics:      metrics.NewMetrics(0),
		shutdownCh:   make(chan struct{}),
		lastActivity: time.Now(),
	}

	if config.TracerProvider != nil {
		pool.tracer = config.TracerProvider.Tracer("threadpool")
	}

	pool.workers.Start()

	// Start worker manager
	go pool.manageWorkers()

	// Start metrics updater
	go pool.updateMetrics()

	return pool
}

// manageWorkers manages worker creation and removal
func (p *CachedPool) manageWorkers() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.shutdownCh:
			return
		case <-ticker.C:
			p.adjustWorkers()
		}
	}
}

// adjustWorkers adjusts the worker count based on load
func (p *CachedPool) adjustWorkers() {
	queueSize := p.queue.Size()
	workerCount := p.workers.Size()
	idleWorkers := p.workers.IdleWorkers()
	p.mu.RLock()
	lastActivity := p.lastActivity
	p.mu.RUnlock()

	// Add workers if needed
	if queueSize > 0 && idleWorkers == 0 && workerCount < p.maxWorkers {
		p.workers.AddWorker()
		if p.metrics != nil {
			p.metrics.SetWorkerCount(p.workers.Size())
		}
	}

	// Remove idle workers after timeout
	if idleWorkers > 0 && time.Since(lastActivity) > p.idleTimeout {
		if workerCount > 1 { // Keep at least 1 worker
			p.workers.RemoveWorker()
			if p.metrics != nil {
				p.metrics.SetWorkerCount(p.workers.Size())
			}
		}
	}
}

// Submit submits a task to the pool, handling dependencies.
func (p *CachedPool) Submit(t *task.Task) error {
	if p.IsShutdown() {
		return ErrPoolClosed
	}

	t.SetSubmitter(p.submit)

	if t.HasDependencies() && t.DependencyCount.Load() > 0 {
		if p.metrics != nil {
			p.metrics.IncrementSubmitted()
		}
		return nil
	}

	return p.submit(t)
}

// submit is the internal method that enqueues a task for execution.
func (p *CachedPool) submit(t *task.Task) error {
	p.mu.Lock()
	p.lastActivity = time.Now()
	p.mu.Unlock()

	if p.metrics != nil && !t.HasDependencies() {
		p.metrics.IncrementSubmitted()
	}

	if err := p.queue.Enqueue(t); err != nil {
		return err
	}

	if p.metrics != nil {
		p.wg.Add(1)
		go p.trackTask(t)
	}

	// Maybe add a worker
	if p.workers.IdleWorkers() == 0 && p.workers.Size() < p.maxWorkers {
		p.workers.AddWorker()
		if p.metrics != nil {
			p.metrics.SetWorkerCount(p.workers.Size())
		}
	}

	return nil
}

// SubmitFunc submits a function as a task
func (p *CachedPool) SubmitFunc(fn task.TaskFunc, opts ...task.TaskOption) (*task.Future, error) {
	if p.tracer != nil {
		opts = append(opts, task.WithTracer(p.tracer))
	}
	t := task.NewTask(fn, opts...)
	if err := p.Submit(t); err != nil {
		return nil, err
	}
	return t.Future(), nil
}

// trackTask tracks task completion
func (p *CachedPool) trackTask(t *task.Task) {
	defer p.wg.Done()

	p.sendTaskEvent("submitted", t)

	future := t.Future()
	_, err := future.Get()

	if p.metrics != nil {
		recordTaskRetries(p.metrics, t.RetryCount())

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

		if err == nil {
			p.metrics.RecordWaitTime(t.WaitTime())
			p.metrics.RecordExecTime(t.Duration())
		}
	}
}

// sendTaskEvent sends a task event if the channel is configured.
func (p *CachedPool) sendTaskEvent(eventType string, t *task.Task) {
	if p.config.TaskEventChan != nil {
		event := TaskEvent{
			EventType: eventType,
			TaskID:    t.ID(),
			Priority:  int(t.Priority()),
			State:     t.State().String(),
			Duration:  t.Duration().Milliseconds(),
		}
		select {
		case p.config.TaskEventChan <- event:
		default:
		}
	}
}

// Shutdown gracefully shuts down the pool
func (p *CachedPool) Shutdown(ctx context.Context) error {
	if !p.shutdown.CompareAndSwap(false, true) {
		return nil
	}

	close(p.shutdownCh)
	p.queue.Close()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		p.workers.Stop()
		return ctx.Err()
	}

	p.workers.StopAndWait()
	return nil
}

// ShutdownNow immediately shuts down the pool
func (p *CachedPool) ShutdownNow() error {
	if !p.shutdown.CompareAndSwap(false, true) {
		return nil
	}

	close(p.shutdownCh)
	p.queue.Close()
	p.workers.Stop()
	return nil
}

// IsShutdown returns true if the pool has been shut down
func (p *CachedPool) IsShutdown() bool {
	return p.shutdown.Load()
}

// AwaitTermination waits for all tasks to complete
func (p *CachedPool) AwaitTermination(ctx context.Context) error {
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
func (p *CachedPool) Metrics() *metrics.Metrics {
	return p.metrics
}

// updateMetrics periodically updates metrics
func (p *CachedPool) updateMetrics() {
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

// Workers returns the list of workers in the pool
func (p *CachedPool) Workers() []*worker.Worker {
	return p.workers.Workers()
}
