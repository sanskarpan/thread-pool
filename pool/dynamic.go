// Dynamic thread pool with auto-scaling
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

// DynamicPool automatically scales workers based on load
type DynamicPool struct {
	config     *Config
	minWorkers int
	maxWorkers int
	queue      queue.Queue
	workers    *worker.WorkerPool
	metrics    *metrics.Metrics
	shutdown   atomic.Bool
	shutdownCh chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
	tracer     trace.Tracer
}

// NewDynamicPool creates a new dynamic thread pool
func NewDynamicPool(config *Config, minWorkers, maxWorkers, queueSize int) *DynamicPool {
	if config == nil {
		config = DefaultConfig()
	}

	var q queue.Queue
	if queueSize > 0 {
		q = queue.NewBoundedQueue(queueSize)
	} else {
		q = queue.NewUnboundedQueue()
	}

	pool := &DynamicPool{
		config:     config,
		minWorkers: minWorkers,
		maxWorkers: maxWorkers,
		queue:      q,
		workers:    worker.NewWorkerPool(minWorkers, q),
		metrics:    metrics.NewMetrics(queueSize),
		shutdownCh: make(chan struct{}),
	}
	if config.TracerProvider != nil {
		pool.tracer = config.TracerProvider.Tracer("threadpool")
	}

	pool.workers.Start()
	pool.metrics.SetWorkerCount(minWorkers)

	// Start auto-scaler
	go pool.autoScale()
	go pool.updateMetrics()

	return pool
}

// autoScale automatically adjusts worker count based on load
func (p *DynamicPool) autoScale() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.shutdownCh:
			return
		case <-ticker.C:
			p.scaleWorkers()
		}
	}
}

// scaleWorkers scales workers up or down
func (p *DynamicPool) scaleWorkers() {
	queueSize := p.queue.Size()
	workerCount := p.workers.Size()
	activeWorkers := p.workers.ActiveWorkers()
	idleWorkers := p.workers.IdleWorkers()

	// Scale up if queue is growing and we're near capacity
	if queueSize > workerCount && activeWorkers == workerCount && workerCount < p.maxWorkers {
		p.workers.AddWorker()
		p.metrics.SetWorkerCount(p.workers.Size())
	}

	// Scale down if many workers are idle
	if idleWorkers > workerCount/2 && workerCount > p.minWorkers {
		p.workers.RemoveWorker()
		p.metrics.SetWorkerCount(p.workers.Size())
	}
}

// Submit submits a task to the pool, handling dependencies.
func (p *DynamicPool) Submit(t *task.Task) error {
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
func (p *DynamicPool) submit(t *task.Task) error {
	if p.metrics != nil && !t.HasDependencies() {
		p.metrics.IncrementSubmitted()
	}

	if err := p.queue.Enqueue(t); err != nil {
		if err == queue.ErrQueueFull {
			return ErrPoolFull
		}
		return err
	}

	if p.metrics != nil {
		p.wg.Add(1)
		go p.trackTask(t)
	}

	return nil
}

// SubmitFunc submits a function as a task
func (p *DynamicPool) SubmitFunc(fn task.TaskFunc, opts ...task.TaskOption) (*task.Future, error) {
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
func (p *DynamicPool) trackTask(t *task.Task) {
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
func (p *DynamicPool) sendTaskEvent(eventType string, t *task.Task) {
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
func (p *DynamicPool) Shutdown(ctx context.Context) error {
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
func (p *DynamicPool) ShutdownNow() error {
	if !p.shutdown.CompareAndSwap(false, true) {
		return nil
	}

	close(p.shutdownCh)
	p.queue.Close()
	p.workers.Stop()
	return nil
}

// IsShutdown returns true if the pool has been shut down
func (p *DynamicPool) IsShutdown() bool {
	return p.shutdown.Load()
}

// AwaitTermination waits for all tasks to complete
func (p *DynamicPool) AwaitTermination(ctx context.Context) error {
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
func (p *DynamicPool) Metrics() *metrics.Metrics {
	return p.metrics
}

// updateMetrics periodically updates metrics
func (p *DynamicPool) updateMetrics() {
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
func (p *DynamicPool) Workers() []*worker.Worker {
	return p.workers.Workers()
}
