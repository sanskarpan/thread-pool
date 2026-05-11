// Scheduled thread pool implementation
package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/sanskarpan/thread-pool/metrics"
	"github.com/sanskarpan/thread-pool/queue"
	"github.com/sanskarpan/thread-pool/task"
	"github.com/sanskarpan/thread-pool/worker"
	"go.opentelemetry.io/otel/trace"
)

// ScheduledTask represents a task with scheduling information
type ScheduledTask struct {
	task      *task.Task
	runAt     time.Time
	period    time.Duration
	recurring bool
	cron      cron.Schedule
}

// ScheduledPool supports delayed and periodic task execution
type ScheduledPool struct {
	config         *Config
	queue          queue.Queue
	scheduledTasks map[string]*ScheduledTask
	workers        *worker.WorkerPool
	metrics        *metrics.Metrics
	shutdown       atomic.Bool
	shutdownCh     chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex
	tracer         trace.Tracer
}

// NewScheduledPool creates a new scheduled thread pool
func NewScheduledPool(config *Config) *ScheduledPool {
	if config == nil {
		config = DefaultConfig()
	}

	q := queue.NewUnboundedQueue()

	pool := &ScheduledPool{
		config:         config,
		queue:          q,
		scheduledTasks: make(map[string]*ScheduledTask),
		workers:        worker.NewWorkerPool(config.WorkerCount, q),
		metrics:        metrics.NewMetrics(0),
		shutdownCh:     make(chan struct{}),
	}
	if config.TracerProvider != nil {
		pool.tracer = config.TracerProvider.Tracer("threadpool")
	}

	pool.workers.Start()
	pool.metrics.SetWorkerCount(config.WorkerCount)

	// Start scheduler
	go pool.scheduler()
	go pool.updateMetrics()

	return pool
}

// scheduler runs scheduled tasks at the right time
func (p *ScheduledPool) scheduler() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.shutdownCh:
			return
		case <-ticker.C:
			p.checkScheduledTasks()
		}
	}
}

// checkScheduledTasks checks if any scheduled tasks should run
func (p *ScheduledPool) checkScheduledTasks() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()

	for id, st := range p.scheduledTasks {
		if now.After(st.runAt) || now.Equal(st.runAt) {
			// For recurring tasks, we create a new task for each execution
			// to ensure each run is independent.
			if st.recurring {
				newTask := task.NewTask(st.task.GetFn(), st.task.GetOpts()...)
				p.Submit(newTask)

				// Schedule the next run
				if st.cron != nil {
					st.runAt = st.cron.Next(now)
				} else {
					st.runAt = now.Add(st.period)
				}
			} else {
				// For non-recurring tasks, just submit the original task
				p.Submit(st.task)
				delete(p.scheduledTasks, id)
			}
		}
	}
}

// Submit submits a task to run immediately
func (p *ScheduledPool) Submit(t *task.Task) error {
	if p.IsShutdown() {
		return ErrPoolClosed
	}

	if p.metrics != nil {
		p.metrics.IncrementSubmitted()
	}

	if err := p.queue.Enqueue(t); err != nil {
		return err
	}

	if p.metrics != nil {
		p.wg.Add(1)
		go p.trackTask(t)
	}

	return nil
}

// SubmitFunc submits a function as a task
func (p *ScheduledPool) SubmitFunc(fn task.TaskFunc, opts ...task.TaskOption) (*task.Future, error) {
	if p.tracer != nil {
		opts = append(opts, task.WithTracer(p.tracer))
	}
	t := task.NewTask(fn, opts...)
	if err := p.Submit(t); err != nil {
		return nil, err
	}
	return t.Future(), nil
}

// Schedule schedules a task to run after a delay
func (p *ScheduledPool) Schedule(t *task.Task, delay time.Duration) error {
	if p.IsShutdown() {
		return ErrPoolClosed
	}

	// Manually add tracer if the task was created outside the pool's SubmitFunc
	if p.tracer != nil {
		// This is a bit of a hack, but ensures tracing is applied.
		// A better long-term solution might be a Task constructor that takes a pool.
		task.WithTracer(p.tracer)(t)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	st := &ScheduledTask{
		task:      t,
		runAt:     time.Now().Add(delay),
		recurring: false,
	}

	p.scheduledTasks[t.ID()] = st

	if p.metrics != nil {
		p.metrics.IncrementSubmitted()
		p.wg.Add(1)
		go p.trackTask(t)
	}

	return nil
}

// ScheduleAtFixedRate schedules a recurring task at fixed intervals
func (p *ScheduledPool) ScheduleAtFixedRate(fn task.TaskFunc, initialDelay, period time.Duration) error {
	if p.IsShutdown() {
		return ErrPoolClosed
	}

	var opts []task.TaskOption
	if p.tracer != nil {
		opts = append(opts, task.WithTracer(p.tracer))
	}
	t := task.NewTask(fn, opts...)

	p.mu.Lock()
	defer p.mu.Unlock()

	st := &ScheduledTask{
		task:      t,
		runAt:     time.Now().Add(initialDelay),
		period:    period,
		recurring: true,
	}

	p.scheduledTasks[t.ID()] = st

	return nil
}

// ScheduleWithCron schedules a task to run based on a cron expression.
func (p *ScheduledPool) ScheduleWithCron(fn task.TaskFunc, cronExpr string) (cron.EntryID, error) {
	if p.IsShutdown() {
		return 0, ErrPoolClosed
	}

	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(cronExpr)
	if err != nil {
		return 0, err
	}

	t := task.NewTask(fn)

	p.mu.Lock()
	defer p.mu.Unlock()

	st := &ScheduledTask{
		task:      t,
		runAt:     schedule.Next(time.Now()),
		recurring: true,
		cron:      schedule,
	}

	p.scheduledTasks[t.ID()] = st

	// Note: We don't return a meaningful EntryID here as our internal scheduler is simple.
	// This could be enhanced in the future to return a value that allows cancellation.
	return 0, nil
}

// CancelScheduled cancels a scheduled task
func (p *ScheduledPool) CancelScheduled(taskID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if st, ok := p.scheduledTasks[taskID]; ok {
		st.task.Cancel()
		delete(p.scheduledTasks, taskID)
	}
}

// trackTask tracks task completion
func (p *ScheduledPool) trackTask(t *task.Task) {
	defer p.wg.Done()

	future := t.Future()
	_, err := future.Get()

	if p.metrics != nil {
		recordTaskRetries(p.metrics, t.RetryCount())

		state := t.State()
		switch state {
		case task.StateCompleted:
			p.metrics.IncrementCompleted()
		case task.StateFailed:
			p.metrics.IncrementFailed()
		case task.StateCancelled:
			p.metrics.IncrementCancelled()
		case task.StateTimeout:
			p.metrics.IncrementTimedOut()
		}

		if err == nil {
			p.metrics.RecordWaitTime(t.WaitTime())
			p.metrics.RecordExecTime(t.Duration())
		}
	}
}

// Shutdown gracefully shuts down the pool
func (p *ScheduledPool) Shutdown(ctx context.Context) error {
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
func (p *ScheduledPool) ShutdownNow() error {
	if !p.shutdown.CompareAndSwap(false, true) {
		return nil
	}

	close(p.shutdownCh)
	p.queue.Close()
	p.workers.Stop()
	return nil
}

// IsShutdown returns true if the pool has been shut down
func (p *ScheduledPool) IsShutdown() bool {
	return p.shutdown.Load()
}

// AwaitTermination waits for all tasks to complete
func (p *ScheduledPool) AwaitTermination(ctx context.Context) error {
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
func (p *ScheduledPool) Metrics() *metrics.Metrics {
	return p.metrics
}

// updateMetrics periodically updates metrics
func (p *ScheduledPool) updateMetrics() {
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
func (p *ScheduledPool) Workers() []*worker.Worker {
	return p.workers.Workers()
}
