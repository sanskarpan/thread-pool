// Package worker provides worker goroutine management
package worker

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/thread-pool/queue"
	"github.com/sanskarpan/thread-pool/task"
)

// State represents worker state
type State int32

const (
	StateIdle State = iota
	StateBusy
	StateStopped
)

// Worker represents a worker goroutine
type Worker struct {
	id            string
	queue         queue.Queue
	state         atomic.Int32
	tasksExecuted atomic.Uint64
	currentTask   *task.Task
	mu            sync.RWMutex
	stopCh        chan struct{}
	stoppedCh     chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewWorker creates a new worker
func NewWorker(id string, q queue.Queue) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	w := &Worker{
		id:        id,
		queue:     q,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
		ctx:       ctx,
		cancel:    cancel,
	}

	w.state.Store(int32(StateIdle))
	return w
}

// ID returns the worker ID
func (w *Worker) ID() string {
	return w.id
}

// State returns the current state
func (w *Worker) State() State {
	return State(w.state.Load())
}

// TasksExecuted returns the number of tasks executed
func (w *Worker) TasksExecuted() uint64 {
	return w.tasksExecuted.Load()
}

// CurrentTask returns the currently executing task
func (w *Worker) CurrentTask() *task.Task {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.currentTask
}

// Start starts the worker
func (w *Worker) Start() {
	go w.run()
}

// run is the main worker loop
func (w *Worker) run() {
	defer close(w.stoppedCh)

	for {
		select {
		case <-w.stopCh:
			w.state.Store(int32(StateStopped))
			return
		case <-w.ctx.Done():
			w.state.Store(int32(StateStopped))
			return
		default:
			// Try to dequeue a task
			t, err := w.queue.DequeueWithContext(w.ctx)
			if err != nil {
				// Queue closed or context cancelled
				if err == queue.ErrQueueClosed || err == context.Canceled {
					w.state.Store(int32(StateStopped))
					return
				}
				continue
			}

			// Execute task
			w.executeTask(t)
		}
	}
}

// executeTask executes a single task
func (w *Worker) executeTask(t *task.Task) {
	w.state.Store(int32(StateBusy))

	w.mu.Lock()
	w.currentTask = t
	w.mu.Unlock()

	// Execute the task
	t.Execute()

	w.mu.Lock()
	w.currentTask = nil
	w.mu.Unlock()

	w.tasksExecuted.Add(1)
	w.state.Store(int32(StateIdle))
}

// Stop stops the worker gracefully
func (w *Worker) Stop() {
	close(w.stopCh)
	w.cancel()
}

// Wait waits for the worker to stop
func (w *Worker) Wait() {
	<-w.stoppedCh
}

// StopAndWait stops the worker and waits for it to finish
func (w *Worker) StopAndWait() {
	w.Stop()
	w.Wait()
}

// WorkerPool manages a pool of workers
type WorkerPool struct {
	workers   []*Worker
	queue     queue.Queue
	size      int
	mu        sync.RWMutex
	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
	stopped   atomic.Bool
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(size int, q queue.Queue) *WorkerPool {
	return &WorkerPool{
		workers: make([]*Worker, 0, size),
		queue:   q,
		size:    size,
	}
}

// Start starts all workers in the pool
func (wp *WorkerPool) Start() {
	wp.startOnce.Do(func() {
		wp.mu.Lock()
		defer wp.mu.Unlock()

		for i := 0; i < wp.size; i++ {
			w := NewWorker(generateWorkerID(i), wp.queue)
			w.Start()
			wp.workers = append(wp.workers, w)
		}

		wp.started.Store(true)
	})
}

// Stop stops all workers gracefully
func (wp *WorkerPool) Stop() {
	wp.stopOnce.Do(func() {
		wp.mu.RLock()
		workers := wp.workers
		wp.mu.RUnlock()

		for _, w := range workers {
			w.Stop()
		}

		wp.stopped.Store(true)
	})
}

// Wait waits for all workers to stop
func (wp *WorkerPool) Wait() {
	wp.mu.RLock()
	workers := wp.workers
	wp.mu.RUnlock()

	for _, w := range workers {
		w.Wait()
	}
}

// StopAndWait stops all workers and waits for them to finish
func (wp *WorkerPool) StopAndWait() {
	wp.Stop()
	wp.Wait()
}

// Size returns the number of workers
func (wp *WorkerPool) Size() int {
	return wp.size
}

// ActiveWorkers returns the number of busy workers
func (wp *WorkerPool) ActiveWorkers() int {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	active := 0
	for _, w := range wp.workers {
		if w.State() == StateBusy {
			active++
		}
	}
	return active
}

// IdleWorkers returns the number of idle workers
func (wp *WorkerPool) IdleWorkers() int {
	return wp.Size() - wp.ActiveWorkers()
}

// TasksExecuted returns the total number of tasks executed by all workers
func (wp *WorkerPool) TasksExecuted() uint64 {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	total := uint64(0)
	for _, w := range wp.workers {
		total += w.TasksExecuted()
	}
	return total
}

// Workers returns a copy of the worker slice
func (wp *WorkerPool) Workers() []*Worker {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	workers := make([]*Worker, len(wp.workers))
	copy(workers, wp.workers)
	return workers
}

// AddWorker adds a new worker to the pool (for dynamic scaling)
func (wp *WorkerPool) AddWorker() *Worker {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	id := generateWorkerID(len(wp.workers))
	w := NewWorker(id, wp.queue)

	if wp.started.Load() {
		w.Start()
	}

	wp.workers = append(wp.workers, w)
	wp.size++
	return w
}

// RemoveWorker removes a worker from the pool (for dynamic scaling)
func (wp *WorkerPool) RemoveWorker() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if len(wp.workers) == 0 {
		return
	}

	// Stop the last worker
	w := wp.workers[len(wp.workers)-1]
	w.Stop()

	// Remove from slice
	wp.workers = wp.workers[:len(wp.workers)-1]
	wp.size--
}

// IsStarted returns true if the pool has been started
func (wp *WorkerPool) IsStarted() bool {
	return wp.started.Load()
}

// IsStopped returns true if the pool has been stopped
func (wp *WorkerPool) IsStopped() bool {
	return wp.stopped.Load()
}

// generateWorkerID generates a unique worker ID
func generateWorkerID(index int) string {
	return "worker-" + time.Now().Format("20060102150405") + "-" + strconv.Itoa(index)
}
