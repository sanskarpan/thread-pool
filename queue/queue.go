// Package queue provides various queue implementations for task storage
package queue

import (
	"container/heap"
	"context"
	"errors"
	"sync"

	"github.com/sanskarpan/thread-pool/task"
)

var (
	// ErrQueueFull is returned when trying to enqueue to a full bounded queue
	ErrQueueFull = errors.New("queue is full")
	// ErrQueueClosed is returned when operating on a closed queue
	ErrQueueClosed = errors.New("queue is closed")
)

// Queue is the interface for task queues
type Queue interface {
	// Enqueue adds a task to the queue
	Enqueue(t *task.Task) error
	// EnqueueWithContext adds a task with context support
	EnqueueWithContext(ctx context.Context, t *task.Task) error
	// Dequeue removes and returns a task from the queue
	Dequeue() (*task.Task, error)
	// DequeueWithContext removes and returns a task with context support
	DequeueWithContext(ctx context.Context) (*task.Task, error)
	// Size returns the current queue size
	Size() int
	// IsEmpty returns true if the queue is empty
	IsEmpty() bool
	// IsFull returns true if the queue is full (for bounded queues)
	IsFull() bool
	// Close closes the queue
	Close()
	// IsClosed returns true if the queue is closed
	IsClosed() bool
}

// UnboundedQueue is a queue with no size limit
type UnboundedQueue struct {
	tasks    []*task.Task
	mu       sync.Mutex
	notEmpty chan struct{}
	closed   bool
}

// NewUnboundedQueue creates a new unbounded queue
func NewUnboundedQueue() *UnboundedQueue {
	q := &UnboundedQueue{
		tasks:    make([]*task.Task, 0),
		notEmpty: make(chan struct{}, 1),
	}
	return q
}

func (q *UnboundedQueue) Enqueue(t *task.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	q.tasks = append(q.tasks, t)
	select {
	case q.notEmpty <- struct{}{}:
	default:
	}
	return nil
}

func (q *UnboundedQueue) EnqueueWithContext(ctx context.Context, t *task.Task) error {
	return q.Enqueue(t)
}

func (q *UnboundedQueue) Dequeue() (*task.Task, error) {
	return q.DequeueWithContext(context.Background())
}

func (q *UnboundedQueue) DequeueWithContext(ctx context.Context) (*task.Task, error) {
	for {
		q.mu.Lock()
		if q.closed && len(q.tasks) == 0 {
			q.mu.Unlock()
			return nil, ErrQueueClosed
		}
		if len(q.tasks) > 0 {
			t := q.tasks[0]
			q.tasks = q.tasks[1:]
			q.mu.Unlock()
			return t, nil
		}
		q.mu.Unlock()

		select {
		case <-q.notEmpty:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (q *UnboundedQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *UnboundedQueue) IsEmpty() bool {
	return q.Size() == 0
}

func (q *UnboundedQueue) IsFull() bool {
	return false
}

func (q *UnboundedQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.notEmpty)
}

func (q *UnboundedQueue) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// BoundedQueue is a queue with a maximum size
type BoundedQueue struct {
	tasks    []*task.Task
	capacity int
	mu       sync.Mutex
	notEmpty chan struct{}
	notFull  chan struct{}
	closed   bool
}

// NewBoundedQueue creates a new bounded queue with the given capacity
func NewBoundedQueue(capacity int) *BoundedQueue {
	q := &BoundedQueue{
		tasks:    make([]*task.Task, 0, capacity),
		capacity: capacity,
		notEmpty: make(chan struct{}, 1),
		notFull:  make(chan struct{}, 1),
	}
	return q
}

func (q *BoundedQueue) Enqueue(t *task.Task) error {
	return q.EnqueueWithContext(context.Background(), t)
}

func (q *BoundedQueue) EnqueueWithContext(ctx context.Context, t *task.Task) error {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ErrQueueClosed
		}
		if len(q.tasks) < q.capacity {
			q.tasks = append(q.tasks, t)
			select {
			case q.notEmpty <- struct{}{}:
			default:
			}
			q.mu.Unlock()
			return nil
		}
		q.mu.Unlock()

		select {
		case <-q.notFull:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (q *BoundedQueue) Dequeue() (*task.Task, error) {
	return q.DequeueWithContext(context.Background())
}

func (q *BoundedQueue) DequeueWithContext(ctx context.Context) (*task.Task, error) {
	for {
		q.mu.Lock()
		if q.closed && len(q.tasks) == 0 {
			q.mu.Unlock()
			return nil, ErrQueueClosed
		}
		if len(q.tasks) > 0 {
			t := q.tasks[0]
			q.tasks = q.tasks[1:]
			if !q.closed {
				select {
				case q.notFull <- struct{}{}:
				default:
				}
			}
			q.mu.Unlock()
			return t, nil
		}
		q.mu.Unlock()

		select {
		case <-q.notEmpty:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (q *BoundedQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *BoundedQueue) IsEmpty() bool {
	return q.Size() == 0
}

func (q *BoundedQueue) IsFull() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks) >= q.capacity
}

func (q *BoundedQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.notEmpty)
	close(q.notFull)
}

func (q *BoundedQueue) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// PriorityQueue is a queue that orders tasks by priority
type PriorityQueue struct {
	tasks    taskHeap
	mu       sync.Mutex
	notEmpty chan struct{}
	closed   bool
}

// NewPriorityQueue creates a new priority queue
func NewPriorityQueue() *PriorityQueue {
	q := &PriorityQueue{
		tasks:    make(taskHeap, 0),
		notEmpty: make(chan struct{}, 1),
	}
	heap.Init(&q.tasks)
	return q
}

func (q *PriorityQueue) Enqueue(t *task.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	heap.Push(&q.tasks, t)
	select {
	case q.notEmpty <- struct{}{}:
	default:
	}
	return nil
}

func (q *PriorityQueue) EnqueueWithContext(ctx context.Context, t *task.Task) error {
	return q.Enqueue(t)
}

func (q *PriorityQueue) Dequeue() (*task.Task, error) {
	return q.DequeueWithContext(context.Background())
}

func (q *PriorityQueue) DequeueWithContext(ctx context.Context) (*task.Task, error) {
	for {
		q.mu.Lock()
		if q.closed && q.tasks.Len() == 0 {
			q.mu.Unlock()
			return nil, ErrQueueClosed
		}
		if q.tasks.Len() > 0 {
			t := heap.Pop(&q.tasks).(*task.Task)
			q.mu.Unlock()
			return t, nil
		}
		q.mu.Unlock()

		select {
		case <-q.notEmpty:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (q *PriorityQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.tasks.Len()
}

func (q *PriorityQueue) IsEmpty() bool {
	return q.Size() == 0
}

func (q *PriorityQueue) IsFull() bool {
	return false
}

func (q *PriorityQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.notEmpty)
}

func (q *PriorityQueue) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// taskHeap implements heap.Interface for priority queue
type taskHeap []*task.Task

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	// Higher priority first
	return h[i].Priority() > h[j].Priority()
}

func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *taskHeap) Push(x interface{}) {
	*h = append(*h, x.(*task.Task))
}

func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
