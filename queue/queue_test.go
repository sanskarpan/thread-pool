package queue

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/thread-pool/task"
)

func TestUnboundedQueue_Basic(t *testing.T) {
	q := NewUnboundedQueue()

	if !q.IsEmpty() {
		t.Error("New queue should be empty")
	}

	if q.IsFull() {
		t.Error("Unbounded queue should never be full")
	}

	// Enqueue tasks
	for i := 0; i < 10; i++ {
		task := task.NewTask(func(ctx context.Context) (interface{}, error) {
			return i, nil
		})

		if err := q.Enqueue(task); err != nil {
			t.Fatalf("Failed to enqueue: %v", err)
		}
	}

	if q.Size() != 10 {
		t.Errorf("Expected size 10, got %d", q.Size())
	}

	// Dequeue all
	for i := 0; i < 10; i++ {
		_, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Failed to dequeue: %v", err)
		}
	}

	if !q.IsEmpty() {
		t.Error("Queue should be empty after dequeuing all")
	}
}

func TestUnboundedQueue_CloseAndEnqueue(t *testing.T) {
	q := NewUnboundedQueue()
	q.Close()

	task := task.NewTask(func(ctx context.Context) (interface{}, error) {
		return nil, nil
	})

	err := q.Enqueue(task)
	if err != ErrQueueClosed {
		t.Errorf("Expected ErrQueueClosed, got %v", err)
	}
}

func TestUnboundedQueue_CloseAndDequeue(t *testing.T) {
	q := NewUnboundedQueue()
	q.Close()

	_, err := q.Dequeue()
	if err != ErrQueueClosed {
		t.Errorf("Expected ErrQueueClosed, got %v", err)
	}
}

func TestBoundedQueue_Basic(t *testing.T) {
	capacity := 5
	q := NewBoundedQueue(capacity)

	// Fill queue
	for i := 0; i < capacity; i++ {
		task := task.NewTask(func(ctx context.Context) (interface{}, error) {
			return i, nil
		})

		if err := q.Enqueue(task); err != nil {
			t.Fatalf("Failed to enqueue: %v", err)
		}
	}

	if !q.IsFull() {
		t.Error("Queue should be full")
	}

	if q.Size() != capacity {
		t.Errorf("Expected size %d, got %d", capacity, q.Size())
	}
}

func TestBoundedQueue_EnqueueWithContext(t *testing.T) {
	q := NewBoundedQueue(1)

	// Fill the queue
	task1 := task.NewTask(func(ctx context.Context) (interface{}, error) {
		return 1, nil
	})
	q.Enqueue(task1)

	// Try to enqueue with timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	task2 := task.NewTask(func(ctx context.Context) (interface{}, error) {
		return 2, nil
	})

	err := q.EnqueueWithContext(ctx, task2)
	if err != context.DeadlineExceeded {
		t.Errorf("Expected DeadlineExceeded, got %v", err)
	}
}

func TestBoundedQueue_DequeueWithContext(t *testing.T) {
	q := NewBoundedQueue(5)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := q.DequeueWithContext(ctx)
	if err != context.DeadlineExceeded {
		t.Errorf("Expected DeadlineExceeded, got %v", err)
	}
}

func TestPriorityQueue_Ordering(t *testing.T) {
	q := NewPriorityQueue()

	// Add tasks with different priorities
	priorities := []task.Priority{
		task.PriorityLow,
		task.PriorityHigh,
		task.PriorityNormal,
		task.PriorityUrgent,
	}

	for _, p := range priorities {
		task := task.NewTask(func(ctx context.Context) (interface{}, error) {
			return nil, nil
		}, task.WithPriority(p))

		if err := q.Enqueue(task); err != nil {
			t.Fatalf("Failed to enqueue: %v", err)
		}
	}

	// Dequeue should return highest priority first
	first, _ := q.Dequeue()
	if first.Priority() != task.PriorityUrgent {
		t.Errorf("Expected Urgent priority first, got %v", first.Priority())
	}

	second, _ := q.Dequeue()
	if second.Priority() != task.PriorityHigh {
		t.Errorf("Expected High priority second, got %v", second.Priority())
	}

	third, _ := q.Dequeue()
	if third.Priority() != task.PriorityNormal {
		t.Errorf("Expected Normal priority third, got %v", third.Priority())
	}

	fourth, _ := q.Dequeue()
	if fourth.Priority() != task.PriorityLow {
		t.Errorf("Expected Low priority fourth, got %v", fourth.Priority())
	}
}

func TestPriorityQueue_Size(t *testing.T) {
	q := NewPriorityQueue()

	if q.Size() != 0 {
		t.Error("New queue should have size 0")
	}

	task := task.NewTask(func(ctx context.Context) (interface{}, error) {
		return nil, nil
	})

	q.Enqueue(task)

	if q.Size() != 1 {
		t.Errorf("Expected size 1, got %d", q.Size())
	}
}

func TestQueue_Concurrent(t *testing.T) {
	q := NewUnboundedQueue()

	done := make(chan bool)

	// Producer
	go func() {
		for i := 0; i < 100; i++ {
			task := task.NewTask(func(ctx context.Context) (interface{}, error) {
				return i, nil
			})
			q.Enqueue(task)
		}
		done <- true
	}()

	// Consumer
	go func() {
		for i := 0; i < 100; i++ {
			q.Dequeue()
		}
		done <- true
	}()

	// Wait for both
	<-done
	<-done

	if q.Size() != 0 {
		t.Errorf("Expected empty queue, got size %d", q.Size())
	}
}
