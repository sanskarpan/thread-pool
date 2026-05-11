package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/thread-pool/queue"
	"github.com/sanskarpan/thread-pool/task"
)

func TestWorker_Basic(t *testing.T) {
	q := queue.NewUnboundedQueue()
	w := NewWorker("test-worker", q)

	if w.ID() != "test-worker" {
		t.Errorf("Expected ID 'test-worker', got %s", w.ID())
	}

	if w.State() != StateIdle {
		t.Errorf("Expected initial state Idle, got %v", w.State())
	}

	if w.TasksExecuted() != 0 {
		t.Error("New worker should have 0 tasks executed")
	}
}

func TestWorker_ExecuteTask(t *testing.T) {
	q := queue.NewUnboundedQueue()
	w := NewWorker("worker-1", q)

	var executed atomic.Bool

	// Add task to queue
	task := task.NewTask(func(ctx context.Context) (interface{}, error) {
		executed.Store(true)
		return "result", nil
	})

	q.Enqueue(task)

	// Start worker
	w.Start()

	// Give it time to execute
	time.Sleep(100 * time.Millisecond)

	if !executed.Load() {
		t.Error("Task should have been executed")
	}

	w.StopAndWait()

	if w.TasksExecuted() != 1 {
		t.Errorf("Expected 1 task executed, got %d", w.TasksExecuted())
	}
}

func TestWorker_MultipleTasksExecution(t *testing.T) {
	q := queue.NewUnboundedQueue()
	w := NewWorker("worker-multi", q)

	var counter atomic.Int32

	// Add multiple tasks
	for i := 0; i < 10; i++ {
		task := task.NewTask(func(ctx context.Context) (interface{}, error) {
			counter.Add(1)
			return nil, nil
		})
		q.Enqueue(task)
	}

	w.Start()
	time.Sleep(200 * time.Millisecond)

	w.StopAndWait()

	if counter.Load() != 10 {
		t.Errorf("Expected 10 tasks executed, got %d", counter.Load())
	}

	if w.TasksExecuted() != 10 {
		t.Errorf("Expected 10 in TasksExecuted, got %d", w.TasksExecuted())
	}
}

func TestWorker_Stop(t *testing.T) {
	q := queue.NewUnboundedQueue()
	w := NewWorker("worker-stop", q)

	w.Start()

	// Add a long-running task
	task := task.NewTask(func(ctx context.Context) (interface{}, error) {
		time.Sleep(5 * time.Second)
		return nil, nil
	})
	q.Enqueue(task)

	// Stop immediately
	w.Stop()

	// Should stop quickly
	done := make(chan bool)
	go func() {
		w.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Good, stopped quickly
	case <-time.After(2 * time.Second):
		t.Error("Worker took too long to stop")
	}
}

func TestWorkerPool_Basic(t *testing.T) {
	q := queue.NewUnboundedQueue()
	wp := NewWorkerPool(4, q)

	if wp.Size() != 4 {
		t.Errorf("Expected 4 workers, got %d", wp.Size())
	}

	wp.Start()

	if !wp.IsStarted() {
		t.Error("Pool should be started")
	}

	wp.StopAndWait()

	if !wp.IsStopped() {
		t.Error("Pool should be stopped")
	}
}

func TestWorkerPool_TaskExecution(t *testing.T) {
	q := queue.NewUnboundedQueue()
	wp := NewWorkerPool(4, q)

	var counter atomic.Int32

	// Add tasks
	for i := 0; i < 20; i++ {
		task := task.NewTask(func(ctx context.Context) (interface{}, error) {
			counter.Add(1)
			time.Sleep(10 * time.Millisecond)
			return nil, nil
		})
		q.Enqueue(task)
	}

	wp.Start()
	time.Sleep(300 * time.Millisecond)

	wp.StopAndWait()

	if counter.Load() != 20 {
		t.Errorf("Expected 20 tasks executed, got %d", counter.Load())
	}

	totalTasks := wp.TasksExecuted()
	if totalTasks != 20 {
		t.Errorf("Expected 20 total tasks, got %d", totalTasks)
	}
}

func TestWorkerPool_ActiveWorkers(t *testing.T) {
	q := queue.NewUnboundedQueue()
	wp := NewWorkerPool(4, q)

	// Add long-running tasks
	for i := 0; i < 4; i++ {
		task := task.NewTask(func(ctx context.Context) (interface{}, error) {
			time.Sleep(500 * time.Millisecond)
			return nil, nil
		})
		q.Enqueue(task)
	}

	wp.Start()

	// Give workers time to pick up tasks
	time.Sleep(50 * time.Millisecond)

	active := wp.ActiveWorkers()
	if active != 4 {
		t.Logf("Expected 4 active workers, got %d (timing may vary)", active)
	}

	idle := wp.IdleWorkers()
	if idle != 0 {
		t.Logf("Expected 0 idle workers, got %d (timing may vary)", idle)
	}

	wp.StopAndWait()
}

func TestWorkerPool_AddRemoveWorker(t *testing.T) {
	q := queue.NewUnboundedQueue()
	wp := NewWorkerPool(2, q)

	wp.Start()

	if wp.Size() != 2 {
		t.Errorf("Expected 2 workers, got %d", wp.Size())
	}

	// Add worker
	wp.AddWorker()

	if wp.Size() != 3 {
		t.Errorf("Expected 3 workers after add, got %d", wp.Size())
	}

	// Remove worker
	wp.RemoveWorker()

	if wp.Size() != 2 {
		t.Errorf("Expected 2 workers after remove, got %d", wp.Size())
	}

	wp.StopAndWait()
}

func TestWorkerPool_GetWorkers(t *testing.T) {
	q := queue.NewUnboundedQueue()
	wp := NewWorkerPool(3, q)

	wp.Start()

	workers := wp.Workers()

	if len(workers) != 3 {
		t.Errorf("Expected 3 workers, got %d", len(workers))
	}

	// Verify each worker has an ID
	for _, w := range workers {
		if w.ID() == "" {
			t.Error("Worker should have an ID")
		}
	}

	wp.StopAndWait()
}
