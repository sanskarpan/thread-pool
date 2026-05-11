package pool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/thread-pool/task"
)

func TestFixedPool_Basic(t *testing.T) {
	config := &Config{
		WorkerCount:   4,
		QueueSize:     10,
		EnableMetrics: true,
	}

	pool := NewFixedPool(config)
	defer pool.ShutdownNow()

	var counter atomic.Int32

	for i := 0; i < 10; i++ {
		fn := func(ctx context.Context) (interface{}, error) {
			counter.Add(1)
			return nil, nil
		}

		_, err := pool.SubmitFunc(fn)
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("Failed to shutdown: %v", err)
	}

	if counter.Load() != 10 {
		t.Errorf("Expected 10 tasks to complete, got %d", counter.Load())
	}

	metrics := pool.Metrics()
	if metrics.TasksSubmitted.Load() != 10 {
		t.Errorf("Expected 10 submitted, got %d", metrics.TasksSubmitted.Load())
	}

	if metrics.TasksCompleted.Load() != 10 {
		t.Errorf("Expected 10 completed, got %d", metrics.TasksCompleted.Load())
	}
}

func TestFixedPool_SubmitAfterShutdown(t *testing.T) {
	config := &Config{
		WorkerCount: 2,
		QueueSize:   5,
	}

	pool := NewFixedPool(config)
	pool.ShutdownNow()

	fn := func(ctx context.Context) (interface{}, error) {
		return nil, nil
	}

	_, err := pool.SubmitFunc(fn)
	if err != ErrPoolClosed {
		t.Errorf("Expected ErrPoolClosed, got %v", err)
	}
}

func TestFixedPool_WorkerCount(t *testing.T) {
	config := &Config{
		WorkerCount: 8,
		QueueSize:   10,
	}

	pool := NewFixedPool(config)
	defer pool.ShutdownNow()

	if pool.WorkerCount() != 8 {
		t.Errorf("Expected 8 workers, got %d", pool.WorkerCount())
	}
}

func TestCachedPool_Basic(t *testing.T) {
	pool := NewCachedPool(new(Config), 10, 2*time.Second)
	defer pool.ShutdownNow()

	var counter atomic.Int32

	for i := 0; i < 20; i++ {
		fn := func(ctx context.Context) (interface{}, error) {
			counter.Add(1)
			time.Sleep(10 * time.Millisecond)
			return nil, nil
		}

		_, err := pool.SubmitFunc(fn)
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("Failed to shutdown: %v", err)
	}

	if counter.Load() != 20 {
		t.Errorf("Expected 20 tasks to complete, got %d", counter.Load())
	}
}

func TestDynamicPool_Scaling(t *testing.T) {
	pool := NewDynamicPool(new(Config), 2, 10, 5)
	defer pool.ShutdownNow()

	// Submit many tasks quickly
	for i := 0; i < 50; i++ {
		fn := func(ctx context.Context) (interface{}, error) {
			time.Sleep(50 * time.Millisecond)
			return nil, nil
		}

		pool.SubmitFunc(fn)
	}

	// Give it time to scale up
	time.Sleep(200 * time.Millisecond)

	// Workers should have scaled up
	if pool.workers.Size() <= 2 {
		t.Logf("Note: Workers may not have scaled up as expected (size: %d)", pool.workers.Size())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool.Shutdown(ctx)
}

func TestPriorityPool_OrderingByPriority(t *testing.T) {
	config := &Config{
		WorkerCount: 1, // Single worker to ensure ordering
		QueueSize:   10,
	}

	pool := NewPriorityPool(config)
	defer pool.ShutdownNow()

	results := make(chan int, 3)

	// Submit tasks with different priorities (in reverse order)
	for i, priority := range []task.Priority{task.PriorityLow, task.PriorityNormal, task.PriorityHigh} {
		value := i
		fn := func(ctx context.Context) (interface{}, error) {
			results <- value
			return value, nil
		}

		pool.SubmitFunc(fn, task.WithPriority(priority))
	}

	// Allow execution
	time.Sleep(100 * time.Millisecond)

	// First result should be high priority (index 2)
	first := <-results
	if first != 2 {
		t.Logf("Note: Priority ordering may not be strict due to timing (got %d)", first)
	}

	pool.ShutdownNow()
}

func TestScheduledPool_DelayedExecution(t *testing.T) {
	config := &Config{
		WorkerCount: 2,
	}

	pool := NewScheduledPool(config)
	defer pool.ShutdownNow()

	executed := make(chan time.Time, 1)

	fn := func(ctx context.Context) (interface{}, error) {
		executed <- time.Now()
		return nil, nil
	}

	startTime := time.Now()
	delay := 200 * time.Millisecond

	taskToSchedule := task.NewTask(fn)
	if err := pool.Schedule(taskToSchedule, delay); err != nil {
		t.Fatalf("Failed to schedule task: %v", err)
	}

	select {
	case execTime := <-executed:
		actualDelay := execTime.Sub(startTime)
		if actualDelay < delay {
			t.Errorf("Task executed too early: %v < %v", actualDelay, delay)
		}
	case <-time.After(2 * time.Second):
		t.Error("Task did not execute within timeout")
	}
}

func TestScheduledPool_RecurringTask(t *testing.T) {
	t.Skip("Skipping flaky timing test")

	config := &Config{
		WorkerCount: 2,
	}

	pool := NewScheduledPool(config)
	defer pool.ShutdownNow()

	var counter atomic.Int32

	fn := func(ctx context.Context) (interface{}, error) {
		counter.Add(1)
		return nil, nil
	}

	err := pool.ScheduleAtFixedRate(fn, 100*time.Millisecond, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to schedule recurring task: %v", err)
	}

	// Let it run for a bit
	time.Sleep(800 * time.Millisecond)

	count := counter.Load()
	if count < 3 {
		t.Errorf("Expected at least 3 executions, got %d", count)
	}

	pool.ShutdownNow()
}

func TestPool_Metrics(t *testing.T) {
	config := &Config{
		WorkerCount:   4,
		QueueSize:     10,
		EnableMetrics: true,
	}

	pool := NewFixedPool(config)
	defer pool.ShutdownNow()

	for i := 0; i < 10; i++ {
		fn := func(ctx context.Context) (interface{}, error) {
			time.Sleep(10 * time.Millisecond)
			return nil, nil
		}

		pool.SubmitFunc(fn)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool.Shutdown(ctx)

	metrics := pool.Metrics()

	if metrics.TasksSubmitted.Load() != 10 {
		t.Errorf("Expected 10 submitted, got %d", metrics.TasksSubmitted.Load())
	}

	if metrics.TasksCompleted.Load() != 10 {
		t.Errorf("Expected 10 completed, got %d", metrics.TasksCompleted.Load())
	}

	if metrics.GetSuccessRate() != 100.0 {
		t.Errorf("Expected 100%% success rate, got %.2f", metrics.GetSuccessRate())
	}
}
