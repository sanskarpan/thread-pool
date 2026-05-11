// Package pool_test contains tests for the pool package
package pool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/thread-pool/task"
)

// TestScheduledPool_ZeroDelay ensures tasks with zero or negative delay execute immediately.
func TestScheduledPool_ZeroDelay(t *testing.T) {
	pool := NewScheduledPool(DefaultConfig())
	defer pool.ShutdownNow()

	executed := make(chan bool, 1)
	fn := func(ctx context.Context) (interface{}, error) {
		executed <- true
		return nil, nil
	}

	taskToSchedule := task.NewTask(fn)
	// Schedule with zero delay
	if err := pool.Schedule(taskToSchedule, 0); err != nil {
		t.Fatalf("Failed to schedule task with zero delay: %v", err)
	}

	select {
	case <-executed:
		// Success
	case <-time.After(200 * time.Millisecond):
		t.Error("Task with zero delay did not execute immediately")
	}
}

// TestScheduledPool_ShortPeriodRecurring ensures recurring tasks with a short period don't overwhelm the scheduler.
func TestScheduledPool_ShortPeriodRecurring(t *testing.T) {
	pool := NewScheduledPool(DefaultConfig())
	defer pool.ShutdownNow()

	var counter atomic.Int32
	fn := func(ctx context.Context) (interface{}, error) {
		counter.Add(1)
		return nil, nil
	}

	// Schedule a task with a period shorter than the scheduler's tick rate
	err := pool.ScheduleAtFixedRate(fn, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to schedule recurring task: %v", err)
	}

	// Let it run for a duration slightly longer than the scheduler's tick (100ms)
	time.Sleep(250 * time.Millisecond)

	count := counter.Load()
	if count < 2 {
		t.Errorf("Expected at least 2 executions for a short-period recurring task, but got %d", count)
	}
}

// TestScheduledPool_CancelConcurrent cancels a task while the scheduler is running.
func TestScheduledPool_CancelConcurrent(t *testing.T) {
	pool := NewScheduledPool(DefaultConfig())
	defer pool.ShutdownNow()

	executed := make(chan bool, 1)
	fn := func(ctx context.Context) (interface{}, error) {
		executed <- true
		return nil, nil
	}

	taskToSchedule := task.NewTask(fn)
	if err := pool.Schedule(taskToSchedule, 200*time.Millisecond); err != nil {
		t.Fatalf("Failed to schedule task: %v", err)
	}

	// Cancel the task before it has a chance to run
	time.Sleep(50 * time.Millisecond)
	pool.CancelScheduled(taskToSchedule.ID())

	// Wait to see if it executes
	select {
	case <-executed:
		t.Error("Canceled task was executed")
	case <-time.After(300 * time.Millisecond):
		// Success, the task did not execute
	}
}

// TestScheduledPool_Cron schedules a task using a cron expression.
func TestScheduledPool_Cron(t *testing.T) {
	pool := NewScheduledPool(DefaultConfig())
	defer pool.ShutdownNow()

	var counter atomic.Int32
	fn := func(ctx context.Context) (interface{}, error) {
		counter.Add(1)
		return nil, nil
	}

	// Schedule a task to run every second
	_, err := pool.ScheduleWithCron(fn, "* * * * * *")
	if err != nil {
		t.Fatalf("Failed to schedule cron task: %v", err)
	}

	// Let it run for a bit
	time.Sleep(2500 * time.Millisecond)

	count := counter.Load()
	if count < 2 || count > 3 {
		t.Errorf("Expected 2-3 executions for a per-second cron task, but got %d", count)
	}
}
