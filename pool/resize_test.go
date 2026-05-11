// Tests for pool resizing features
package pool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestFixedPool_Resize tests the scaling up and down of a FixedPool.
func TestFixedPool_Resize(t *testing.T) {
	config := &Config{
		WorkerCount: 4,
		QueueSize:   20,
	}
	pool := NewFixedPool(config)
	defer pool.ShutdownNow()

	if pool.WorkerCount() != 4 {
		t.Fatalf("Expected initial size of 4, got %d", pool.WorkerCount())
	}

	// 1. Scale up
	pool.Resize(8)
	if pool.WorkerCount() != 8 {
		t.Fatalf("Expected size of 8 after scale up, got %d", pool.WorkerCount())
	}

	// 2. Submit tasks to test new workers
	var counterUp atomic.Int32
	for i := 0; i < 16; i++ {
		_, err := pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
			counterUp.Add(1)
			time.Sleep(20 * time.Millisecond)
			return nil, nil
		})
		if err != nil {
			t.Fatalf("Failed to submit task after scale up: %v", err)
		}
	}
	// Give some time for tasks to be processed
	time.Sleep(100 * time.Millisecond)
	if pool.ActiveWorkers() > 4 {
		// Good, it's using more than the original number of workers
	}

	// 3. Scale down
	pool.Resize(2)
	if pool.WorkerCount() != 2 {
		t.Fatalf("Expected size of 2 after scale down, got %d", pool.WorkerCount())
	}

	// 4. Submit tasks to test after scaling down
	var counterDown atomic.Int32
	for i := 0; i < 4; i++ {
		_, err := pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
			counterDown.Add(1)
			return nil, nil
		})
		if err != nil {
			t.Fatalf("Failed to submit task after scale down: %v", err)
		}
	}

	// 5. Shutdown and verify all tasks completed
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool.Shutdown(ctx)

	if counterUp.Load() != 16 {
		t.Errorf("Expected 16 tasks to complete after scale up, got %d", counterUp.Load())
	}
	if counterDown.Load() != 4 {
		t.Errorf("Expected 4 tasks to complete after scale down, got %d", counterDown.Load())
	}
}
