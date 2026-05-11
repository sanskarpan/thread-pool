// Tests for task dependency features
package pool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/thread-pool/task"
)

// TestTaskDependencies ensures tasks execute only after their dependencies are met.
func TestTaskDependencies(t *testing.T) {
	pool := NewFixedPool(DefaultConfig())
	defer pool.ShutdownNow()

	results := make(chan int, 3)
	var wg sync.WaitGroup
	wg.Add(3)

	// Task 1 (no dependencies)
	f1, err := pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		results <- 1
		return 1, nil
	})
	if err != nil {
		t.Fatalf("Failed to submit task 1: %v", err)
	}

	// Task 2 (depends on Task 1)
	f2, err := pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		results <- 2
		return 2, nil
	}, task.After(f1))
	if err != nil {
		t.Fatalf("Failed to submit task 2: %v", err)
	}

	// Task 3 (depends on Task 2)
	_, err = pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
		defer wg.Done()
		results <- 3
		return 3, nil
	}, task.After(f2))
	if err != nil {
		t.Fatalf("Failed to submit task 3: %v", err)
	}

	// Wait for all tasks to complete
	wg.Wait()
	close(results)

	// Verify execution order
	expectedOrder := []int{1, 2, 3}
	order := 0
	for result := range results {
		if order >= len(expectedOrder) || result != expectedOrder[order] {
			t.Errorf("Expected result %d, but got %d. Execution order is wrong.", expectedOrder[order], result)
			break
		}
		order++
	}
}
