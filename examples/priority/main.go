// Priority pool example
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sanskarpan/thread-pool/pool"
	"github.com/sanskarpan/thread-pool/task"
)

func main() {
	fmt.Println("=== Priority Thread Pool Example ===")
	fmt.Println()

	// Create a priority pool with 2 workers (to see priority in action)
	config := &pool.Config{
		WorkerCount:   2,
		EnableMetrics: true,
	}

	threadPool := pool.NewPriorityPool(config)

	// Submit tasks with different priorities
	priorities := []task.Priority{
		task.PriorityLow,
		task.PriorityNormal,
		task.PriorityHigh,
		task.PriorityUrgent,
		task.PriorityLow,
		task.PriorityHigh,
	}

	fmt.Println("Submitting tasks with priorities:")
	for i, priority := range priorities {
		taskID := i
		fn := func(ctx context.Context) (interface{}, error) {
			fmt.Printf("Task %d (Priority: %d): Executing\n", taskID, priority)
			time.Sleep(100 * time.Millisecond)
			return nil, nil
		}

		_, err := threadPool.SubmitFunc(fn, task.WithPriority(priority))
		if err != nil {
			log.Fatalf("Failed to submit task: %v", err)
		}

		fmt.Printf("  Task %d submitted with priority %d\n", i, priority)
	}

	// Wait for all tasks to complete
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	time.Sleep(1 * time.Second)

	fmt.Println("\nShutting down pool...")
	if err := threadPool.Shutdown(ctx); err != nil {
		log.Fatalf("Failed to shutdown: %v", err)
	}

	// Print metrics
	metrics := threadPool.Metrics()
	fmt.Printf("\n=== Pool Metrics ===\n")
	fmt.Printf("Tasks Completed: %d\n", metrics.TasksCompleted.Load())
	fmt.Printf("Success Rate:    %.2f%%\n", metrics.GetSuccessRate())

	fmt.Println("Done!")
}
