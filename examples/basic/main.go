// Basic fixed pool example
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
	fmt.Println("=== Fixed Thread Pool Example ===")
	fmt.Println()

	// Create a fixed pool with 4 workers
	config := &pool.Config{
		WorkerCount:   4,
		QueueSize:     20,
		EnableMetrics: true,
	}

	threadPool := pool.NewFixedPool(config)

	// Submit 10 tasks
	futures := make([]*task.Future, 10)

	for i := 0; i < 10; i++ {
		taskID := i
		fn := func(ctx context.Context) (interface{}, error) {
			fmt.Printf("Task %d: Starting\n", taskID)
			time.Sleep(time.Duration(100+taskID*10) * time.Millisecond)
			fmt.Printf("Task %d: Completed\n", taskID)
			return fmt.Sprintf("Result-%d", taskID), nil
		}

		future, err := threadPool.SubmitFunc(fn)
		if err != nil {
			log.Fatalf("Failed to submit task: %v", err)
		}

		futures[i] = future
	}

	// Wait for all tasks to complete
	fmt.Println("\nWaiting for all tasks to complete...")

	for i, future := range futures {
		result, err := future.Get()
		if err != nil {
			log.Printf("Task %d failed: %v", i, err)
		} else {
			fmt.Printf("Task %d result: %v\n", i, result)
		}
	}

	// Print metrics
	metrics := threadPool.Metrics()
	fmt.Printf("\n=== Pool Metrics ===\n")
	fmt.Printf("Tasks Submitted:  %d\n", metrics.TasksSubmitted.Load())
	fmt.Printf("Tasks Completed:  %d\n", metrics.TasksCompleted.Load())
	fmt.Printf("Tasks Failed:     %d\n", metrics.TasksFailed.Load())
	fmt.Printf("Success Rate:     %.2f%%\n", metrics.GetSuccessRate())
	fmt.Printf("Avg Exec Time:    %v\n", metrics.GetAvgExecTime())
	fmt.Printf("Throughput:       %d tasks/sec\n", metrics.GetThroughput())

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Println("\nShutting down pool...")
	if err := threadPool.Shutdown(ctx); err != nil {
		log.Fatalf("Failed to shutdown: %v", err)
	}

	fmt.Println("Pool shut down successfully!")
}
