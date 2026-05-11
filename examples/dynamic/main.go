// Dynamic pool example
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sanskarpan/thread-pool/pool"
)

func main() {
	fmt.Println("=== Dynamic Thread Pool Example ===")
	fmt.Println()

	// Create a dynamic pool that scales between 2 and 10 workers
	threadPool := pool.NewDynamicPool(new(pool.Config), 2, 10, 50)

	fmt.Println("Initial workers: 2")
	fmt.Println("Max workers: 10")
	fmt.Println()
	fmt.Println("Submitting 50 tasks...")
	fmt.Println()

	// Submit a burst of tasks
	for i := 0; i < 50; i++ {
		taskID := i
		fn := func(ctx context.Context) (interface{}, error) {
			fmt.Printf("Task %d executing\n", taskID)
			time.Sleep(200 * time.Millisecond)
			return nil, nil
		}

		if _, err := threadPool.SubmitFunc(fn); err != nil {
			log.Printf("Failed to submit task %d: %v", i, err)
		}
	}

	// Monitor the pool scaling
	fmt.Println()
	fmt.Println("Monitoring pool scaling:")
	fmt.Println()
	for i := 0; i < 10; i++ {
		metrics := threadPool.Metrics()
		workerCount := metrics.WorkerCount.Load()
		activeWorkers := metrics.ActiveWorkers.Load()
		queueSize := metrics.QueueSize.Load()

		fmt.Printf("Time %ds: Workers=%d, Active=%d, Queue=%d\n",
			i, workerCount, activeWorkers, queueSize)

		time.Sleep(500 * time.Millisecond)
	}

	// Shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("\nShutting down pool...")
	if err := threadPool.Shutdown(ctx); err != nil {
		log.Fatalf("Failed to shutdown: %v", err)
	}

	// Print final metrics
	metrics := threadPool.Metrics()
	fmt.Printf("\n=== Final Metrics ===\n")
	fmt.Printf("Tasks Completed: %d\n", metrics.TasksCompleted.Load())
	fmt.Printf("Success Rate:    %.2f%%\n", metrics.GetSuccessRate())

	fmt.Println("Done!")
}
