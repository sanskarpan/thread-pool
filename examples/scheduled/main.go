// Scheduled pool example
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/thread-pool/pool"
	"github.com/sanskarpan/thread-pool/task"
)

func main() {
	fmt.Println("=== Scheduled Thread Pool Example ===")
	fmt.Println()

	config := &pool.Config{
		WorkerCount: 4,
	}

	threadPool := pool.NewScheduledPool(config)

	// Example 1: Delayed task
	fmt.Println("Scheduling a task to run after 1 second...")
	delayedTask := task.NewTask(func(ctx context.Context) (interface{}, error) {
		fmt.Println("⏰ Delayed task executed!")
		return nil, nil
	})

	if err := threadPool.Schedule(delayedTask, 1*time.Second); err != nil {
		log.Fatalf("Failed to schedule task: %v", err)
	}

	// Example 2: Recurring task
	fmt.Println("Scheduling a recurring task (every 500ms)...")
	var counter atomic.Int32

	recurringFn := func(ctx context.Context) (interface{}, error) {
		count := counter.Add(1)
		fmt.Printf("🔄 Recurring task execution #%d\n", count)
		return nil, nil
	}

	if err := threadPool.ScheduleAtFixedRate(recurringFn, 500*time.Millisecond, 500*time.Millisecond); err != nil {
		log.Fatalf("Failed to schedule recurring task: %v", err)
	}

	// Let it run for a while
	fmt.Println()
	fmt.Println("Letting tasks run for 3 seconds...")
	fmt.Println()
	time.Sleep(3 * time.Second)

	// Shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	fmt.Println("\nShutting down pool...")
	if err := threadPool.Shutdown(ctx); err != nil {
		log.Fatalf("Failed to shutdown: %v", err)
	}

	fmt.Printf("\nRecurring task executed %d times\n", counter.Load())
	fmt.Println("Done!")
}
