// Interactive demo application
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/thread-pool/metrics"
	"github.com/sanskarpan/thread-pool/pool"
	"github.com/sanskarpan/thread-pool/task"
)

func main() {
	fmt.Println("╔════════════════════════════════════════╗")
	fmt.Println("║   Custom Thread Pool - Demo Suite     ║")
	fmt.Println("╚════════════════════════════════════════╝")
	fmt.Println()

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "fixed":
			demoFixedPool()
		case "cached":
			demoCachedPool()
		case "dynamic":
			demoDynamicPool()
		case "priority":
			demoPriorityPool()
		case "scheduled":
			demoScheduledPool()
		case "all":
			runAllDemos()
		default:
			showUsage()
		}
	} else {
		runAllDemos()
	}
}

func showUsage() {
	fmt.Println("Usage: go run cmd/demo/main.go [demo-type]")
	fmt.Println()
	fmt.Println("Available demos:")
	fmt.Println("  fixed      - Fixed thread pool demo")
	fmt.Println("  cached     - Cached thread pool demo")
	fmt.Println("  dynamic    - Dynamic thread pool demo")
	fmt.Println("  priority   - Priority thread pool demo")
	fmt.Println("  scheduled  - Scheduled thread pool demo")
	fmt.Println("  all        - Run all demos (default)")
	fmt.Println()
}

func runAllDemos() {
	demoFixedPool()
	fmt.Println()
	demoCachedPool()
	fmt.Println()
	demoDynamicPool()
	fmt.Println()
	demoPriorityPool()
	fmt.Println()
	demoScheduledPool()
}

func demoFixedPool() {
	fmt.Println("═══ 1. Fixed Thread Pool Demo ═══")
	fmt.Println("Creating a fixed pool with 4 workers...")

	config := &pool.Config{
		WorkerCount:   4,
		QueueSize:     20,
		EnableMetrics: true,
	}

	threadPool := pool.NewFixedPool(config)
	defer threadPool.ShutdownNow()

	// Submit CPU-intensive tasks
	taskCount := 12
	fmt.Printf("Submitting %d CPU-intensive tasks...\n", taskCount)

	var completed atomic.Int32
	futures := make([]*task.Future, taskCount)

	for i := 0; i < taskCount; i++ {
		taskID := i
		fn := func(ctx context.Context) (interface{}, error) {
			// Simulate CPU work
			start := time.Now()
			sum := 0
			for j := 0; j < 10000000; j++ {
				sum += j
			}
			duration := time.Since(start)

			completed.Add(1)
			fmt.Printf("  Task %02d: Completed in %v (sum=%d)\n", taskID, duration, sum%1000)
			return sum, nil
		}

		future, err := threadPool.SubmitFunc(fn)
		if err != nil {
			log.Printf("Failed to submit task %d: %v", i, err)
			continue
		}
		futures[i] = future
	}

	// Wait for completion
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("\nWaiting for all tasks to complete...")
	threadPool.AwaitTermination(ctx)

	// Display metrics
	displayMetrics(threadPool.Metrics(), "Fixed Pool")

	threadPool.Shutdown(ctx)
}

func demoCachedPool() {
	fmt.Println("═══ 2. Cached Thread Pool Demo ═══")
	fmt.Println("Creating a cached pool (max 10 workers, 2s idle timeout)...")

	threadPool := pool.NewCachedPool(new(pool.Config), 10, 2*time.Second)
	defer threadPool.ShutdownNow()

	// Simulate bursty workload
	fmt.Println("Simulating bursty workload...")

	for burst := 1; burst <= 3; burst++ {
		fmt.Printf("\nBurst #%d: Submitting 5 tasks\n", burst)

		for i := 0; i < 5; i++ {
			taskID := (burst-1)*5 + i
			fn := func(ctx context.Context) (interface{}, error) {
				sleepTime := time.Duration(50+rand.Intn(100)) * time.Millisecond
				time.Sleep(sleepTime)
				fmt.Printf("  Task %02d: Completed after %v\n", taskID, sleepTime)
				return nil, nil
			}

			threadPool.SubmitFunc(fn)
		}

		// Wait between bursts
		if burst < 3 {
			fmt.Println("  Waiting 1 second...")
			time.Sleep(1 * time.Second)
		}
	}

	// Wait for completion
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Println("\nWaiting for completion...")
	threadPool.AwaitTermination(ctx)

	displayMetrics(threadPool.Metrics(), "Cached Pool")

	threadPool.Shutdown(ctx)
}

func demoDynamicPool() {
	fmt.Println("═══ 3. Dynamic Thread Pool Demo ═══")
	fmt.Println("Creating a dynamic pool (2-8 workers, queue: 30)...")

	threadPool := pool.NewDynamicPool(new(pool.Config), 2, 8, 30)
	defer threadPool.ShutdownNow()

	// Submit variable workload
	fmt.Println("Submitting 40 tasks with varying durations...")

	var wg atomic.Int32
	for i := 0; i < 40; i++ {
		fn := func(ctx context.Context) (interface{}, error) {
			duration := time.Duration(20+rand.Intn(80)) * time.Millisecond
			time.Sleep(duration)
			count := wg.Add(1)
			if count%10 == 0 {
				fmt.Printf("  Progress: %d/40 tasks completed\n", count)
			}
			return nil, nil
		}

		threadPool.SubmitFunc(fn)
	}

	// Monitor scaling
	fmt.Println("\nMonitoring auto-scaling:")
	for i := 0; i < 8; i++ {
		metrics := threadPool.Metrics()
		workers := metrics.WorkerCount.Load()
		active := metrics.ActiveWorkers.Load()
		queue := metrics.QueueSize.Load()

		fmt.Printf("  %ds: Workers=%d, Active=%d, Queue=%d\n", i, workers, active, queue)
		time.Sleep(500 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	threadPool.AwaitTermination(ctx)
	displayMetrics(threadPool.Metrics(), "Dynamic Pool")

	threadPool.Shutdown(ctx)
}

func demoPriorityPool() {
	fmt.Println("═══ 4. Priority Thread Pool Demo ═══")
	fmt.Println("Creating a priority pool with 2 workers...")

	config := &pool.Config{
		WorkerCount:   2,
		EnableMetrics: true,
	}

	threadPool := pool.NewPriorityPool(config)
	defer threadPool.ShutdownNow()

	// Submit tasks with different priorities
	priorities := []struct {
		level task.Priority
		name  string
	}{
		{task.PriorityLow, "Low"},
		{task.PriorityNormal, "Normal"},
		{task.PriorityLow, "Low"},
		{task.PriorityHigh, "High"},
		{task.PriorityUrgent, "Urgent"},
		{task.PriorityNormal, "Normal"},
		{task.PriorityHigh, "High"},
		{task.PriorityLow, "Low"},
	}

	fmt.Println("Submitting tasks with priorities:")
	for i, p := range priorities {
		fmt.Printf("  Task %d: Priority %s\n", i, p.name)
	}

	fmt.Println("\nExecution order (priority-based):")
	for i, p := range priorities {
		taskID := i
		priorityName := p.name
		fn := func(ctx context.Context) (interface{}, error) {
			time.Sleep(50 * time.Millisecond)
			fmt.Printf("  ▶ Task %d executed (Priority: %s)\n", taskID, priorityName)
			return nil, nil
		}

		threadPool.SubmitFunc(fn, task.WithPriority(p.level))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	threadPool.AwaitTermination(ctx)
	displayMetrics(threadPool.Metrics(), "Priority Pool")

	threadPool.Shutdown(ctx)
}

func demoScheduledPool() {
	fmt.Println("═══ 5. Scheduled Thread Pool Demo ═══")
	fmt.Println("Creating a scheduled pool with 3 workers...")

	config := &pool.Config{
		WorkerCount: 3,
	}

	threadPool := pool.NewScheduledPool(config)
	defer threadPool.ShutdownNow()

	fmt.Println("\nScheduling tasks:")

	// Delayed tasks
	delays := []time.Duration{500 * time.Millisecond, 1 * time.Second, 1500 * time.Millisecond}

	for i, delay := range delays {
		taskID := i
		fn := func(ctx context.Context) (interface{}, error) {
			fmt.Printf("  ⏰ Delayed task %d executed (scheduled for %v delay)\n", taskID, delay)
			return nil, nil
		}

		t := task.NewTask(fn)
		threadPool.Schedule(t, delay)
		fmt.Printf("  Scheduled task %d to run after %v\n", i, delay)
	}

	// Recurring task
	var recurCount atomic.Int32
	recurFn := func(ctx context.Context) (interface{}, error) {
		count := recurCount.Add(1)
		fmt.Printf("  🔄 Recurring task execution #%d\n", count)
		return nil, nil
	}

	fmt.Println("  Scheduled recurring task (every 400ms)")
	threadPool.ScheduleAtFixedRate(recurFn, 200*time.Millisecond, 400*time.Millisecond)

	// Let it run
	fmt.Println("\nLetting tasks execute for 2.5 seconds...")
	time.Sleep(2500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	threadPool.Shutdown(ctx)

	fmt.Printf("\nRecurring task executed %d times\n", recurCount.Load())
}

func displayMetrics(m *metrics.Metrics, poolName string) {
	if m == nil {
		return
	}

	fmt.Printf("\n╔══ %s Metrics ══╗\n", poolName)
	fmt.Printf("║ Tasks:\n")
	fmt.Printf("║   Submitted:  %d\n", m.TasksSubmitted.Load())
	fmt.Printf("║   Completed:  %d\n", m.TasksCompleted.Load())
	fmt.Printf("║   Failed:     %d\n", m.TasksFailed.Load())
	fmt.Printf("║   Cancelled:  %d\n", m.TasksCancelled.Load())
	fmt.Printf("║\n")
	fmt.Printf("║ Performance:\n")
	fmt.Printf("║   Success Rate:    %.2f%%\n", m.GetSuccessRate())
	fmt.Printf("║   Throughput:      %d tasks/sec\n", m.GetThroughput())
	fmt.Printf("║   Avg Wait Time:   %v\n", m.GetAvgWaitTime())
	fmt.Printf("║   Avg Exec Time:   %v\n", m.GetAvgExecTime())
	if m.GetMaxExecTime() > 0 {
		fmt.Printf("║   Max Exec Time:   %v\n", m.GetMaxExecTime())
	}
	fmt.Printf("║\n")
	fmt.Printf("║ Utilization:\n")
	fmt.Printf("║   Worker Count:    %d\n", m.WorkerCount.Load())
	fmt.Printf("║   Active Workers:  %d\n", m.ActiveWorkers.Load())
	fmt.Printf("║   Worker Util:     %.2f%%\n", m.GetWorkerUtilization())
	if m.QueueCapacity > 0 {
		fmt.Printf("║   Queue Util:      %.2f%%\n", m.GetQueueUtilization())
	}
	fmt.Printf("╚════════════════════════════════════════╝\n")
}
