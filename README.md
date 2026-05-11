# Custom Thread Pool for Go

A production-ready, feature-rich thread pool implementation in Go with support for multiple execution patterns, comprehensive metrics, and a stunning real-time web visualizer.

![Thread Pool Visualizer](https://img.shields.io/badge/Go-1.25+-00ADD8?style=for-the-badge&logo=go)
![Tests](https://img.shields.io/badge/tests-51%20passing-success?style=for-the-badge)
![Coverage](https://img.shields.io/badge/coverage-85%25-success?style=for-the-badge)

## Features

### 5 Pool Types

1. **Fixed Pool** - Fixed number of workers for predictable resource usage
2. **Cached Pool** - Dynamically creates/removes workers based on demand
3. **Dynamic Pool** - Auto-scales workers between min and max bounds
4. **Priority Pool** - Executes tasks based on priority levels
5. **Scheduled Pool** - Supports delayed and recurring task execution

### Core Features

- **Task Management**
  - Task priorities (Low, Normal, High, Urgent)
  - Task timeouts and cancellation
  - Automatic retry on failure
  - Panic recovery
  - Task metadata

- **Queue Types**
  - Bounded queues (with capacity limits)
  - Unbounded queues (unlimited capacity)
  - Priority queues (heap-based)

- **Worker Management**
  - Configurable worker count
  - Worker health monitoring
  - Dynamic scaling (for applicable pools)
  - Worker statistics

- **Metrics & Monitoring**
  - Task counters (submitted, completed, failed, cancelled)
  - Throughput tracking (tasks/sec)
  - Timing metrics (wait time, execution time)
  - Success/completion rates
  - Queue utilization
  - Worker utilization
  - Historical snapshots

- **Future/Promise Pattern**
  - Async result retrieval
  - Chaining with Then/Catch/Finally
  - Timeout support
  - Context cancellation
  - Combine multiple futures (All, Any, Race)

### Web Visualizer 🎨

A professional, modern web UI with real-time visualization:

- **Glassmorphism Design**: Modern UI with blur effects, animated gradients, and cyberpunk-inspired aesthetics
- **Real-time Updates**: WebSocket-based live data streaming (100ms refresh rate)
- **Worker Grid**: Visual representation of each worker's state (idle/busy) with smooth animations
- **Interactive Charts**:
  - Real-time throughput line chart with gradient fills
  - Worker utilization doughnut chart
- **Dynamic Controls**:
  - Pool type selection (Fixed, Cached, Dynamic, Priority, Scheduled)
  - Worker count adjustment (1-16 workers)
  - Queue size configuration (10-500 tasks)
  - Priority level selection (Low, Normal, High, Urgent)
  - Task duration control (10-2000ms)
  - Batch task submission (1-50 tasks)
- **Live Metrics Dashboard**: Real-time task statistics and performance indicators
- **Queue Visualization**: Animated queue depth with color-coded utilization alerts
- **Responsive Design**: Works on desktop and tablet devices

**Tech Stack**: Vanilla JavaScript, Chart.js, Gorilla WebSocket backend, No external CSS frameworks

## Installation

```bash
go get github.com/sanskarpan/thread-pool
```

## Quick Start

### Web Visualizer (Recommended)

The fastest way to explore all pool types and features:

```bash
# Build the visualizer
go build -o bin/webui ./cmd/webui

# Run it
./bin/webui

# Or specify custom host/port
./bin/webui -host localhost -port 8080

# Optional: load config from YAML + enforce API key auth
./bin/webui -config ./config.yaml -api-key my-secret-key
```

Then open your browser to `http://localhost:8080`

If API key auth is enabled, open the UI with the key in query params:

```text
http://localhost:8080/?api_key=my-secret-key
```

Environment overrides are also supported:

- `THREAD_POOL_HOST`
- `THREAD_POOL_PORT`
- `THREAD_POOL_TRACING`
- `THREAD_POOL_API_KEY`
- `THREAD_POOL_PROTECT_METRICS`
- `THREAD_POOL_PROTECT_SWAGGER`

**Features:**
- Create different pool types on-the-fly
- Submit tasks with varying priorities and durations
- Watch workers process tasks in real-time
- Monitor metrics with live charts
- Experiment with different configurations

### REST API (`/api/v1/*`)

Create a pool first, then submit tasks:

```bash
curl -X POST "http://localhost:8080/api/v1/pool/create" \
  -H "Content-Type: application/json" \
  -d '{"pool_type":"fixed","worker_count":4,"queue_size":100}'
```

Basic submit:

```bash
curl -X POST "http://localhost:8080/api/v1/task/submit" \
  -H "Content-Type: application/json" \
  -d '{"priority":2,"duration_ms":250,"task_count":3}'
```

Advanced submit options:

```bash
# Delay execution (scheduled pool only)
curl -X POST "http://localhost:8080/api/v1/task/submit" \
  -H "Content-Type: application/json" \
  -d '{"duration_ms":150,"task_count":2,"delay_ms":1200}'

# Recurring schedule via cron (scheduled pool only, 6 fields)
curl -X POST "http://localhost:8080/api/v1/task/submit" \
  -H "Content-Type: application/json" \
  -d '{"duration_ms":100,"cron_expression":"*/5 * * * * *"}'

# Chain dependencies in one batch
curl -X POST "http://localhost:8080/api/v1/task/submit" \
  -H "Content-Type: application/json" \
  -d '{"duration_ms":200,"task_count":4,"chain_dependencies":true}'
```

Swagger contract is served at:

```text
http://localhost:8080/api/swagger.json
```

API-key usage (when `-api-key` is configured):

```bash
# Header form
curl -X GET "http://localhost:8080/api/v1/pools" -H "X-API-Key: my-secret-key"

# Bearer token form
curl -X GET "http://localhost:8080/api/v1/pools" -H "Authorization: Bearer my-secret-key"

# Query parameter form
curl -X GET "http://localhost:8080/api/v1/pools?api_key=my-secret-key"
```

Optional protection flags/env:

- `-protect-metrics` / `THREAD_POOL_PROTECT_METRICS=true` to require API key for `/metrics`
- `-protect-swagger` / `THREAD_POOL_PROTECT_SWAGGER=true` to require API key for `/api/swagger.json`
- If these are false (default), `/metrics` and `/api/swagger.json` stay publicly readable

### Basic Fixed Pool

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/thread-pool/pool"
)

func main() {
	// Create a fixed pool with 4 workers
	config := &pool.Config{
		WorkerCount:   4,
		QueueSize:     20,
		EnableMetrics: true,
	}

	threadPool := pool.NewFixedPool(config)
	defer threadPool.ShutdownNow()

	// Submit a task
	future, err := threadPool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
		// Do work here
		return "result", nil
	})

	if err != nil {
		panic(err)
	}

	// Wait for result
	result, err := future.Get()
	if err != nil {
		panic(err)
	}

	fmt.Println("Result:", result)

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	threadPool.Shutdown(ctx)
}
```

## Pool Types

### 1. Fixed Pool

Fixed number of workers for predictable resource usage.

```go
config := &pool.Config{
	WorkerCount:   8,
	QueueSize:     100,
	EnableMetrics: true,
}

pool := pool.NewFixedPool(config)
```

**When to use:**
- Predictable workload
- Need consistent resource usage
- Want to limit concurrency

### 2. Cached Pool

Creates workers as needed, removes idle workers after timeout.

```go
config := &pool.Config{
	QueueSize:     100,
	EnableMetrics: true,
}

pool := pool.NewCachedPool(
	config,
	20,             // max workers
	30*time.Second, // idle timeout
)
```

**When to use:**
- Bursty workloads
- Want automatic worker cleanup
- Unpredictable task arrival rate

### 3. Dynamic Pool

Auto-scales workers between min and max based on queue depth.

```go
config := &pool.Config{
	QueueSize:     100,
	EnableMetrics: true,
}

pool := pool.NewDynamicPool(
	config,
	4,   // min workers
	20,  // max workers
	100, // queue size
)
```

**When to use:**
- Variable workload
- Want automatic scaling
- Need balance between resource usage and performance

### 4. Priority Pool

Processes tasks based on priority (Urgent > High > Normal > Low).

```go
config := &pool.Config{
	WorkerCount: 4,
}

pool := pool.NewPriorityPool(config)

// Submit with priority
pool.SubmitFunc(fn, task.WithPriority(task.PriorityHigh))
```

**When to use:**
- Tasks have different importance levels
- Need to prioritize critical work
- Want control over execution order

### 5. Scheduled Pool

Supports delayed and recurring task execution.

```go
config := &pool.Config{
	WorkerCount: 4,
}

pool := pool.NewScheduledPool(config)

// Delayed execution
task := task.NewTask(fn)
pool.Schedule(task, 5*time.Second)

// Recurring execution
pool.ScheduleAtFixedRate(fn, 1*time.Second, 5*time.Second)
```

**When to use:**
- Need delayed task execution
- Periodic/recurring tasks
- Scheduled job processing

## Task Options

Tasks support various configuration options:

```go
task := task.NewTask(fn,
	task.WithPriority(task.PriorityHigh),      // Set priority
	task.WithTimeout(30*time.Second),          // Task timeout
	task.WithRetry(3),                         // Retry on failure
	task.WithContext(ctx),                     // Custom context
	task.WithMetadata("key", "value"),         // Attach metadata
)
```

## Future Pattern

Tasks return futures for async result handling:

```go
future, err := pool.SubmitFunc(fn)

// Wait for result
result, err := future.Get()

// Wait with timeout
result, err := future.GetWithTimeout(5 * time.Second)

// Wait with context
result, err := future.GetWithContext(ctx)

// Chain operations
future.Then(func(result interface{}) {
	fmt.Println("Success:", result)
}).Catch(func(err error) {
	fmt.Println("Error:", err)
}).Finally(func() {
	fmt.Println("Done")
})

// Combine multiple futures
all := task.All(future1, future2, future3)   // Wait for all
any := task.Any(future1, future2, future3)   // First success
race := task.Race(future1, future2, future3) // First to complete
```

## Metrics

All pools support comprehensive metrics:

```go
metrics := pool.Metrics()

// Task counters
submitted := metrics.TasksSubmitted.Load()
completed := metrics.TasksCompleted.Load()
failed := metrics.TasksFailed.Load()
cancelled := metrics.TasksCancelled.Load()
timedOut := metrics.TasksTimedOut.Load()

// Performance metrics
successRate := metrics.GetSuccessRate()         // Percentage
throughput := metrics.GetThroughput()           // Tasks/sec
avgWaitTime := metrics.GetAvgWaitTime()         // Duration
avgExecTime := metrics.GetAvgExecTime()         // Duration
queueUtil := metrics.GetQueueUtilization()      // Percentage
workerUtil := metrics.GetWorkerUtilization()    // Percentage

// Historical data
snapshots := metrics.GetSnapshots()
```

## Graceful Shutdown

```go
// Graceful shutdown - waits for tasks to complete
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := pool.Shutdown(ctx); err != nil {
	log.Printf("Shutdown error: %v", err)
}

// Immediate shutdown - cancels pending tasks
pool.ShutdownNow()
```

## Examples

Complete examples are available in the `examples/` directory:

```bash
# Fixed pool
go run examples/basic/main.go

# Priority pool
go run examples/priority/main.go

# Scheduled pool
go run examples/scheduled/main.go

# Dynamic pool
go run examples/dynamic/main.go
```

## Testing

```bash
# Run all tests
go test ./... -v

# Run with coverage
go test ./... -cover

# Run with race detector
go test ./... -race

# Run specific package tests
go test ./pool -v
go test ./task -v
```

## Architecture

```
Custom-thread-pool/
├── task/              # Task abstractions and futures
│   ├── task.go        # Task definition, states, execution
│   └── future.go      # Future/Promise implementation
├── queue/             # Queue implementations
│   └── queue.go       # Bounded, unbounded, priority queues
├── worker/            # Worker management
│   └── worker.go      # Worker pool, lifecycle management
├── pool/              # Pool implementations
│   ├── pool.go        # Base interface and config
│   ├── fixed.go       # Fixed thread pool
│   ├── cached.go      # Cached thread pool
│   ├── dynamic.go     # Dynamic thread pool
│   ├── priority.go    # Priority thread pool
│   └── scheduled.go   # Scheduled thread pool
├── metrics/           # Metrics and monitoring
│   └── metrics.go     # Metrics collection and reporting
├── web/               # Web visualizer
│   ├── server/        # HTTP server and WebSocket handler
│   │   ├── server.go      # REST API and state management
│   │   └── websocket.go   # Custom WebSocket implementation
│   └── static/        # Web UI
│       ├── index.html     # Modern glassmorphism UI
│       ├── style.css      # Professional CSS with animations
│       └── app.js         # Interactive JavaScript with Chart.js
├── cmd/               # Command-line tools
│   └── webui/         # Web visualizer entry point
│       └── main.go
└── examples/          # Working examples
    ├── basic/
    ├── priority/
    ├── scheduled/
    └── dynamic/
```

## Performance

All pools are optimized for high performance:

- **Lock-free operations** where possible (atomic operations)
- **Efficient queue implementations** (heap for priority, slice for others)
- **Worker pooling** to avoid goroutine creation overhead
- **Minimal allocations** in hot paths
- **Panic recovery** without affecting other workers

## Best Practices

1. **Choose the Right Pool Type**
   - Fixed: Predictable workloads
   - Cached: Bursty workloads
   - Dynamic: Variable workloads
   - Priority: Mixed importance tasks
   - Scheduled: Time-based execution

2. **Set Appropriate Queue Sizes**
   - Bounded queues prevent memory issues
   - Size based on expected burst capacity
   - Monitor queue utilization metrics

3. **Configure Worker Count**
   - CPU-bound: `runtime.NumCPU()`
   - I/O-bound: Higher than CPU count
   - Monitor worker utilization

4. **Use Context for Cancellation**
   - Always use contexts with timeouts
   - Propagate cancellation to tasks
   - Handle context.Canceled errors

5. **Enable Metrics in Production**
   - Track performance over time
   - Alert on high queue utilization
   - Monitor success rates

6. **Handle Errors Gracefully**
   - Check task results
   - Implement retry logic where appropriate
   - Use panic recovery for safety

## Advanced Usage

### Custom Task Execution

```go
// Create task with options
task := task.NewTask(func(ctx context.Context) (interface{}, error) {
	// Check for cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Do work
	result := doWork()

	return result, nil
}, task.WithTimeout(30*time.Second), task.WithRetry(3))

// Submit to pool
pool.Submit(task)
```

### Monitoring Worker Pool

```go
// Get pool statistics
workerCount := pool.WorkerCount()
activeWorkers := pool.ActiveWorkers()
idleWorkers := pool.IdleWorkers()
queueSize := pool.QueueSize()

fmt.Printf("Workers: %d (active: %d, idle: %d), Queue: %d\n",
	workerCount, activeWorkers, idleWorkers, queueSize)
```

### Batch Processing

```go
// Submit batch of tasks
futures := make([]*task.Future, 100)

for i := 0; i < 100; i++ {
	id := i
	future, _ := pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
		return processItem(id), nil
	})
	futures[i] = future
}

// Wait for all to complete
allDone := task.All(futures...)
results, err := allDone.Get()
```

## Contributing

Contributions are welcome! Please ensure:
- All tests pass (`go test ./...`)
- Code is formatted (`go fmt ./...`)
- No race conditions (`go test -race ./...`)

## License

MIT License - see LICENSE file for details

## Author

Built as a comprehensive, production-ready thread pool implementation demonstrating advanced Go concurrency patterns.
