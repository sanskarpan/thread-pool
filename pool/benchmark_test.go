package pool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// Benchmark fixed pool with varying task counts
func BenchmarkFixedPool_10Tasks(b *testing.B) {
	benchmarkFixedPool(b, 10)
}

func BenchmarkFixedPool_100Tasks(b *testing.B) {
	benchmarkFixedPool(b, 100)
}

func BenchmarkFixedPool_1000Tasks(b *testing.B) {
	benchmarkFixedPool(b, 1000)
}

func benchmarkFixedPool(b *testing.B, taskCount int) {
	config := &Config{
		WorkerCount:   4,
		QueueSize:     taskCount * 2,
		EnableMetrics: false, // Disable for pure performance
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool := NewFixedPool(config)

		var counter atomic.Int32

		for j := 0; j < taskCount; j++ {
			pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
				counter.Add(1)
				return nil, nil
			})
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		pool.Shutdown(ctx)
		cancel()
	}
}

// Benchmark cached pool
func BenchmarkCachedPool_BurstyLoad(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool := NewCachedPool(new(Config), 20, 5*time.Second)

		for j := 0; j < 100; j++ {
			pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
				time.Sleep(1 * time.Millisecond)
				return nil, nil
			})
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pool.Shutdown(ctx)
		cancel()
	}
}

// Benchmark dynamic pool
func BenchmarkDynamicPool_Scaling(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool := NewDynamicPool(new(Config), 2, 10, 100)

		for j := 0; j < 100; j++ {
			pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
				time.Sleep(1 * time.Millisecond)
				return nil, nil
			})
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pool.Shutdown(ctx)
		cancel()
	}
}

// Benchmark priority pool
func BenchmarkPriorityPool_MixedPriorities(b *testing.B) {
	config := &Config{
		WorkerCount: 4,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool := NewPriorityPool(config)

		for j := 0; j < 100; j++ {
			pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
				return nil, nil
			})
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pool.Shutdown(ctx)
		cancel()
	}
}

// Benchmark task submission throughput
func BenchmarkFixedPool_SubmissionThroughput(b *testing.B) {
	config := &Config{
		WorkerCount:   8,
		QueueSize:     10000,
		EnableMetrics: false,
	}

	pool := NewFixedPool(config)
	defer pool.ShutdownNow()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
			return nil, nil
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	pool.Shutdown(ctx)
	cancel()
}

// Benchmark worker pool with different worker counts
func BenchmarkFixedPool_2Workers(b *testing.B) {
	benchmarkWorkerCount(b, 2)
}

func BenchmarkFixedPool_4Workers(b *testing.B) {
	benchmarkWorkerCount(b, 4)
}

func BenchmarkFixedPool_8Workers(b *testing.B) {
	benchmarkWorkerCount(b, 8)
}

func BenchmarkFixedPool_16Workers(b *testing.B) {
	benchmarkWorkerCount(b, 16)
}

func benchmarkWorkerCount(b *testing.B, workerCount int) {
	config := &Config{
		WorkerCount:   workerCount,
		QueueSize:     1000,
		EnableMetrics: false,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool := NewFixedPool(config)

		for j := 0; j < 100; j++ {
			pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
				// Simulate some work
				sum := 0
				for k := 0; k < 1000; k++ {
					sum += k
				}
				return sum, nil
			})
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		pool.Shutdown(ctx)
		cancel()
	}
}

// Benchmark with metrics enabled vs disabled
func BenchmarkFixedPool_WithMetrics(b *testing.B) {
	benchmarkWithMetrics(b, true)
}

func BenchmarkFixedPool_WithoutMetrics(b *testing.B) {
	benchmarkWithMetrics(b, false)
}

func benchmarkWithMetrics(b *testing.B, enableMetrics bool) {
	config := &Config{
		WorkerCount:   4,
		QueueSize:     500,
		EnableMetrics: enableMetrics,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool := NewFixedPool(config)

		for j := 0; j < 100; j++ {
			pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
				return nil, nil
			})
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pool.Shutdown(ctx)
		cancel()
	}
}

// Parallel benchmarks
func BenchmarkFixedPool_Parallel(b *testing.B) {
	config := &Config{
		WorkerCount:   8,
		QueueSize:     10000,
		EnableMetrics: false,
	}

	pool := NewFixedPool(config)
	defer pool.ShutdownNow()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.SubmitFunc(func(ctx context.Context) (interface{}, error) {
				return nil, nil
			})
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	pool.Shutdown(ctx)
	cancel()
}
