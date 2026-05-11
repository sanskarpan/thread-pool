// Package server provides the web UI backend
package server

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sanskarpan/thread-pool/pool"
)

// MetricsExporter handles the translation of internal metrics to Prometheus metrics.
type MetricsExporter struct {
	pool     pool.Pool
	mu       sync.RWMutex
	lastPool pool.Pool

	// Prometheus Metrics
	tasksSubmitted    prometheus.Gauge
	tasksCompleted    prometheus.Gauge
	tasksFailed       prometheus.Gauge
	tasksCancelled    prometheus.Gauge
	tasksTimedOut     prometheus.Gauge
	tasksRetried      prometheus.Gauge
	throughput        prometheus.Gauge
	avgWaitTime       prometheus.Gauge
	avgExecTime       prometheus.Gauge
	queueUtilization  prometheus.Gauge
	workerUtilization prometheus.Gauge
	workerCount       prometheus.Gauge
	activeWorkers     prometheus.Gauge
	wsClientsDropped  prometheus.Counter
	wsBroadcastDrops  prometheus.Counter
	taskEventDrops    prometheus.Counter
	stopOnce          sync.Once
	stopCh            chan struct{}
	wg                sync.WaitGroup
}

// NewMetricsExporter creates a new Prometheus metrics exporter and registers its metrics with the given registry.
func NewMetricsExporter(reg *prometheus.Registry) *MetricsExporter {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	exporter := &MetricsExporter{
		stopCh: make(chan struct{}),
		tasksSubmitted: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_tasks_submitted_total",
			Help: "The total number of submitted tasks.",
		}),
		tasksCompleted: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_tasks_completed_total",
			Help: "The total number of completed tasks.",
		}),
		tasksFailed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_tasks_failed_total",
			Help: "The total number of failed tasks.",
		}),
		tasksCancelled: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_tasks_cancelled_total",
			Help: "The total number of cancelled tasks.",
		}),
		tasksTimedOut: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_tasks_timed_out_total",
			Help: "The total number of timed out tasks.",
		}),
		tasksRetried: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_tasks_retried_total",
			Help: "The total number of retry attempts performed.",
		}),
		throughput: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_throughput_tasks_per_second",
			Help: "The number of tasks processed per second.",
		}),
		avgWaitTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_avg_wait_time_ms",
			Help: "The average time tasks spend waiting in the queue in milliseconds.",
		}),
		avgExecTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_avg_exec_time_ms",
			Help: "The average execution time of tasks in milliseconds.",
		}),
		queueUtilization: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_queue_utilization_percent",
			Help: "The utilization of the task queue as a percentage.",
		}),
		workerUtilization: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_worker_utilization_percent",
			Help: "The utilization of the workers as a percentage.",
		}),
		workerCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_workers_count",
			Help: "The current number of workers in the pool.",
		}),
		activeWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threadpool_active_workers_count",
			Help: "The current number of active (busy) workers.",
		}),
		wsClientsDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "threadpool_ws_clients_dropped_total",
			Help: "The total number of websocket clients dropped due to backpressure.",
		}),
		wsBroadcastDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "threadpool_ws_broadcast_drops_total",
			Help: "The total number of websocket broadcast messages dropped due to backpressure.",
		}),
		taskEventDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "threadpool_task_event_drops_total",
			Help: "The total number of task event messages dropped due to backpressure.",
		}),
	}

	reg.MustRegister(
		exporter.tasksSubmitted,
		exporter.tasksCompleted,
		exporter.tasksFailed,
		exporter.tasksCancelled,
		exporter.tasksTimedOut,
		exporter.tasksRetried,
		exporter.throughput,
		exporter.avgWaitTime,
		exporter.avgExecTime,
		exporter.queueUtilization,
		exporter.workerUtilization,
		exporter.workerCount,
		exporter.activeWorkers,
		exporter.wsClientsDropped,
		exporter.wsBroadcastDrops,
		exporter.taskEventDrops,
	)

	return exporter
}

// SetPool sets the active pool for the exporter to monitor.
func (e *MetricsExporter) SetPool(p pool.Pool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pool = p
}

// Start starts the metrics collection loop.
func (e *MetricsExporter) Start() {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-e.stopCh:
				return
			case <-ticker.C:
				e.updateMetrics()
			}
		}
	}()
}

// Stop stops the metrics collection loop.
func (e *MetricsExporter) Stop() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
	})
	e.wg.Wait()
}

func (e *MetricsExporter) IncrementWSClientsDropped() {
	e.wsClientsDropped.Inc()
}

func (e *MetricsExporter) IncrementWSBroadcastDrops() {
	e.wsBroadcastDrops.Inc()
}

func (e *MetricsExporter) IncrementTaskEventDrops() {
	e.taskEventDrops.Inc()
}

// updateMetrics fetches the latest metrics from the pool and updates Prometheus gauges.
func (e *MetricsExporter) updateMetrics() {
	e.mu.RLock()
	pool := e.pool
	e.mu.RUnlock()

	if pool == nil {
		if e.lastPool != nil {
			// If pool is stopped, reset metrics to 0
			e.tasksSubmitted.Set(0)
			e.tasksCompleted.Set(0)
			e.tasksFailed.Set(0)
			e.tasksCancelled.Set(0)
			e.tasksTimedOut.Set(0)
			e.tasksRetried.Set(0)
			e.throughput.Set(0)
			e.avgWaitTime.Set(0)
			e.avgExecTime.Set(0)
			e.queueUtilization.Set(0)
			e.workerUtilization.Set(0)
			e.workerCount.Set(0)
			e.activeWorkers.Set(0)
			e.lastPool = nil
		}
		return
	}
	e.lastPool = pool

	m := pool.Metrics()
	if m == nil {
		return
	}

	e.tasksSubmitted.Set(float64(m.TasksSubmitted.Load()))
	e.tasksCompleted.Set(float64(m.TasksCompleted.Load()))
	e.tasksFailed.Set(float64(m.TasksFailed.Load()))
	e.tasksCancelled.Set(float64(m.TasksCancelled.Load()))
	e.tasksTimedOut.Set(float64(m.TasksTimedOut.Load()))
	e.tasksRetried.Set(float64(m.TasksRetried.Load()))
	e.throughput.Set(float64(m.GetThroughput()))
	e.avgWaitTime.Set(float64(m.GetAvgWaitTime().Milliseconds()))
	e.avgExecTime.Set(float64(m.GetAvgExecTime().Milliseconds()))
	e.queueUtilization.Set(m.GetQueueUtilization())
	e.workerUtilization.Set(m.GetWorkerUtilization())
	e.workerCount.Set(float64(m.WorkerCount.Load()))
	e.activeWorkers.Set(float64(m.ActiveWorkers.Load()))
}
