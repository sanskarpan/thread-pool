// Package metrics provides monitoring and metrics collection
package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics holds pool statistics
type Metrics struct {
	// Task metrics
	TasksSubmitted atomic.Uint64
	TasksCompleted atomic.Uint64
	TasksFailed    atomic.Uint64
	TasksCancelled atomic.Uint64
	TasksTimedOut  atomic.Uint64
	TasksRetried   atomic.Uint64

	// Worker metrics
	WorkerCount   atomic.Int32
	ActiveWorkers atomic.Int32
	IdleWorkers   atomic.Int32

	// Queue metrics
	QueueSize     atomic.Int32
	QueueCapacity int

	// Timing metrics
	mu            sync.RWMutex
	avgWaitTime   time.Duration
	avgExecTime   time.Duration
	minExecTime   time.Duration
	maxExecTime   time.Duration
	totalWaitTime time.Duration
	totalExecTime time.Duration

	// Throughput
	startTime  time.Time
	throughput atomic.Uint64 // tasks per second

	// Snapshots for reporting
	snapshots  []Snapshot
	snapshotMu sync.RWMutex
}

// Snapshot represents a point-in-time metrics snapshot
type Snapshot struct {
	Timestamp      time.Time
	TasksSubmitted uint64
	TasksCompleted uint64
	TasksFailed    uint64
	QueueSize      int32
	ActiveWorkers  int32
	Throughput     uint64
	AvgWaitTime    time.Duration
	AvgExecTime    time.Duration
}

// NewMetrics creates a new metrics instance
func NewMetrics(queueCapacity int) *Metrics {
	return &Metrics{
		startTime:     time.Now(),
		QueueCapacity: queueCapacity,
		minExecTime:   time.Duration(1<<63 - 1), // max duration
		snapshots:     make([]Snapshot, 0),
	}
}

// IncrementSubmitted increments submitted tasks counter
func (m *Metrics) IncrementSubmitted() {
	m.TasksSubmitted.Add(1)
}

// IncrementCompleted increments completed tasks counter
func (m *Metrics) IncrementCompleted() {
	m.TasksCompleted.Add(1)
	m.updateThroughput()
}

// IncrementFailed increments failed tasks counter
func (m *Metrics) IncrementFailed() {
	m.TasksFailed.Add(1)
}

// IncrementCancelled increments cancelled tasks counter
func (m *Metrics) IncrementCancelled() {
	m.TasksCancelled.Add(1)
}

// IncrementTimedOut increments timed out tasks counter
func (m *Metrics) IncrementTimedOut() {
	m.TasksTimedOut.Add(1)
}

// IncrementRetried increments retried tasks counter
func (m *Metrics) IncrementRetried() {
	m.TasksRetried.Add(1)
}

// SetWorkerCount sets the current worker count
func (m *Metrics) SetWorkerCount(count int) {
	m.WorkerCount.Store(int32(count))
}

// SetActiveWorkers sets the number of active workers
func (m *Metrics) SetActiveWorkers(count int) {
	m.ActiveWorkers.Store(int32(count))
	m.IdleWorkers.Store(m.WorkerCount.Load() - int32(count))
}

// SetQueueSize sets the current queue size
func (m *Metrics) SetQueueSize(size int) {
	m.QueueSize.Store(int32(size))
}

// RecordWaitTime records task wait time
func (m *Metrics) RecordWaitTime(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalWaitTime += d
	completed := m.TasksCompleted.Load()
	if completed > 0 {
		m.avgWaitTime = m.totalWaitTime / time.Duration(completed)
	}
}

// RecordExecTime records task execution time
func (m *Metrics) RecordExecTime(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalExecTime += d
	completed := m.TasksCompleted.Load()
	if completed > 0 {
		m.avgExecTime = m.totalExecTime / time.Duration(completed)
	}

	if d < m.minExecTime {
		m.minExecTime = d
	}
	if d > m.maxExecTime {
		m.maxExecTime = d
	}
}

// updateThroughput updates the throughput metric
func (m *Metrics) updateThroughput() {
	elapsed := time.Since(m.startTime).Seconds()
	if elapsed > 0 {
		completed := m.TasksCompleted.Load()
		m.throughput.Store(uint64(float64(completed) / elapsed))
	}
}

// GetSnapshot returns the current metrics snapshot
func (m *Metrics) GetSnapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return Snapshot{
		Timestamp:      time.Now(),
		TasksSubmitted: m.TasksSubmitted.Load(),
		TasksCompleted: m.TasksCompleted.Load(),
		TasksFailed:    m.TasksFailed.Load(),
		QueueSize:      m.QueueSize.Load(),
		ActiveWorkers:  m.ActiveWorkers.Load(),
		Throughput:     m.throughput.Load(),
		AvgWaitTime:    m.avgWaitTime,
		AvgExecTime:    m.avgExecTime,
	}
}

// RecordSnapshot records a snapshot for historical analysis
func (m *Metrics) RecordSnapshot() {
	snapshot := m.GetSnapshot()

	m.snapshotMu.Lock()
	defer m.snapshotMu.Unlock()

	m.snapshots = append(m.snapshots, snapshot)

	// Keep only last 1000 snapshots
	if len(m.snapshots) > 1000 {
		m.snapshots = m.snapshots[1:]
	}
}

// GetSnapshots returns all recorded snapshots
func (m *Metrics) GetSnapshots() []Snapshot {
	m.snapshotMu.RLock()
	defer m.snapshotMu.RUnlock()

	snapshots := make([]Snapshot, len(m.snapshots))
	copy(snapshots, m.snapshots)
	return snapshots
}

// GetSuccessRate returns the success rate as a percentage
func (m *Metrics) GetSuccessRate() float64 {
	completed := m.TasksCompleted.Load()
	failed := m.TasksFailed.Load()
	total := completed + failed

	if total == 0 {
		return 0
	}

	return float64(completed) / float64(total) * 100
}

// GetCompletionRate returns the completion rate as a percentage
func (m *Metrics) GetCompletionRate() float64 {
	submitted := m.TasksSubmitted.Load()
	completed := m.TasksCompleted.Load()

	if submitted == 0 {
		return 0
	}

	return float64(completed) / float64(submitted) * 100
}

// GetQueueUtilization returns queue utilization as a percentage
func (m *Metrics) GetQueueUtilization() float64 {
	if m.QueueCapacity == 0 {
		return 0
	}

	return float64(m.QueueSize.Load()) / float64(m.QueueCapacity) * 100
}

// GetWorkerUtilization returns worker utilization as a percentage
func (m *Metrics) GetWorkerUtilization() float64 {
	workerCount := m.WorkerCount.Load()
	if workerCount == 0 {
		return 0
	}

	return float64(m.ActiveWorkers.Load()) / float64(workerCount) * 100
}

// GetAvgWaitTime returns average wait time
func (m *Metrics) GetAvgWaitTime() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.avgWaitTime
}

// GetAvgExecTime returns average execution time
func (m *Metrics) GetAvgExecTime() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.avgExecTime
}

// GetMinExecTime returns minimum execution time
func (m *Metrics) GetMinExecTime() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.minExecTime == time.Duration(1<<63-1) {
		return 0
	}
	return m.minExecTime
}

// GetMaxExecTime returns maximum execution time
func (m *Metrics) GetMaxExecTime() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.maxExecTime
}

// GetThroughput returns current throughput
func (m *Metrics) GetThroughput() uint64 {
	return m.throughput.Load()
}

// GetUptime returns the pool uptime
func (m *Metrics) GetUptime() time.Duration {
	return time.Since(m.startTime)
}

// Reset resets all metrics
func (m *Metrics) Reset() {
	m.TasksSubmitted.Store(0)
	m.TasksCompleted.Store(0)
	m.TasksFailed.Store(0)
	m.TasksCancelled.Store(0)
	m.TasksTimedOut.Store(0)
	m.TasksRetried.Store(0)
	m.throughput.Store(0)

	m.mu.Lock()
	m.avgWaitTime = 0
	m.avgExecTime = 0
	m.minExecTime = time.Duration(1<<63 - 1)
	m.maxExecTime = 0
	m.totalWaitTime = 0
	m.totalExecTime = 0
	m.startTime = time.Now()
	m.mu.Unlock()

	m.snapshotMu.Lock()
	m.snapshots = make([]Snapshot, 0)
	m.snapshotMu.Unlock()
}

// String returns a string representation of the metrics
func (m *Metrics) String() string {
	snapshot := m.GetSnapshot()
	return fmt.Sprintf(
		"Pool Metrics:\n  Tasks: %d submitted, %d completed, %d failed\n  Queue: %d tasks\n  Workers: %d active\n  Throughput: %d tasks/sec\n",
		snapshot.TasksSubmitted,
		snapshot.TasksCompleted,
		snapshot.TasksFailed,
		snapshot.QueueSize,
		snapshot.ActiveWorkers,
		snapshot.Throughput,
	)
}
