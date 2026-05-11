package metrics

import (
	"testing"
	"time"
)

func TestNewMetrics(t *testing.T) {
	m := NewMetrics(100)

	if m == nil {
		t.Fatal("NewMetrics should return a non-nil metrics instance")
	}

	if m.QueueCapacity != 100 {
		t.Errorf("Expected queue capacity 100, got %d", m.QueueCapacity)
	}
}

func TestMetrics_Counters(t *testing.T) {
	m := NewMetrics(10)

	m.IncrementSubmitted()
	m.IncrementSubmitted()
	m.IncrementCompleted()
	m.IncrementFailed()
	m.IncrementCancelled()
	m.IncrementTimedOut()
	m.IncrementRetried()

	if m.TasksSubmitted.Load() != 2 {
		t.Errorf("Expected 2 submitted, got %d", m.TasksSubmitted.Load())
	}

	if m.TasksCompleted.Load() != 1 {
		t.Errorf("Expected 1 completed, got %d", m.TasksCompleted.Load())
	}

	if m.TasksFailed.Load() != 1 {
		t.Errorf("Expected 1 failed, got %d", m.TasksFailed.Load())
	}

	if m.TasksCancelled.Load() != 1 {
		t.Errorf("Expected 1 cancelled, got %d", m.TasksCancelled.Load())
	}

	if m.TasksTimedOut.Load() != 1 {
		t.Errorf("Expected 1 timed out, got %d", m.TasksTimedOut.Load())
	}

	if m.TasksRetried.Load() != 1 {
		t.Errorf("Expected 1 retried, got %d", m.TasksRetried.Load())
	}
}

func TestMetrics_WorkerTracking(t *testing.T) {
	m := NewMetrics(10)

	m.SetWorkerCount(8)
	m.SetActiveWorkers(5)

	if m.WorkerCount.Load() != 8 {
		t.Errorf("Expected 8 workers, got %d", m.WorkerCount.Load())
	}

	if m.ActiveWorkers.Load() != 5 {
		t.Errorf("Expected 5 active workers, got %d", m.ActiveWorkers.Load())
	}

	if m.IdleWorkers.Load() != 3 {
		t.Errorf("Expected 3 idle workers, got %d", m.IdleWorkers.Load())
	}
}

func TestMetrics_QueueTracking(t *testing.T) {
	m := NewMetrics(100)

	m.SetQueueSize(50)

	if m.QueueSize.Load() != 50 {
		t.Errorf("Expected queue size 50, got %d", m.QueueSize.Load())
	}
}

func TestMetrics_TimingMetrics(t *testing.T) {
	m := NewMetrics(10)

	m.RecordWaitTime(100 * time.Millisecond)
	m.RecordWaitTime(200 * time.Millisecond)

	m.RecordExecTime(50 * time.Millisecond)
	m.RecordExecTime(150 * time.Millisecond)

	if m.GetMinExecTime() != 50*time.Millisecond {
		t.Errorf("Expected min exec time 50ms, got %v", m.GetMinExecTime())
	}

	if m.GetMaxExecTime() != 150*time.Millisecond {
		t.Errorf("Expected max exec time 150ms, got %v", m.GetMaxExecTime())
	}
}

func TestMetrics_SuccessRate(t *testing.T) {
	m := NewMetrics(10)

	// 7 completed, 3 failed = 70% success rate
	for i := 0; i < 7; i++ {
		m.IncrementCompleted()
	}
	for i := 0; i < 3; i++ {
		m.IncrementFailed()
	}

	rate := m.GetSuccessRate()
	if rate != 70.0 {
		t.Errorf("Expected 70%% success rate, got %.2f%%", rate)
	}
}

func TestMetrics_CompletionRate(t *testing.T) {
	m := NewMetrics(10)

	// 8 submitted, 6 completed = 75% completion rate
	for i := 0; i < 8; i++ {
		m.IncrementSubmitted()
	}
	for i := 0; i < 6; i++ {
		m.IncrementCompleted()
	}

	rate := m.GetCompletionRate()
	if rate != 75.0 {
		t.Errorf("Expected 75%% completion rate, got %.2f%%", rate)
	}
}

func TestMetrics_QueueUtilization(t *testing.T) {
	m := NewMetrics(100)

	m.SetQueueSize(50)

	util := m.GetQueueUtilization()
	if util != 50.0 {
		t.Errorf("Expected 50%% queue utilization, got %.2f%%", util)
	}
}

func TestMetrics_WorkerUtilization(t *testing.T) {
	m := NewMetrics(10)

	m.SetWorkerCount(10)
	m.SetActiveWorkers(8)

	util := m.GetWorkerUtilization()
	if util != 80.0 {
		t.Errorf("Expected 80%% worker utilization, got %.2f%%", util)
	}
}

func TestMetrics_Throughput(t *testing.T) {
	m := NewMetrics(10)

	// Simulate some completions
	for i := 0; i < 100; i++ {
		m.IncrementCompleted()
	}

	// Wait a bit for time to pass
	time.Sleep(50 * time.Millisecond)

	throughput := m.GetThroughput()
	if throughput == 0 {
		t.Error("Expected non-zero throughput")
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	m := NewMetrics(100)

	m.IncrementSubmitted()
	m.IncrementCompleted()
	m.SetQueueSize(25)
	m.SetWorkerCount(4)
	m.SetActiveWorkers(3)

	snapshot := m.GetSnapshot()

	if snapshot.TasksSubmitted != 1 {
		t.Errorf("Expected 1 submitted in snapshot, got %d", snapshot.TasksSubmitted)
	}

	if snapshot.TasksCompleted != 1 {
		t.Errorf("Expected 1 completed in snapshot, got %d", snapshot.TasksCompleted)
	}

	if snapshot.QueueSize != 25 {
		t.Errorf("Expected queue size 25 in snapshot, got %d", snapshot.QueueSize)
	}

	if snapshot.ActiveWorkers != 3 {
		t.Errorf("Expected 3 active workers in snapshot, got %d", snapshot.ActiveWorkers)
	}
}

func TestMetrics_RecordSnapshot(t *testing.T) {
	m := NewMetrics(10)

	m.IncrementSubmitted()
	m.RecordSnapshot()

	m.IncrementCompleted()
	m.RecordSnapshot()

	snapshots := m.GetSnapshots()

	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots, got %d", len(snapshots))
	}
}

func TestMetrics_Reset(t *testing.T) {
	m := NewMetrics(10)

	m.IncrementSubmitted()
	m.IncrementCompleted()
	m.IncrementFailed()

	m.Reset()

	if m.TasksSubmitted.Load() != 0 {
		t.Error("Tasks submitted should be 0 after reset")
	}

	if m.TasksCompleted.Load() != 0 {
		t.Error("Tasks completed should be 0 after reset")
	}

	if m.TasksFailed.Load() != 0 {
		t.Error("Tasks failed should be 0 after reset")
	}
}

func TestMetrics_Uptime(t *testing.T) {
	m := NewMetrics(10)

	time.Sleep(100 * time.Millisecond)

	uptime := m.GetUptime()
	if uptime < 100*time.Millisecond {
		t.Errorf("Expected uptime >= 100ms, got %v", uptime)
	}
}
