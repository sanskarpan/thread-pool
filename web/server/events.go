// Package server provides the web UI backend
package server

// TaskEvent represents a single lifecycle event for a task.
type TaskEvent struct {
	EventType string    `json:"event_type"` // e.g., "submitted", "completed", "failed"
	TaskInfo  *TaskInfo `json:"task_info"`
}

// TaskInfo represents task information for UI events.
type TaskInfo struct {
	ID       string `json:"id"`
	Priority int    `json:"priority"`
	State    string `json:"state"`
	Duration int64  `json:"duration_ms"`
}
