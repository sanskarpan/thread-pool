// Package server provides the web UI backend for the Thread Pool Visualizer.
//
// This package contains the HTTP server, WebSocket handling, and API endpoints
// for creating, monitoring, and interacting with thread pools in real-time.
//
//	Schemes: http, https
//	Host: localhost:8080
//	BasePath: /api/v1
//	Version: 1.0.0
//
//	Consumes:
//	- application/json
//
//	Produces:
//	- application/json
//
// swagger:meta
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/robfig/cron/v3"
	"github.com/sanskarpan/thread-pool/pool"
	"github.com/sanskarpan/thread-pool/task"
	"github.com/sanskarpan/thread-pool/worker"
	"golang.org/x/time/rate"
)

// WebSocketMessage is a wrapper for all messages sent over WebSocket.
type WebSocketMessage struct {
	Type    string      `json:"type"` // "state" or "task_event"
	Payload interface{} `json:"payload"`
}

// Server handles HTTP and WebSocket connections
type Server struct {
	currentPool     pool.Pool
	poolType        string
	config          *pool.Config
	mu              sync.RWMutex
	shutdownCh      chan struct{}
	shutdownOnce    sync.Once
	wg              sync.WaitGroup
	clients         map[*Client]bool
	broadcast       chan *WebSocketMessage
	taskEventChan   chan pool.TaskEvent
	register        chan *Client
	unregister      chan *Client
	taskCounter     atomic.Uint64
	metricsExporter *MetricsExporter
	metricsRegistry *prometheus.Registry
	apiKey          string
	protectMetrics  bool
	protectSwagger  bool
	auditLogger     *slog.Logger
}

const maxRequestBodyBytes = 1 << 20

const maxTaskBatchSize = 1000

// PoolState represents the current state of the pool
type PoolState struct {
	PoolType      string           `json:"pool_type"`
	WorkerCount   int              `json:"worker_count"`
	ActiveWorkers int              `json:"active_workers"`
	QueueSize     int              `json:"queue_size"`
	Workers       []*WorkerState   `json:"workers"`
	Metrics       *MetricsSnapshot `json:"metrics"`
}

// WorkerState represents a single worker's state
type WorkerState struct {
	ID          int    `json:"id"`
	State       string `json:"state"` // idle, busy, stopped
	CurrentTask string `json:"current_task"`
	TasksCount  int    `json:"tasks_count"`
}

// MetricsSnapshot for JSON serialization
type MetricsSnapshot struct {
	TasksSubmitted    uint64  `json:"tasks_submitted"`
	TasksCompleted    uint64  `json:"tasks_completed"`
	TasksFailed       uint64  `json:"tasks_failed"`
	Throughput        uint64  `json:"throughput"`
	AvgWaitTime       int64   `json:"avg_wait_time_ms"`
	AvgExecTime       int64   `json:"avg_exec_time_ms"`
	SuccessRate       float64 `json:"success_rate"`
	QueueUtilization  float64 `json:"queue_utilization"`
	WorkerUtilization float64 `json:"worker_utilization"`
}

// NewServer creates a new server instance
func NewServer() *Server {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return &Server{
		clients:         make(map[*Client]bool),
		broadcast:       make(chan *WebSocketMessage, 100),
		taskEventChan:   make(chan pool.TaskEvent, 100),
		register:        make(chan *Client),
		unregister:      make(chan *Client),
		shutdownCh:      make(chan struct{}),
		metricsExporter: NewMetricsExporter(reg),
		metricsRegistry: reg,
		auditLogger:     slog.Default(),
	}
}

// ConfigureSecurity configures optional API-key authentication and audit logging.
func (s *Server) ConfigureSecurity(apiKey string, protectMetrics bool, protectSwagger bool, logger *slog.Logger) {
	s.apiKey = apiKey
	s.protectMetrics = protectMetrics
	s.protectSwagger = protectSwagger
	if logger != nil {
		s.auditLogger = logger
	}
}

// Start starts the HTTP server and its background goroutines.
func (s *Server) Start(httpServer *http.Server) error {
	limiter := rate.NewLimiter(10, 20)
	rateLimitMiddleware := func(next http.Handler) http.Handler {
		return rateLimit(next, limiter)
	}
	apiMiddleware := []middleware{requestID(), auditLog(s.auditLogger), apiKeyAuth(s.apiKey), rateLimitMiddleware}
	metricsMiddleware := []middleware{requestID(), auditLog(s.auditLogger)}
	swaggerMiddleware := []middleware{requestID(), auditLog(s.auditLogger)}
	if s.protectMetrics {
		metricsMiddleware = append(metricsMiddleware, apiKeyAuth(s.apiKey))
	}
	if s.protectSwagger {
		swaggerMiddleware = append(swaggerMiddleware, apiKeyAuth(s.apiKey))
	}

	mux := http.NewServeMux()
	mux.Handle("/api/v1/pools", chainMiddlewares(http.HandlerFunc(s.HandleGetPools), apiMiddleware...))
	mux.Handle("/api/v1/pool/create", chainMiddlewares(http.HandlerFunc(s.HandleCreatePool), apiMiddleware...))
	mux.Handle("/api/v1/pool/stop", chainMiddlewares(http.HandlerFunc(s.HandleStopPool), apiMiddleware...))
	mux.Handle("/api/v1/task/submit", chainMiddlewares(http.HandlerFunc(s.HandleSubmitTask), apiMiddleware...))
	mux.Handle("/api/v1/ws", chainMiddlewares(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWs(s, w, r)
	}), apiMiddleware...))
	if s.metricsRegistry == nil {
		s.metricsRegistry = prometheus.NewRegistry()
	}
	mux.Handle("/metrics", chainMiddlewares(promhttp.HandlerFor(s.metricsRegistry, promhttp.HandlerOpts{}), metricsMiddleware...))
	mux.Handle("/api/swagger.json", chainMiddlewares(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./web/static/swagger.json")
	}), swaggerMiddleware...))
	mux.Handle("/", http.FileServer(http.Dir("./web/static")))

	httpServer.Handler = mux

	s.startBackgroundLoops()
	s.metricsExporter.Start()

	return httpServer.ListenAndServe()
}

func (s *Server) startBackgroundLoops() {
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.run()
	}()
	go func() {
		defer s.wg.Done()
		s.stateUpdater()
	}()
}

// Stop shuts down background loops and websocket clients.
func (s *Server) Stop() {
	s.shutdownOnce.Do(func() {
		close(s.shutdownCh)
		s.wg.Wait()
		s.metricsExporter.Stop()
		s.mu.Lock()
		clients := make([]*Client, 0, len(s.clients))
		for client := range s.clients {
			clients = append(clients, client)
		}
		s.clients = make(map[*Client]bool)
		s.mu.Unlock()

		for _, client := range clients {
			client.close()
		}
	})
}

// HandleGetPools returns a list of available pool types.
// swagger:route GET /pools pools getPools
func (s *Server) HandleGetPools(w http.ResponseWriter, r *http.Request) {
	pools := []map[string]interface{}{
		{"id": "fixed", "name": "Fixed Pool", "description": "Fixed number of workers"},
		{"id": "cached", "name": "Cached Pool", "description": "Auto-scaling with idle timeout"},
		{"id": "dynamic", "name": "Dynamic Pool", "description": "Scales between min/max"},
		{"id": "priority", "name": "Priority Pool", "description": "Priority-based execution"},
		{"id": "scheduled", "name": "Scheduled Pool", "description": "Delayed/recurring tasks"},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"pools": pools})
}

// PoolCreateRequest is the model for a new pool creation request.
// swagger:parameters createPool
type PoolCreateRequest struct {
	PoolType    string `json:"pool_type"`
	WorkerCount int    `json:"worker_count"`
	QueueSize   int    `json:"queue_size"`
	MinWorkers  int    `json:"min_workers"`
	MaxWorkers  int    `json:"max_workers"`
}

// HandleCreatePool creates a new thread pool based on the provided configuration.
// swagger:route POST /pool/create pools createPool
func (s *Server) HandleCreatePool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PoolCreateRequest
	if err := decodeStrictJSON(w, r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := validatePoolCreateRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if s.currentPool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		s.currentPool.Shutdown(ctx)
		cancel()
	}

	s.poolType = req.PoolType
	s.config = &pool.Config{
		WorkerCount:   req.WorkerCount,
		QueueSize:     req.QueueSize,
		EnableMetrics: true,
		TaskEventChan: s.taskEventChan,
	}

	switch req.PoolType {
	case "fixed":
		s.currentPool = pool.NewFixedPool(s.config)
	case "cached":
		s.currentPool = pool.NewCachedPool(s.config, req.MaxWorkers, 5*time.Second)
	case "dynamic":
		s.currentPool = pool.NewDynamicPool(s.config, req.MinWorkers, req.MaxWorkers, req.QueueSize)
	case "priority":
		s.currentPool = pool.NewPriorityPool(s.config)
	case "scheduled":
		s.currentPool = pool.NewScheduledPool(s.config)
	}
	s.metricsExporter.SetPool(s.currentPool)
	s.mu.Unlock()

	if s.auditLogger != nil {
		s.auditLogger.Info("pool_created",
			"request_id", requestIDFromContext(r.Context()),
			"pool_type", req.PoolType,
			"worker_count", req.WorkerCount,
			"queue_size", req.QueueSize,
		)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

// HandleStopPool stops the currently running thread pool.
// swagger:route POST /pool/stop pools stopPool
func (s *Server) HandleStopPool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.currentPool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.currentPool.Shutdown(ctx)
		s.currentPool = nil
		s.metricsExporter.SetPool(nil)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
}

// TaskSubmitRequest is the model for a new task submission request.
// swagger:parameters submitTask
type TaskSubmitRequest struct {
	// The priority of the task (0=Low, 1=Normal, 2=High, 3=Urgent).
	Priority int `json:"priority"`
	// Simulated duration for each task in milliseconds.
	Duration int64 `json:"duration_ms"`
	// Whether to simulate task failure.
	ShouldFail bool `json:"should_fail"`
	// Number of tasks to submit in one request.
	TaskCount int `json:"task_count"`
	// Delay in milliseconds before execution (scheduled pool only).
	DelayMS int64 `json:"delay_ms"`
	// Cron expression for recurring runs (scheduled pool only).
	CronExpression string `json:"cron_expression"`
	// Chain batch tasks so each task waits for previous completion.
	ChainDependencies bool `json:"chain_dependencies"`
}

func decodeStrictJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return err
	}

	var extra struct{}
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("request body must contain a single JSON object")
		}
		return err
	}

	return nil
}

func validatePoolCreateRequest(req PoolCreateRequest) error {
	if req.QueueSize < 0 {
		return fmt.Errorf("queue_size must be greater than or equal to zero")
	}

	switch req.PoolType {
	case "fixed", "priority", "scheduled":
		if req.WorkerCount < 1 {
			return fmt.Errorf("worker_count must be at least 1 for %s pools", req.PoolType)
		}
	case "cached":
		if req.MaxWorkers < 1 {
			return fmt.Errorf("max_workers must be at least 1 for cached pools")
		}
	case "dynamic":
		if req.MinWorkers < 1 {
			return fmt.Errorf("min_workers must be at least 1 for dynamic pools")
		}
		if req.MaxWorkers < req.MinWorkers {
			return fmt.Errorf("max_workers must be greater than or equal to min_workers")
		}
	default:
		return fmt.Errorf("unsupported pool_type %q", req.PoolType)
	}

	return nil
}

func validateTaskSubmitRequest(req TaskSubmitRequest) error {
	if req.TaskCount < 1 {
		return fmt.Errorf("task_count must be at least 1")
	}
	if req.TaskCount > maxTaskBatchSize {
		return fmt.Errorf("task_count must not exceed %d", maxTaskBatchSize)
	}
	if req.Priority < int(task.PriorityLow) || req.Priority > int(task.PriorityUrgent) {
		return fmt.Errorf("priority must be between %d and %d", task.PriorityLow, task.PriorityUrgent)
	}
	if req.Duration < 0 {
		return fmt.Errorf("duration_ms must be greater than or equal to zero")
	}
	if req.DelayMS < 0 {
		return fmt.Errorf("delay_ms must be greater than or equal to zero")
	}
	if req.DelayMS > 0 && req.CronExpression != "" {
		return fmt.Errorf("delay_ms and cron_expression cannot be used together")
	}
	if req.CronExpression != "" {
		parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		if _, err := parser.Parse(req.CronExpression); err != nil {
			return fmt.Errorf("invalid cron_expression: %w", err)
		}
	}

	return nil
}

// HandleSubmitTask submits one or more new tasks to the currently active pool via /api/v1/task/submit.
// swagger:route POST /task/submit tasks submitTask
func (s *Server) HandleSubmitTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req TaskSubmitRequest
	if err := decodeStrictJSON(w, r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateTaskSubmitRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	currentPool := s.currentPool
	s.mu.RUnlock()

	if currentPool == nil {
		http.Error(w, "No active pool", http.StatusBadRequest)
		return
	}

	count := req.TaskCount

	priority := task.Priority(req.Priority)
	duration := time.Duration(req.Duration) * time.Millisecond
	isScheduledRequest := req.DelayMS > 0 || req.CronExpression != ""

	if isScheduledRequest {
		scheduledPool, ok := currentPool.(*pool.ScheduledPool)
		if !ok {
			http.Error(w, "delay_ms and cron_expression require a scheduled pool", http.StatusBadRequest)
			return
		}

		for i := 0; i < count; i++ {
			taskID := s.taskCounter.Add(1)
			fn := func(ctx context.Context) (interface{}, error) {
				time.Sleep(duration)
				if req.ShouldFail {
					return nil, &task.PanicError{Recovered: "simulated failure"}
				}
				return taskID, nil
			}

			if req.CronExpression != "" {
				if _, err := scheduledPool.ScheduleWithCron(fn, req.CronExpression); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				continue
			}

			t := task.NewTask(fn, task.WithPriority(priority))
			if err := scheduledPool.Schedule(t, time.Duration(req.DelayMS)*time.Millisecond); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "submitted", "count": count})
		if s.auditLogger != nil {
			s.auditLogger.Info("task_batch_submitted",
				"request_id", requestIDFromContext(r.Context()),
				"pool_type", s.poolType,
				"task_count", count,
				"priority", priority,
				"delay_ms", req.DelayMS,
				"cron_expression", req.CronExpression,
				"chain_dependencies", req.ChainDependencies,
			)
		}
		return
	}

	var previous *task.Future
	for i := 0; i < count; i++ {
		taskID := s.taskCounter.Add(1)
		fn := func(ctx context.Context) (interface{}, error) {
			time.Sleep(duration)
			if req.ShouldFail {
				return nil, &task.PanicError{Recovered: "simulated failure"}
			}
			return taskID, nil
		}

		opts := []task.TaskOption{task.WithPriority(priority)}
		if req.ChainDependencies && previous != nil {
			opts = append(opts, task.After(previous))
		}

		future, err := currentPool.SubmitFunc(fn, opts...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		previous = future
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "submitted", "count": count})
	if s.auditLogger != nil {
		s.auditLogger.Info("task_batch_submitted",
			"request_id", requestIDFromContext(r.Context()),
			"pool_type", s.poolType,
			"task_count", count,
			"priority", priority,
			"delay_ms", req.DelayMS,
			"cron_expression", req.CronExpression,
			"chain_dependencies", req.ChainDependencies,
		)
	}
}

// stateUpdater periodically updates and broadcasts pool state
func (s *Server) stateUpdater() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdownCh:
			return
		case <-ticker.C:
		}

		s.mu.RLock()
		currentPool := s.currentPool
		poolType := s.poolType
		config := s.config
		s.mu.RUnlock()

		if currentPool == nil {
			continue
		}
		state := s.collectPoolState(currentPool, poolType, config)
		select {
		case s.broadcast <- &WebSocketMessage{Type: "state", Payload: state}:
		case <-s.shutdownCh:
			return
		}
	}
}

// collectPoolState collects current pool state
func (s *Server) collectPoolState(p pool.Pool, poolType string, config *pool.Config) *PoolState {
	m := p.Metrics()
	if m == nil {
		return &PoolState{}
	}
	poolWorkers := p.Workers()
	workers := make([]*WorkerState, len(poolWorkers))
	for i, w := range poolWorkers {
		state := "idle"
		if w.State() == worker.StateBusy {
			state = "busy"
		}
		var currentTaskID string
		if task := w.CurrentTask(); task != nil {
			currentTaskID = task.ID()
		}
		workers[i] = &WorkerState{
			ID:          i,
			State:       state,
			TasksCount:  int(w.TasksExecuted()),
			CurrentTask: currentTaskID,
		}
	}

	return &PoolState{
		PoolType:      poolType,
		WorkerCount:   len(poolWorkers),
		ActiveWorkers: int(m.ActiveWorkers.Load()),
		QueueSize:     int(m.QueueSize.Load()),
		Workers:       workers,
		Metrics: &MetricsSnapshot{
			TasksSubmitted:    m.TasksSubmitted.Load(),
			TasksCompleted:    m.TasksCompleted.Load(),
			TasksFailed:       m.TasksFailed.Load(),
			Throughput:        m.GetThroughput(),
			AvgWaitTime:       m.GetAvgWaitTime().Milliseconds(),
			AvgExecTime:       m.GetAvgExecTime().Milliseconds(),
			SuccessRate:       m.GetSuccessRate(),
			QueueUtilization:  m.GetQueueUtilization(),
			WorkerUtilization: m.GetWorkerUtilization(),
		},
	}
}

// run is the main hub for the server, handling WebSocket clients and broadcasting messages.
func (s *Server) run() {
	for {
		select {
		case <-s.shutdownCh:
			return
		case client := <-s.register:
			s.mu.Lock()
			s.clients[client] = true
			s.mu.Unlock()
			s.mu.RLock()
			currentPool := s.currentPool
			poolType := s.poolType
			config := s.config
			s.mu.RUnlock()
			if currentPool != nil {
				state := s.collectPoolState(currentPool, poolType, config)
				client.send <- &WebSocketMessage{Type: "state", Payload: state}
			}
		case client := <-s.unregister:
			s.mu.Lock()
			if _, ok := s.clients[client]; ok {
				delete(s.clients, client)
				client.close()
			}
			s.mu.Unlock()
		case message := <-s.broadcast:
			s.mu.RLock()
			clients := make([]*Client, 0, len(s.clients))
			for client := range s.clients {
				clients = append(clients, client)
			}
			s.mu.RUnlock()
			for _, client := range clients {
				select {
				case client.send <- message:
				default:
					s.metricsExporter.IncrementWSClientsDropped()
					s.metricsExporter.IncrementWSBroadcastDrops()
					slog.Warn("websocket_client_dropped", "component", "server", "request_id", client.requestID)
					client.close()
					s.mu.Lock()
					delete(s.clients, client)
					s.mu.Unlock()
				}
			}
		case event := <-s.taskEventChan:
			message := &WebSocketMessage{
				Type: "task_event",
				Payload: TaskEvent{
					EventType: event.EventType,
					TaskInfo: &TaskInfo{
						ID:       event.TaskID,
						Priority: event.Priority,
						State:    event.State,
						Duration: event.Duration,
					},
				},
			}
			s.mu.RLock()
			clients := make([]*Client, 0, len(s.clients))
			for client := range s.clients {
				clients = append(clients, client)
			}
			s.mu.RUnlock()
			for _, client := range clients {
				select {
				case client.send <- message:
				default:
					s.metricsExporter.IncrementWSClientsDropped()
					s.metricsExporter.IncrementTaskEventDrops()
					slog.Warn("websocket_client_dropped", "component", "server", "request_id", client.requestID)
					client.close()
					s.mu.Lock()
					delete(s.clients, client)
					s.mu.Unlock()
				}
			}
		}
	}
}
