// Package server_test provides E2E tests for the web server
package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sanskarpan/thread-pool/pool"
)

// newTestServer creates a new server instance for testing with a specific registry.
func newTestServer(reg *prometheus.Registry) *Server {
	return &Server{
		clients:         make(map[*Client]bool),
		broadcast:       make(chan *WebSocketMessage, 100),
		taskEventChan:   make(chan pool.TaskEvent, 100),
		register:        make(chan *Client),
		unregister:      make(chan *Client),
		shutdownCh:      make(chan struct{}),
		metricsExporter: NewMetricsExporter(reg),
		metricsRegistry: reg,
	}
}

// TestServer_E2E performs an end-to-end test of the web server
func TestServer_E2E(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := newTestServer(reg)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/pools", s.HandleGetPools)
	mux.HandleFunc("/api/v1/pool/create", s.HandleCreatePool)
	mux.HandleFunc("/api/v1/task/submit", s.HandleSubmitTask)
	mux.HandleFunc("/api/v1/ws", func(w http.ResponseWriter, r *http.Request) {
		ServeWs(s, w, r)
	})
	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	// Start server dependencies
	s.startBackgroundLoops()
	defer s.Stop()

	t.Run("API_GetPools", func(t *testing.T) {
		resp, err := http.Get(testServer.URL + "/api/v1/pools")
		if err != nil {
			t.Fatalf("Failed to get pools: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status OK; got %v", resp.Status)
		}
	})

	t.Run("API_CreatePool", func(t *testing.T) {
		body := `{"pool_type": "fixed", "worker_count": 2, "queue_size": 10}`
		resp, err := http.Post(testServer.URL+"/api/v1/pool/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status OK; got %v", resp.Status)
		}
	})

	t.Run("WebSocket_E2E", func(t *testing.T) {
		wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/api/v1/ws"
		ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("Failed to dial websocket: %v", err)
		}
		defer ws.Close()

		var msg WebSocketMessage
		err = ws.ReadJSON(&msg)
		if err != nil {
			t.Fatalf("Failed to read initial message from websocket: %v", err)
		}

		if msg.Type != "state" {
			t.Fatalf("Expected initial message of type 'state', got '%s'", msg.Type)
		}

		// Unmarshal the payload into a PoolState
		var initialState PoolState
		payloadBytes, _ := json.Marshal(msg.Payload)
		json.Unmarshal(payloadBytes, &initialState)

		if initialState.PoolType != "fixed" || initialState.WorkerCount != 2 {
			t.Errorf("Unexpected initial state: %+v", initialState)
		}

		body := `{"duration_ms": 100, "task_count": 1}`
		resp, err := http.Post(testServer.URL+"/api/v1/task/submit", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}
		resp.Body.Close()

		var updatedState PoolState
		for i := 0; i < 10; i++ { // Increased loop to allow for event messages
			ws.SetReadDeadline(time.Now().Add(2 * time.Second))
			var nextMsg WebSocketMessage
			err = ws.ReadJSON(&nextMsg)
			if err != nil {
				t.Fatalf("Failed to read updated state from websocket: %v", err)
			}
			if nextMsg.Type == "state" {
				payloadBytes, _ = json.Marshal(nextMsg.Payload)
				json.Unmarshal(payloadBytes, &updatedState)
				if updatedState.Metrics.TasksSubmitted > initialState.Metrics.TasksSubmitted {
					break
				}
			}
		}

		if updatedState.Metrics.TasksSubmitted != 1 {
			t.Errorf("Expected TasksSubmitted to be 1; got %d", updatedState.Metrics.TasksSubmitted)
		}
	})
}

// TestServer_MetricsEndpoint verifies the /metrics endpoint.
func TestServer_MetricsEndpoint(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := newTestServer(reg)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/pool/create", s.HandleCreatePool)
	mux.HandleFunc("/api/v1/task/submit", s.HandleSubmitTask)
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	// Start server dependencies
	s.startBackgroundLoops()
	s.metricsExporter.Start()
	defer s.Stop()

	createBody := `{"pool_type": "fixed", "worker_count": 1, "queue_size": 10}`
	resp, err := http.Post(testServer.URL+"/api/v1/pool/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	resp.Body.Close()

	submitBody := `{"duration_ms": 10, "task_count": 1}`
	resp, err = http.Post(testServer.URL+"/api/v1/task/submit", "application/json", strings.NewReader(submitBody))
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}
	resp.Body.Close()

	// Allow time for processing and metrics update. The exporter updates every 2s.
	time.Sleep(2100 * time.Millisecond)

	// 3. Scrape the /metrics endpoint
	resp, err = http.Get(testServer.URL + "/metrics")
	if err != nil {
		t.Fatalf("Failed to scrape /metrics endpoint: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "threadpool_tasks_submitted_total 1") {
		t.Errorf("Expected submitted tasks to be 1, but metric was not found or incorrect in:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "threadpool_tasks_completed_total 1") {
		t.Errorf("Expected completed tasks to be 1, but metric was not found or incorrect in:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "threadpool_workers_count 1") {
		t.Errorf("Expected worker count to be 1, but metric was not found or incorrect in:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "threadpool_tasks_cancelled_total") {
		t.Errorf("Expected cancelled task metric to be registered, body:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "threadpool_tasks_timed_out_total") {
		t.Errorf("Expected timed out task metric to be registered, body:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "threadpool_tasks_retried_total") {
		t.Errorf("Expected retried task metric to be registered, body:\n%s", bodyStr)
	}
}

// TestServer_SwaggerEndpoint verifies that the swagger.json file is served correctly.
func TestServer_SwaggerEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/swagger.json", func(w http.ResponseWriter, r *http.Request) {
		// In a test environment, the file path is relative to the package dir
		http.ServeFile(w, r, "../../web/static/swagger.json")
	})
	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	resp, err := http.Get(testServer.URL + "/api/swagger.json")
	if err != nil {
		t.Fatalf("Failed to get swagger.json: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK; got %v", resp.Status)
	}

	// Check if we got some JSON content
	body, _ := io.ReadAll(resp.Body)
	if len(body) < 10 || !strings.Contains(string(body), "swagger") {
		t.Errorf("Response body does not look like a swagger file: %s", string(body))
	}
}

func TestServer_EndpointProtectionToggle(t *testing.T) {
	const apiKey = "secret"

	t.Run("metrics unprotected by default", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.Handle("/metrics", chainMiddlewares(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
			auditLog(nil),
		))

		ts := httptest.NewServer(mux)
		defer ts.Close()

		resp, err := http.Get(ts.URL + "/metrics")
		if err != nil {
			t.Fatalf("failed to call /metrics: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", resp.StatusCode)
		}
	})

	t.Run("metrics protected when enabled", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.Handle("/metrics", chainMiddlewares(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
			auditLog(nil),
			apiKeyAuth(apiKey),
		))

		ts := httptest.NewServer(mux)
		defer ts.Close()

		resp, err := http.Get(ts.URL + "/metrics")
		if err != nil {
			t.Fatalf("failed to call /metrics without key: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected status 401 without key, got %d", resp.StatusCode)
		}

		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/metrics", nil)
		req.Header.Set("X-API-Key", apiKey)
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to call /metrics with key: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 with key, got %d", resp.StatusCode)
		}
	})

	t.Run("swagger protected when enabled", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.Handle("/api/swagger.json", chainMiddlewares(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
			auditLog(nil),
			apiKeyAuth(apiKey),
		))

		ts := httptest.NewServer(mux)
		defer ts.Close()

		resp, err := http.Get(ts.URL + "/api/swagger.json")
		if err != nil {
			t.Fatalf("failed to call /api/swagger.json without key: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected status 401 without key, got %d", resp.StatusCode)
		}

		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/swagger.json", nil)
		req.Header.Set("X-API-Key", apiKey)
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to call /api/swagger.json with key: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 with key, got %d", resp.StatusCode)
		}
	})
}

func TestHandleCreatePool_Validation(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := newTestServer(reg)

	tests := []struct {
		name string
		body string
	}{
		{name: "unknown pool type", body: `{"pool_type":"bogus","worker_count":1,"queue_size":10}`},
		{name: "negative worker count", body: `{"pool_type":"fixed","worker_count":-1,"queue_size":10}`},
		{name: "cached max workers zero", body: `{"pool_type":"cached","worker_count":2,"queue_size":10,"max_workers":0}`},
		{name: "dynamic invalid min max", body: `{"pool_type":"dynamic","min_workers":3,"max_workers":2,"queue_size":10}`},
		{name: "unknown field rejected", body: `{"pool_type":"fixed","worker_count":1,"queue_size":10,"extra":true}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/pool/create", strings.NewReader(tt.body))
			rr := httptest.NewRecorder()
			s.HandleCreatePool(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d with body %s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestHandleSubmitTask_Validation(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := newTestServer(reg)
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/pool/create", strings.NewReader(`{"pool_type":"fixed","worker_count":1,"queue_size":10}`))
	createRR := httptest.NewRecorder()
	s.HandleCreatePool(createRR, createReq)
	if createRR.Code != http.StatusOK {
		t.Fatalf("expected pool creation to succeed, got %d: %s", createRR.Code, createRR.Body.String())
	}

	tests := []struct {
		name string
		body string
	}{
		{name: "task count zero", body: `{"duration_ms":10,"task_count":0}`},
		{name: "task count too large", body: `{"duration_ms":10,"task_count":1001}`},
		{name: "invalid priority", body: `{"priority":9,"duration_ms":10,"task_count":1}`},
		{name: "delay and cron together", body: `{"duration_ms":10,"task_count":1,"delay_ms":5,"cron_expression":"*/5 * * * * *"}`},
		{name: "invalid cron", body: `{"duration_ms":10,"task_count":1,"cron_expression":"not-a-cron"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/task/submit", strings.NewReader(tt.body))
			rr := httptest.NewRecorder()
			s.HandleSubmitTask(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d with body %s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestServer_StopStopsBackgroundLoops(t *testing.T) {
	s := newTestServer(prometheus.NewRegistry())
	s.startBackgroundLoops()
	s.metricsExporter.Start()

	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("server stop did not return")
	}
}
