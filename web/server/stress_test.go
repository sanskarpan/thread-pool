// Package server_test provides E2E and stress tests for the web server
package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
)

// TestServer_StressTest performs a stress and regression test on dynamic pools
func TestServer_StressTest(t *testing.T) {
	// Use a new registry for this test to avoid metric collisions
	reg := prometheus.NewRegistry()
	s := newTestServer(reg)

	mux := http.NewServeMux()
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

	// 1. Create a Dynamic Pool
	minWorkers, maxWorkers := 2, 10
	createBody := `{"pool_type": "dynamic", "min_workers": 2, "max_workers": 10, "queue_size": 100}`
	resp, err := http.Post(testServer.URL+"/api/v1/pool/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	resp.Body.Close()

	// 2. Connect WebSocket client
	wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/api/v1/ws"
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to dial websocket: %v", err)
	}
	defer ws.Close()

	// 3. Goroutine to monitor WebSocket state for scaling
	var maxWorkersObserved int32
	var scaledDown atomic.Bool
	go func() {
		for {
			var msg WebSocketMessage
			ws.SetReadDeadline(time.Now().Add(10 * time.Second))
			if err := ws.ReadJSON(&msg); err != nil {
				return
			}

			if msg.Type == "state" {
				var state PoolState
				payloadBytes, _ := json.Marshal(msg.Payload)
				json.Unmarshal(payloadBytes, &state)

				if state.WorkerCount > int(atomic.LoadInt32(&maxWorkersObserved)) {
					atomic.StoreInt32(&maxWorkersObserved, int32(state.WorkerCount))
				}
				if atomic.LoadInt32(&maxWorkersObserved) > int32(minWorkers) && state.WorkerCount == minWorkers {
					scaledDown.Store(true)
				}
			}
		}
	}()

	// 4. Stress the server with concurrent task submissions
	taskCount := 1000
	concurrency := 50
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < taskCount/concurrency; j++ {
				submitBody := `{"duration_ms": 10, "task_count": 1}`
				http.Post(testServer.URL+"/api/v1/task/submit", "application/json", strings.NewReader(submitBody))
			}
		}()
	}

	wg.Wait()

	// 5. Wait for all tasks to complete and for the pool to scale down
	// Give it some time to process everything and scale down
	time.Sleep(5 * time.Second)

	// 6. Verification
	if int(atomic.LoadInt32(&maxWorkersObserved)) <= minWorkers {
		t.Errorf("Expected pool to scale up beyond min workers (%d), but max observed was %d", minWorkers, maxWorkersObserved)
	}

	if int(atomic.LoadInt32(&maxWorkersObserved)) > maxWorkers {
		t.Errorf("Pool scaled beyond max workers (%d), observed %d", maxWorkers, maxWorkersObserved)
	}

	if !scaledDown.Load() {
		t.Errorf("Pool did not scale down to min workers after the load.")
	}
}
