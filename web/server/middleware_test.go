package server

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIKeyAuth_Disabled(t *testing.T) {
	h := apiKeyAuth("")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/pools", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
}

func TestAPIKeyAuth_Enabled(t *testing.T) {
	key := "super-secret"
	h := apiKeyAuth(key)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	t.Run("missing key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pools", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)

		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected status 401, got %d", rr.Code)
		}
	})

	t.Run("header key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pools", nil)
		req.Header.Set("X-API-Key", key)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rr.Code)
		}
	})

	t.Run("query key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pools?api_key=super-secret", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rr.Code)
		}
	})
}

func TestAuditLog_EmitsStructuredEntry(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	h := auditLog(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/pool/create", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	logLine := buf.String()
	if !strings.Contains(logLine, "api_request") {
		t.Fatalf("expected audit log message, got: %s", logLine)
	}
	if !strings.Contains(logLine, `"status":201`) {
		t.Fatalf("expected status code in log, got: %s", logLine)
	}
	if !strings.Contains(logLine, `"path":"/api/v1/pool/create"`) {
		t.Fatalf("expected request path in log, got: %s", logLine)
	}
}

func TestRequestIDMiddleware(t *testing.T) {
	h := requestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := requestIDFromContext(r.Context()); got == "" {
			t.Fatal("expected request id in context")
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/pools", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if rr.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID response header")
	}
}
