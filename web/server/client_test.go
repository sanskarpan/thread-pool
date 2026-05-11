package server

import (
	"net/http/httptest"
	"testing"
)

func TestCheckWebSocketOrigin(t *testing.T) {
	t.Run("allows request with no origin", func(t *testing.T) {
		t.Setenv(allowedOriginsEnvVar, "")
		r := httptest.NewRequest("GET", "http://example.com/api/v1/ws", nil)

		if !checkWebSocketOrigin(r) {
			t.Fatal("expected origin check to allow request without Origin header")
		}
	})

	t.Run("allows same host origin by default", func(t *testing.T) {
		t.Setenv(allowedOriginsEnvVar, "")
		r := httptest.NewRequest("GET", "http://example.com/api/v1/ws", nil)
		r.Header.Set("Origin", "https://example.com")

		if !checkWebSocketOrigin(r) {
			t.Fatal("expected same-host origin to be allowed")
		}
	})

	t.Run("denies different host origin by default", func(t *testing.T) {
		t.Setenv(allowedOriginsEnvVar, "")
		r := httptest.NewRequest("GET", "http://example.com/api/v1/ws", nil)
		r.Header.Set("Origin", "https://evil.example")

		if checkWebSocketOrigin(r) {
			t.Fatal("expected different-host origin to be denied")
		}
	})

	t.Run("allows configured allowlist origin", func(t *testing.T) {
		t.Setenv(allowedOriginsEnvVar, "https://allowed.example, https://other.example")
		r := httptest.NewRequest("GET", "http://example.com/api/v1/ws", nil)
		r.Header.Set("Origin", "https://allowed.example")

		if !checkWebSocketOrigin(r) {
			t.Fatal("expected allowlisted origin to be allowed")
		}
	})

	t.Run("denies non-allowlisted origin", func(t *testing.T) {
		t.Setenv(allowedOriginsEnvVar, "https://allowed.example")
		r := httptest.NewRequest("GET", "http://example.com/api/v1/ws", nil)
		r.Header.Set("Origin", "https://other.example")

		if checkWebSocketOrigin(r) {
			t.Fatal("expected non-allowlisted origin to be denied")
		}
	})
}
