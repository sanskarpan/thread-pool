package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	content := []byte("host: 0.0.0.0\nport: 9090\ntracing: true\napi_key: test-key\nprotect_metrics: true\nprotect_swagger: true\n")
	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := loadConfigFile(configPath)
	if err != nil {
		t.Fatalf("loadConfigFile failed: %v", err)
	}

	if cfg.Host != "0.0.0.0" || cfg.Port != 9090 || !cfg.Tracing || cfg.APIKey != "test-key" || !cfg.ProtectMetrics || !cfg.ProtectSwagger {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestApplyEnvOverrides(t *testing.T) {
	cfg := defaultConfig()

	t.Setenv("THREAD_POOL_HOST", " 127.0.0.1 ")
	t.Setenv("THREAD_POOL_PORT", "8181")
	t.Setenv("THREAD_POOL_TRACING", "true")
	t.Setenv("THREAD_POOL_API_KEY", " env-key ")
	t.Setenv("THREAD_POOL_PROTECT_METRICS", "true")
	t.Setenv("THREAD_POOL_PROTECT_SWAGGER", "true")

	if err := applyEnvOverrides(&cfg); err != nil {
		t.Fatalf("applyEnvOverrides failed: %v", err)
	}

	if cfg.Host != "127.0.0.1" || cfg.Port != 8181 || !cfg.Tracing || cfg.APIKey != "env-key" || !cfg.ProtectMetrics || !cfg.ProtectSwagger {
		t.Fatalf("unexpected env-applied config: %+v", cfg)
	}
}

func TestApplyEnvOverrides_InvalidPort(t *testing.T) {
	cfg := defaultConfig()
	t.Setenv("THREAD_POOL_PORT", "invalid")

	if err := applyEnvOverrides(&cfg); err == nil {
		t.Fatal("expected error for invalid THREAD_POOL_PORT")
	}
}

func TestValidateConfig(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.APIKey = "key"
		cfg.ProtectMetrics = true
		if err := validateConfig(cfg); err != nil {
			t.Fatalf("expected valid config, got error: %v", err)
		}
	})

	t.Run("protection without api key", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.ProtectSwagger = true
		if err := validateConfig(cfg); err == nil {
			t.Fatal("expected error when protection enabled without api key")
		}
	})

	t.Run("port out of range", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.Port = 70000
		if err := validateConfig(cfg); err == nil {
			t.Fatal("expected error for port out of range")
		}
	})
}
