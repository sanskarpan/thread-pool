// Thread Pool Visualizer Web UI
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sanskarpan/thread-pool/web/server"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.yaml.in/yaml/v2"
)

type appConfig struct {
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	Tracing        bool   `yaml:"tracing"`
	APIKey         string `yaml:"api_key"`
	ProtectMetrics bool   `yaml:"protect_metrics"`
	ProtectSwagger bool   `yaml:"protect_swagger"`
}

func defaultConfig() appConfig {
	return appConfig{
		Host: "0.0.0.0",
		Port: 8080,
	}
}

func loadConfigFile(path string) (appConfig, error) {
	cfg := defaultConfig()
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	cfg.Host = strings.TrimSpace(cfg.Host)
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)

	if cfg.Host == "" {
		cfg.Host = "0.0.0.0"
	}
	if cfg.Port <= 0 {
		cfg.Port = 8080
	}

	return cfg, nil
}

func applyEnvOverrides(cfg *appConfig) error {
	if host := os.Getenv("THREAD_POOL_HOST"); host != "" {
		cfg.Host = strings.TrimSpace(host)
	}

	if port := os.Getenv("THREAD_POOL_PORT"); port != "" {
		parsed, err := strconv.Atoi(port)
		if err != nil {
			return fmt.Errorf("invalid THREAD_POOL_PORT value %q: %w", port, err)
		}
		cfg.Port = parsed
	}

	if tracing := os.Getenv("THREAD_POOL_TRACING"); tracing != "" {
		parsed, err := strconv.ParseBool(tracing)
		if err != nil {
			return fmt.Errorf("invalid THREAD_POOL_TRACING value %q: %w", tracing, err)
		}
		cfg.Tracing = parsed
	}

	if apiKey := os.Getenv("THREAD_POOL_API_KEY"); apiKey != "" {
		cfg.APIKey = strings.TrimSpace(apiKey)
	}

	if protectMetrics := os.Getenv("THREAD_POOL_PROTECT_METRICS"); protectMetrics != "" {
		parsed, err := strconv.ParseBool(protectMetrics)
		if err != nil {
			return fmt.Errorf("invalid THREAD_POOL_PROTECT_METRICS value %q: %w", protectMetrics, err)
		}
		cfg.ProtectMetrics = parsed
	}

	if protectSwagger := os.Getenv("THREAD_POOL_PROTECT_SWAGGER"); protectSwagger != "" {
		parsed, err := strconv.ParseBool(protectSwagger)
		if err != nil {
			return fmt.Errorf("invalid THREAD_POOL_PROTECT_SWAGGER value %q: %w", protectSwagger, err)
		}
		cfg.ProtectSwagger = parsed
	}

	return nil
}

func detectConfigPath(args []string) string {
	fs := flag.NewFlagSet("preload", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	config := fs.String("config", "", "Path to YAML config file")
	_ = fs.Parse(args)
	return *config
}

func validateConfig(cfg appConfig) error {
	if cfg.Port < 1 || cfg.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}

	if (cfg.ProtectMetrics || cfg.ProtectSwagger) && cfg.APIKey == "" {
		return fmt.Errorf("api_key is required when protect_metrics or protect_swagger is enabled")
	}

	return nil
}

func newExporter() (trace.SpanExporter, error) {
	return stdouttrace.New(
		stdouttrace.WithWriter(os.Stdout),
		stdouttrace.WithPrettyPrint(),
	)
}

func newTraceProvider(exp trace.SpanExporter) *trace.TracerProvider {
	return trace.NewTracerProvider(
		trace.WithBatcher(exp),
	)
}

const banner = `
╔════════════════════════════════════════════════════════════════╗
║                                                                ║
║     ████████╗██╗  ██╗██████╗ ███████╗ █████╗ ██████╗          ║
║     ╚══██╔══╝██║  ██║██╔══██╗██╔════╝██╔══██╗██╔══██╗         ║
║        ██║   ███████║██████╔╝█████╗  ███████║██║  ██║         ║
║        ██║   ██╔══██║██╔══██╗██╔══╝  ██╔══██║██║  ██║         ║
║        ██║   ██║  ██║██║  ██║███████╗██║  ██║██████╔╝         ║
║        ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═════╝          ║
║                                                                ║
║              ██████╗  ██████╗  ██████╗ ██╗                    ║
║              ██╔══██╗██╔═══██╗██╔═══██╗██║                    ║
║              ██████╔╝██║   ██║██║   ██║██║                    ║
║              ██╔═══╝ ██║   ██║██║   ██║██║                    ║
║              ██║     ╚██████╔╝╚██████╔╝███████╗               ║
║              ╚═╝      ╚═════╝  ╚═════╝ ╚══════╝               ║
║                                                                ║
║            Real-time Thread Pool Visualizer                   ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝
`

func main() {
	// Setup structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	configPath := detectConfigPath(os.Args[1:])
	cfg, err := loadConfigFile(configPath)
	if err != nil {
		slog.Error("Failed to load config file", "err", err, "path", configPath)
		os.Exit(1)
	}
	if err := applyEnvOverrides(&cfg); err != nil {
		slog.Error("Failed to parse environment overrides", "err", err)
		os.Exit(1)
	}

	// Parse command-line flags
	port := flag.Int("port", cfg.Port, "HTTP server port")
	host := flag.String("host", cfg.Host, "HTTP server host")
	tracing := flag.Bool("tracing", cfg.Tracing, "Enable OpenTelemetry console tracing")
	apiKey := flag.String("api-key", cfg.APIKey, "API key required for API endpoints (optional)")
	protectMetrics := flag.Bool("protect-metrics", cfg.ProtectMetrics, "Require API key for /metrics")
	protectSwagger := flag.Bool("protect-swagger", cfg.ProtectSwagger, "Require API key for /api/swagger.json")
	flag.String("config", configPath, "Path to YAML config file")
	flag.Parse()

	cfg.Port = *port
	cfg.Host = strings.TrimSpace(*host)
	cfg.Tracing = *tracing
	cfg.APIKey = strings.TrimSpace(*apiKey)
	cfg.ProtectMetrics = *protectMetrics
	cfg.ProtectSwagger = *protectSwagger

	if err := validateConfig(cfg); err != nil {
		slog.Error("Invalid configuration", "err", err)
		os.Exit(1)
	}

	// Setup OpenTelemetry if enabled
	if cfg.Tracing {
		slog.Info("OpenTelemetry tracing enabled", "component", "main")
		exp, err := newExporter()
		if err != nil {
			slog.Error("Failed to create trace exporter", "err", err, "component", "main")
			os.Exit(1)
		}
		tp := newTraceProvider(exp)
		otel.SetTracerProvider(tp)
		defer tp.Shutdown(context.Background())
	}

	// Print banner
	fmt.Print(banner)
	fmt.Print("\n\n")

	// Create server
	s := server.NewServer()
	s.ConfigureSecurity(cfg.APIKey, cfg.ProtectMetrics, cfg.ProtectSwagger, logger)
	if cfg.APIKey != "" {
		slog.Info("API key authentication enabled", "component", "main")
	}

	// Create a new HTTP server
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	httpServer := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		slog.Info("Starting Thread Pool Visualizer...", "component", "main", "addr", addr)
		if err := s.Start(httpServer); err != http.ErrServerClosed {
			slog.Error("Server error", "err", err, "component", "main")
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Graceful shutdown
	fmt.Print("\n")
	slog.Info("Shutting down server...", "component", "main")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		slog.Error("Server shutdown failed", "err", err, "component", "main")
	}
	s.Stop()

	slog.Info("Server stopped gracefully", "component", "main")
}
