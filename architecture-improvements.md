# Architecture Improvements

This document outlines proposed architectural improvements to enhance the scalability, maintainability, and reliability of the Thread Pool system.

---

### 1. Decoupled Metrics Exposure

*   **Current State:** Metrics are collected internally but only accessible via the `Pool` interface and custom WebSocket messages. There is no standard way for external monitoring systems to scrape them.
*   **Proposed Change:** Introduce a dedicated, standard metrics endpoint.
    *   **Implementation:** Add a `/metrics` HTTP endpoint to the web server, serving data in the Prometheus exposition format.
    *   **Why:** This is the industry standard for observability. It allows seamless integration with monitoring systems like Prometheus, Grafana, and Datadog, enabling robust alerting, dashboarding, and long-term trend analysis without relying on the custom UI.

### 2. Distributed Tracing Integration

*   **Current State:** It's difficult to trace the lifecycle of a single task, especially in a distributed system where a task submitted in one service might be processed by another.
*   **Proposed Change:** Integrate OpenTelemetry hooks into the task lifecycle.
    *   **Implementation:** The `task` package will be modified to create and manage OpenTelemetry spans. A span will be started when a task is submitted and ended when it completes, fails, or is cancelled. The trace context can be propagated via the task's `context.Context`.
    *   **Why:** In modern microservices architectures, understanding the full journey of a request is critical. Distributed tracing allows developers to visualize the entire flow, identify bottlenecks, and debug complex, multi-service interactions.

### 3. Formalized Configuration Management

*   **Current State:** The web server is configured only via command-line flags. The pool library is configured programmatically.
*   **Proposed Change:** Externalize configuration to files and environment variables.
    *   **Implementation:** Use a library like Viper to allow the web server's configuration (e.g., port, host) to be loaded from a `config.yaml` file or overridden by environment variables.
    *   **Why:** This is standard practice for production applications. It decouples the configuration from the code, making it easier to manage different environments (development, staging, production) without recompiling the application.

### 4. Containerization & Environment Abstraction

*   **Current State:** The application runs as a local binary. There is no standardized environment.
*   **Proposed Change:** Provide a `Dockerfile` to containerize the web UI application.
    *   **Implementation:** Create a multi-stage `Dockerfile` that builds the Go binary and then copies it into a minimal, secure runtime container image.
    *   **Why:** Containerization ensures a consistent, reproducible runtime environment, which is the foundation of modern DevOps and CI/CD. It simplifies deployment, scaling, and dependency management.

### 5. API Versioning & Specification

*   **Current State:** The REST API for the web server has no versioning. Its contract is implicit.
*   **Proposed Change:** Introduce API versioning and generate a formal specification.
    *   **Implementation:** Restructure API endpoints under a versioned path (e.g., `/api/v1/pools`). Use a tool like `go-swagger` to generate an OpenAPI 3.0 specification from code annotations.
    *   **Why:** Versioning is essential for long-term API maintainability, allowing for backward-incompatible changes without breaking existing clients. An OpenAPI specification provides a clear, machine-readable contract for API consumers and enables auto-generation of clients and documentation.

### 6. WebSocket and Auxiliary Endpoint Hardening

*   **Current State:** Browser-origin checks and non-core endpoints (metrics/spec) can become accidental exposure points in production if left permissive.
*   **Proposed Change:** Apply secure defaults plus explicit opt-in access controls.
    *   **Implementation:**
        *   Validate WebSocket origins by default using same-host policy, with explicit allowlist support via `THREAD_POOL_ALLOWED_ORIGINS`.
        *   Keep `/metrics` and `/api/swagger.json` public by default for compatibility, but allow optional API-key protection through CLI/env/YAML toggles.
    *   **Why:** This balances deployment safety with operational flexibility. Teams can keep local/dev ergonomics while tightening production exposure without code changes.
