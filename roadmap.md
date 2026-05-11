# Product Roadmap: Thread Pool Visualizer

This roadmap outlines the major features and improvements required to evolve the Thread Pool Visualizer into an enterprise-grade, production-ready system.

---

### Phase 1: Production Readiness & Observability

*Goal: Harden the existing system and implement industry-standard observability.*

*   **Epic 1: Enterprise-Grade Observability**
    *   **Feature:** Implement a Prometheus `/metrics` endpoint to expose internal metrics.
    *   **Feature:** Integrate OpenTelemetry for distributed tracing of task lifecycles.
    *   **Story:** Enhance the Web UI to display historical metric charts (e.g., last 5 minutes).

*   **Epic 2: Infrastructure & DevOps Foundation**
    *   **Feature:** Create a `Dockerfile` for containerizing the web UI application.
    *   **Feature:** Implement a CI/CD pipeline (e.g., GitHub Actions) to automate testing on every commit.

---

### Phase 2: Advanced Pool Features & API Hardening

*Goal: Expand the core library's capabilities and formalize its API contracts.*

*   **Epic 3: Advanced Task Management**
    *   **Feature:** Implement cron-style scheduling (`ScheduleWithCron`) in the `ScheduledPool`.
    *   **Feature:** Introduce task dependency management (e.g., `task.After(otherTask)`).
    *   **Feature:** Allow runtime adjustment of `FixedPool` worker counts.

*   **Epic 4: API & Security Enhancements**
    *   **Feature:** Introduce API versioning (e.g., `/api/v1/...`).
    *   **Feature:** Implement rate limiting on the web server's API endpoints.
    *   **Story:** Generate OpenAPI (Swagger) documentation for the REST API.

---

### Phase 3: Enhanced User Experience & Enterprise Features

*Goal: Improve the web visualizer's diagnostic capabilities and add enterprise-focused features.*

*   **Epic 5: Rich Web Visualizer UI**
    *   **Feature:** Display a real-time log of task events (submitted, failed, completed) in the UI.
    *   **Feature:** Add UI controls for advanced features (e.g., cron scheduling, task dependencies).
    *   **Story:** Implement a light/dark mode theme for the Web UI.

*   **Epic 6: Enterprise Readiness**
    *   **Feature:** Implement basic authentication (e.g., API key) for the web server.
    *   **Feature:** Add structured audit logging for all API actions.
    *   **Story:** Allow system configuration via a YAML file or environment variables.

---

### Phase 4: Security Hardening & Contract Polish

*Goal: Lock down externally exposed surfaces and keep API contracts explicit for operators and clients.*

*   **Epic 7: Runtime Surface Hardening**
    *   **Feature:** Enforce secure WebSocket origin checks with same-host defaults and allowlist override (`THREAD_POOL_ALLOWED_ORIGINS`).
    *   **Feature:** Add optional API-key protection for `/metrics` and `/api/swagger.json` via CLI/env/YAML toggles.

*   **Epic 8: API Contract Accuracy**
    *   **Story:** Keep OpenAPI and README aligned with server behavior for advanced submit fields (`delay_ms`, `cron_expression`, `chain_dependencies`).

---
