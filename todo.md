# Engineering Todo List

This document lists the high-level engineering tasks derived from the product roadmap.

### Phase 1: Production Readiness & Observability

- [x] **Epic 1: Enterprise-Grade Observability**
  - [x] **Ticket #1:** Implement Prometheus `/metrics` Endpoint.
  - [x] **Ticket #2:** Integrate OpenTelemetry for Distributed Tracing.
  - [x] **Ticket #3:** Enhance Web UI with Historical Metric Charts.

- [x] **Epic 2: Infrastructure & DevOps Foundation**
  - [x] **Ticket #4:** Create `Dockerfile` for Web UI Application.
  - [x] **Ticket #5:** Implement Basic CI/CD Pipeline with GitHub Actions.

### Phase 2: Advanced Pool Features & API Hardening

- [x] **Epic 3: Advanced Task Management**
  - [x] **Ticket #6:** Implement Cron-Style Scheduling in `ScheduledPool`.
  - [x] **Ticket #7:** Add Task Dependency Management.
  - [x] **Ticket #8:** Allow Runtime Adjustment of `FixedPool` Worker Count.

- [x] **Epic 4: API & Security Enhancements**
  - [x] **Ticket #9:** Introduce API Versioning.
  - [x] **Ticket #10:** Implement Rate Limiting on API Endpoints.
  - [x] **Ticket #11:** Generate OpenAPI (Swagger) Documentation.

### Phase 3: Enhanced User Experience & Enterprise Features

- [x] **Epic 5: Rich Web Visualizer UI**
  - [x] **Ticket #12:** Implement Real-Time Task Event Log in UI.
  - [x] **Ticket #13:** Add UI Controls for Advanced Scheduling.
  - [x] **Ticket #14:** Implement Light/Dark Mode Theme.

- [x] **Epic 6: Enterprise Readiness**
  - [x] **Ticket #15:** Add API Key Authentication to Web Server.
  - [x] **Ticket #16:** Implement Structured Audit Logging.
  - [x] **Ticket #17:** Externalize Configuration to YAML/Env Vars.

### Post-Roadmap Hardening (Completed)

- [x] **Security Hardening #1:** Restrict WebSocket origins with secure defaults and explicit allowlist (`THREAD_POOL_ALLOWED_ORIGINS`).
- [x] **Security Hardening #2:** Add optional API-key protection toggles for `/metrics` and `/api/swagger.json`.
- [x] **API Contract #1:** Update OpenAPI/README docs for advanced task submit fields (`delay_ms`, `cron_expression`, `chain_dependencies`).
