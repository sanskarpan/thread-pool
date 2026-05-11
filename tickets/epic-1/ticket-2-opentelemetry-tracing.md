**Ticket #2: Integrate OpenTelemetry for Distributed Tracing**

*   **Problem Statement:** It is difficult to trace the full lifecycle of a task from submission to completion, especially in a distributed environment where the submitter and worker might be in different services.
*   **Goals:**
    *   Integrate OpenTelemetry to create traces for each task.
    *   A trace should represent the entire lifecycle: `pending -> running -> finished`.
    *   Allow trace context to be passed into a task.
*   **Technical Design:**
    *   Add the OpenTelemetry Go libraries (`go.opentelemetry.io/otel`).
    *   Modify the `task` package. The `Task` struct will be updated to hold an `otel.Tracer`.
    *   When `Submit` is called, a new span "task-lifecycle" will be created.
    *   The trace context will be injected into the task's `context.Context`.
    *   Events will be added to the span when the task starts executing and when it finishes.
    *   The `WithContext` task option will be updated to extract a parent span from the incoming context, allowing for traces to be connected across service boundaries.
*   **API Changes:** The `SubmitFunc` and `NewTask` functions will be updated to optionally accept an `otel.TracerProvider`.
*   **Database Changes:** Not applicable.
*   **Validation Strategy:**
    *   **Unit Tests:** Add unit tests to the `task` package to verify that spans are created correctly and that parent-child relationships are established when using `WithContext`.
    *   **Integration Tests:** An example in the `examples/` directory will be created to show how to set up an OTel exporter (e.g., to console) and see the traces.
*   **Security Concerns:** Ensure that trace data does not contain sensitive information from task payloads.
*   **Rollback Plan:** The integration can be disabled by not providing a `TracerProvider`. The changes will be backward-compatible.
*   **Definition of Done:**
    *   Tasks generate traces with correct spans and events.
    *   Trace context propagation is working correctly.
    *   An example demonstrates the tracing functionality.
    *   All tests are passing.
