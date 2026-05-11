**Ticket #1: Implement Prometheus `/metrics` Endpoint**

*   **Problem Statement:** The application's valuable internal metrics are not exposed in a standardized way, preventing integration with industry-standard monitoring and alerting systems.
*   **Goals:**
    *   Create a new HTTP endpoint at `/metrics`.
    *   Expose all key metrics from the `pool.Metrics` struct in the Prometheus exposition format.
    *   Ensure the endpoint is efficient and does not negatively impact application performance.
*   **Technical Design:**
    *   Add the official Prometheus Go client library: `prometheus/client_golang`.
    *   Create a new `metrics_exporter.go` file in the `web/server` package.
    *   This file will define a new struct, `MetricsExporter`, which holds Prometheus Gauge and CounterVec metrics.
    *   The exporter will be initialized with the `Server` and will update the Prometheus metrics in a separate goroutine, periodically fetching data from the active pool's `Metrics()` method.
    *   The `/metrics` endpoint will use the `promhttp.Handler()` to serve the metrics.
*   **API Changes:** None. This is a new, additive endpoint.
*   **Database Changes:** Not applicable.
*   **Validation Strategy:**
    *   **Unit Tests:** Add a unit test to verify that the exporter correctly translates the pool's internal metrics into Prometheus metrics.
    *   **E2E Tests:** Add a test to the `server_test.go` that scrapes the `/metrics` endpoint and verifies that the output is in the correct format and that values change after submitting tasks.
*   **Monitoring Requirements:** The Prometheus server that scrapes this endpoint will be responsible for monitoring. We should see the new metrics appear once scraped.
*   **Rollback Plan:** The change can be rolled back by removing the endpoint handler and the exporter. This can be controlled via a feature flag if necessary (though it's low-risk).
*   **Definition of Done:**
    *   The `/metrics` endpoint is available on the web server.
    *   Metrics for all pool types are successfully exported.
    *   New and updated tests are passing.
    *   Documentation is updated to reflect the new endpoint.
