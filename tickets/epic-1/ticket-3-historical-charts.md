**Ticket #3: Enhance Web UI with Historical Metric Charts**

*   **Problem Statement:** The web UI only shows real-time metrics, making it impossible to see trends or analyze performance over a short period (e.g., during a burst of activity).
*   **Goals:**
    *   Modify the UI charts to display data for the last 3-5 minutes.
    *   Ensure the frontend remains performant while handling more data.
*   **Technical Design:**
    *   **Backend:** No changes are required. The backend already sends periodic updates.
    *   **Frontend (`app.js`):**
        *   The `updateCharts` function will be modified. Instead of replacing data, it will push new data points and only remove old ones that are outside the desired time window.
        *   The Chart.js configuration will be updated to handle a larger number of data points. We will increase the circular buffer size from 20 to ~300 (e.g., one point every second for 5 minutes).
        *   A timestamp will be used on the x-axis to represent the time of the metric.
*   **API Changes:** None.
*   **Database Changes:** Not applicable.
*   **Validation Strategy:**
    *   **Manual QA:** Manually test the web UI to confirm that the charts now display a moving window of historical data and that the UI remains responsive.
    *   **E2E Tests:** While we lack a frontend testing framework, we can extend the existing Go E2E test to keep a WebSocket connection open for a longer duration to verify the backend continues to send data reliably.
*   **Definition of Done:**
    *   The throughput and utilization charts in the web UI display a moving window of data.
    *   The UI remains smooth and responsive.
    *   The changes are deployed and verified.
