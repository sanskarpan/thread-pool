package pool

import "github.com/sanskarpan/thread-pool/metrics"

func recordTaskRetries(m *metrics.Metrics, retries int) {
	if m == nil || retries <= 0 {
		return
	}

	for i := 0; i < retries; i++ {
		m.IncrementRetried()
	}
}
