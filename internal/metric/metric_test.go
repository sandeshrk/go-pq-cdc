package metric

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestReconnectMetrics(t *testing.T) {
	m := NewMetric("test_slot").(*metric)

	assert.Equal(t, float64(0), testutil.ToFloat64(m.reconnectAttempts))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.reconnectSuccesses))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.reconnectFailures))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.reconnecting))

	m.ReconnectAttemptIncrement()
	m.ReconnectAttemptIncrement()
	assert.Equal(t, float64(2), testutil.ToFloat64(m.reconnectAttempts))

	m.SetReconnecting(true)
	assert.Equal(t, float64(1), testutil.ToFloat64(m.reconnecting))
	m.SetReconnecting(false)
	assert.Equal(t, float64(0), testutil.ToFloat64(m.reconnecting))

	m.ReconnectSuccessIncrement()
	assert.Equal(t, float64(1), testutil.ToFloat64(m.reconnectSuccesses))

	m.ReconnectFailureIncrement()
	assert.Equal(t, float64(1), testutil.ToFloat64(m.reconnectFailures))
}

func TestUnackedLSNLagMetric(t *testing.T) {
	m := NewMetric("test_slot").(*metric)

	assert.Equal(t, float64(0), testutil.ToFloat64(m.unackedLSNLag))

	m.SetUnackedLSNLag(42)
	assert.Equal(t, float64(42), testutil.ToFloat64(m.unackedLSNLag))

	m.SetUnackedLSNLag(0)
	assert.Equal(t, float64(0), testutil.ToFloat64(m.unackedLSNLag))
}

func TestWALBacklogWarningMetric(t *testing.T) {
	m := NewMetric("test_slot").(*metric)

	assert.Equal(t, float64(0), testutil.ToFloat64(m.walBacklogWarnings))

	m.WALBacklogWarningIncrement()
	m.WALBacklogWarningIncrement()
	assert.Equal(t, float64(2), testutil.ToFloat64(m.walBacklogWarnings))
}

func TestReconnectMetricsRegisteredAsCollectors(t *testing.T) {
	m := NewMetric("test_slot").(*metric)
	collectors := m.PrometheusCollectors()

	found := map[string]bool{}
	for _, c := range collectors {
		switch c {
		case m.reconnectAttempts:
			found["attempts"] = true
		case m.reconnectSuccesses:
			found["successes"] = true
		case m.reconnectFailures:
			found["failures"] = true
		case m.reconnecting:
			found["reconnecting"] = true
		case m.replayedMessages:
			found["replayedMessages"] = true
		}
	}

	assert.True(t, found["attempts"])
	assert.True(t, found["successes"])
	assert.True(t, found["failures"])
	assert.True(t, found["reconnecting"])
	assert.True(t, found["replayedMessages"])
}

func TestReplayedMessageMetric(t *testing.T) {
	m := NewMetric("test_slot").(*metric)

	assert.Equal(t, float64(0), testutil.ToFloat64(m.replayedMessages))

	m.ReplayedMessageIncrement()
	m.ReplayedMessageIncrement()
	assert.Equal(t, float64(2), testutil.ToFloat64(m.replayedMessages))
}
