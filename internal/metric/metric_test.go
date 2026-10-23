package metric

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestReconnectMetrics(t *testing.T) {
	m := NewMetric("test_slot").(*metric)

	assert.InDelta(t, float64(0), testutil.ToFloat64(m.reconnectAttempts), 0)
	assert.InDelta(t, float64(0), testutil.ToFloat64(m.reconnectSuccesses), 0)
	assert.InDelta(t, float64(0), testutil.ToFloat64(m.reconnectFailures), 0)
	assert.InDelta(t, float64(0), testutil.ToFloat64(m.reconnecting), 0)

	m.ReconnectAttemptIncrement()
	m.ReconnectAttemptIncrement()
	assert.InDelta(t, float64(2), testutil.ToFloat64(m.reconnectAttempts), 0)

	m.SetReconnecting(true)
	assert.InDelta(t, float64(1), testutil.ToFloat64(m.reconnecting), 0)
	m.SetReconnecting(false)
	assert.InDelta(t, float64(0), testutil.ToFloat64(m.reconnecting), 0)

	m.ReconnectSuccessIncrement()
	assert.InDelta(t, float64(1), testutil.ToFloat64(m.reconnectSuccesses), 0)

	m.ReconnectFailureIncrement()
	assert.InDelta(t, float64(1), testutil.ToFloat64(m.reconnectFailures), 0)
}

func TestUnackedLSNLagMetric(t *testing.T) {
	m := NewMetric("test_slot").(*metric)

	assert.InDelta(t, float64(0), testutil.ToFloat64(m.unackedLSNLag), 0)

	m.SetUnackedLSNLag(42)
	assert.InDelta(t, float64(42), testutil.ToFloat64(m.unackedLSNLag), 0)

	m.SetUnackedLSNLag(0)
	assert.InDelta(t, float64(0), testutil.ToFloat64(m.unackedLSNLag), 0)
}

func TestWALBacklogWarningMetric(t *testing.T) {
	m := NewMetric("test_slot").(*metric)

	assert.InDelta(t, float64(0), testutil.ToFloat64(m.walBacklogWarnings), 0)

	m.WALBacklogWarningIncrement()
	m.WALBacklogWarningIncrement()
	assert.InDelta(t, float64(2), testutil.ToFloat64(m.walBacklogWarnings), 0)
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

	assert.InDelta(t, float64(0), testutil.ToFloat64(m.replayedMessages), 0)

	m.ReplayedMessageIncrement()
	m.ReplayedMessageIncrement()
	assert.InDelta(t, float64(2), testutil.ToFloat64(m.replayedMessages), 0)
}
