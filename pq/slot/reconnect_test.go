package slot

import (
	"context"
	goerrors "errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// reconnectProbeConn records whether the status query is preceded by a
// reconnect attempt.
type reconnectProbeConn struct {
	connectErr   error
	connectCalls int
	execCalls    int
}

func (c *reconnectProbeConn) Connect(context.Context) error {
	c.connectCalls++
	return c.connectErr
}

func (c *reconnectProbeConn) IsClosed() bool { return c.connectErr != nil }

func (c *reconnectProbeConn) Close(context.Context) error { return nil }

func (c *reconnectProbeConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, nil
}

func (c *reconnectProbeConn) Frontend() *pgproto3.Frontend { return nil }

func (c *reconnectProbeConn) Exec(context.Context, string) *pgconn.MultiResultReader {
	c.execCalls++
	return nil
}

func newProbeSlot(t *testing.T, conn *reconnectProbeConn) *Slot {
	t.Helper()
	s := NewSlot("", "", Config{Name: "test_slot", SlotActivityCheckerInterval: 1000}, nil, nil)
	t.Cleanup(s.ticker.Stop)
	s.conn = conn
	return s
}

// CaptureSlot, Metrics and the /slot-info handler hold this connection for the
// whole process lifetime, so a dropped connection must be re-dialed instead of
// failing every subsequent query with "conn closed".
func TestInfoReconnectsBeforeQuery(t *testing.T) {
	conn := &reconnectProbeConn{connectErr: goerrors.New("connection refused")}
	s := newProbeSlot(t, conn)

	if _, err := s.Info(context.Background()); err == nil {
		t.Fatal("expected an error when the reconnect fails")
	}

	if conn.connectCalls != 1 {
		t.Fatalf("expected 1 reconnect attempt, got %d", conn.connectCalls)
	}
	if conn.execCalls != 0 {
		t.Fatalf("status query must not run on a dead connection, ran %d times", conn.execCalls)
	}
}

// The reconnect must not resurrect a connection for an already closed slot.
func TestInfoDoesNotReconnectAfterClose(t *testing.T) {
	conn := &reconnectProbeConn{}
	s := newProbeSlot(t, conn)
	s.closed.Store(true)

	_, err := s.Info(context.Background())
	if !goerrors.Is(err, ErrorSlotClosed) {
		t.Fatalf("expected ErrorSlotClosed, got %v", err)
	}
	if conn.connectCalls != 0 {
		t.Fatalf("expected no reconnect attempt on a closed slot, got %d", conn.connectCalls)
	}
}
