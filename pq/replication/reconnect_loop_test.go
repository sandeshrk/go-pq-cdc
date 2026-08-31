package replication

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// fakeDialError simulates a realistic transient network error, the way a real
// TCP dial failure actually surfaces (e.g. *net.OpError, which implements
// net.Error). A bare errors.New(...) does not implement net.Error and is
// correctly classified permanent by pq.IsRetryableConnectionError, so these
// fakes must use this type to keep simulating a transient failure.
type fakeDialError struct{ msg string }

func (e fakeDialError) Error() string { return e.msg }
func (fakeDialError) Timeout() bool   { return false }
func (fakeDialError) Temporary() bool { return true }

var errDialRefused = fakeDialError{msg: "dial refused"}

// reconnectFakeConn is a pq.Connection fake for driving stream.reconnect()
// directly. Connect() fails for its first failConnectUntil calls and
// succeeds afterwards; ReceiveMessage always reports the handshake success
// replication.Test() (called from setup()) needs, since Connect succeeding
// is the only thing under test here.
type reconnectFakeConn struct {
	mu               sync.Mutex
	connectCalls     int
	failConnectUntil int
	closed           bool
	fe               *pgproto3.Frontend
}

func newReconnectFakeConn(failConnectUntil int) *reconnectFakeConn {
	return &reconnectFakeConn{
		failConnectUntil: failConnectUntil,
		fe:               pgproto3.NewFrontend(strings.NewReader(""), io.Discard),
	}
}

func (c *reconnectFakeConn) Connect(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connectCalls++
	if c.connectCalls <= c.failConnectUntil {
		return errDialRefused
	}
	c.closed = false
	return nil
}

func (c *reconnectFakeConn) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectCalls
}

func (c *reconnectFakeConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *reconnectFakeConn) Close(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *reconnectFakeConn) Frontend() *pgproto3.Frontend { return c.fe }

func (c *reconnectFakeConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }

func (c *reconnectFakeConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return &pgproto3.CopyBothResponse{}, nil
}

func newTestReconnectStream(cfg config.ReconnectConfig) *stream {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	return NewStream("", config.Config{Reconnect: cfg}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
}

func TestReconnectSucceedsOnSecondAttempt(t *testing.T) {
	s := newTestReconnectStream(config.ReconnectConfig{
		Enabled: true, InitialDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond,
		MaxJitter: time.Millisecond, MaxElapsed: 5 * time.Second,
	})
	conn := newReconnectFakeConn(1) // fails once, succeeds on the 2nd Connect() call
	s.conn = conn

	ok := s.reconnect(context.Background())

	if !ok {
		t.Fatal("expected reconnect to succeed once Connect() stops failing")
	}
	if got := conn.callCount(); got != 2 {
		t.Fatalf("expected exactly 2 Connect() calls, got %d", got)
	}
}

func TestReconnectGivesUpAfterMaxElapsed(t *testing.T) {
	s := newTestReconnectStream(config.ReconnectConfig{
		Enabled: true, InitialDelay: 2 * time.Millisecond, MaxDelay: 5 * time.Millisecond,
		MaxJitter: time.Millisecond, MaxElapsed: 30 * time.Millisecond,
	})
	conn := newReconnectFakeConn(1_000_000) // never succeeds
	s.conn = conn

	start := time.Now()
	ok := s.reconnect(context.Background())
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected reconnect to give up once the connection never comes back")
	}
	if got := conn.callCount(); got < 2 {
		t.Fatalf("expected more than one Connect() attempt before giving up, got %d", got)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("reconnect took %s to give up on a 30ms budget; it should not hang", elapsed)
	}
}

// permanentAuthErrorConn simulates a connection that fails Connect() with a
// permanent PostgreSQL error (e.g. a credential rotated to an invalid
// password) on every attempt -- retrying it can never succeed.
type permanentAuthErrorConn struct{ connectCalls *int }

func newPermanentAuthErrorConn() *permanentAuthErrorConn {
	return &permanentAuthErrorConn{connectCalls: new(int)}
}

func (c *permanentAuthErrorConn) Connect(context.Context) error {
	*c.connectCalls++
	return &pgconn.PgError{Code: "28P01", Message: "password authentication failed"}
}

func (*permanentAuthErrorConn) IsClosed() bool                                         { return true }
func (*permanentAuthErrorConn) Close(context.Context) error                            { return nil }
func (*permanentAuthErrorConn) Frontend() *pgproto3.Frontend                           { return nil }
func (*permanentAuthErrorConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }
func (*permanentAuthErrorConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, errDialRefused // unreachable: Connect always fails first
}

// TestReconnectFailsFastOnPermanentError verifies a permanent error (e.g. a
// rotated-away password) stops retrying immediately instead of consuming the
// whole MaxElapsed budget on an attempt that can never succeed.
//
// MaxElapsed is set to a deliberately absurd 1 hour: if the classification
// were broken and reconnect fell back to retrying on wall-clock alone, this
// test would hang for up to an hour instead of failing fast -- an
// unmistakable signal rather than a coincidence of timing (the same
// technique that caught the *pgconn.ConnectError ordering bug in
// cdc.IsRetryableStartupError).
func TestReconnectFailsFastOnPermanentError(t *testing.T) {
	s := newTestReconnectStream(config.ReconnectConfig{
		Enabled: true, InitialDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond,
		MaxJitter: time.Millisecond, MaxElapsed: time.Hour,
	})
	conn := newPermanentAuthErrorConn()
	s.conn = conn

	start := time.Now()
	ok := s.reconnect(context.Background())
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected reconnect to give up on a permanent authentication error")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("reconnect took %s to give up with MaxElapsed=1h; classification is not failing fast", elapsed)
	}
	if got := *conn.connectCalls; got != 1 {
		t.Fatalf("expected exactly 1 Connect() attempt before giving up on a permanent error, got %d", got)
	}
}

func TestReconnectNotAttemptedWhenAlreadyClosed(t *testing.T) {
	s := newTestReconnectStream(config.ReconnectConfig{
		Enabled: true, InitialDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond,
		MaxJitter: time.Millisecond, MaxElapsed: 5 * time.Second,
	})
	conn := newReconnectFakeConn(0)
	s.conn = conn
	s.closed.Store(true)

	ok := s.reconnect(context.Background())

	if ok {
		t.Fatal("expected reconnect to refuse to run once the stream is already closed")
	}
	if got := conn.callCount(); got != 0 {
		t.Fatalf("expected Connect() to never be called once the stream is already closed, got %d calls", got)
	}
}

// TestReconnectWithZeroJitterDoesNotPanic guards against a real retry-go
// footgun: RandomDelay (part of its default DelayType) calls
// rand.Int63n(maxJitter) unconditionally and panics with "invalid argument
// to Int63n" if maxJitter is 0. MaxJitter: 0 is a valid config value (no
// jitter), and must not crash the reconnect loop.
func TestReconnectWithZeroJitterDoesNotPanic(t *testing.T) {
	s := newTestReconnectStream(config.ReconnectConfig{
		Enabled: true, InitialDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond,
		MaxJitter: 0, MaxElapsed: 5 * time.Second,
	})
	conn := newReconnectFakeConn(1) // fails once, so at least one delay is computed
	s.conn = conn

	ok := s.reconnect(context.Background())

	if !ok {
		t.Fatal("expected reconnect to succeed once Connect() stops failing")
	}
}

func TestReconnectStopsWhenContextCancelledMidWait(t *testing.T) {
	s := newTestReconnectStream(config.ReconnectConfig{
		Enabled: true, InitialDelay: time.Second, MaxDelay: time.Second, MaxJitter: 0,
		MaxElapsed: time.Minute, // long enough that only context cancellation should end this test quickly
	})
	conn := newReconnectFakeConn(1_000_000) // never succeeds, forcing a wait after attempt 1
	s.conn = conn

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	ok := s.reconnect(ctx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected reconnect to stop once its context is cancelled")
	}
	if elapsed > time.Second {
		t.Fatalf("context cancellation (as stream.Close() triggers) should interrupt the reconnect wait almost immediately, took %s", elapsed)
	}
}

// alwaysCorruptConn simulates a connection that never recovers: reads always
// fail, and so does every reconnect attempt.
type alwaysCorruptConn struct{}

func (alwaysCorruptConn) Connect(context.Context) error { return errDialRefused }

func (alwaysCorruptConn) IsClosed() bool { return true }

func (alwaysCorruptConn) Close(context.Context) error { return nil }

func (alwaysCorruptConn) Frontend() *pgproto3.Frontend { return nil }

func (alwaysCorruptConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }

func (alwaysCorruptConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, io.ErrUnexpectedEOF
}

// TestSinkReportsFatalErrorAfterReconnectBudgetExhausted proves A4 doesn't
// change the ultimate failure mode when reconnecting is enabled but never
// succeeds: the stream reports ErrStreamCorrupted on Err() (replacing the
// panic this used to raise, see T3.3), just after trying to recover
// in-process first.
func TestSinkReportsFatalErrorAfterReconnectBudgetExhausted(t *testing.T) {
	s := newTestReconnectStream(config.ReconnectConfig{
		Enabled: true, InitialDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond,
		MaxJitter: time.Millisecond, MaxElapsed: 20 * time.Millisecond,
	})
	s.conn = alwaysCorruptConn{}

	s.sink(context.Background())

	select {
	case err := <-s.Err():
		if !errors.Is(err, ErrStreamCorrupted) {
			t.Fatalf("unexpected fatal error: %v", err)
		}
	default:
		t.Fatal("expected a fatal error on Err() after reconnect budget exhausted")
	}
	if !s.closed.Load() {
		t.Fatal("expected the stream to close before reporting the fatal error")
	}
}

// recoversOnSecondConnectConn simulates a real disconnect/reconnect cycle
// end-to-end through sink(): the first read fails (triggering reconnect), the
// first Connect() attempt fails, the second succeeds, setup()'s handshake
// then succeeds, and once the sink loop resumes it observes a clean shutdown
// (as Close() would cause) rather than another failure - proving sink()
// itself, not just reconnect() in isolation, resumes normally with no panic.
type recoversOnSecondConnectConn struct {
	mu           sync.Mutex
	connectCalls int
	recvCalls    int
	closedFlag   *atomic.Bool
}

func (c *recoversOnSecondConnectConn) Connect(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connectCalls++
	if c.connectCalls == 1 {
		return errDialRefused
	}
	return nil
}

func (c *recoversOnSecondConnectConn) IsClosed() bool { return false }

func (c *recoversOnSecondConnectConn) Close(context.Context) error { return nil }

func (c *recoversOnSecondConnectConn) Frontend() *pgproto3.Frontend {
	return pgproto3.NewFrontend(strings.NewReader(""), io.Discard)
}

func (c *recoversOnSecondConnectConn) Exec(context.Context, string) *pgconn.MultiResultReader {
	return nil
}

func (c *recoversOnSecondConnectConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	c.mu.Lock()
	c.recvCalls++
	n := c.recvCalls
	c.mu.Unlock()

	switch n {
	case 1:
		// sinkLoop's first read: looks like an unexpected disconnect.
		return nil, io.ErrUnexpectedEOF
	case 2:
		// replication.Test(), called from setup() during the successful
		// reconnect attempt, expects this on the handshake.
		return &pgproto3.CopyBothResponse{}, nil
	default:
		// sinkLoop resumes after reconnecting; simulate the user closing the
		// stream right after it came back up, same as a clean shutdown.
		c.closedFlag.Store(true)
		return nil, io.EOF
	}
}

func TestSinkRecoversAfterReconnectAndDoesNotPanic(t *testing.T) {
	s := newTestReconnectStream(config.ReconnectConfig{
		Enabled: true, InitialDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond,
		MaxJitter: time.Millisecond, MaxElapsed: 5 * time.Second,
	})
	conn := &recoversOnSecondConnectConn{closedFlag: &s.closed}
	s.conn = conn

	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
		}()
		s.sink(context.Background())
	}()

	if didPanic {
		t.Fatal("sink() panicked even though reconnect succeeded on the second attempt")
	}
	if conn.connectCalls != 2 {
		t.Fatalf("expected exactly 2 Connect() calls (1 failed + 1 succeeded), got %d", conn.connectCalls)
	}
}

// TestSinkReportsFatalErrorImmediatelyWhenReconnectDisabled documents,
// alongside the pre-existing TestSinkReportsFatalErrorAfterClosingOnUnexpectedDisconnect,
// that leaving Reconnect.Enabled false (the default) preserves today's exact
// behavior other than the panic->error change from T3.3: an unexpected
// disconnect reports ErrStreamCorrupted immediately with no reconnect
// attempt at all.
func TestSinkReportsFatalErrorImmediatelyWhenReconnectDisabled(t *testing.T) {
	s := newTestReconnectStream(config.ReconnectConfig{Enabled: false})
	s.conn = alwaysCorruptConn{}

	s.sink(context.Background())

	select {
	case err := <-s.Err():
		if !errors.Is(err, ErrStreamCorrupted) {
			t.Fatalf("unexpected fatal error: %v", err)
		}
	default:
		t.Fatal("expected a fatal error on Err() after unexpected disconnect when reconnect is disabled")
	}
}
