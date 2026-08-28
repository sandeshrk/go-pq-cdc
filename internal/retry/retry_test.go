package retry

import (
	"errors"
	"testing"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/stretchr/testify/assert"
)

// sentinel errors distinguished by the If predicate in tests below.
var (
	errRetryable    = errors.New("retryable")
	errNotRetryable = errors.New("not retryable")
)

func TestConfigDoHonorsIf(t *testing.T) {
	t.Run("stops as soon as If reports false, without exhausting Attempts", func(t *testing.T) {
		calls := 0
		cfg := Config[int]{
			If: func(err error) bool { return errors.Is(err, errRetryable) },
			Options: []retry.Option{
				retry.Attempts(5),
				retry.Delay(0),
			},
		}

		_, err := cfg.Do(func() (int, error) {
			calls++
			if calls == 2 {
				return 0, errNotRetryable
			}
			return 0, errRetryable
		})

		assert.ErrorIs(t, err, errNotRetryable)
		assert.Equal(t, 2, calls, "should stop the attempt where If first returns false")
	})

	t.Run("retries while If reports true, up to Attempts", func(t *testing.T) {
		calls := 0
		cfg := Config[int]{
			If: func(err error) bool { return errors.Is(err, errRetryable) },
			Options: []retry.Option{
				retry.Attempts(3),
				retry.Delay(0),
			},
		}

		_, err := cfg.Do(func() (int, error) {
			calls++
			return 0, errRetryable
		})

		assert.ErrorIs(t, err, errRetryable)
		assert.Equal(t, 3, calls, "should exhaust all attempts when If keeps returning true")
	})
}

func TestConfigDoWithoutIfRetriesEveryError(t *testing.T) {
	calls := 0
	cfg := Config[int]{
		// If deliberately left nil.
		Options: []retry.Option{
			retry.Attempts(4),
			retry.Delay(0),
		},
	}

	_, err := cfg.Do(func() (int, error) {
		calls++
		return 0, errNotRetryable
	})

	assert.ErrorIs(t, err, errNotRetryable)
	assert.Equal(t, 4, calls, "with no If predicate, every error should be retried up to Attempts")
}

func TestOnErrorConfigMatchesConnectSemantics(t *testing.T) {
	// Characterizes pq/connection.go's connect(): OnErrorConfig(5, nil) must
	// retry any error 5 times with a fixed delay, since nothing sets If.
	calls := 0
	start := time.Now()

	cfg := OnErrorConfig[int](3, nil)
	// Override the delay so the test doesn't take real seconds; the shape
	// (fixed delay, retry-any-error, attempt count) is what's under test.
	cfg.Options = append(cfg.Options, retry.Delay(0))

	_, err := cfg.Do(func() (int, error) {
		calls++
		return 0, errNotRetryable
	})

	assert.ErrorIs(t, err, errNotRetryable)
	assert.Equal(t, 3, calls)
	assert.Nil(t, cfg.If, "connect() passes nil: no RetryIf predicate should be set")
	assert.Less(t, time.Since(start), time.Second, "test should not depend on the real 1s default delay")
}
