package retry

import (
	"time"

	"github.com/avast/retry-go/v4"
)

var DefaultOptions = []retry.Option{
	retry.LastErrorOnly(true),
	retry.Delay(time.Second),
	retry.DelayType(retry.FixedDelay),
}

type Config[T any] struct {
	If      func(err error) bool
	Options []retry.Option
}

// Do runs f under rc.Options. If rc.If is set, it is honored via
// retry.RetryIf: f is retried only while If(err) reports true for the most
// recent failure. If rc.If is nil, every error is retried (the retry-go
// default when no RetryIf option is supplied).
func (rc Config[T]) Do(f retry.RetryableFuncWithData[T]) (T, error) {
	options := append([]retry.Option(nil), rc.Options...)
	if rc.If != nil {
		options = append(options, retry.RetryIf(rc.If))
	}
	return retry.DoWithData(f, options...)
}

func OnErrorConfig[T any](attemptCount uint, check func(error) bool) Config[T] {
	cfg := Config[T]{
		If:      check,
		Options: []retry.Option{retry.Attempts(attemptCount)},
	}
	cfg.Options = append(cfg.Options, DefaultOptions...)
	return cfg
}
