package replication

import (
	"errors"
	"testing"
)

// TestSentinelErrorsAreStdlibComparable guards against a real, previously
// undiscovered bug: go-playground/errors.New returns a Chain ([]*Link), a
// slice type Go considers non-comparable. Stdlib errors.Is skips its
// identity check entirely for a non-comparable target and Chain implements
// neither Is(error) bool nor Unwrap() error, so errors.Is(x, ErrorSlotInUse)
// silently returns false unconditionally -- even when x IS ErrorSlotInUse.
// ErrorSlotInUse/ErrorNotConnected are meant to be checked by callers via
// errors.Is (connector.go does exactly this), so they must stay plain
// stdlib errors, not go-playground/errors.Chain values.
func TestSentinelErrorsAreStdlibComparable(t *testing.T) {
	if !errors.Is(ErrorSlotInUse, ErrorSlotInUse) {
		t.Fatal("errors.Is(ErrorSlotInUse, ErrorSlotInUse) is false -- ErrorSlotInUse regressed to a non-comparable error type")
	}
	if !errors.Is(ErrorNotConnected, ErrorNotConnected) {
		t.Fatal("errors.Is(ErrorNotConnected, ErrorNotConnected) is false -- ErrorNotConnected regressed to a non-comparable error type")
	}
}
