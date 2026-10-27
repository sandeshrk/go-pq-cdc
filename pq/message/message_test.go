package message

import (
	goerrors "errors"
	"testing"
	"time"
)

func TestNewSkipsMessagesThatHaveNoRow(t *testing.T) {
	for _, b := range []Type{TypeByte, OriginByte, LogicalByte} {
		_, err := New([]byte{byte(b)}, false, time.Time{}, nil, false)
		if !goerrors.Is(err, ErrMessageNotDecoded) {
			t.Fatalf("byte %q: got %v", b, err)
		}
	}
}

func TestNewStillRejectsAnUnknownByte(t *testing.T) {
	_, err := New([]byte{'Z'}, false, time.Time{}, nil, false)
	if err == nil {
		t.Fatal("expected an error for an unknown message byte")
	}
	if goerrors.Is(err, ErrMessageNotDecoded) {
		t.Fatal("an unknown message byte was treated as a message with no row")
	}
}
