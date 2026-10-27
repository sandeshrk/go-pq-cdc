package replication

import (
	"encoding/binary"
	"log/slog"
	"testing"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleXLogDataDoesNotAdvancePastUndecodableMessage(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	confirmed := pq.LSN(10)
	stream.UpdateConfirmedXLogPos(confirmed)
	walEnd := pq.LSN(0x16B6D90)
	out := make(chan *Message, 1)

	t.Run("truncated header", func(t *testing.T) {
		err := stream.handleXLogData([]byte{1, 2, 3}, &messageBuffer{outCh: out}, &streamTxBuffer{})
		require.ErrorIs(t, err, ErrUndecodableMessage)
		assert.Equal(t, pq.LSN(0), stream.LoadXLogPos())
		assert.Equal(t, confirmed, stream.LoadConfirmedXLogPos())
	})

	t.Run("unsupported message", func(t *testing.T) {
		data := make([]byte, 25)
		binary.BigEndian.PutUint64(data[0:], uint64(0x16B6D80)) // WAL start
		binary.BigEndian.PutUint64(data[8:], uint64(walEnd))    // server WAL end
		binary.BigEndian.PutUint64(data[16:], 0)                // server time
		data[24] = 'Z'                                          // unsupported logical message

		err := stream.handleXLogData(data, &messageBuffer{outCh: out}, &streamTxBuffer{})
		require.ErrorIs(t, err, ErrUndecodableMessage)
		assert.Equal(t, pq.LSN(0), stream.LoadXLogPos())
		assert.Equal(t, confirmed, stream.LoadConfirmedXLogPos())
		select {
		case msg := <-out:
			t.Fatalf("published a message that could not be decoded: %v", msg.message)
		default:
		}
	})
}

func TestHandleXLogDataSkipsMessagesThatHaveNoRow(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	confirmed := pq.LSN(10)
	stream.UpdateConfirmedXLogPos(confirmed)
	walEnd := pq.LSN(0x16B6D90)
	out := make(chan *Message, 1)

	for _, b := range []byte{'Y', 'O', 'M'} {
		data := make([]byte, 25)
		binary.BigEndian.PutUint64(data[0:], uint64(0x16B6D80))
		binary.BigEndian.PutUint64(data[8:], uint64(walEnd))
		data[24] = b

		err := stream.handleXLogData(data, &messageBuffer{outCh: out}, &streamTxBuffer{})
		require.NoError(t, err)
		assert.Equal(t, walEnd, stream.LoadXLogPos())
		assert.Equal(t, confirmed, stream.LoadConfirmedXLogPos())
		select {
		case msg := <-out:
			t.Fatalf("published a message that has no row: %v", msg.message)
		default:
		}
	}
}
