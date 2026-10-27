package message

import (
	goerrors "errors"
	"fmt"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/go-playground/errors"
)

const (
	StreamAbortByte  Type = 'A'
	BeginByte        Type = 'B'
	CommitByte       Type = 'C'
	DeleteByte       Type = 'D'
	StreamStopByte   Type = 'E'
	InsertByte       Type = 'I'
	LogicalByte      Type = 'M'
	OriginByte       Type = 'O'
	RelationByte     Type = 'R'
	StreamStartByte  Type = 'S'
	TruncateByte     Type = 'T'
	UpdateByte       Type = 'U'
	TypeByte         Type = 'Y'
	StreamCommitByte Type = 'c'
)

const (
	XLogDataByteID                = 'w'
	PrimaryKeepaliveMessageByteID = 'k'
)

var ErrorByteNotSupported = errors.New("message byte not supported")

// ErrMessageNotDecoded is a replication message this driver recognizes and
// does not decode: a type definition, a replication origin, or a logical
// message. It is not a row, and it is not a corrupt record. The stream
// skips it. A stdlib error so errors.Is can see it through fmt.Errorf.
var ErrMessageNotDecoded = goerrors.New("replication message is not decoded")

type Type uint8

// New decodes a single pgoutput logical replication message. streamedTransaction
// reports whether the stream is currently inside a streamed in-progress
// transaction chunk (proto v2), in which case DML and Relation messages carry an
// XID prefix. The caller owns that state (it flips on the StreamStart/StreamStop/
// StreamCommit/StreamAbort messages this function returns) — it must NOT be
// process-global, since one process may run several replication streams.
// skipMapDecode forwards config.Config.SkipTupleMapDecode to Insert/Update/
// Delete messages; other message types ignore it.
func New(data []byte, streamedTransaction bool, serverTime time.Time, relation map[uint32]*format.Relation, skipMapDecode bool) (any, error) {
	if len(data) == 0 {
		return nil, errors.New("empty message data")
	}
	switch Type(data[0]) {
	case BeginByte:
		return format.NewBegin(data)
	case CommitByte:
		return format.NewCommit(data)
	case InsertByte:
		return format.NewInsert(data, streamedTransaction, relation, serverTime, skipMapDecode)
	case UpdateByte:
		return format.NewUpdate(data, streamedTransaction, relation, serverTime, skipMapDecode)
	case DeleteByte:
		return format.NewDelete(data, streamedTransaction, relation, serverTime, skipMapDecode)
	case TruncateByte:
		return format.NewTruncate(data, streamedTransaction, relation, serverTime)
	case StreamStartByte:
		return format.NewStreamStart(data)
	case StreamStopByte:
		return &format.StreamStop{}, nil
	case StreamAbortByte:
		return format.NewStreamAbort(data)
	case StreamCommitByte:
		return format.NewStreamCommit(data)
	case RelationByte:
		msg, err := format.NewRelation(data, streamedTransaction)
		if err == nil {
			relation[msg.OID] = msg
		}
		return msg, err
	case TypeByte, OriginByte, LogicalByte:
		return nil, fmt.Errorf("%w: %q", ErrMessageNotDecoded, data[0])
	default:
		return nil, errors.Wrap(ErrorByteNotSupported, string(data[0]))
	}
}
