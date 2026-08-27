package slot

import (
	"errors"
	"strings"
	"time"
)

type Config struct {
	Name                        string        `json:"name" yaml:"name"`
	SlotActivityCheckerInterval time.Duration `json:"slotActivityCheckerInterval" yaml:"slotActivityCheckerInterval"`
	// ProtoVersion selects the pgoutput logical replication protocol version.
	//   1 – compatible with PostgreSQL 10+; no streaming transaction support (default).
	//   2 – requires PostgreSQL 14+; enables Streaming/Messages below.
	ProtoVersion int `json:"protoVersion" yaml:"protoVersion"`
	// Streaming enables PostgreSQL sending a large in-progress transaction's
	// changes before it commits (requires ProtoVersion 2). Off by default: the
	// driver must buffer an in-progress transaction's messages in memory until
	// STREAM COMMIT/ABORT, with no upper bound, so only enable this once the
	// workload's largest transactions are known to fit in memory.
	Streaming bool `json:"streaming" yaml:"streaming"`
	// Messages enables delivery of generic logical decoding messages emitted
	// via pg_logical_emit_message (requires ProtoVersion 2). This driver does
	// not decode them; off by default.
	Messages          bool `json:"messages" yaml:"messages"`
	CreateIfNotExists bool `json:"createIfNotExists" yaml:"createIfNotExists"`
}

func (c Config) Validate() error {
	var err error
	if strings.TrimSpace(c.Name) == "" {
		err = errors.Join(err, errors.New("slot name cannot be empty"))
	}

	if c.SlotActivityCheckerInterval < 1000 {
		err = errors.Join(err, errors.New("slot activity checker interval cannot be lower than 1000 ms"))
	}

	if c.ProtoVersion != 0 && c.ProtoVersion != 1 && c.ProtoVersion != 2 {
		err = errors.Join(err, errors.New("slot protoVersion must be 1 or 2"))
	}

	return err
}
