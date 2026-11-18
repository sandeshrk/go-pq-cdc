package slot

import (
	"context"
	goerrors "errors"
	"fmt"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	libpq "github.com/lib/pq"
)

// Snapshot is the atomic snapshot/stream boundary returned by slot creation.
// Its session must remain idle and open until all readers import Name.
type Snapshot struct {
	conn pq.Connection
	Name string
	LSN  pq.LSN
}

// Close releases the export session. Readers that already imported the snapshot
// can finish, but subsequent imports fail. Closing does not drop the slot.
func (s *Snapshot) Close(ctx context.Context) error { return s.conn.Close(ctx) }

// CreateSnapshot creates a slot and retains its exported snapshot session.
// An existing slot is refused unless recreate is true; an active slot is never
// dropped. The caller must serialize snapshot generations and close the result.
func (s *Slot) CreateSnapshot(ctx context.Context, recreate bool) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, ErrorSlotClosed
	}
	if err := s.conn.Connect(ctx); err != nil {
		return nil, errors.Wrap(err, "slot connect")
	}
	defer s.conn.Close(ctx)
	info, err := s.infoLocked(ctx)
	switch {
	case err == nil:
		if !recreate {
			return nil, errors.Newf("exact snapshot requires a new replication slot: %s already exists; use snapshot.resnapshot to recreate it", s.cfg.Name)
		}
		if info.Active {
			return nil, errors.Newf("cannot recreate active replication slot %s for snapshot", s.cfg.Name)
		}
		if err := pq.ExecSQL(ctx, s.conn, "SELECT pg_drop_replication_slot("+libpq.QuoteLiteral(s.cfg.Name)+")"); err != nil {
			return nil, errors.Wrap(err, "drop snapshot slot")
		}
	case !goerrors.Is(err, ErrorSlotIsNotExists):
		return nil, err
	case !s.cfg.CreateIfNotExists:
		return nil, errors.Wrap(err, "snapshot requires slot creation")
	}
	if err := s.replicationConn.Connect(ctx); err != nil {
		return nil, errors.Wrap(err, "slot replication connect")
	}
	result, err := s.replicationConn.Exec(ctx, fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput EXPORT_SNAPSHOT", libpq.QuoteIdentifier(s.cfg.Name))).ReadAll()
	if err != nil {
		_ = s.replicationConn.Close(ctx)
		return nil, errors.Wrap(err, "create snapshot slot")
	}
	snapshot, err := decodeSnapshot(result)
	if err != nil {
		_ = s.replicationConn.Close(ctx)
		return nil, err
	}
	snapshot.conn = s.replicationConn
	// No further commands may run on this session: even SELECT 1 ends the export.
	logger.Info("replication slot created", "name", s.cfg.Name)
	return snapshot, nil
}

// decodeSnapshot validates the slot creation response; missing boundary fields
// are fatal rather than a reason to take a different database snapshot.
func decodeSnapshot(results []*pgconn.Result) (*Snapshot, error) {
	if len(results) != 1 || len(results[0].Rows) != 1 {
		return nil, errors.New("slot creation did not return one snapshot boundary")
	}
	snapshot := &Snapshot{}
	for i, field := range results[0].FieldDescriptions {
		if i >= len(results[0].Rows[0]) {
			return nil, errors.New("incomplete slot snapshot response")
		}
		value := string(results[0].Rows[0][i])
		switch field.Name {
		case "snapshot_name":
			snapshot.Name = value
		case "consistent_point":
			lsn, err := pq.ParseLSN(value)
			if err != nil {
				return nil, errors.Wrap(err, "parse slot consistent point")
			}
			snapshot.LSN = lsn
		}
	}
	if snapshot.Name == "" || snapshot.LSN == 0 {
		return nil, errors.New("slot creation returned no exported snapshot or consistent point")
	}
	return snapshot, nil
}
