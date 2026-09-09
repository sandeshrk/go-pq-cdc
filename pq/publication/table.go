package publication

import (
	"slices"
	"strings"

	"github.com/go-playground/errors"
)

// SnapshotPartitionStrategy defines how a table should be partitioned during snapshot.
// If empty, the strategy is auto-detected based on primary key type.
type SnapshotPartitionStrategy string

const (
	// SnapshotPartitionStrategyAuto lets the system decide based on PK type (default)
	SnapshotPartitionStrategyAuto SnapshotPartitionStrategy = ""
	// SnapshotPartitionStrategyIntegerRange uses MIN/MAX range for integer PKs
	SnapshotPartitionStrategyIntegerRange SnapshotPartitionStrategy = "integer_range"
	// SnapshotPartitionStrategyCTIDBlock uses PostgreSQL physical block locations
	SnapshotPartitionStrategyCTIDBlock SnapshotPartitionStrategy = "ctid_block"
	// SnapshotPartitionStrategyOffset uses LIMIT/OFFSET (slow, fallback)
	SnapshotPartitionStrategyOffset SnapshotPartitionStrategy = "offset"
)

// ValidSnapshotPartitionStrategies contains all valid partition strategy options
var ValidSnapshotPartitionStrategies = []SnapshotPartitionStrategy{
	SnapshotPartitionStrategyAuto,
	SnapshotPartitionStrategyIntegerRange,
	SnapshotPartitionStrategyCTIDBlock,
	SnapshotPartitionStrategyOffset,
}

type Table struct {
	Name                 string `json:"name" yaml:"name"`
	ReplicaIdentity      string `json:"replicaIdentity" yaml:"replicaIdentity"`
	ReplicaIdentityIndex string `json:"replicaIdentityIndex,omitempty" yaml:"replicaIdentityIndex,omitempty"`
	Schema               string `json:"schema,omitempty" yaml:"schema,omitempty"`
	// SnapshotPartitionStrategy allows overriding the auto-detected partition strategy.
	// Useful when integer PKs are hash-based (not sequential) and range partitioning performs poorly.
	// Options: "" (auto), "integer_range", "ctid_block", "offset"
	SnapshotPartitionStrategy SnapshotPartitionStrategy `json:"snapshotPartitionStrategy,omitempty" yaml:"snapshotPartitionStrategy,omitempty"`
	QueryCondition            string                    `json:"queryCondition,omitempty" yaml:"queryCondition,omitempty"`
	Columns                   []string                  `json:"columns,omitempty" yaml:"columns,omitempty"`
	// PublicationFilter is a native Postgres publication row filter (CREATE/ALTER
	// PUBLICATION ... WHERE (...), PG 15+): rows for which it evaluates false/null
	// are never sent over the replication stream. Unlike QueryCondition, this
	// DOES affect CDC, not just the initial snapshot. Set to ClearPublicationFilter
	// to remove an existing live filter (plain "" means "not configured, leave
	// the live filter alone", not "clear it").
	PublicationFilter string `json:"publicationFilter,omitempty" yaml:"publicationFilter,omitempty"`
	// Boolean flag to indicate if the table is partitioned, used for creating the publication on the root table.
	Partitioned bool `json:"partitioned,omitempty" yaml:"partitioned,omitempty"`
}

func (tc Table) Validate() error {
	if strings.TrimSpace(tc.Name) == "" {
		return errors.New("table name cannot be empty")
	}

	if !slices.Contains(ReplicaIdentityOptions, tc.ReplicaIdentity) {
		return errors.Newf("undefined replica identity option. valid identity options are: %v", ReplicaIdentityOptions)
	}

	if tc.ReplicaIdentity == ReplicaIdentityFull && len(tc.Columns) > 0 {
		return errors.New("cannot specify columns when replica identity is FULL. Must be ReplicaIdentityDefault")
	}

	if tc.ReplicaIdentity == ReplicaIdentityUsingIndex {
		if strings.TrimSpace(tc.ReplicaIdentityIndex) == "" {
			return errors.New("replicaIdentityIndex cannot be empty when replicaIdentity is USING INDEX")
		}
	} else if strings.TrimSpace(tc.ReplicaIdentityIndex) != "" {
		return errors.New("replicaIdentityIndex can only be set when replicaIdentity is USING INDEX")
	}

	if tc.QueryCondition != "" {
		if err := ValidateQueryCondition(tc.QueryCondition); err != nil {
			return errors.Wrap(err, "queryCondition")
		}
	}

	if tc.PublicationFilter != "" && tc.PublicationFilter != ClearPublicationFilter {
		if err := ValidateQueryCondition(tc.PublicationFilter); err != nil {
			return errors.Wrap(err, "publicationFilter")
		}
		// NOTHING never exposes old-row values, which Postgres needs to evaluate
		// the filter on UPDATE/DELETE.
		if tc.ReplicaIdentity == ReplicaIdentityNothing {
			return errors.New("cannot specify publicationFilter when replicaIdentity is NOTHING")
		}
	}

	return nil
}

// ClearPublicationFilter, set as Table.PublicationFilter, removes an existing
// live publication row filter. Needed because the zero value "" already means
// "not configured, leave the live filter alone" -- an explicit sentinel is the
// only way to distinguish "no opinion" from "remove it".
const ClearPublicationFilter = "-"

type Tables []Table

const defaultTableSchema = "public"

func (ts Tables) Contains(schema, name string) bool {
	if schema == "" {
		schema = defaultTableSchema
	}
	for _, t := range ts {
		tblSchema := t.Schema
		if tblSchema == "" {
			tblSchema = defaultTableSchema
		}
		if tblSchema == schema && t.Name == name {
			return true
		}
	}
	return false
}

func (ts Tables) Validate() error {
	if len(ts) == 0 {
		return errors.New("at least one table must be defined")
	}

	for _, t := range ts {
		if err := t.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (ts Tables) Diff(tss Tables) Tables {
	res := Tables{}
	tssMap := make(map[string]Table)

	for _, t := range tss {
		tssMap[t.Schema+"."+t.Name] = t
	}

	for _, t := range ts {
		if v, found := tssMap[t.Schema+"."+t.Name]; !found || v.ReplicaIdentity != t.ReplicaIdentity || v.ReplicaIdentityIndex != t.ReplicaIdentityIndex || !slices.Equal(v.Columns, t.Columns) || v.Partitioned != t.Partitioned || v.PublicationFilter != t.PublicationFilter {
			res = append(res, t)
		}
	}

	return res
}
