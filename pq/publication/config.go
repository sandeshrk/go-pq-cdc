package publication

import (
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
)

type Config struct {
	Name              string     `json:"name" yaml:"name"`
	Operations        Operations `json:"operations" yaml:"operations"`
	Tables            Tables     `json:"tables" yaml:"tables"`
	CreateIfNotExists bool       `json:"createIfNotExists" yaml:"createIfNotExists"`
	// PruneTables, when true, treats Tables as the full authoritative table
	// membership for this publication: any live publication member missing
	// from Tables is removed via ALTER PUBLICATION ... SET TABLE. Tables
	// present in config but not yet live are NOT added automatically -- this
	// only removes/updates members already in the publication.
	PruneTables bool `json:"pruneTables,omitempty" yaml:"pruneTables,omitempty"`
	AllTables   bool `json:"-" yaml:"-"`
}

func (c Config) Validate() error {
	var err error
	if strings.TrimSpace(c.Name) == "" {
		err = errors.Join(err, errors.New("publication name cannot be empty"))
	}

	if !c.CreateIfNotExists {
		return err
	}

	if validateErr := c.Tables.Validate(); validateErr != nil {
		err = errors.Join(err, validateErr)
	}

	if validateErr := c.Operations.Validate(); validateErr != nil {
		err = errors.Join(err, validateErr)
	}

	return err
}

func (c Config) createQuery() string {
	sqlStatement := fmt.Sprintf(`CREATE PUBLICATION %s`, pq.QuoteIdentifier(c.Name))

	sqlStatement += " FOR TABLE " + strings.Join(tableClauses(c.Tables), ", ")
	sqlStatement += fmt.Sprintf(" WITH (publish = '%s', publish_via_partition_root = %t)", c.Operations.String(), hasPartitionedTable(c.Tables))

	return sqlStatement
}

// setTableQuery rebuilds the publication's full table membership (columns and
// row filters) via ALTER PUBLICATION ... SET TABLE. Per Postgres docs, SET TABLE
// replaces the ENTIRE table list of the publication, so tables must include
// every table that should remain published, not just the ones whose filter
// changed.
func (c Config) setTableQuery(tables Tables) string {
	return fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s", pq.QuoteIdentifier(c.Name), strings.Join(tableClauses(tables), ", "))
}

// tableClauses renders each table's `schema.table [(cols)] [WHERE (filter)]`
// clause, shared by CREATE PUBLICATION ... FOR TABLE and ALTER PUBLICATION ... SET TABLE.
func tableClauses(tables Tables) []string {
	clauses := make([]string, len(tables))
	for i, table := range tables {
		clause := fmt.Sprintf("%s.%s", pq.QuoteIdentifier(table.Schema), pq.QuoteIdentifier(table.Name))
		if len(table.Columns) > 0 {
			clause += fmt.Sprintf("(%s)", strings.Join(table.Columns, ", "))
		}
		if filter := table.PublicationFilter; filter != "" && filter != ClearPublicationFilter {
			clause += fmt.Sprintf(" WHERE (%s)", filter)
		}
		clauses[i] = clause
	}
	return clauses
}

func hasPartitionedTable(tables Tables) bool {
	for _, table := range tables {
		if table.Partitioned {
			return true
		}
	}
	return false
}

func (c Config) infoQuery() string {
	q := fmt.Sprintf(`WITH publication_details AS (
    SELECT
        p.oid AS pubid,
        p.pubname,
        p.puballtables,
        p.pubinsert,
        p.pubupdate,
        p.pubdelete,
        p.pubtruncate
    FROM pg_publication p
    WHERE p.pubname = '%s'
	),
	expanded_tables AS (
		SELECT
			pubname,
			array_agg(schemaname || '.' || tablename) AS tables
		FROM pg_publication_tables
		WHERE pubname = '%s'
		GROUP BY pubname
	)
	SELECT
		pd.pubname,
		pd.puballtables,
		pd.pubinsert,
		pd.pubupdate,
		pd.pubdelete,
		pd.pubtruncate,
		COALESCE(et.tables, ARRAY[]::text[]) AS pubtables
	FROM publication_details pd
	LEFT JOIN expanded_tables et ON pd.pubname = et.pubname;`, c.Name, c.Name)
	return q
}
