package publication

import (
	"context"
	"fmt"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
)

// ApplyPublicationFilters reconciles native Postgres publication row filters
// (PG 15+) and, if PruneTables is set, table membership itself, against what's
// live. New publications get filters embedded directly in CREATE PUBLICATION,
// so this only does work against an already-existing publication.
//
// ALTER PUBLICATION ... SET TABLE replaces the publication's ENTIRE table
// list, so the live table set is read first: tables not mentioned in config
// are carried over unchanged unless PruneTables is set, in which case they're
// dropped. Tables present in config but not yet live are never added here --
// that only happens via the initial CREATE PUBLICATION.
func (c *Publication) ApplyPublicationFilters(ctx context.Context) error {
	if !c.cfg.PruneTables && !hasConfiguredFilter(c.cfg.Tables) {
		return nil
	}

	actual, err := c.GetPublicationTables(ctx)
	if err != nil {
		return err
	}

	desired, changed := mergeDesiredPublicationTables(actual, c.cfg.Tables, c.cfg.PruneTables)
	if !changed {
		return nil
	}

	resultReader := c.conn.Exec(ctx, c.cfg.setTableQuery(desired))
	_, err = resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "publication set table result")
	}
	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "publication set table result reader close")
	}

	logger.Info("publication table filters updated", "publication", c.cfg.Name)

	return nil
}

func hasConfiguredFilter(tables Tables) bool {
	for _, t := range tables {
		if t.PublicationFilter != "" {
			return true
		}
	}
	return false
}

// mergeDesiredPublicationTables overlays configured filters onto the live
// table set and, if pruneTables is set, drops live tables missing from
// configured. Reports whether the result differs from actual.
func mergeDesiredPublicationTables(actual, configured Tables, pruneTables bool) (Tables, bool) {
	configuredMap := make(map[string]Table, len(configured))
	for _, t := range configured {
		configuredMap[t.Schema+"."+t.Name] = t
	}

	changed := false
	desired := make(Tables, 0, len(actual))
	for _, t := range actual {
		cfgTable, ok := configuredMap[t.Schema+"."+t.Name]
		if !ok {
			if pruneTables {
				changed = true
				continue
			}
			desired = append(desired, t)
			continue
		}

		resolvedFilter := t.PublicationFilter
		switch cfgTable.PublicationFilter {
		case "":
			// not configured for this table: leave the live filter untouched
		case ClearPublicationFilter:
			resolvedFilter = ""
		default:
			resolvedFilter = cfgTable.PublicationFilter
		}

		if resolvedFilter != t.PublicationFilter {
			changed = true
		}
		t.PublicationFilter = resolvedFilter
		desired = append(desired, t)
	}

	return desired, changed
}

// GetPublicationTables returns the publication's full, live table membership
// (schema, name, published columns, row filter). Queries pg_publication_rel
// directly rather than the pg_publication_tables convenience view: the view's
// attnames always resolves to the full column list even when no explicit
// column list was configured, which would round-trip back out as an explicit
// list on SET TABLE -- and Postgres rejects an explicit column list (even one
// naming every column) combined with REPLICA IDENTITY FULL.
func (c *Publication) GetPublicationTables(ctx context.Context) (Tables, error) {
	query := fmt.Sprintf(`
		SELECT
			n.nspname AS schema_name,
			c.relname AS table_name,
			CASE WHEN pr.prattrs IS NOT NULL THEN (
				SELECT array_agg(a.attname ORDER BY a.attnum)
				FROM pg_attribute a
				WHERE a.attrelid = pr.prrelid AND a.attnum = ANY(pr.prattrs)
			) END AS columns,
			pg_get_expr(pr.prqual, pr.prrelid) AS row_filter
		FROM pg_publication_rel pr
		JOIN pg_class c ON c.oid = pr.prrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_publication p ON p.oid = pr.prpubid
		WHERE p.pubname = %s
	`, pq.QuoteLiteral(c.cfg.Name))

	logger.Debug("executing query: ", query)

	resultReader := c.conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "publication tables result")
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication tables result reader close")
	}

	if len(results) == 0 {
		return nil, nil
	}

	return decodePublicationTablesResult(results)
}

func decodePublicationTablesResult(results []*pgconn.Result) (Tables, error) {
	var res Tables

	for _, result := range results {
		for i := range len(result.Rows) {
			var t Table
			for j, fd := range result.FieldDescriptions {
				v, err := decodeTextColumnData(result.Rows[i][j], fd.DataTypeOID)
				if err != nil {
					return nil, err
				}

				if v == nil {
					continue
				}

				switch fd.Name {
				case "table_name":
					t.Name = v.(string)
				case "schema_name":
					t.Schema = v.(string)
				case "columns":
					for _, col := range v.([]any) {
						t.Columns = append(t.Columns, col.(string))
					}
				case "row_filter":
					t.PublicationFilter = v.(string)
				}
			}
			res = append(res, t)
		}
	}

	return res, nil
}
