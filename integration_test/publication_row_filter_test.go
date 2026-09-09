package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPublicationRowFilterEndToEnd verifies a native publication row filter
// (embedded in CREATE PUBLICATION) actually filters the live CDC stream, and
// that Postgres performs the documented UPDATE<->INSERT/DELETE boundary
// transform when a row moves across the filter boundary.
func TestPublicationRowFilterEndToEnd(t *testing.T) {
	ctx := context.Background()

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS row_filter_orders;
		CREATE TABLE row_filter_orders (
			id SERIAL PRIMARY KEY,
			status TEXT NOT NULL,
			amount NUMERIC(10,2) NOT NULL
		);
	`)
	require.NoError(t, err)

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_row_filter_e2e"
	cdcCfg.Publication.Name = "pub_row_filter_e2e"
	cdcCfg.Publication.CreateIfNotExists = true
	cdcCfg.Publication.Operations = publication.Operations{
		publication.OperationInsert,
		publication.OperationUpdate,
		publication.OperationDelete,
	}
	cdcCfg.Publication.Tables = []publication.Table{
		{
			Name:              "row_filter_orders",
			Schema:            "public",
			ReplicaIdentity:   publication.ReplicaIdentityFull,
			PublicationFilter: "status = 'active'",
		},
	}

	insertCh := make(chan *format.Insert, 50)
	updateCh := make(chan *format.Update, 50)
	deleteCh := make(chan *format.Delete, 50)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert:
			insertCh <- msg
		case *format.Update:
			updateCh <- msg
		case *format.Delete:
			deleteCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		_ = pgExec(ctx, postgresConn, "DROP TABLE IF EXISTS row_filter_orders")
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	t.Run("insert not matching filter is never published", func(t *testing.T) {
		require.NoError(t, pgExec(ctx, postgresConn,
			"INSERT INTO row_filter_orders(id, status, amount) VALUES(1, 'pending', 10.00)"))

		select {
		case msg := <-insertCh:
			t.Fatalf("expected no insert for non-matching row, got: %+v", msg.Decoded)
		case <-time.After(2 * time.Second):
		}
	})

	t.Run("insert matching filter is published", func(t *testing.T) {
		require.NoError(t, pgExec(ctx, postgresConn,
			"INSERT INTO row_filter_orders(id, status, amount) VALUES(2, 'active', 20.00)"))

		select {
		case msg := <-insertCh:
			assert.Equal(t, int32(2), msg.Decoded["id"])
			assert.Equal(t, "active", msg.Decoded["status"])
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for matching insert")
		}
	})

	t.Run("update leaving the filtered set is transformed into a delete", func(t *testing.T) {
		require.NoError(t, pgExec(ctx, postgresConn,
			"UPDATE row_filter_orders SET status = 'cancelled' WHERE id = 2"))

		select {
		case msg := <-deleteCh:
			assert.Equal(t, int32(2), msg.OldDecoded["id"])
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for delete (update-out-of-filter transform)")
		}

		select {
		case msg := <-updateCh:
			t.Fatalf("expected no plain update once row left the filtered set, got: %+v", msg.NewDecoded)
		case <-time.After(2 * time.Second):
		}
	})

	t.Run("update entering the filtered set is transformed into an insert", func(t *testing.T) {
		require.NoError(t, pgExec(ctx, postgresConn,
			"UPDATE row_filter_orders SET status = 'active' WHERE id = 1"))

		select {
		case msg := <-insertCh:
			assert.Equal(t, int32(1), msg.Decoded["id"])
			assert.Equal(t, "active", msg.Decoded["status"])
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for insert (update-into-filter transform)")
		}
	})
}

// TestApplyPublicationFiltersReconciliation verifies that an existing
// publication's live row filter can be added, changed, and cleared via
// ApplyPublicationFilters (ALTER PUBLICATION ... SET TABLE), without
// recreating the publication.
func TestApplyPublicationFiltersReconciliation(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer postgresConn.Close(ctx)

	pubName := "pub_row_filter_reconcile"

	require.NoError(t, pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName)))
	require.NoError(t, pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS row_filter_reconcile;
		CREATE TABLE row_filter_reconcile (id SERIAL PRIMARY KEY, status TEXT NOT NULL);
		ALTER TABLE row_filter_reconcile REPLICA IDENTITY FULL;
	`))

	t.Cleanup(func() {
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
		_ = pgExec(ctx, postgresConn, "DROP TABLE IF EXISTS row_filter_reconcile")
	})

	baseCfg := publication.Config{
		Name:              pubName,
		CreateIfNotExists: true,
		Operations:        publication.Operations{"INSERT", "UPDATE", "DELETE"},
		Tables: publication.Tables{
			{Name: "row_filter_reconcile", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull},
		},
	}
	pub := publication.New(baseCfg, postgresConn)
	_, err = pub.Create(ctx)
	require.NoError(t, err)

	tables, err := pub.GetPublicationTables(ctx)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	assert.Empty(t, tables[0].PublicationFilter, "freshly created publication should have no filter")

	t.Run("adds a filter to an already-existing publication", func(t *testing.T) {
		cfg := baseCfg
		cfg.Tables = publication.Tables{
			{Name: "row_filter_reconcile", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull, PublicationFilter: "status = 'active'"},
		}
		require.NoError(t, publication.New(cfg, postgresConn).ApplyPublicationFilters(ctx))

		tables, err := pub.GetPublicationTables(ctx)
		require.NoError(t, err)
		require.Len(t, tables, 1)
		// Postgres re-serializes the expression (adds parens/casts), so it
		// won't match our raw config string byte-for-byte.
		assert.Contains(t, tables[0].PublicationFilter, "active")

		// The table must still be updatable/deletable after SET TABLE applied
		// a filter -- this is the actual regression this reconciliation path
		// needs to guard against.
		require.NoError(t, pgExec(ctx, postgresConn,
			"INSERT INTO row_filter_reconcile(status) VALUES ('active')"))
		require.NoError(t, pgExec(ctx, postgresConn,
			"UPDATE row_filter_reconcile SET status = 'active' WHERE status = 'active'"))
	})

	t.Run("changes an existing filter", func(t *testing.T) {
		cfg := baseCfg
		cfg.Tables = publication.Tables{
			{Name: "row_filter_reconcile", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull, PublicationFilter: "status = 'archived'"},
		}
		require.NoError(t, publication.New(cfg, postgresConn).ApplyPublicationFilters(ctx))

		tables, err := pub.GetPublicationTables(ctx)
		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Contains(t, tables[0].PublicationFilter, "archived")

		require.NoError(t, pgExec(ctx, postgresConn,
			"UPDATE row_filter_reconcile SET status = 'archived' WHERE status = 'active'"))
	})

	t.Run("ClearPublicationFilter sentinel removes the live filter", func(t *testing.T) {
		cfg := baseCfg
		cfg.Tables = publication.Tables{
			{Name: "row_filter_reconcile", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull, PublicationFilter: publication.ClearPublicationFilter},
		}
		require.NoError(t, publication.New(cfg, postgresConn).ApplyPublicationFilters(ctx))

		tables, err := pub.GetPublicationTables(ctx)
		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Empty(t, tables[0].PublicationFilter)
	})
}

// TestApplyPublicationFiltersPreservesUntouchedTables verifies that
// reconciling one table's filter never disturbs a second, already-filtered
// table in the same publication -- whether that second table is explicitly
// configured with no filter opinion, or missing from config entirely
// (PruneTables false).
func TestApplyPublicationFiltersPreservesUntouchedTables(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer postgresConn.Close(ctx)

	pubName := "pub_row_filter_preserve"

	require.NoError(t, pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName)))
	require.NoError(t, pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS row_filter_preserve_a, row_filter_preserve_b;
		CREATE TABLE row_filter_preserve_a (id SERIAL PRIMARY KEY, status TEXT NOT NULL);
		CREATE TABLE row_filter_preserve_b (id SERIAL PRIMARY KEY, status TEXT NOT NULL);
		ALTER TABLE row_filter_preserve_a REPLICA IDENTITY FULL;
		ALTER TABLE row_filter_preserve_b REPLICA IDENTITY FULL;
	`))

	t.Cleanup(func() {
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
		_ = pgExec(ctx, postgresConn, "DROP TABLE IF EXISTS row_filter_preserve_a, row_filter_preserve_b")
	})

	// Both tables start with their own filter, set at CREATE time.
	baseCfg := publication.Config{
		Name:              pubName,
		CreateIfNotExists: true,
		Operations:        publication.Operations{"INSERT", "UPDATE", "DELETE"},
		Tables: publication.Tables{
			{Name: "row_filter_preserve_a", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull, PublicationFilter: "status = 'a-initial'"},
			{Name: "row_filter_preserve_b", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull, PublicationFilter: "status = 'b-initial'"},
		},
	}
	pub := publication.New(baseCfg, postgresConn)
	_, err = pub.Create(ctx)
	require.NoError(t, err)

	findFilter := func(tables publication.Tables, name string) string {
		for _, t := range tables {
			if t.Name == name {
				return t.PublicationFilter
			}
		}
		t.Fatalf("table %s not found in publication", name)
		return ""
	}

	t.Run("table B keeps its filter when configured with no filter opinion", func(t *testing.T) {
		cfg := baseCfg
		cfg.Tables = publication.Tables{
			{Name: "row_filter_preserve_a", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull, PublicationFilter: "status = 'a-changed'"},
			{Name: "row_filter_preserve_b", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull}, // no PublicationFilter set
		}
		require.NoError(t, publication.New(cfg, postgresConn).ApplyPublicationFilters(ctx))

		tables, err := pub.GetPublicationTables(ctx)
		require.NoError(t, err)
		require.Len(t, tables, 2)
		assert.Contains(t, findFilter(tables, "row_filter_preserve_a"), "a-changed")
		assert.Contains(t, findFilter(tables, "row_filter_preserve_b"), "b-initial",
			"table B's live filter must survive a reconciliation that only touches table A")

		require.NoError(t, pgExec(ctx, postgresConn, "INSERT INTO row_filter_preserve_b(status) VALUES ('b-initial')"))
		require.NoError(t, pgExec(ctx, postgresConn, "UPDATE row_filter_preserve_b SET status = 'b-initial' WHERE status = 'b-initial'"))
	})

	t.Run("table B keeps its filter when omitted from config entirely (PruneTables false)", func(t *testing.T) {
		cfg := baseCfg
		cfg.Tables = publication.Tables{
			{Name: "row_filter_preserve_a", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull, PublicationFilter: "status = 'a-changed-again'"},
			// row_filter_preserve_b not mentioned at all
		}
		require.NoError(t, publication.New(cfg, postgresConn).ApplyPublicationFilters(ctx))

		tables, err := pub.GetPublicationTables(ctx)
		require.NoError(t, err)
		require.Len(t, tables, 2, "table B must still be a publication member")
		assert.Contains(t, findFilter(tables, "row_filter_preserve_a"), "a-changed-again")
		assert.Contains(t, findFilter(tables, "row_filter_preserve_b"), "b-initial")
	})
}

// TestApplyPublicationFiltersPruneTables verifies that PruneTables drops a
// live publication member that's missing from config, via the same SET TABLE
// reconciliation, while leaving still-configured tables untouched.
func TestApplyPublicationFiltersPruneTables(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer postgresConn.Close(ctx)

	pubName := "pub_row_filter_prune"

	require.NoError(t, pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName)))
	require.NoError(t, pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS row_filter_prune_keep, row_filter_prune_drop;
		CREATE TABLE row_filter_prune_keep (id SERIAL PRIMARY KEY);
		CREATE TABLE row_filter_prune_drop (id SERIAL PRIMARY KEY);
	`))

	t.Cleanup(func() {
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
		_ = pgExec(ctx, postgresConn, "DROP TABLE IF EXISTS row_filter_prune_keep, row_filter_prune_drop")
	})

	baseCfg := publication.Config{
		Name:              pubName,
		CreateIfNotExists: true,
		Operations:        publication.Operations{"INSERT", "UPDATE", "DELETE"},
		Tables: publication.Tables{
			{Name: "row_filter_prune_keep", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityDefault},
			{Name: "row_filter_prune_drop", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityDefault},
		},
	}
	pub := publication.New(baseCfg, postgresConn)
	_, err = pub.Create(ctx)
	require.NoError(t, err)

	tables, err := pub.GetPublicationTables(ctx)
	require.NoError(t, err)
	require.Len(t, tables, 2, "publication should start with both tables")

	pruneCfg := baseCfg
	pruneCfg.PruneTables = true
	pruneCfg.Tables = publication.Tables{
		{Name: "row_filter_prune_keep", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityDefault},
	}
	require.NoError(t, publication.New(pruneCfg, postgresConn).ApplyPublicationFilters(ctx))

	tables, err = pub.GetPublicationTables(ctx)
	require.NoError(t, err)
	require.Len(t, tables, 1, "table missing from config should have been dropped")
	assert.Equal(t, "row_filter_prune_keep", tables[0].Name)
}

// TestApplyPublicationFiltersAddsMissingTable verifies that a table present
// in config but not yet a member of an already-existing publication gets
// added (with its configured filter), and that it's fully usable afterward.
func TestApplyPublicationFiltersAddsMissingTable(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer postgresConn.Close(ctx)

	pubName := "pub_row_filter_add_table"

	require.NoError(t, pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName)))
	require.NoError(t, pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS row_filter_add_existing, row_filter_add_new;
		CREATE TABLE row_filter_add_existing (id SERIAL PRIMARY KEY);
		CREATE TABLE row_filter_add_new (id SERIAL PRIMARY KEY, status TEXT NOT NULL);
		ALTER TABLE row_filter_add_new REPLICA IDENTITY FULL;
	`))

	t.Cleanup(func() {
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
		_ = pgExec(ctx, postgresConn, "DROP TABLE IF EXISTS row_filter_add_existing, row_filter_add_new")
	})

	baseCfg := publication.Config{
		Name:              pubName,
		CreateIfNotExists: true,
		Operations:        publication.Operations{"INSERT", "UPDATE", "DELETE"},
		Tables: publication.Tables{
			{Name: "row_filter_add_existing", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityDefault},
		},
	}
	pub := publication.New(baseCfg, postgresConn)
	_, err = pub.Create(ctx)
	require.NoError(t, err)

	tables, err := pub.GetPublicationTables(ctx)
	require.NoError(t, err)
	require.Len(t, tables, 1, "publication should start with only the pre-existing table")

	cfg := baseCfg
	cfg.Tables = publication.Tables{
		{Name: "row_filter_add_existing", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityDefault},
		{Name: "row_filter_add_new", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull, PublicationFilter: "status = 'active'"},
	}
	require.NoError(t, publication.New(cfg, postgresConn).ApplyPublicationFilters(ctx))

	tables, err = pub.GetPublicationTables(ctx)
	require.NoError(t, err)
	require.Len(t, tables, 2, "the new table should now be a publication member")

	var newTable publication.Table
	for _, tbl := range tables {
		if tbl.Name == "row_filter_add_new" {
			newTable = tbl
		}
	}
	assert.Contains(t, newTable.PublicationFilter, "active")

	// The added table must be fully usable: insert/update through it.
	require.NoError(t, pgExec(ctx, postgresConn, "INSERT INTO row_filter_add_new(status) VALUES ('active')"))
	require.NoError(t, pgExec(ctx, postgresConn, "UPDATE row_filter_add_new SET status = 'cancelled' WHERE status = 'active'"))
}

// TestAlterPublicationSafeWhileStreamActive answers "is altering the
// publication safe while a replication stream is live": it adds a brand new
// table to an already-running connector's publication via
// ApplyPublicationFilters WHILE the connector is actively streaming, using a
// separate connection (simulating another pod/process doing so concurrently),
// then verifies the running stream is undisturbed and picks up the new
// table's changes without a reconnect.
func TestAlterPublicationSafeWhileStreamActive(t *testing.T) {
	ctx := context.Background()

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	require.NoError(t, pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS stream_active_existing, stream_active_new;
		CREATE TABLE stream_active_existing (id SERIAL PRIMARY KEY);
		CREATE TABLE stream_active_new (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
	`))

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_alter_pub_while_active"
	cdcCfg.Publication.Name = "pub_alter_while_active"
	cdcCfg.Publication.CreateIfNotExists = true
	cdcCfg.Publication.Operations = publication.Operations{
		publication.OperationInsert,
		publication.OperationUpdate,
		publication.OperationDelete,
	}
	cdcCfg.Publication.Tables = []publication.Table{
		{Name: "stream_active_existing", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityDefault},
	}

	insertCh := make(chan *format.Insert, 50)
	handlerFunc := func(ctx *replication.ListenerContext) {
		if msg, ok := ctx.Message.(*format.Insert); ok {
			insertCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		_ = pgExec(ctx, postgresConn, "DROP TABLE IF EXISTS stream_active_existing, stream_active_new")
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	// Sanity: the stream works before the ALTER.
	require.NoError(t, pgExec(ctx, postgresConn, "INSERT INTO stream_active_existing DEFAULT VALUES"))
	select {
	case <-insertCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for pre-ALTER insert")
	}

	// Alter the publication -- adding a table -- via a SEPARATE connection,
	// while the connector's replication stream is actively connected and
	// streaming, simulating a concurrent config change from another process.
	alterConn, err := newPostgresConn()
	require.NoError(t, err)
	defer alterConn.Close(ctx)

	alterCfg := cdcCfg.Publication
	alterCfg.Tables = []publication.Table{
		{Name: "stream_active_existing", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityDefault},
		{Name: "stream_active_new", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityDefault},
	}
	require.NoError(t, publication.New(alterCfg, alterConn).ApplyPublicationFilters(ctx),
		"ALTER PUBLICATION ... SET TABLE must succeed while a walsender is actively streaming")

	// The existing table's stream must still be alive and undisturbed.
	require.NoError(t, pgExec(ctx, postgresConn, "INSERT INTO stream_active_existing DEFAULT VALUES"))
	select {
	case <-insertCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for post-ALTER insert on the pre-existing table; stream may have been disrupted")
	}

	// The newly added table must stream too, with no reconnect required.
	require.NoError(t, pgExec(ctx, postgresConn, "INSERT INTO stream_active_new(name) VALUES ('hello')"))
	select {
	case msg := <-insertCh:
		assert.Equal(t, "stream_active_new", msg.TableName)
		assert.Equal(t, "hello", msg.Decoded["name"])
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for insert on the newly added table")
	}
}

