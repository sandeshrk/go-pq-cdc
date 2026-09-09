package publication

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeDesiredPublicationTables(t *testing.T) {
	t.Run("no configured filters means no change", func(t *testing.T) {
		actual := Tables{{Schema: "public", Name: "users"}}

		desired, changed := mergeDesiredPublicationTables(actual, Tables{}, false)
		assert.False(t, changed)
		assert.Equal(t, actual, desired)
	})

	t.Run("updates only the configured table's filter, preserving others", func(t *testing.T) {
		actual := Tables{
			{Schema: "public", Name: "users", PublicationFilter: "status = 'active'"},
			{Schema: "public", Name: "orders", Columns: []string{"id", "total"}},
		}
		configured := Tables{{Schema: "public", Name: "users", PublicationFilter: "status = 'inactive'"}}

		desired, changed := mergeDesiredPublicationTables(actual, configured, false)
		assert.True(t, changed)
		assert.Equal(t, "status = 'inactive'", desired[0].PublicationFilter)
		assert.Equal(t, actual[1], desired[1], "table not in configured must be carried over unchanged")
	})

	t.Run("no-op when configured filter already matches live filter", func(t *testing.T) {
		actual := Tables{{Schema: "public", Name: "users", PublicationFilter: "status = 'active'"}}
		configured := Tables{{Schema: "public", Name: "users", PublicationFilter: "status = 'active'"}}

		_, changed := mergeDesiredPublicationTables(actual, configured, false)
		assert.False(t, changed)
	})

	t.Run("configured table missing from live publication gets added", func(t *testing.T) {
		actual := Tables{{Schema: "public", Name: "orders"}}
		configured := Tables{
			{Schema: "public", Name: "orders"},
			{Schema: "public", Name: "users", PublicationFilter: "status = 'active'"},
		}

		desired, changed := mergeDesiredPublicationTables(actual, configured, false)
		assert.True(t, changed)
		require.Len(t, desired, 2)
		assert.Equal(t, actual[0], desired[0], "already-live table must be carried over unchanged")
		assert.Equal(t, "users", desired[1].Name)
		assert.Equal(t, "status = 'active'", desired[1].PublicationFilter)
	})

	t.Run("ClearPublicationFilter sentinel on a brand new table resolves to no filter", func(t *testing.T) {
		actual := Tables{}
		configured := Tables{{Schema: "public", Name: "users", PublicationFilter: ClearPublicationFilter}}

		desired, changed := mergeDesiredPublicationTables(actual, configured, false)
		assert.True(t, changed)
		require.Len(t, desired, 1)
		assert.Empty(t, desired[0].PublicationFilter)
	})

	t.Run("empty configured filter leaves the live filter untouched", func(t *testing.T) {
		actual := Tables{{Schema: "public", Name: "users", PublicationFilter: "status = 'active'"}}
		configured := Tables{{Schema: "public", Name: "users"}}

		desired, changed := mergeDesiredPublicationTables(actual, configured, false)
		assert.False(t, changed)
		assert.Equal(t, "status = 'active'", desired[0].PublicationFilter)
	})

	t.Run("ClearPublicationFilter sentinel removes the live filter", func(t *testing.T) {
		actual := Tables{{Schema: "public", Name: "users", PublicationFilter: "status = 'active'"}}
		configured := Tables{{Schema: "public", Name: "users", PublicationFilter: ClearPublicationFilter}}

		desired, changed := mergeDesiredPublicationTables(actual, configured, false)
		assert.True(t, changed)
		assert.Empty(t, desired[0].PublicationFilter)
	})

	t.Run("live table missing from config is kept when pruneTables is false", func(t *testing.T) {
		actual := Tables{{Schema: "public", Name: "orders"}}

		desired, changed := mergeDesiredPublicationTables(actual, Tables{}, false)
		assert.False(t, changed)
		assert.Equal(t, actual, desired)
	})

	t.Run("live table missing from config is dropped when pruneTables is true", func(t *testing.T) {
		actual := Tables{
			{Schema: "public", Name: "users"},
			{Schema: "public", Name: "orders"},
		}
		configured := Tables{{Schema: "public", Name: "users"}}

		desired, changed := mergeDesiredPublicationTables(actual, configured, true)
		assert.True(t, changed)
		assert.Len(t, desired, 1)
		assert.Equal(t, "users", desired[0].Name)
	})
}
