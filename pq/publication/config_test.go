package publication

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateQuery(t *testing.T) {
	t.Run("plain table with no columns or filter", func(t *testing.T) {
		cfg := Config{
			Name:       "cdc_publication",
			Operations: Operations{OperationInsert},
			Tables:     Tables{{Schema: "public", Name: "users"}},
		}

		assert.Equal(t,
			`CREATE PUBLICATION "cdc_publication" FOR TABLE "public"."users" WITH (publish = 'INSERT', publish_via_partition_root = false)`,
			cfg.createQuery())
	})

	t.Run("table with columns and a publication filter", func(t *testing.T) {
		cfg := Config{
			Name:       "cdc_publication",
			Operations: Operations{OperationInsert},
			Tables: Tables{{
				Schema:            "public",
				Name:              "users",
				Columns:           []string{"id", "email"},
				PublicationFilter: "status = 'active'",
			}},
		}

		assert.Equal(t,
			`CREATE PUBLICATION "cdc_publication" FOR TABLE "public"."users"(id, email) WHERE (status = 'active') WITH (publish = 'INSERT', publish_via_partition_root = false)`,
			cfg.createQuery())
	})

	t.Run("filter without columns", func(t *testing.T) {
		cfg := Config{
			Name:       "cdc_publication",
			Operations: Operations{OperationInsert},
			Tables: Tables{{
				Schema:            "public",
				Name:              "users",
				PublicationFilter: "status = 'active'",
			}},
		}

		assert.Contains(t, cfg.createQuery(), `"public"."users" WHERE (status = 'active')`)
	})
}

func TestSetTableQuery(t *testing.T) {
	cfg := Config{Name: "cdc_publication"}

	tables := Tables{
		{Schema: "public", Name: "users", PublicationFilter: "status = 'active'"},
		{Schema: "public", Name: "orders"},
	}

	assert.Equal(t,
		`ALTER PUBLICATION "cdc_publication" SET TABLE "public"."users" WHERE (status = 'active'), "public"."orders"`,
		cfg.setTableQuery(tables))
}
