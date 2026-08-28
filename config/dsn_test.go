package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDSNTimeouts(t *testing.T) {
	base := Config{
		Host:     "127.0.0.1",
		Port:     5432,
		Username: "cdc_user",
		Password: "cdc_pass",
		Database: "cdc_db",
	}

	t.Run("no timeouts configured keeps the plain dsn", func(t *testing.T) {
		cfg := base
		assert.Equal(t, "postgres://cdc_user:cdc_pass@127.0.0.1:5432/cdc_db", cfg.DSN())
		assert.Equal(t, "postgres://cdc_user:cdc_pass@127.0.0.1:5432/cdc_db?replication=database", cfg.ReplicationDSN())
		assert.Equal(t, "postgres://cdc_user:cdc_pass@127.0.0.1:5432/cdc_db?sslmode=disable", cfg.DSNWithoutSSL())
	})

	t.Run("connect timeout applies to every dsn", func(t *testing.T) {
		cfg := base
		cfg.ConnectTimeout = 10 * time.Second

		assert.Contains(t, cfg.DSN(), "connect_timeout=10")
		assert.Contains(t, cfg.ReplicationDSN(), "connect_timeout=10")
		assert.Contains(t, cfg.ReplicationDSN(), "replication=database")
		assert.Contains(t, cfg.DSNWithoutSSL(), "connect_timeout=10")
		assert.Contains(t, cfg.DSNWithoutSSL(), "sslmode=disable")
	})

	t.Run("sub-second connect timeout is rounded up to one second", func(t *testing.T) {
		cfg := base
		cfg.ConnectTimeout = 200 * time.Millisecond

		assert.Contains(t, cfg.DSN(), "connect_timeout=1")
	})

	t.Run("lock timeout is set in milliseconds and only on regular connections", func(t *testing.T) {
		cfg := base
		cfg.LockTimeout = 5 * time.Second

		assert.Contains(t, cfg.DSN(), "options=-c+lock_timeout%3D5000")
		assert.NotContains(t, cfg.ReplicationDSN(), "lock_timeout")
	})

	t.Run("default sets a connect timeout but leaves lock timeout off", func(t *testing.T) {
		cfg := base
		cfg.SetDefault()

		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout)
		assert.Zero(t, cfg.LockTimeout)
		assert.NotContains(t, cfg.DSN(), "lock_timeout")
	})
}
