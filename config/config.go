package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

const defaultSchema = "public"

type Config struct {
	Logger           LoggerConfig       `json:"logger" yaml:"logger"`
	Host             string             `json:"host" yaml:"host"`
	Username         string             `json:"username" yaml:"username"`
	Password         string             `json:"password" yaml:"password"`
	Database         string             `json:"database" yaml:"database"`
	Publication      publication.Config `json:"publication" yaml:"publication"`
	Heartbeat        HeartbeatConfig    `json:"heartbeat" yaml:"heartbeat"`
	Slot             slot.Config        `json:"slot" yaml:"slot"`
	Snapshot         SnapshotConfig     `json:"snapshot" yaml:"snapshot"`
	Port             int                `json:"port" yaml:"port"`
	Metric           MetricConfig       `json:"metric" yaml:"metric"`
	// ConnectTimeout bounds establishing a connection. Without it a blackholed
	// host is bounded only by the OS TCP timeout. Defaults to 10s.
	ConnectTimeout time.Duration `json:"connectTimeout" yaml:"connectTimeout"`
	// LockTimeout bounds how long a statement waits for a lock on the regular
	// (non-replication) connections. Disabled by default; setting it makes the
	// publication DDL fail fast instead of queueing behind a conflicting lock,
	// which in PostgreSQL's FIFO lock queue also blocks every reader behind it.
	LockTimeout      time.Duration    `json:"lockTimeout" yaml:"lockTimeout"`
	DebugMode        bool             `json:"debugMode" yaml:"debugMode"`
	ExtensionSupport ExtensionSupport `json:"extensionSupport" yaml:"extensionSupport"`
	// SkipTupleMapDecode skips building the Decoded/NewDecoded/OldDecoded map
	// on Insert/Update/Delete messages (see format package). Off by default
	// to preserve existing behavior; a consumer that resolves columns
	// directly from TupleData/NewTupleData/OldTupleData can enable it to
	// avoid the extra allocation and decode pass this map costs per row.
	SkipTupleMapDecode bool `json:"skipTupleMapDecode" yaml:"skipTupleMapDecode"`
}

type MetricConfig struct {
	Port int `json:"port" yaml:"port"`
}

type LoggerConfig struct {
	Logger   logger.Logger `json:"-" yaml:"-"`         // custom logger
	LogLevel slog.Level    `json:"level" yaml:"level"` // if custom logger is nil, set the slog log level
}

type ExtensionSupport struct {
	EnableTimeScaleDB bool `json:"enableTimeScaleDB" yaml:"enableTimescaleDB"`
}

type HeartbeatConfig struct {
	Table    publication.Table `json:"table" yaml:"table"`
	Interval time.Duration     `json:"interval" yaml:"interval"`
}

// DSN returns a normal PostgreSQL connection string for regular database operations
// (publication, metadata, snapshot chunks, etc.)
func (c *Config) DSN() string {
	params := url.Values{}
	if c.LockTimeout > 0 {
		ms := max(c.LockTimeout.Milliseconds(), 1)
		params.Set("options", fmt.Sprintf("-c lock_timeout=%d", ms))
	}
	return c.dsn(params)
}

// ReplicationDSN returns a replication connection string for CDC streaming
// This connection counts against max_wal_senders limit
func (c *Config) ReplicationDSN() string {
	// lock_timeout is deliberately not set here: CREATE_REPLICATION_SLOT waits
	// for a consistent point rather than for locks, and a walsender holds none.
	params := url.Values{"replication": []string{"database"}}
	return c.dsn(params)
}

func (c *Config) DSNWithoutSSL() string {
	params := url.Values{"sslmode": []string{"disable"}}
	return c.dsn(params)
}

func (c *Config) dsn(params url.Values) string {
	if c.ConnectTimeout > 0 {
		seconds := max(int64(c.ConnectTimeout.Round(time.Second).Seconds()), 1)
		params.Set("connect_timeout", strconv.FormatInt(seconds, 10))
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", url.QueryEscape(c.Username), url.QueryEscape(c.Password), c.Host, c.Port, c.Database)
	if len(params) == 0 {
		return dsn
	}
	return dsn + "?" + params.Encode()
}

func (c *Config) SetDefault() {
	if c.Port == 0 {
		c.Port = 5432
	}

	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10 * time.Second
	}

	if c.Metric.Port == 0 {
		c.Metric.Port = 8080
	}

	// Default heartbeat interval when table is configured
	if c.Heartbeat.Table.Name != "" {
		if c.Heartbeat.Interval == 0 {
			c.Heartbeat.Interval = 100 * time.Millisecond
		}
		if c.Heartbeat.Table.Schema == "" {
			c.Heartbeat.Table.Schema = defaultSchema
		}
	}

	if c.Slot.SlotActivityCheckerInterval == 0 {
		c.Slot.SlotActivityCheckerInterval = 1000
	}

	if c.Slot.ProtoVersion == 0 {
		c.Slot.ProtoVersion = 1
	}

	if c.Logger.Logger == nil {
		c.Logger.Logger = logger.NewSlog(c.Logger.LogLevel)
	}

	// Set default schema names for tables
	for tableID, table := range c.Publication.Tables {
		if table.Schema == "" {
			c.Publication.Tables[tableID].Schema = defaultSchema
		}
	}

	// Set default snapshot config
	if c.Snapshot.Enabled {
		if c.Snapshot.Mode == "" {
			c.Snapshot.Mode = SnapshotModeNever
		}
		if c.Snapshot.ChunkSize == 0 {
			c.Snapshot.ChunkSize = 8_000
		}
		if c.Snapshot.ClaimTimeout == 0 {
			c.Snapshot.ClaimTimeout = 30 * time.Second
		}
		if c.Snapshot.HeartbeatInterval == 0 {
			c.Snapshot.HeartbeatInterval = 5 * time.Second
		}

		// Set default schema names for snapshot tables
		for tableID, table := range c.Snapshot.Tables {
			if table.Schema == "" {
				c.Snapshot.Tables[tableID].Schema = defaultSchema
			}
		}
	}
}

// IsSnapshotOnlyMode returns true if snapshot is enabled and mode is snapshot_only
func (c *Config) IsSnapshotOnlyMode() bool {
	return c.Snapshot.Enabled && c.Snapshot.Mode == SnapshotModeSnapshotOnly
}

// IsHeartbeatEnabled returns true if heartbeat table is configured
func (c *Config) IsHeartbeatEnabled() bool {
	return c.Heartbeat.Table.Name != ""
}

// GetSnapshotTables returns the tables to snapshot based on the configuration and publication info.
// For snapshot_only mode: uses snapshot.tables (independent from publication)
// For initial mode (snapshot + CDC):
//   - If snapshot.tables specified: validates it's a subset of publication tables and returns snapshot.tables
//   - If snapshot.tables not specified: returns all tables from publication
func (c *Config) GetSnapshotTables(publicationInfo *publication.Config) (publication.Tables, error) {
	// Mode 1: snapshot_only - independent from publication
	if c.IsSnapshotOnlyMode() {
		if len(c.Snapshot.Tables) == 0 {
			return nil, errors.New("snapshot.tables must be specified for snapshot_only mode")
		}
		return c.Snapshot.Tables, nil
	}

	// Mode 2: initial (snapshot + CDC)
	// If snapshot.tables specified, validate it's a subset of publication tables
	if len(c.Snapshot.Tables) > 0 {
		return c.validateSnapshotSubset(publicationInfo.Tables)
	}

	// Mode 3: initial with no snapshot.tables specified
	// Use all tables from publication with merged user config (preserves SnapshotPartitionStrategy)
	return c.mergePublicationTableConfig(publicationInfo.Tables), nil
}

// validateSnapshotSubset ensures snapshot.tables is a subset of publication tables
// and returns the validated snapshot tables with publication metadata (like replica identity)
// while preserving user's SnapshotPartitionStrategy from snapshot.tables config
func (c *Config) validateSnapshotSubset(pubTables publication.Tables) (publication.Tables, error) {
	if len(pubTables) == 0 {
		return nil, errors.New("publication has no tables defined. Either specify tables in publication.tables or query an existing publication")
	}

	// Create map of publication tables for quick lookup
	pubMap := make(map[string]publication.Table)
	for _, t := range pubTables {
		key := t.Schema + "." + t.Name
		pubMap[key] = t
	}

	// Validate each snapshot table exists in publication
	validatedTables := make(publication.Tables, 0, len(c.Snapshot.Tables))
	for _, st := range c.Snapshot.Tables {
		key := st.Schema + "." + st.Name
		pubTable, exists := pubMap[key]
		if !exists {
			return nil, fmt.Errorf(
				"snapshot table '%s' not found in publication '%s'. "+
					"For snapshot+CDC mode, snapshot.tables must be a subset of publication tables",
				key, c.Publication.Name,
			)
		}
		mergedTable := pubTable
		if st.SnapshotPartitionStrategy != "" {
			mergedTable.SnapshotPartitionStrategy = st.SnapshotPartitionStrategy
		}
		if st.QueryCondition != "" {
			mergedTable.QueryCondition = st.QueryCondition
		}
		validatedTables = append(validatedTables, mergedTable)
	}

	return validatedTables, nil
}

func (c *Config) ValidateHeartbeatInPublication(pubInfo *publication.Config) error {
	if !c.IsHeartbeatEnabled() || pubInfo == nil {
		return nil
	}

	if c.Publication.AllTables || pubInfo.AllTables {
		return nil
	}

	schema := c.Heartbeat.Table.Schema
	if schema == "" {
		schema = defaultSchema
	}
	name := c.Heartbeat.Table.Name
	if !pubInfo.Tables.Contains(schema, name) {
		return fmt.Errorf(
			"heartbeat table %s.%s is not included in publication %q; add it to publication.tables so heartbeat changes reach the replication slot",
			schema, name, pubInfo.Name,
		)
	}

	return nil
}

func (c *Config) Validate() error {
	var err error
	if isEmpty(c.Host) {
		err = errors.Join(err, errors.New("host cannot be empty"))
	}

	if isEmpty(c.Username) {
		err = errors.Join(err, errors.New("username cannot be empty"))
	}

	if isEmpty(c.Password) {
		err = errors.Join(err, errors.New("password cannot be empty"))
	}

	if isEmpty(c.Database) {
		err = errors.Join(err, errors.New("database cannot be empty"))
	}

	// Skip CDC-related validation for snapshot_only mode
	if !c.IsSnapshotOnlyMode() {
		if cErr := c.Publication.Validate(); cErr != nil {
			err = errors.Join(err, cErr)
		}

		if cErr := c.Slot.Validate(); cErr != nil {
			err = errors.Join(err, cErr)
		}
	}

	if cErr := c.Snapshot.Validate(); cErr != nil {
		err = errors.Join(err, cErr)
	}

	if c.IsHeartbeatEnabled() {
		if c.Heartbeat.Interval <= 0 {
			err = errors.Join(err, errors.New("heartbeat.interval must be greater than 0 when heartbeat table is configured"))
		}
		if !c.IsSnapshotOnlyMode() && !c.Publication.AllTables && len(c.Publication.Tables) > 0 {
			if hErr := c.ValidateHeartbeatInPublication(&publication.Config{
				Name:   c.Publication.Name,
				Tables: c.Publication.Tables,
			}); hErr != nil {
				err = errors.Join(err, hErr)
			}
		}
	}

	return err
}

func (c *Config) Print() {
	cfg := *c
	cfg.Password = "*******"
	b, _ := json.Marshal(cfg)
	fmt.Println("used config: " + string(b))
}

func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}

func (c *Config) mergePublicationTableConfig(pubInfoTables publication.Tables) publication.Tables {
	if len(c.Publication.Tables) == 0 {
		return pubInfoTables
	}

	userConfigMap := make(map[string]publication.Table)
	for _, t := range c.Publication.Tables {
		key := t.Schema + "." + t.Name
		userConfigMap[key] = t
	}

	result := make(publication.Tables, len(pubInfoTables))
	for i, t := range pubInfoTables {
		result[i] = t
		key := t.Schema + "." + t.Name
		if userTable, exists := userConfigMap[key]; exists {
			if userTable.SnapshotPartitionStrategy != "" {
				result[i].SnapshotPartitionStrategy = userTable.SnapshotPartitionStrategy
			}
			if userTable.QueryCondition != "" {
				result[i].QueryCondition = userTable.QueryCondition
			}
		}
	}
	return result
}

type SnapshotConfig struct {
	Mode              SnapshotMode       `json:"mode" yaml:"mode"`
	InstanceID        string             `json:"instanceId" yaml:"instanceId"`
	ID                string             `json:"id" yaml:"id"`
	QueryCondition    string             `json:"queryCondition,omitempty" yaml:"queryCondition,omitempty"`
	Tables            publication.Tables `json:"tables" yaml:"tables"`
	ChunkSize         int64              `json:"chunkSize" yaml:"chunkSize"`
	ClaimTimeout      time.Duration      `json:"claimTimeout" yaml:"claimTimeout"`
	HeartbeatInterval time.Duration      `json:"heartbeatInterval" yaml:"heartbeatInterval"`
	Enabled           bool               `json:"enabled" yaml:"enabled"`
	Resnapshot        bool               `json:"resnapshot" yaml:"resnapshot"`
}

func (s *SnapshotConfig) Validate() error {
	if !s.Enabled {
		return nil
	}

	validModes := []SnapshotMode{SnapshotModeInitial, SnapshotModeNever, SnapshotModeSnapshotOnly}
	isValid := slices.Contains(validModes, s.Mode)
	if !isValid {
		return errors.New("snapshot mode must be 'initial', 'never', or 'snapshot_only'")
	}

	// Validate chunk-based config
	if s.ChunkSize <= 0 {
		return errors.New("snapshot chunk size must be greater than 0")
	}
	if s.ClaimTimeout <= 0 {
		return errors.New("snapshot claim timeout must be greater than 0")
	}
	if s.HeartbeatInterval <= 0 {
		return errors.New("snapshot heartbeat interval must be greater than 0")
	}

	// For snapshot_only mode, tables must be specified
	if s.Mode == SnapshotModeSnapshotOnly && len(s.Tables) == 0 {
		return errors.New("snapshot.tables must be specified for snapshot_only mode")
	}

	if s.QueryCondition != "" {
		if err := publication.ValidateQueryCondition(s.QueryCondition); err != nil {
			return err
		}
	}
	for _, t := range s.Tables {
		if t.QueryCondition != "" {
			if err := publication.ValidateQueryCondition(t.QueryCondition); err != nil {
				return fmt.Errorf("snapshot.tables %s.%s: %w", t.Schema, t.Name, err)
			}
		}
	}

	return nil
}

type SnapshotMode string

const (
	SnapshotModeInitial      SnapshotMode = "initial"
	SnapshotModeNever        SnapshotMode = "never"
	SnapshotModeSnapshotOnly SnapshotMode = "snapshot_only"
)
