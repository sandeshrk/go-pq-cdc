package replication

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
)

func TestBuildStartReplicationSQL(t *testing.T) {
	tests := []struct {
		name         string
		want         string
		protoVersion int
		streaming    bool
		messages     bool
	}{
		{
			name:         "protoVersion 1 ignores streaming and messages",
			protoVersion: 1,
			streaming:    true,
			messages:     true,
			want:         "START_REPLICATION SLOT slot LOGICAL 0/0 (proto_version '1',publication_names 'pub')",
		},
		{
			name:         "protoVersion 2 without streaming or messages",
			protoVersion: 2,
			streaming:    false,
			messages:     false,
			want:         "START_REPLICATION SLOT slot LOGICAL 0/0 (proto_version '2',publication_names 'pub')",
		},
		{
			name:         "protoVersion 2 with streaming only",
			protoVersion: 2,
			streaming:    true,
			messages:     false,
			want:         "START_REPLICATION SLOT slot LOGICAL 0/0 (proto_version '2',streaming 'true',publication_names 'pub')",
		},
		{
			name:         "protoVersion 2 with messages only",
			protoVersion: 2,
			streaming:    false,
			messages:     true,
			want:         "START_REPLICATION SLOT slot LOGICAL 0/0 (proto_version '2',messages 'true',publication_names 'pub')",
		},
		{
			name:         "protoVersion 2 with both streaming and messages",
			protoVersion: 2,
			streaming:    true,
			messages:     true,
			want:         "START_REPLICATION SLOT slot LOGICAL 0/0 (proto_version '2',messages 'true',streaming 'true',publication_names 'pub')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildStartReplicationSQL("slot", "pub", pq.LSN(0), tt.protoVersion, tt.streaming, tt.messages)
			assert.Equal(t, tt.want, got)
		})
	}
}
