package tables

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/fgrzl/fazure/internal/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Simulate lexkey.Encode() - encoding a uint64 as big-endian hex
func encodeSeq(seq uint64) string {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, seq)
	return hex.EncodeToString(buf)
}

func TestShouldReturnEntitiesGivenPartitionKeyRangeAndRowKeyRangeHex(t *testing.T) {
	// Arrange - Create a test store
	dir := t.TempDir()
	store, err := common.NewStore(filepath.Join(dir, "test.db"))
	require.NoError(t, err)
	defer store.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ts, err := NewTableStore(store, logger, NewMetrics())
	require.NoError(t, err)

	ctx := context.Background()
	tableName := "testhexkeys"

	// Create table
	require.NoError(t, ts.CreateTable(ctx, tableName))

	table, getErr := ts.GetTable(ctx, tableName)
	require.NoError(t, getErr)

	// Insert test data spanning 3 partitions with hex-encoded RowKeys
	// Simulating the streamkit pattern
	basePartition := "444154410053454754454e545300737061636530007365676d656e743000000000000000000"

	for partition := 1; partition <= 3; partition++ {
		pk := basePartition + fmt.Sprintf("%d", partition)
		startSeq := (partition-1)*100 + 1
		endSeq := partition * 100
		if partition == 3 {
			endSeq = 253
		}

		for seq := startSeq; seq <= endSeq; seq++ {
			rk := encodeSeq(uint64(seq))
			props := map[string]interface{}{
				"Sequence": seq,
			}
			_, insertErr := table.InsertEntity(ctx, pk, rk, props)
			require.NoError(t, insertErr)
		}
	}

	// Act - Query with PartitionKey range and RowKey range (hex-encoded)
	pLower := "444154410053454754454e545300737061636530007365676d656e743000"
	pUpper := "444154410053454754454e545300737061636530007365676d656e7430ff"

	// RowKey > encode(233) - should match sequences 234-253 (20 entries)
	// Using 'gt' (greater than) instead of 'ge' (greater than or equal)
	rLower := encodeSeq(233)
	rUpper := encodeSeq(0xFFFFFFFFFFFFFFFF)

	filter := "PartitionKey ge '" + pLower + "' and PartitionKey le '" + pUpper +
		"' and RowKey gt '" + rLower + "' and RowKey le '" + rUpper + "'"

	t.Logf("Filter: %s", filter)
	t.Logf("rLower (encode(233)): %s", rLower)

	entities, _, _, err := table.QueryEntities(ctx, filter, 1000, nil, "", "")
	require.NoError(t, err)

	// Assert - Should return 20 entities (sequences 234-253)
	t.Logf("Found %d entities", len(entities))
	for _, e := range entities {
		seqVal := e.Properties["Sequence"]
		var seq int
		switch v := seqVal.(type) {
		case int:
			seq = v
		case float64:
			seq = int(v)
		case int64:
			seq = int(v)
		}
		t.Logf("Found entity: PK=%s, RK=%s, Seq=%d", e.PartitionKey, e.RowKey, seq)
	}

	assert.Equal(t, 20, len(entities), "Should return 20 entities matching RowKey > encode(233)")

	// Verify the returned entities are in the expected range
	for _, e := range entities {
		seqVal := e.Properties["Sequence"]
		var seq int
		switch v := seqVal.(type) {
		case int:
			seq = v
		case float64:
			seq = int(v)
		case int64:
			seq = int(v)
		}
		assert.GreaterOrEqual(t, seq, 234, "Sequence should be >= 234")
		assert.LessOrEqual(t, seq, 253, "Sequence should be <= 253")
	}
}
