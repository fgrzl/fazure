package tables

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/fgrzl/fazure/internal/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnEntitiesGivenPartitionKeyRangeAndRowKeyRange(t *testing.T) {
	// Arrange - Create a test store
	dir := t.TempDir()
	store, err := common.NewStore(filepath.Join(dir, "test.db"))
	require.NoError(t, err)
	defer store.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ts, err := NewTableStore(store, logger, NewMetrics())
	require.NoError(t, err)

	ctx := context.Background()
	tableName := "testpartitionrowkeyrange"

	// Create table
	require.NoError(t, ts.CreateTable(ctx, tableName))

	table, getErr := ts.GetTable(ctx, tableName)
	require.NoError(t, getErr)

	// Insert test data spanning 3 partitions
	// Partition 1: ...01, RowKeys: 0010, 0020, 0030, ..., 0100
	// Partition 2: ...02, RowKeys: 0110, 0120, 0130, ..., 0200
	// Partition 3: ...03, RowKeys: 0210, 0220, 0230, ..., 0260
	basePartition := "444154410053454754454e545300737061636530007365676d656e743000000000000000000"

	for partition := 1; partition <= 3; partition++ {
		pk := basePartition + fmt.Sprintf("%d", partition)
		startSeq := (partition-1)*100 + 10
		endSeq := partition * 100
		if partition == 3 {
			endSeq = 260
		}

		for seq := startSeq; seq <= endSeq; seq += 10 {
			rk := fmt.Sprintf("%04d", seq)
			props := map[string]interface{}{
				"Sequence": seq,
			}
			_, insertErr := table.InsertEntity(ctx, pk, rk, props)
			require.NoError(t, insertErr)
		}
	}

	// Act - Query with PartitionKey range and RowKey range
	// PartitionKey range: covers all 3 partitions
	pLower := "444154410053454754454e545300737061636530007365676d656e743000"
	pUpper := "444154410053454754454e545300737061636530007365676d656e7430ff"

	// RowKey range: >= "0230" (should match sequences 230, 240, 250, 260)
	rLower := "0230"
	rUpper := "9999"

	filter := "PartitionKey ge '" + pLower + "' and PartitionKey le '" + pUpper +
		"' and RowKey ge '" + rLower + "' and RowKey le '" + rUpper + "'"

	t.Logf("Filter: %s", filter)

	entities, _, _, err := table.QueryEntities(ctx, filter, 1000, nil, "", "")
	require.NoError(t, err)

	// Assert - Should return 4 entities (sequences 230, 240, 250, 260)
	assert.Equal(t, 4, len(entities), "Should return 4 entities matching RowKey >= 0230")

	// Verify the returned entities
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
		assert.GreaterOrEqual(t, seq, 230, "Sequence should be >= 230")
		assert.LessOrEqual(t, seq, 260, "Sequence should be <= 260")
	}
}
