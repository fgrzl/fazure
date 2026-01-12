package tables

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fgrzl/fazure/internal/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStore(t *testing.T) (*TableStore, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "tables_test_*")
	require.NoError(t, err)

	store, err := common.NewStore(filepath.Join(dir, "test.db"))
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	tableStore, err := NewTableStore(store, logger, NewMetrics())
	require.NoError(t, err)

	cleanup := func() {
		store.Close()
		os.RemoveAll(dir)
	}

	return tableStore, cleanup
}

// ============================================================================
// Table CRUD Tests
// ============================================================================

func TestShouldCreateTableGivenValidName(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	// Act
	err := store.CreateTable(ctx, "testtable")

	// Assert
	require.NoError(t, err)
}

func TestShouldReturnErrorGivenDuplicateTableName(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)

	// Act
	err = store.CreateTable(ctx, "testtable")

	// Assert
	assert.Equal(t, ErrTableExists, err)
}

func TestShouldListAllTablesGivenMultipleTables(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	tables := []string{"table1", "table2", "table3"}
	for _, name := range tables {
		err := store.CreateTable(ctx, name)
		require.NoError(t, err)
	}

	// Act
	result, err := store.ListTables(ctx)

	// Assert
	require.NoError(t, err)
	assert.Len(t, result, 3)
	names := make([]string, len(result))
	for i, r := range result {
		names[i] = r["TableName"]
	}
	assert.ElementsMatch(t, tables, names)
}

func TestShouldDeleteTableGivenExistingTable(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)

	// Act
	err = store.DeleteTable(ctx, "testtable")

	// Assert
	require.NoError(t, err)
	result, err := store.ListTables(ctx)
	require.NoError(t, err)
	assert.Len(t, result, 0)
}

func TestShouldReturnErrorGivenDeletingNonExistentTable(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	// Act
	err := store.DeleteTable(ctx, "nonexistent")

	// Assert
	assert.Equal(t, ErrTableNotFound, err)
}

func TestShouldDeleteAllEntitiesGivenTableDeletion(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "pk1", "rk1", map[string]interface{}{"Value": "test"})
	require.NoError(t, err)

	// Act
	err = store.DeleteTable(ctx, "testtable")
	require.NoError(t, err)

	// Assert - recreate and verify no entities
	err = store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err = store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	entities, err := table.ListEntities(ctx)
	require.NoError(t, err)
	assert.Len(t, entities, 0)
}

func TestShouldReturnErrorGivenGettingNonExistentTable(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	// Act
	_, err := store.GetTable(ctx, "nonexistent")

	// Assert
	assert.Equal(t, ErrTableNotFound, err)
}

// ============================================================================
// Entity CRUD Tests
// ============================================================================

func TestShouldInsertEntityGivenValidData(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	properties := map[string]interface{}{
		"Name": "Alice",
		"Age":  float64(30),
	}

	// Act
	entity, err := table.InsertEntity(ctx, "pk1", "rk1", properties)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "pk1", entity.PartitionKey)
	assert.Equal(t, "rk1", entity.RowKey)
	assert.NotEmpty(t, entity.ETag)
	assert.False(t, entity.Timestamp.IsZero())
	assert.Equal(t, "Alice", entity.Properties["Name"])
}

func TestInsertEntityWithInvalidCharactersShouldFail(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)

	_, err = table.InsertEntity(ctx, "inva/lid", "rk1", map[string]interface{}{"Value": "x"})
	assert.Error(t, err)

	_, err = table.InsertEntity(ctx, "pk1", "inva\tlid", map[string]interface{}{"Value": "x"})
	assert.Error(t, err)
}

func TestShouldReturnErrorGivenDuplicateEntityKey(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	// Act
	_, err = table.InsertEntity(ctx, "pk1", "rk1", nil)

	// Assert
	assert.Equal(t, ErrEntityExists, err)
}

func TestShouldGetEntityGivenExistingKey(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	properties := map[string]interface{}{"Name": "Bob"}
	_, err = table.InsertEntity(ctx, "pk1", "rk1", properties)
	require.NoError(t, err)

	// Act
	entity, err := table.GetEntity(ctx, "pk1", "rk1")

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "pk1", entity.PartitionKey)
	assert.Equal(t, "rk1", entity.RowKey)
	assert.Equal(t, "Bob", entity.Properties["Name"])
}

func TestShouldReturnErrorGivenNonExistentEntityKey(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)

	// Act
	_, err = table.GetEntity(ctx, "nonexistent", "nonexistent")

	// Assert
	assert.Equal(t, ErrEntityNotFound, err)
}

func TestShouldReplaceAllPropertiesGivenUpdateWithMergeFalse(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	initial := map[string]interface{}{
		"Name":  "Alice",
		"Age":   float64(30),
		"Email": "alice@example.com",
	}
	_, err = table.InsertEntity(ctx, "pk1", "rk1", initial)
	require.NoError(t, err)
	updated := map[string]interface{}{
		"Name": "Alice Updated",
		"Age":  float64(31),
	}

	// Act
	entity, err := table.UpdateEntity(ctx, "pk1", "rk1", updated, false)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "Alice Updated", entity.Properties["Name"])
	assert.Equal(t, float64(31), entity.Properties["Age"])
	assert.NotContains(t, entity.Properties, "Email")
}

func TestShouldMergePropertiesGivenUpdateWithMergeTrue(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	initial := map[string]interface{}{
		"Name":  "Alice",
		"Age":   float64(30),
		"Email": "alice@example.com",
	}
	_, err = table.InsertEntity(ctx, "pk1", "rk1", initial)
	require.NoError(t, err)
	updated := map[string]interface{}{
		"Name": "Alice Updated",
		"City": "Seattle",
	}

	// Act
	entity, err := table.UpdateEntity(ctx, "pk1", "rk1", updated, true)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "Alice Updated", entity.Properties["Name"])
	assert.Equal(t, float64(30), entity.Properties["Age"])
	assert.Equal(t, "alice@example.com", entity.Properties["Email"])
	assert.Equal(t, "Seattle", entity.Properties["City"])
}

func TestShouldReturnErrorGivenUpdatingNonExistentEntity(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)

	// Act
	_, err = table.UpdateEntity(ctx, "nonexistent", "nonexistent", nil, false)

	// Assert
	assert.Equal(t, ErrEntityNotFound, err)
}

func TestShouldUpdateEntityGivenMatchingETag(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	entity, err := table.InsertEntity(ctx, "pk1", "rk1", map[string]interface{}{"Value": "v1"})
	require.NoError(t, err)
	originalETag := entity.ETag

	// Act
	_, err = table.UpdateEntityWithETag(ctx, "pk1", "rk1", map[string]interface{}{"Value": "v2"}, false, originalETag)

	// Assert
	require.NoError(t, err)
}

func TestShouldReturnErrorGivenStaleETag(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	entity, err := table.InsertEntity(ctx, "pk1", "rk1", map[string]interface{}{"Value": "v1"})
	require.NoError(t, err)
	originalETag := entity.ETag
	_, err = table.UpdateEntity(ctx, "pk1", "rk1", map[string]interface{}{"Value": "v2"}, false)
	require.NoError(t, err)

	// Act
	_, err = table.UpdateEntityWithETag(ctx, "pk1", "rk1", map[string]interface{}{"Value": "v3"}, false, originalETag)

	// Assert
	assert.Equal(t, ErrPreconditionFailed, err)
}

func TestShouldUpdateEntityGivenWildcardETag(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "pk1", "rk1", map[string]interface{}{"Value": "v1"})
	require.NoError(t, err)

	// Act
	_, err = table.UpdateEntityWithETag(ctx, "pk1", "rk1", map[string]interface{}{"Value": "v2"}, false, "*")

	// Assert
	require.NoError(t, err)
}

func TestShouldInsertEntityGivenUpsertWithNonExistentKey(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)

	// Act
	entity, err := table.UpsertEntity(ctx, "pk1", "rk1", map[string]interface{}{"Value": "test"}, false)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "pk1", entity.PartitionKey)
	assert.Equal(t, "test", entity.Properties["Value"])
}

func TestShouldReplaceEntityGivenUpsertWithExistingKeyAndMergeFalse(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "pk1", "rk1", map[string]interface{}{"Value": "v1", "Extra": "keep"})
	require.NoError(t, err)

	// Act
	entity, err := table.UpsertEntity(ctx, "pk1", "rk1", map[string]interface{}{"Value": "v2"}, false)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "v2", entity.Properties["Value"])
	assert.NotContains(t, entity.Properties, "Extra")
}

func TestShouldMergeEntityGivenUpsertWithExistingKeyAndMergeTrue(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "pk1", "rk1", map[string]interface{}{"Value": "v1", "Extra": "keep"})
	require.NoError(t, err)

	// Act
	entity, err := table.UpsertEntity(ctx, "pk1", "rk1", map[string]interface{}{"Value": "v2"}, true)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "v2", entity.Properties["Value"])
	assert.Equal(t, "keep", entity.Properties["Extra"])
}

func TestShouldDeleteEntityGivenExistingKey(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	// Act
	err = table.DeleteEntity(ctx, "pk1", "rk1")

	// Assert
	require.NoError(t, err)
	_, err = table.GetEntity(ctx, "pk1", "rk1")
	assert.Equal(t, ErrEntityNotFound, err)
}

func TestShouldReturnErrorGivenDeletingNonExistentEntity(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)

	// Act
	err = table.DeleteEntity(ctx, "nonexistent", "nonexistent")

	// Assert
	assert.Equal(t, ErrEntityNotFound, err)
}

// ============================================================================
// Query Tests
// ============================================================================

func TestShouldListAllEntitiesGivenMultipleEntities(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		_, err = table.InsertEntity(ctx, "pk", "rk"+string(rune('0'+i)), nil)
		require.NoError(t, err)
	}

	// Act
	entities, err := table.ListEntities(ctx)

	// Assert
	require.NoError(t, err)
	assert.Len(t, entities, 5)
}

func TestShouldQueryAllEntitiesGivenNoFilter(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err = table.InsertEntity(ctx, "pk", "rk"+string(rune('0'+i)), nil)
		require.NoError(t, err)
	}

	// Act
	entities, nextPK, nextRK, err := table.QueryEntities(ctx, "", 0, nil, "", "")

	// Assert
	require.NoError(t, err)
	assert.Len(t, entities, 3)
	assert.Empty(t, nextPK)
	assert.Empty(t, nextRK)
}

func TestShouldFilterEntitiesGivenStatusFilter(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "pk", "rk1", map[string]interface{}{"Status": "active"})
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "pk", "rk2", map[string]interface{}{"Status": "inactive"})
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "pk", "rk3", map[string]interface{}{"Status": "active"})
	require.NoError(t, err)

	// Act
	entities, _, _, err := table.QueryEntities(ctx, "Status eq 'active'", 0, nil, "", "")

	// Assert
	require.NoError(t, err)
	assert.Len(t, entities, 2)
}

func TestShouldReturnErrorGivenUnsupportedFilter(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "pk", "rk1", map[string]interface{}{"Name": "Alice"})
	require.NoError(t, err)

	// Act
	_, _, _, err = table.QueryEntities(ctx, "contains(Name,'Alice')", 0, nil, "", "")

	// Assert
	require.ErrorIs(t, err, ErrInvalidFilter)
}

func TestShouldScanOnlyPartitionGivenPartitionKeyFilter(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "partition1", "rk1", map[string]interface{}{"Value": "v1"})
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "partition1", "rk2", map[string]interface{}{"Value": "v2"})
	require.NoError(t, err)
	_, err = table.InsertEntity(ctx, "partition2", "rk1", map[string]interface{}{"Value": "v3"})
	require.NoError(t, err)

	// Act
	entities, _, _, err := table.QueryEntities(ctx, "PartitionKey eq 'partition1'", 0, nil, "", "")

	// Assert
	require.NoError(t, err)
	assert.Len(t, entities, 2)
	for _, e := range entities {
		assert.Equal(t, "partition1", e.PartitionKey)
	}
}

func TestShouldPaginateResultsGivenLargeDataset(t *testing.T) {
	// Arrange
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	err := store.CreateTable(ctx, "testtable")
	require.NoError(t, err)
	table, err := store.GetTable(ctx, "testtable")
	require.NoError(t, err)
	for i := 0; i < 15; i++ {
		_, err = table.InsertEntity(ctx, "pk", "rk"+string(rune('A'+i)), nil)
		require.NoError(t, err)
	}

	// Act - first page
	entities, nextPK, nextRK, err := table.QueryEntities(ctx, "", 10, nil, "", "")

	// Assert - first page
	require.NoError(t, err)
	assert.Len(t, entities, 10)
	assert.NotEmpty(t, nextPK)
	assert.NotEmpty(t, nextRK)

	// Act - second page
	entities2, nextPK2, nextRK2, err := table.QueryEntities(ctx, "", 10, nil, nextPK, nextRK)

	// Assert - second page
	require.NoError(t, err)
	assert.Len(t, entities2, 5)
	assert.Empty(t, nextPK2)
	assert.Empty(t, nextRK2)
}

// ============================================================================
// Entity JSON Serialization Tests
// ============================================================================

func TestShouldMarshalEntityToJSON(t *testing.T) {
	// Arrange
	entity := &Entity{
		PartitionKey: "pk1",
		RowKey:       "rk1",
		Timestamp:    time.Unix(1000000000, 0).UTC(),
		ETag:         "etag123",
		Properties: map[string]interface{}{
			"Name": "Alice",
			"Age":  30,
		},
	}

	// Act
	data, err := json.Marshal(entity)
	require.NoError(t, err)
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "pk1", result["PartitionKey"])
	assert.Equal(t, "rk1", result["RowKey"])
	assert.Equal(t, "etag123", result["ETag"])
	assert.Equal(t, "Alice", result["Name"])
	assert.Equal(t, float64(30), result["Age"])
}

func TestShouldUnmarshalEntityFromJSON(t *testing.T) {
	// Arrange
	data := []byte(`{
		"PartitionKey": "pk1",
		"RowKey": "rk1",
		"Timestamp": 1000000000,
		"ETag": "etag123",
		"Name": "Alice",
		"Age": 30
	}`)

	// Act
	var entity Entity
	err := json.Unmarshal(data, &entity)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "pk1", entity.PartitionKey)
	assert.Equal(t, "rk1", entity.RowKey)
	assert.Equal(t, "etag123", entity.ETag)
	assert.Equal(t, time.Unix(1000000000, 0).UTC(), entity.Timestamp)
	assert.Equal(t, "Alice", entity.Properties["Name"])
	assert.Equal(t, float64(30), entity.Properties["Age"])
}

func TestShouldRoundTripEntityThroughJSON(t *testing.T) {
	// Arrange
	original := &Entity{
		PartitionKey: "pk1",
		RowKey:       "rk1",
		Timestamp:    time.Unix(1000000000, 0).UTC(),
		ETag:         "etag123",
		Properties: map[string]interface{}{
			"String":  "hello",
			"Number":  float64(42),
			"Boolean": true,
		},
	}

	// Act
	data, err := json.Marshal(original)
	require.NoError(t, err)
	var restored Entity
	err = json.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, original.PartitionKey, restored.PartitionKey)
	assert.Equal(t, original.RowKey, restored.RowKey)
	assert.Equal(t, original.ETag, restored.ETag)
	assert.Equal(t, original.Properties["String"], restored.Properties["String"])
	assert.Equal(t, original.Properties["Number"], restored.Properties["Number"])
	assert.Equal(t, original.Properties["Boolean"], restored.Properties["Boolean"])
}
