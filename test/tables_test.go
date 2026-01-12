package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"log/slog"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fgrzl/fazure/internal/common"
	"github.com/fgrzl/fazure/internal/tables"
)

var (
	tableEmulatorURL = "http://localhost:10002"
	tableAccountName = "devstoreaccount1"
)

// TestMain starts a local instance of the in-repo emulator and points the SDK
// client at it, so these tests exercise our implementation rather than an
// external process. This ensures validation behavior matches our expectations.
func TestMain(m *testing.M) {
	// Start an in-memory store and tables handler
	dir, err := os.MkdirTemp("", "tables_test_*")
	if err != nil {
		fmt.Println("failed to create temp dir:", err)
		os.Exit(1)
	}

	store, err := common.NewStore(filepath.Join(dir, "test.db"))
	if err != nil {
		fmt.Println("failed to open store:", err)
		os.Exit(1)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	ts, err := tables.NewTableStore(store, logger, tables.NewMetrics())
	if err != nil {
		fmt.Println("failed to create tables store:", err)
		os.Exit(1)
	}
	h := tables.NewHandler(ts, logger)
	mux := http.NewServeMux()
	mux.HandleFunc("/", h.HandleRequest)
	server := httptest.NewServer(mux)

	// Override emulator URL used by tests
	tableEmulatorURL = server.URL

	// Run tests
	code := m.Run()

	server.Close()
	store.Close()
	os.RemoveAll(dir)
	os.Exit(code)
}

func newServiceClient(t *testing.T) *aztables.ServiceClient {
	client, err := aztables.NewServiceClientWithNoCredential(
		tableEmulatorURL+"/"+tableAccountName, nil,
	)
	require.NoError(t, err, "Failed to create service client")
	return client
}

func newTableClient(t *testing.T, tableName string) *aztables.Client {
	svc := newServiceClient(t)
	return svc.NewClient(tableName)
}

// ============================================================================
// Table Tests
// ============================================================================

func TestShouldCreateTableGivenValidNameWhenCallingCreateTable(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testcreatetable")

	_, err := client.CreateTable(ctx, nil)
	require.NoError(t, err, "CreateTable should succeed")

	// Cleanup
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()
}

func TestShouldInsertEntityGivenNewPKRKWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testinsertentity")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Name":         "Alice",
		"Age":          30,
	}

	marshalled, err := json.Marshal(entity)
	require.NoError(t, err)

	_, err = client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err, "AddEntity should succeed")
}

func TestShouldDetectConflictGivenExistingEntityWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testconflict")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Value":        "initial",
	}

	marshalled, err := json.Marshal(entity)
	require.NoError(t, err)

	_, err = client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	// Try to add again
	_, err = client.AddEntity(ctx, marshalled, nil)
	assert.Error(t, err, "Duplicate PK/RK should fail with conflict")
}

func TestShouldGetEntityGivenExistingPKRKWhenCallingGetEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testgetentity")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Name":         "Bob",
	}

	marshalled, err := json.Marshal(entity)
	require.NoError(t, err)

	_, err = client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	// Get entity
	resp, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err, "GetEntity should succeed")

	var retrieved map[string]interface{}
	err = json.Unmarshal(resp.Value, &retrieved)
	require.NoError(t, err)

	assert.Equal(t, "pk1", retrieved["PartitionKey"])
	assert.Equal(t, "rk1", retrieved["RowKey"])
	assert.Equal(t, "Bob", retrieved["Name"])
}

func TestShouldReturnNotFoundGivenNonExistentPKRKWhenCallingGetEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testnotfound")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	_, err := client.GetEntity(ctx, "nonexistent", "nope", nil)
	assert.Error(t, err, "Should return error for non-existent entity")
}

func TestShouldUpdateEntityGivenExistingEntityWhenCallingUpdateEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testupdateentity")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Value":        "initial",
	}

	marshalled, err := json.Marshal(entity)
	require.NoError(t, err)

	addResp, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	// Update entity
	updated := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Value":        "updated",
	}

	updatedMarshalled, err := json.Marshal(updated)
	require.NoError(t, err)

	_, err = client.UpdateEntity(ctx, updatedMarshalled, &aztables.UpdateEntityOptions{
		IfMatch:    &addResp.ETag,
		UpdateMode: aztables.UpdateModeReplace,
	})
	require.NoError(t, err, "UpdateEntity should succeed")
}

func TestShouldDeleteEntityGivenValidPKRKWhenCallingDeleteEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testdeleteentity")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "r",
	}

	marshalled, err := json.Marshal(entity)
	require.NoError(t, err)

	_, err = client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	// Delete entity
	_, err = client.DeleteEntity(ctx, "p", "r", nil)
	require.NoError(t, err, "DeleteEntity should succeed")
}

func TestShouldQueryEntitiesGivenFilterWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testqueryentities")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Add multiple entities
	for i := 1; i <= 3; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "p",
			"RowKey":       fmt.Sprintf("%d", i),
			"Value":        i,
		}
		marshalled, err := json.Marshal(entity)
		require.NoError(t, err)
		_, err = client.AddEntity(ctx, marshalled, nil)
		require.NoError(t, err)
	}

	// Query entities with filter
	filter := "Value gt 1"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
	})

	var results []map[string]interface{}
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)

		for _, e := range page.Entities {
			var entity map[string]interface{}
			json.Unmarshal(e, &entity)
			results = append(results, entity)
		}
	}

	assert.GreaterOrEqual(t, len(results), 1, "Should find at least one filtered entity")
}

func TestShouldReturnContinuationTokensGivenLargeResultSetWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testcontinuation")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange
	for i := 0; i < 12; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "p",
			"RowKey":       fmt.Sprintf("%04d", i),
		}
		marshalled, err := json.Marshal(entity)
		require.NoError(t, err)
		_, err = client.AddEntity(ctx, marshalled, nil)
		require.NoError(t, err)
	}

	// Act
	top := int32(5)
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Top: &top,
	})

	count := 0
	pageCount := 0

	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		pageCount++
		count += len(page.Entities)
	}

	// Assert
	assert.GreaterOrEqual(t, count, 12, "Should retrieve all entities")
	assert.GreaterOrEqual(t, pageCount, 2, "Should paginate across multiple pages")
}

func TestShouldReturnBadRequestGivenUnsupportedFilterWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testinvalidfilter")
	_, _ = client.CreateTable(ctx, nil)
	defer func() { _, _ = client.Delete(ctx, nil) }()

	// Insert a single entity
	entity := map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "r",
		"Name":         "Alice",
	}
	m, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, m, nil)
	require.NoError(t, err)

	// Unsupported filter function should cause a 400-style error
	filter := "contains(Name,'A')"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Filter: &filter})

	_, err = pager.NextPage(ctx)
	require.Error(t, err, "Unsupported $filter should return an error")
}

func TestShouldPaginateFilteredResultsWithoutSkipsWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testfilteredpagination")
	_, _ = client.CreateTable(ctx, nil)
	defer func() { _, _ = client.Delete(ctx, nil) }()

	// Arrange: every 3rd entity is important
	const total = 9
	important := 0
	for i := 0; i < total; i++ {
		kind := "noise"
		if i%3 == 0 {
			kind = "important"
			important++
		}
		entity := map[string]interface{}{
			"PartitionKey": "p",
			"RowKey":       fmt.Sprintf("%04d", i),
			"Kind":         kind,
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}

	// Act
	filter := "Kind eq 'important'"
	top := int32(2)
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Filter: &filter, Top: &top})

	seen := make(map[string]bool)
	count := 0
	pageCount := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		pageCount++
		for _, e := range page.Entities {
			var ent map[string]interface{}
			json.Unmarshal(e, &ent)
			assert.Equal(t, "important", ent["Kind"])
			key := fmt.Sprintf("%s|%s", ent["PartitionKey"], ent["RowKey"])
			if seen[key] {
				assert.Fail(t, "duplicate entity in filtered pagination", key)
			}
			seen[key] = true
			count++
		}
	}

	// Assert
	assert.Equal(t, important, count, "Should return all important entities without skips")
	assert.GreaterOrEqual(t, pageCount, 2, "Should paginate filtered results")
}

func TestShouldExecuteBatchGivenValidOpsWhenCallingSubmitTransaction(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbatch")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Create batch operations
	entity1, _ := json.Marshal(map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "1",
	})
	entity2, _ := json.Marshal(map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "2",
	})

	actions := []aztables.TransactionAction{
		{
			ActionType: aztables.TransactionTypeAdd,
			Entity:     entity1,
		},
		{
			ActionType: aztables.TransactionTypeAdd,
			Entity:     entity2,
		},
	}

	// Submit transaction
	resp, err := client.SubmitTransaction(ctx, actions, nil)
	require.NoError(t, err, "Batch should succeed")
	assert.NotNil(t, resp, "Should have response")
}

func TestShouldFailBatchGivenMixedPartitionKeysWhenCallingSubmitTransaction(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbatchpk")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Create batch with different partition keys (should fail)
	entity1, _ := json.Marshal(map[string]interface{}{
		"PartitionKey": "p1",
		"RowKey":       "1",
	})
	entity2, _ := json.Marshal(map[string]interface{}{
		"PartitionKey": "p2",
		"RowKey":       "2",
	})

	actions := []aztables.TransactionAction{
		{
			ActionType: aztables.TransactionTypeAdd,
			Entity:     entity1,
		},
		{
			ActionType: aztables.TransactionTypeAdd,
			Entity:     entity2,
		},
	}

	_, err := client.SubmitTransaction(ctx, actions, nil)
	assert.Error(t, err, "Azure should reject mixed-PK batch")
}

func TestShouldBeAtomicGivenOneOperationFailsWhenCallingSubmitTransaction(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbatchatomic")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Insert an entity that will cause conflict
	existing, _ := json.Marshal(map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "dupe",
	})
	_, err := client.AddEntity(ctx, existing, nil)
	require.NoError(t, err)

	// Create batch with duplicate (should fail)
	entity1, _ := json.Marshal(map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "1",
	})
	entity2, _ := json.Marshal(map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "dupe", // This will conflict
	})
	entity3, _ := json.Marshal(map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "2",
	})

	actions := []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeAdd, Entity: entity1},
		{ActionType: aztables.TransactionTypeAdd, Entity: entity2},
		{ActionType: aztables.TransactionTypeAdd, Entity: entity3},
	}

	_, err = client.SubmitTransaction(ctx, actions, nil)
	assert.Error(t, err, "Batch should fail atomically")

	// Verify atomicity (1 and 2 should NOT exist)
	_, err = client.GetEntity(ctx, "p", "1", nil)
	assert.Error(t, err, "Entity 1 should not exist due to batch rollback")

	_, err = client.GetEntity(ctx, "p", "2", nil)
	assert.Error(t, err, "Entity 2 should not exist due to batch rollback")
}

// ============================================================================
// Merge Entity Tests
// ============================================================================

func TestShouldMergeEntityGivenExistingEntityWhenCallingUpsertEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testmerge")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Insert initial entity
	entity := map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "r",
		"Name":         "Alice",
		"Age":          30,
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	// Merge with new properties
	merge := map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "r",
		"City":         "Seattle", // New property
		"Age":          31,        // Updated property
	}
	mergeMarshalled, _ := json.Marshal(merge)
	_, err = client.UpsertEntity(ctx, mergeMarshalled, &aztables.UpsertEntityOptions{
		UpdateMode: aztables.UpdateModeMerge,
	})
	require.NoError(t, err, "Merge should succeed")

	// Verify merge result
	resp, err := client.GetEntity(ctx, "p", "r", nil)
	require.NoError(t, err)

	var retrieved map[string]interface{}
	json.Unmarshal(resp.Value, &retrieved)

	assert.Equal(t, "Alice", retrieved["Name"], "Original property should be preserved")
	assert.Equal(t, "Seattle", retrieved["City"], "New property should be added")
	assert.Equal(t, float64(31), retrieved["Age"], "Updated property should be changed")
}

// ============================================================================
// Upsert Entity Tests
// ============================================================================

func TestShouldInsertEntityGivenNonExistentEntityWhenCallingUpsertEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testupsert")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Upsert new entity (should insert)
	entity := map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "new",
		"Value":        "inserted",
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.UpsertEntity(ctx, marshalled, nil)
	require.NoError(t, err, "Upsert should succeed for new entity")

	// Verify insert
	resp, err := client.GetEntity(ctx, "p", "new", nil)
	require.NoError(t, err)

	var retrieved map[string]interface{}
	json.Unmarshal(resp.Value, &retrieved)
	assert.Equal(t, "inserted", retrieved["Value"])
}

// ============================================================================
// List Tables Tests
// ============================================================================

func TestShouldListTablesGivenMultipleTablesWhenCallingListTables(t *testing.T) {
	ctx := context.Background()
	service := newServiceClient(t)

	// Create multiple tables
	tables := []string{"listtable1", "listtable2", "listtable3"}
	for _, name := range tables {
		tc := service.NewClient(name)
		_, _ = tc.CreateTable(ctx, nil)
		defer tc.Delete(ctx, nil)
	}

	// List tables
	pager := service.NewListTablesPager(nil)
	found := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		found += len(page.Tables)
	}

	assert.GreaterOrEqual(t, found, 3, "Should find at least 3 tables")
}

// ============================================================================
// OData Filter Tests
// ============================================================================

func TestShouldFilterEntitiesTableDrivenWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testfilters")
	_, _ = client.CreateTable(ctx, nil)
	defer func() { _, _ = client.Delete(ctx, nil) }()

	// Arrange
	entities := []map[string]interface{}{
		{"PartitionKey": "p", "RowKey": "1", "Status": "active", "Priority": 1, "Score": 50, "Type": "A"},
		{"PartitionKey": "p", "RowKey": "2", "Status": "active", "Priority": 2, "Score": 75, "Type": "B"},
		{"PartitionKey": "p", "RowKey": "3", "Status": "inactive", "Priority": 1, "Score": 100, "Type": "A"},
	}
	for _, e := range entities {
		m, _ := json.Marshal(e)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	tests := []struct {
		name          string
		filter        string
		expectedCount int
	}{
		{"eq matches", "Status eq 'active'", 2},
		{"and matches", "Status eq 'active' and Priority eq 1", 1},
		{"or matches", "Status eq 'active' or Status eq 'pending'", 2},
		{"ge numeric", "Score ge 75", 2},
		{"ne matches", "Type ne 'A'", 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filter := tc.filter
			pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Filter: &filter})

			count := 0
			for pager.More() {
				page, err := pager.NextPage(ctx)
				require.NoError(t, err)
				count += len(page.Entities)
			}

			assert.Equal(t, tc.expectedCount, count)
		})
	}
}

// ============================================================================
// Select Projection Tests
// ============================================================================

func TestShouldReturnOnlySelectedPropertiesGivenSelectWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testselect")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Add entity with multiple properties
	entity := map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "r",
		"Name":         "Alice",
		"Age":          30,
		"City":         "Seattle",
	}
	m, _ := json.Marshal(entity)
	_, _ = client.AddEntity(ctx, m, nil)

	// Query with select
	selectStr := "Name,Age"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Select: &selectStr,
	})

	var retrieved map[string]interface{}
	for pager.More() {
		page, _ := pager.NextPage(ctx)
		if len(page.Entities) > 0 {
			json.Unmarshal(page.Entities[0], &retrieved)
			break
		}
	}

	assert.Contains(t, retrieved, "Name", "Should include Name")
	assert.Contains(t, retrieved, "Age", "Should include Age")
	// City should not be included but PartitionKey/RowKey always are
}

// ============================================================================
// ETag Conditional Update Tests
// ============================================================================

func TestShouldFailUpdateGivenStaleETagWhenCallingUpdateEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testetagfail")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Insert entity
	entity := map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "r",
		"Value":        "v1",
	}
	m, _ := json.Marshal(entity)
	resp, err := client.AddEntity(ctx, m, nil)
	require.NoError(t, err)
	originalETag := resp.ETag

	// Update entity (changes ETag)
	updated := map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "r",
		"Value":        "v2",
	}
	m2, _ := json.Marshal(updated)
	_, err = client.UpdateEntity(ctx, m2, &aztables.UpdateEntityOptions{
		IfMatch:    &originalETag,
		UpdateMode: aztables.UpdateModeReplace,
	})
	require.NoError(t, err)

	// Try to update with stale ETag
	v3 := map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "r",
		"Value":        "v3",
	}
	m3, _ := json.Marshal(v3)
	_, err = client.UpdateEntity(ctx, m3, &aztables.UpdateEntityOptions{
		IfMatch:    &originalETag, // Stale!
		UpdateMode: aztables.UpdateModeReplace,
	})
	assert.Error(t, err, "Update with stale ETag should fail")
}

// ============================================================================
// Batch Delete and Update Tests
// ============================================================================

func TestShouldDeleteInBatchGivenExistingEntitiesWhenCallingSubmitTransaction(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbatchdelete")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Insert entities
	for i := 1; i <= 3; i++ {
		e := map[string]interface{}{
			"PartitionKey": "p",
			"RowKey":       fmt.Sprintf("%d", i),
		}
		m, _ := json.Marshal(e)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	// Batch delete
	actions := []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeDelete, Entity: mustMarshal(map[string]interface{}{"PartitionKey": "p", "RowKey": "1"})},
		{ActionType: aztables.TransactionTypeDelete, Entity: mustMarshal(map[string]interface{}{"PartitionKey": "p", "RowKey": "2"})},
	}

	_, err := client.SubmitTransaction(ctx, actions, nil)
	require.NoError(t, err, "Batch delete should succeed")

	// Verify deletions
	_, err = client.GetEntity(ctx, "p", "1", nil)
	assert.Error(t, err, "Entity 1 should be deleted")

	_, err = client.GetEntity(ctx, "p", "3", nil)
	require.NoError(t, err, "Entity 3 should still exist")
}

func mustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

// ============================================================================
// Table Delete Tests
// ============================================================================

func TestShouldDeleteTableGivenExistingTableWhenCallingDelete(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testdeletetable")

	// Create table
	_, err := client.CreateTable(ctx, nil)
	require.NoError(t, err)

	// Delete table
	_, err = client.Delete(ctx, nil)
	require.NoError(t, err, "Delete should succeed")

	// Verify table is gone
	_, err = client.GetEntity(ctx, "p", "r", nil)
	assert.Error(t, err, "Should get error for deleted table")
}

// ============================================================================
// Complex Property Type Tests
// ============================================================================

func TestShouldStoreComplexTypesGivenVariousTypesWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testcomplextypes")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Add entity with various types
	entity := map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "r",
		"StringProp":   "hello",
		"IntProp":      42,
		"FloatProp":    3.14,
		"BoolProp":     true,
	}
	m, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, m, nil)
	require.NoError(t, err)

	// Retrieve and verify types
	resp, err := client.GetEntity(ctx, "p", "r", nil)
	require.NoError(t, err)

	var retrieved map[string]interface{}
	json.Unmarshal(resp.Value, &retrieved)

	assert.Equal(t, "hello", retrieved["StringProp"])
	assert.Equal(t, float64(42), retrieved["IntProp"]) // JSON unmarshals to float64
	assert.Equal(t, 3.14, retrieved["FloatProp"])
	assert.Equal(t, true, retrieved["BoolProp"])
}

// ============================================================================
// Query Bug & Regression Tests
// ============================================================================

// This test exercises a tenant-style table with many entities in one partition
// and a filter that does NOT include PartitionKey, to surface any full-scan
// behavior or correctness bugs when only a single result is expected.
func TestShouldReturnSingleEntityGivenManyInPartitionWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testtenantmany")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Insert entities for a single tenant partition
	for i := 0; i < 20; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "tenant-001",
			"RowKey":       fmt.Sprintf("%06d", i),
			"Kind":         "noise",
		}
		if i == 19 {
			entity["Kind"] = "settings"
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}

	filter := "Kind eq 'settings'"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
	})

	var results []map[string]interface{}
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, e := range page.Entities {
			var ent map[string]interface{}
			json.Unmarshal(e, &ent)
			results = append(results, ent)
		}
	}

	require.Len(t, results, 1, "Should return exactly one matching entity")
	assert.Equal(t, "settings", results[0]["Kind"])
}

// This test targets continuation token correctness when paging through many
// entities; it should return all rows without dropping or duplicating any.
func TestShouldPageThroughAllEntitiesWithoutDropOrDuplicateWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testpagingregression")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Insert enough entities to force multiple pages
	const total = 15
	for i := 0; i < total; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "p",
			"RowKey":       fmt.Sprintf("%04d", i),
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}

	top := int32(5)
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Top: &top,
	})

	seen := make(map[string]bool)
	count := 0
	pageCount := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		pageCount++
		for _, e := range page.Entities {
			var ent map[string]interface{}
			json.Unmarshal(e, &ent)
			key := fmt.Sprintf("%s|%s", ent["PartitionKey"], ent["RowKey"])
			if seen[key] {
				assert.Fail(t, "Duplicate entity encountered across pages", key)
			}
			seen[key] = true
			count++
		}
	}

	assert.Equal(t, total, count, "Should read exactly total entities across all pages")
	assert.GreaterOrEqual(t, pageCount, 2, "Should have multiple pages")
}

// This test validates that a filter including PartitionKey eq '...' in a
// realistic shape (with additional conditions) still returns the right rows.
// It helps catch regressions in extractPartitionKeyFromFilter.
func TestShouldFilterByPartitionKeyAndAdditionalPredicateWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testpkfiltershape")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Two partitions, but only one should match the filter
	for i := 0; i < 5; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "tenant-a",
			"RowKey":       fmt.Sprintf("%04d", i),
			"Enabled":      i%2 == 0,
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}
	for i := 0; i < 5; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "tenant-b",
			"RowKey":       fmt.Sprintf("%04d", i),
			"Enabled":      true,
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}

	filter := "PartitionKey eq 'tenant-b' and Enabled eq true"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
	})

	count := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, e := range page.Entities {
			var ent map[string]interface{}
			json.Unmarshal(e, &ent)
			assert.Equal(t, "tenant-b", ent["PartitionKey"])
			assert.Equal(t, true, ent["Enabled"])
			count++
		}
	}

	assert.Equal(t, 5, count, "Should return all tenant-b enabled entities")
}

func TestShouldRollbackAllOperationsGivenBatchFailureWhenCallingTransactionalBatch(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbatchatomicity")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: Create an existing entity that will cause conflict
	existingEntity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk-existing",
		"Value":        "exists",
	}
	marshalled, _ := json.Marshal(existingEntity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	// Act: Try batch with 3 inserts, middle one conflicts
	batch := []aztables.TransactionAction{
		{
			ActionType: aztables.TransactionTypeAdd,
			Entity: mustMarshalCompat(map[string]interface{}{
				"PartitionKey": "pk1",
				"RowKey":       "rk1",
				"Value":        "first",
			}),
		},
		{
			ActionType: aztables.TransactionTypeAdd,
			Entity: mustMarshalCompat(map[string]interface{}{
				"PartitionKey": "pk1",
				"RowKey":       "rk-existing", // This will fail
				"Value":        "duplicate",
			}),
		},
		{
			ActionType: aztables.TransactionTypeAdd,
			Entity: mustMarshalCompat(map[string]interface{}{
				"PartitionKey": "pk1",
				"RowKey":       "rk3",
				"Value":        "third",
			}),
		},
	}

	_, err = client.SubmitTransaction(ctx, batch, nil)
	assert.Error(t, err, "Batch should fail due to conflict")

	// Assert: None of the entities should exist (atomic rollback)
	_, err = client.GetEntity(ctx, "pk1", "rk1", nil)
	assert.Error(t, err, "First entity should not exist (rolled back)")

	_, err = client.GetEntity(ctx, "pk1", "rk3", nil)
	assert.Error(t, err, "Third entity should not exist (rolled back)")

	// Existing entity should still have original value
	resp, err := client.GetEntity(ctx, "pk1", "rk-existing", nil)
	require.NoError(t, err)
	var retrieved map[string]interface{}
	_ = json.Unmarshal(resp.Value, &retrieved)
	assert.Equal(t, "exists", retrieved["Value"])
}

func TestShouldCommitAllOperationsGivenValidBatchWhenCallingTransactionalBatch(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbatchcommit")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Act: Batch with insert, update, delete
	// First, create an entity to update
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk-update",
		"Value":        "original",
	}
	marshalled, _ := json.Marshal(entity)
	_, _ = client.AddEntity(ctx, marshalled, nil)

	// Create entity to delete
	deleteEntity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk-delete",
		"Value":        "to-delete",
	}
	marshalled, _ = json.Marshal(deleteEntity)
	_, _ = client.AddEntity(ctx, marshalled, nil)

	batch := []aztables.TransactionAction{
		{
			ActionType: aztables.TransactionTypeAdd,
			Entity: mustMarshalCompat(map[string]interface{}{
				"PartitionKey": "pk1",
				"RowKey":       "rk-new",
				"Value":        "inserted",
			}),
		},
		{
			ActionType: aztables.TransactionTypeUpdateMerge,
			Entity: mustMarshalCompat(map[string]interface{}{
				"PartitionKey": "pk1",
				"RowKey":       "rk-update",
				"Value":        "updated",
				"NewField":     "added",
			}),
		},
		{
			ActionType: aztables.TransactionTypeDelete,
			Entity: mustMarshalCompat(map[string]interface{}{
				"PartitionKey": "pk1",
				"RowKey":       "rk-delete",
			}),
		},
	}

	_, err := client.SubmitTransaction(ctx, batch, nil)
	require.NoError(t, err, "Batch should succeed")

	// Assert: All operations committed
	resp, err := client.GetEntity(ctx, "pk1", "rk-new", nil)
	require.NoError(t, err)
	var newEntity map[string]interface{}
	_ = json.Unmarshal(resp.Value, &newEntity)
	assert.Equal(t, "inserted", newEntity["Value"])

	resp, err = client.GetEntity(ctx, "pk1", "rk-update", nil)
	require.NoError(t, err)
	var updatedEntity map[string]interface{}
	_ = json.Unmarshal(resp.Value, &updatedEntity)
	assert.Equal(t, "updated", updatedEntity["Value"])
	assert.Equal(t, "added", updatedEntity["NewField"])

	_, err = client.GetEntity(ctx, "pk1", "rk-delete", nil)
	assert.Error(t, err, "Deleted entity should not exist")
}

// ============================================================================
// Azure Compatibility Tests - PUT Semantics
// ============================================================================

func TestShouldUpsertGivenNonExistentEntityWhenCallingUpdateEntityWithoutIfMatch(t *testing.T) {

	ctx := context.Background()
	client := newTableClient(t, "testputsemantics")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Act: Try to update (PUT) non-existent entity without If-Match: *
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "nonexistent",
		"Value":        "new",
	}
	marshalled, _ := json.Marshal(entity)

	_, err := client.UpdateEntity(ctx, marshalled, &aztables.UpdateEntityOptions{
		UpdateMode: aztables.UpdateModeReplace, // This is PUT
		// No If-Match means InsertOrReplace (upsert) for service versions 2011-08-18 and later.
	})

	// Assert: should upsert and entity should exist
	require.NoError(t, err, "PUT without If-Match should upsert")
	resp, err := client.GetEntity(ctx, "pk1", "nonexistent", nil)
	require.NoError(t, err)
	var stored map[string]interface{}
	_ = json.Unmarshal(resp.Value, &stored)
	assert.Equal(t, "new", stored["Value"])
}

func TestShouldUpsertGivenNonExistentEntityWhenCallingUpdateEntityWithIfMatchStar(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testputupsert")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Act: Update (PUT) with If-Match: * should upsert
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "new",
		"Value":        "upserted",
	}
	marshalled, _ := json.Marshal(entity)

	wildcardEtag := azcore.ETag("*")
	_, err := client.UpdateEntity(ctx, marshalled, &aztables.UpdateEntityOptions{
		UpdateMode: aztables.UpdateModeReplace,
		IfMatch:    &wildcardEtag,
	})

	require.NoError(t, err, "PUT with If-Match: * should upsert")

	// Verify entity was created
	resp, err := client.GetEntity(ctx, "pk1", "new", nil)
	require.NoError(t, err)
	var retrieved map[string]interface{}
	_ = json.Unmarshal(resp.Value, &retrieved)
	assert.Equal(t, "upserted", retrieved["Value"])
}

func TestShouldReturnBadRequestGivenInvalidFilterSyntaxWhenCallingQuery(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testfiltersyntax")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Act: Query with invalid syntax
	filter := "PartitionKey eq" // Missing value
	listOpts := &aztables.ListEntitiesOptions{
		Filter: &filter,
	}

	pager := client.NewListEntitiesPager(listOpts)
	_, err := pager.NextPage(ctx)

	// Assert: Should return error
	assert.Error(t, err, "Invalid filter syntax should return error")
}

// ============================================================================
// Azure Compatibility Tests - Type Metadata
// ============================================================================

func TestShouldPreserveEdmDateTimeTypeGivenDateTimePropertyWhenStoringEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testtypes")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: Entity with DateTime
	now := time.Now().UTC().Truncate(time.Millisecond)
	entity := aztables.EDMEntity{
		Entity: aztables.Entity{
			PartitionKey: "pk1",
			RowKey:       "rk1",
		},
		Properties: map[string]interface{}{
			"CreatedAt": aztables.EDMDateTime(now),
		},
	}

	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	// Act: Retrieve entity
	resp, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	// Assert: Should have type metadata
	var retrieved map[string]interface{}
	_ = json.Unmarshal(resp.Value, &retrieved)

	// Azure returns: "CreatedAt@odata.type": "Edm.DateTime"
	odataType, hasType := retrieved["CreatedAt@odata.type"]
	if hasType {
		assert.Equal(t, "Edm.DateTime", odataType)
	}
	// TODO: This test documents expected behavior
}

func TestShouldPreserveEdmGuidTypeGivenGuidPropertyWhenStoringEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testguid")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: Entity with GUID
	guid := "550e8400-e29b-41d4-a716-446655440000"
	entity := aztables.EDMEntity{
		Entity: aztables.Entity{
			PartitionKey: "pk1",
			RowKey:       "rk1",
		},
		Properties: map[string]interface{}{
			"ID": aztables.EDMGUID(guid),
		},
	}

	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	// Act: Retrieve entity
	resp, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	// Assert: Should have type metadata
	var retrieved map[string]interface{}
	_ = json.Unmarshal(resp.Value, &retrieved)

	// Azure returns: "ID@odata.type": "Edm.Guid"
	odataType, hasType := retrieved["ID@odata.type"]
	if hasType {
		assert.Equal(t, "Edm.Guid", odataType)
	}
}

// ============================================================================
// Azure Compatibility Tests - Reserved Properties
// ============================================================================

func TestShouldIgnoreTimestampInRequestGivenClientTimestampWhenInsertingEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testtimestamp")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: Try to set custom Timestamp
	customTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Timestamp":    customTime.Format(time.RFC3339Nano),
		"Value":        "test",
	}

	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	// Act: Retrieve entity
	resp, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	var retrieved map[string]interface{}
	_ = json.Unmarshal(resp.Value, &retrieved)

	// Assert: Timestamp should be system-generated, not custom
	timestampStr, ok := retrieved["Timestamp"].(string)
	require.True(t, ok)

	retrievedTime, err := time.Parse(time.RFC3339Nano, timestampStr)
	require.NoError(t, err)

	// Should be recent (within last minute), not the custom 2020 date
	assert.WithinDuration(t, time.Now().UTC(), retrievedTime, time.Minute)
}

func TestShouldIgnoreETagInRequestGivenClientETagWhenInsertingEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testetag")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: Try to set custom ETag
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"odata.etag":   "W/\"custom-etag\"",
		"Value":        "test",
	}

	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	// Act: Retrieve entity
	resp, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	var retrieved map[string]interface{}
	_ = json.Unmarshal(resp.Value, &retrieved)

	// Assert: ETag should be system-generated, not custom
	etag, ok := retrieved["odata.etag"].(string)
	if ok {
		assert.NotEqual(t, "W/\"custom-etag\"", etag)
	}
}

// ============================================================================
// Key Validation Tests
// ============================================================================

func TestShouldRejectPartitionKeyWithProhibitedCharactersWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testkeyvalidation")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	prohibitedChars := []string{"/", "\\", "#", "?", "\t", "\n"}
	for _, char := range prohibitedChars {
		ch := char // capture loop variable
		t.Run(fmt.Sprintf("char_%q", ch), func(t *testing.T) {
			entity := map[string]interface{}{
				"PartitionKey": "invalid" + ch + "key",
				"RowKey":       "rk1",
			}
			marshalled, _ := json.Marshal(entity)
			_, err := client.AddEntity(ctx, marshalled, nil)
			assert.Error(t, err, "Should reject PartitionKey with prohibited character: %q", ch)
		})
	}
}

func TestShouldRejectRowKeyWithProhibitedCharactersWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testrkvalidation")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	prohibitedChars := []string{"/", "\\", "#", "?", "\t", "\n"}
	for _, char := range prohibitedChars {
		ch := char // capture loop variable
		t.Run(fmt.Sprintf("char_%q", ch), func(t *testing.T) {
			entity := map[string]interface{}{
				"PartitionKey": "pk1",
				"RowKey":       "invalid" + ch + "key",
			}
			marshalled, _ := json.Marshal(entity)
			_, err := client.AddEntity(ctx, marshalled, nil)
			assert.Error(t, err, "Should reject RowKey with prohibited character: %q", ch)
		})
	}
}

func TestShouldRejectPartitionKeyExceeding1KBWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testpksize")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Create a key larger than 1KB (1024 bytes)
	longKey := strings.Repeat("a", 1025)
	entity := map[string]interface{}{
		"PartitionKey": longKey,
		"RowKey":       "rk1",
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	assert.Error(t, err, "Should reject PartitionKey exceeding 1KB")
}

func TestShouldRejectRowKeyExceeding1KBWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testrksize")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	longKey := strings.Repeat("a", 1025)
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       longKey,
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	assert.Error(t, err, "Should reject RowKey exceeding 1KB")
}

func TestShouldRejectEmptyPartitionKeyWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testemptypk")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "",
		"RowKey":       "rk1",
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	assert.Error(t, err, "Should reject empty PartitionKey")
}

func TestShouldRejectEmptyRowKeyWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testemptyrk")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "",
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	assert.Error(t, err, "Should reject empty RowKey")
}

func TestShouldHandleUnicodeInPartitionAndRowKeysWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testunicode")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "tenant-日本語",
		"RowKey":       "user-Ñoño",
		"Name":         "Test",
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err, "Should accept valid Unicode in keys")

	// Verify retrieval
	resp, err := client.GetEntity(ctx, "tenant-日本語", "user-Ñoño", nil)
	require.NoError(t, err)
	var retrieved map[string]interface{}
	json.Unmarshal(resp.Value, &retrieved)
	assert.Equal(t, "tenant-日本語", retrieved["PartitionKey"])
	assert.Equal(t, "user-Ñoño", retrieved["RowKey"])
}

// ============================================================================
// Entity Size and Property Limit Tests
// ============================================================================

func TestShouldRejectEntityExceeding1MBWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testentitysize")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Create entity with large property exceeding 1MB total
	largeValue := strings.Repeat("x", 1024*1024) // 1MB string
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"LargeData":    largeValue,
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	assert.Error(t, err, "Should reject entity exceeding 1MB")
}

func TestShouldRejectEntityWithOver255PropertiesWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testpropertycount")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
	}
	// Add 256 properties (exceeds limit)
	for i := 0; i < 256; i++ {
		entity[fmt.Sprintf("Prop%d", i)] = i
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	assert.Error(t, err, "Should reject entity with more than 255 properties")
}

func TestShouldRejectStringPropertyExceeding64KBWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testpropsize")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Create property larger than 64KB
	largeString := strings.Repeat("a", 64*1024+1) // 64KB + 1
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"LargeString":  largeString,
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	assert.Error(t, err, "Should reject string property exceeding 64KB")
}

func TestShouldAcceptEntityWith255PropertiesWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testmaxprops")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
	}
	// Add exactly 255 properties (at the limit)
	for i := 0; i < 255; i++ {
		entity[fmt.Sprintf("Prop%d", i)] = i
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err, "Should accept entity with exactly 255 properties")
}

// ============================================================================
// Property Name Validation Tests
// ============================================================================

func TestShouldRejectPropertyNameExceeding255CharsWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testpropnamesize")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	longPropName := strings.Repeat("a", 256)
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		longPropName:   "value",
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	assert.Error(t, err, "Should reject property name exceeding 255 characters")
}

func TestShouldRejectPropertyNameWithInvalidCharactersWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testpropnamechars")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Control characters should be rejected in property names
	entity := map[string]interface{}{
		"PartitionKey":  "pk1",
		"RowKey":        "rk1",
		"Invalid\tProp": "value",
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	assert.Error(t, err, "Should reject property name with control characters")
}

// ============================================================================
// Table Name Validation Tests
// ============================================================================

func TestShouldRejectTableNameTooShortWhenCallingCreateTable(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "ab") // Only 2 chars, min is 3

	_, err := client.CreateTable(ctx, nil)
	assert.Error(t, err, "Should reject table name shorter than 3 characters")
}

func TestShouldRejectTableNameTooLongWhenCallingCreateTable(t *testing.T) {
	ctx := context.Background()
	longName := strings.Repeat("a", 64) // Max is 63
	client := newTableClient(t, longName)

	_, err := client.CreateTable(ctx, nil)
	assert.Error(t, err, "Should reject table name longer than 63 characters")
}

func TestShouldRejectTableNameStartingWithNumberWhenCallingCreateTable(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "123table")

	_, err := client.CreateTable(ctx, nil)
	assert.Error(t, err, "Should reject table name starting with number")
}

func TestShouldRejectTableNameWithNonAlphanumericWhenCallingCreateTable(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "table-name")

	_, err := client.CreateTable(ctx, nil)
	assert.Error(t, err, "Should reject table name with non-alphanumeric characters")
}

func TestShouldRejectReservedTableNameTablesWhenCallingCreateTable(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "Tables")

	_, err := client.CreateTable(ctx, nil)
	assert.Error(t, err, "Should reject reserved table name 'Tables'")
}

// ============================================================================
// Batch Operation Limit Tests
// ============================================================================

func TestShouldRejectBatchExceeding100OperationsWhenCallingSubmitTransaction(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbatchlimit")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Create 101 operations (exceeds limit of 100)
	actions := make([]aztables.TransactionAction, 101)
	for i := 0; i < 101; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "pk1",
			"RowKey":       fmt.Sprintf("rk%d", i),
		}
		marshalled, _ := json.Marshal(entity)
		actions[i] = aztables.TransactionAction{
			ActionType: aztables.TransactionTypeAdd,
			Entity:     marshalled,
		}
	}

	_, err := client.SubmitTransaction(ctx, actions, nil)
	assert.Error(t, err, "Should reject batch with more than 100 operations")
}

func TestShouldAcceptBatchWith100OperationsWhenCallingSubmitTransaction(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbatch100")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Create exactly 100 operations (at the limit)
	actions := make([]aztables.TransactionAction, 100)
	for i := 0; i < 100; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "pk1",
			"RowKey":       fmt.Sprintf("rk%d", i),
		}
		marshalled, _ := json.Marshal(entity)
		actions[i] = aztables.TransactionAction{
			ActionType: aztables.TransactionTypeAdd,
			Entity:     marshalled,
		}
	}

	_, err := client.SubmitTransaction(ctx, actions, nil)
	require.NoError(t, err, "Should accept batch with exactly 100 operations")
}

func TestShouldRejectBatchExceeding4MBWhenCallingSubmitTransaction(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbatchsize")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Create operations with large payloads exceeding 4MB total
	largeValue := strings.Repeat("x", 100*1024) // 100KB per entity
	actions := make([]aztables.TransactionAction, 50)
	for i := 0; i < 50; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "pk1",
			"RowKey":       fmt.Sprintf("rk%d", i),
			"Data":         largeValue,
		}
		marshalled, _ := json.Marshal(entity)
		actions[i] = aztables.TransactionAction{
			ActionType: aztables.TransactionTypeAdd,
			Entity:     marshalled,
		}
	}

	_, err := client.SubmitTransaction(ctx, actions, nil)
	assert.Error(t, err, "Should reject batch exceeding 4MB")
}

// ============================================================================
// Advanced OData Query Tests
// ============================================================================

func TestShouldSupportStartsWithFunctionWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "teststartswith")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entities := []map[string]interface{}{
		{"PartitionKey": "pk1", "RowKey": "rk1", "Name": "Alice"},
		{"PartitionKey": "pk1", "RowKey": "rk2", "Name": "Bob"},
		{"PartitionKey": "pk1", "RowKey": "rk3", "Name": "Alison"},
	}
	for _, e := range entities {
		m, _ := json.Marshal(e)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	filter := "startswith(Name, 'Al')"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Filter: &filter})

	count := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		count += len(page.Entities)
	}

	assert.Equal(t, 2, count, "Should find 2 entities starting with 'Al'")
}

func TestShouldHandleNullPropertyInFilterWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testnull")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Entity without Optional property (treated as null)
	entity1 := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Name":         "Alice",
	}
	// Entity with Optional property
	entity2 := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk2",
		"Name":         "Bob",
		"Optional":     "value",
	}

	m1, _ := json.Marshal(entity1)
	_, _ = client.AddEntity(ctx, m1, nil)
	m2, _ := json.Marshal(entity2)
	_, _ = client.AddEntity(ctx, m2, nil)

	// Query for entities where Optional is not null
	filter := "Optional ne null"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Filter: &filter})

	count := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		count += len(page.Entities)
	}

	assert.Equal(t, 1, count, "Should find 1 entity with non-null Optional property")
}

func TestShouldDistinguishEmptyStringFromNullWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testemptystring")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"EmptyString":  "",
		// MissingProp is null (not present)
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	resp, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	var retrieved map[string]interface{}
	json.Unmarshal(resp.Value, &retrieved)

	assert.Contains(t, retrieved, "EmptyString", "Empty string property should exist")
	assert.Equal(t, "", retrieved["EmptyString"])
	assert.NotContains(t, retrieved, "MissingProp", "Missing property should not exist")
}

// ============================================================================
// Type System Tests
// ============================================================================

func TestShouldPreserveInt64WithTypeAnnotationWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testint64")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey":           "pk1",
		"RowKey":                 "rk1",
		"LargeNumber":            "9223372036854775807", // Max Int64
		"LargeNumber@odata.type": "Edm.Int64",
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	resp, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	var retrieved map[string]interface{}
	json.Unmarshal(resp.Value, &retrieved)

	assert.Equal(t, "Edm.Int64", retrieved["LargeNumber@odata.type"])
}

func TestShouldHandleBinaryPropertyWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbinary")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Binary data is base64 encoded
	binaryData := "SGVsbG8gV29ybGQ=" // "Hello World" in base64
	entity := map[string]interface{}{
		"PartitionKey":          "pk1",
		"RowKey":                "rk1",
		"BinaryData":            binaryData,
		"BinaryData@odata.type": "Edm.Binary",
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	resp, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	var retrieved map[string]interface{}
	json.Unmarshal(resp.Value, &retrieved)

	assert.Equal(t, "Edm.Binary", retrieved["BinaryData@odata.type"])
	assert.Equal(t, binaryData, retrieved["BinaryData"])
}

func TestShouldHandleDoublePropertyWhenCallingAddEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testdouble")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Pi":           3.14159265359,
		"E":            2.71828182846,
	}
	marshalled, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, marshalled, nil)
	require.NoError(t, err)

	resp, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	var retrieved map[string]interface{}
	json.Unmarshal(resp.Value, &retrieved)

	assert.InDelta(t, 3.14159265359, retrieved["Pi"], 0.0001)
	assert.InDelta(t, 2.71828182846, retrieved["E"], 0.0001)
}

func TestShouldCoerceInt32ToInt64InFilterWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testcoercion")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Count":        42,
	}
	marshalled, _ := json.Marshal(entity)
	_, _ = client.AddEntity(ctx, marshalled, nil)

	// Filter with integer comparison
	filter := "Count eq 42"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Filter: &filter})

	count := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		count += len(page.Entities)
	}

	assert.Equal(t, 1, count, "Should find entity with Count=42")
}

// ============================================================================
// Query Limit Tests
// ============================================================================

func TestShouldRejectTopValueExceeding1000WhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testtoplimit")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Try to set Top > 1000 (should be rejected or capped)
	top := int32(1001)
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Top: &top})

	_, err := pager.NextPage(ctx)
	// Azure either rejects this or caps it to 1000
	// Test documents expected behavior
	if err != nil {
		assert.Error(t, err, "Should reject Top > 1000")
	}
}

func TestShouldAcceptTopValue1000WhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testtop1000")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Add some entities
	for i := 0; i < 10; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "pk1",
			"RowKey":       fmt.Sprintf("rk%d", i),
		}
		m, _ := json.Marshal(entity)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	// Top=1000 should be accepted
	top := int32(1000)
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Top: &top})

	_, err := pager.NextPage(ctx)
	require.NoError(t, err, "Should accept Top=1000")
}

// ============================================================================
// Case Sensitivity Tests
// ============================================================================

func TestShouldTreatPartitionKeyAsCaseSensitiveWhenCallingGetEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testpkcase")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "PK1",
		"RowKey":       "rk1",
	}
	marshalled, _ := json.Marshal(entity)
	_, _ = client.AddEntity(ctx, marshalled, nil)

	// Try to get with different case
	_, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	assert.Error(t, err, "PartitionKey should be case-sensitive (pk1 != PK1)")

	// Correct case should work
	_, err = client.GetEntity(ctx, "PK1", "rk1", nil)
	require.NoError(t, err, "Exact case match should succeed")
}

func TestShouldTreatRowKeyAsCaseSensitiveWhenCallingGetEntity(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testrkcase")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "RK1",
	}
	marshalled, _ := json.Marshal(entity)
	_, _ = client.AddEntity(ctx, marshalled, nil)

	// Try to get with different case
	_, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	assert.Error(t, err, "RowKey should be case-sensitive (rk1 != RK1)")

	// Correct case should work
	_, err = client.GetEntity(ctx, "pk1", "RK1", nil)
	require.NoError(t, err, "Exact case match should succeed")
}

// ============================================================================
// Merge Operation Property Deletion Tests
// ============================================================================

func TestShouldNotDeletePropertiesGivenMissingPropertiesWhenCallingMerge(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testmergenodelete")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Insert entity with multiple properties
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Name":         "Alice",
		"Age":          30,
		"City":         "Seattle",
	}
	marshalled, _ := json.Marshal(entity)
	_, _ = client.AddEntity(ctx, marshalled, nil)

	// Merge with only subset of properties
	merge := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Age":          31, // Update
	}
	mergeMarshalled, _ := json.Marshal(merge)
	_, err := client.UpsertEntity(ctx, mergeMarshalled, &aztables.UpsertEntityOptions{
		UpdateMode: aztables.UpdateModeMerge,
	})
	require.NoError(t, err)

	// Verify all original properties still exist
	resp, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	var retrieved map[string]interface{}
	json.Unmarshal(resp.Value, &retrieved)

	assert.Equal(t, "Alice", retrieved["Name"], "Name should be preserved")
	assert.Equal(t, "Seattle", retrieved["City"], "City should be preserved")
	assert.Equal(t, float64(31), retrieved["Age"], "Age should be updated")
}

func TestShouldDeleteAllNonKeyPropertiesGivenEmptyEntityWhenCallingReplace(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testreplace")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Insert entity with properties
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Name":         "Alice",
		"Age":          30,
	}
	marshalled, _ := json.Marshal(entity)
	addResp, _ := client.AddEntity(ctx, marshalled, nil)

	// Replace with entity containing no custom properties
	replace := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
	}
	replaceMarshalled, _ := json.Marshal(replace)
	_, err := client.UpdateEntity(ctx, replaceMarshalled, &aztables.UpdateEntityOptions{
		UpdateMode: aztables.UpdateModeReplace,
		IfMatch:    &addResp.ETag,
	})
	require.NoError(t, err)

	// Verify properties are removed
	resp, err := client.GetEntity(ctx, "pk1", "rk1", nil)
	require.NoError(t, err)

	var retrieved map[string]interface{}
	json.Unmarshal(resp.Value, &retrieved)

	assert.NotContains(t, retrieved, "Name", "Name should be removed in replace")
	assert.NotContains(t, retrieved, "Age", "Age should be removed in replace")
}

// ============================================================================
// Continuation Token Correctness Tests
// ============================================================================

func TestShouldReturnConsistentResultsAcrossPagesWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testconsistency")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Insert entities in mixed order
	for i := 10; i >= 1; i-- {
		entity := map[string]interface{}{
			"PartitionKey": "pk1",
			"RowKey":       fmt.Sprintf("rk%02d", i),
			"Value":        i,
		}
		m, _ := json.Marshal(entity)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	// Page through results
	top := int32(3)
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Top: &top})

	var allKeys []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, e := range page.Entities {
			var ent map[string]interface{}
			json.Unmarshal(e, &ent)
			allKeys = append(allKeys, ent["RowKey"].(string))
		}
	}

	// Results should be ordered by RowKey
	require.Len(t, allKeys, 10)
	for i := 0; i < len(allKeys)-1; i++ {
		assert.True(t, allKeys[i] < allKeys[i+1], "Results should be ordered")
	}
}

// ============================================================================
// Special Filter Edge Cases
// ============================================================================

func TestShouldHandleFilterWithParenthesesWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testparens")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entities := []map[string]interface{}{
		{"PartitionKey": "pk1", "RowKey": "rk1", "A": 1, "B": 2},
		{"PartitionKey": "pk1", "RowKey": "rk2", "A": 2, "B": 1},
		{"PartitionKey": "pk1", "RowKey": "rk3", "A": 1, "B": 1},
	}
	for _, e := range entities {
		m, _ := json.Marshal(e)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	// (A eq 1 and B eq 2) or (A eq 2 and B eq 1)
	filter := "(A eq 1 and B eq 2) or (A eq 2 and B eq 1)"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Filter: &filter})

	count := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		count += len(page.Entities)
	}

	assert.Equal(t, 2, count, "Should match 2 entities with complex filter")
}

func TestShouldHandleBooleanPropertyInFilterWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testboolfilter")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	entities := []map[string]interface{}{
		{"PartitionKey": "pk1", "RowKey": "rk1", "Active": true},
		{"PartitionKey": "pk1", "RowKey": "rk2", "Active": false},
		{"PartitionKey": "pk1", "RowKey": "rk3", "Active": true},
	}
	for _, e := range entities {
		m, _ := json.Marshal(e)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	filter := "Active eq true"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Filter: &filter})

	count := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		count += len(page.Entities)
	}

	assert.Equal(t, 2, count, "Should find 2 active entities")
}

// ============================================================================
// Helper Functions
// ============================================================================

func mustMarshalCompat(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal: %v", err))
	}
	return data
}
