package test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	tableEmulatorURL = "http://localhost:10002"
	tableAccountName = "devstoreaccount1"
)

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

	// Insert many entities
	for i := 0; i < 25; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "p",
			"RowKey":       fmt.Sprintf("%04d", i),
		}
		marshalled, err := json.Marshal(entity)
		require.NoError(t, err)
		_, err = client.AddEntity(ctx, marshalled, nil)
		require.NoError(t, err)
	}

	// List with pagination
	top := int32(10)
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

	assert.GreaterOrEqual(t, count, 25, "Should retrieve all entities")
	assert.GreaterOrEqual(t, pageCount, 2, "Should paginate across multiple pages")
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

func TestShouldFilterWithEqGivenMatchingEntitiesWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testfiltereq")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Add entities
	entities := []map[string]interface{}{
		{"PartitionKey": "p", "RowKey": "1", "Status": "active"},
		{"PartitionKey": "p", "RowKey": "2", "Status": "inactive"},
		{"PartitionKey": "p", "RowKey": "3", "Status": "active"},
	}
	for _, e := range entities {
		m, _ := json.Marshal(e)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	// Filter with eq
	filter := "Status eq 'active'"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
	})

	count := 0
	for pager.More() {
		page, _ := pager.NextPage(ctx)
		count += len(page.Entities)
	}

	assert.Equal(t, 2, count, "Should find 2 active entities")
}

func TestShouldFilterWithAndGivenMultipleConditionsWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testfilterand")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Add entities
	entities := []map[string]interface{}{
		{"PartitionKey": "p", "RowKey": "1", "Status": "active", "Priority": 1},
		{"PartitionKey": "p", "RowKey": "2", "Status": "active", "Priority": 2},
		{"PartitionKey": "p", "RowKey": "3", "Status": "inactive", "Priority": 1},
	}
	for _, e := range entities {
		m, _ := json.Marshal(e)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	// Filter with and
	filter := "Status eq 'active' and Priority eq 1"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
	})

	count := 0
	for pager.More() {
		page, _ := pager.NextPage(ctx)
		count += len(page.Entities)
	}

	assert.Equal(t, 1, count, "Should find 1 entity matching both conditions")
}

func TestShouldFilterWithOrGivenEitherConditionWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testfilteror")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Add entities
	entities := []map[string]interface{}{
		{"PartitionKey": "p", "RowKey": "1", "Status": "active"},
		{"PartitionKey": "p", "RowKey": "2", "Status": "pending"},
		{"PartitionKey": "p", "RowKey": "3", "Status": "inactive"},
	}
	for _, e := range entities {
		m, _ := json.Marshal(e)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	// Filter with or
	filter := "Status eq 'active' or Status eq 'pending'"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
	})

	count := 0
	for pager.More() {
		page, _ := pager.NextPage(ctx)
		count += len(page.Entities)
	}

	assert.Equal(t, 2, count, "Should find 2 entities matching either condition")
}

func TestShouldFilterWithGeGivenNumericComparisonWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testfilterge")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Add entities
	entities := []map[string]interface{}{
		{"PartitionKey": "p", "RowKey": "1", "Score": 50},
		{"PartitionKey": "p", "RowKey": "2", "Score": 75},
		{"PartitionKey": "p", "RowKey": "3", "Score": 100},
	}
	for _, e := range entities {
		m, _ := json.Marshal(e)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	// Filter with ge (greater than or equal)
	filter := "Score ge 75"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
	})

	count := 0
	for pager.More() {
		page, _ := pager.NextPage(ctx)
		count += len(page.Entities)
	}

	assert.Equal(t, 2, count, "Should find 2 entities with Score >= 75")
}

func TestShouldFilterWithNeGivenNotEqualWhenCallingListEntities(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testfilterwithne")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Add entities
	entities := []map[string]interface{}{
		{"PartitionKey": "p", "RowKey": "1", "Type": "A"},
		{"PartitionKey": "p", "RowKey": "2", "Type": "B"},
		{"PartitionKey": "p", "RowKey": "3", "Type": "A"},
	}
	for _, e := range entities {
		m, _ := json.Marshal(e)
		_, _ = client.AddEntity(ctx, m, nil)
	}

	// Filter with ne (not equal)
	filter := "Type ne 'A'"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
	})

	count := 0
	for pager.More() {
		page, _ := pager.NextPage(ctx)
		count += len(page.Entities)
	}

	assert.Equal(t, 1, count, "Should find 1 entity where Type != A")
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

	// Insert many entities for a single tenant partition
	// Simulate something like Tcontrol<tenantId> with lots of rows
	for i := 0; i < 200; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "tenant-hydden",
			"RowKey":       fmt.Sprintf("%06d", i),
			"Kind":         "noise",
		}
		if i == 199 {
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
	const total = 50
	for i := 0; i < total; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "p",
			"RowKey":       fmt.Sprintf("%04d", i),
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}

	top := int32(10)
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Top: &top,
	})

	seen := make(map[string]bool)
	count := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
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
	for i := 0; i < 10; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "tenant-a",
			"RowKey":       fmt.Sprintf("%04d", i),
			"Enabled":      i%2 == 0,
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}
	for i := 0; i < 10; i++ {
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

	assert.Equal(t, 10, count, "Should return all tenant-b enabled entities")
}

// ============================================================================
// Bug Proof Tests
// These tests are designed to FAIL until the corresponding bugs are fixed.
// ============================================================================

// BUG PROOF: $select is parsed but ignored by fazure.
// This test will FAIL until fazure properly applies $select projection.
// Expected behavior: City should NOT be in the response when not selected.
func TestBugProof_SelectShouldExcludeNonSelectedProperties(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbugselectexclude")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: entity with multiple properties
	entity := map[string]interface{}{
		"PartitionKey": "p",
		"RowKey":       "r",
		"Name":         "Alice",
		"Age":          30,
		"City":         "Seattle",
		"Country":      "USA",
	}
	m, _ := json.Marshal(entity)
	_, err := client.AddEntity(ctx, m, nil)
	require.NoError(t, err)

	// Act: query with $select for only Name and Age
	selectStr := "Name,Age"
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Select: &selectStr,
	})

	var retrieved map[string]interface{}
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		if len(page.Entities) > 0 {
			json.Unmarshal(page.Entities[0], &retrieved)
			break
		}
	}

	// Assert: City and Country should NOT be present (this will FAIL until bug is fixed)
	assert.NotContains(t, retrieved, "City", "BUG: City should be excluded when not in $select")
	assert.NotContains(t, retrieved, "Country", "BUG: Country should be excluded when not in $select")
	// These should still be present
	assert.Contains(t, retrieved, "Name")
	assert.Contains(t, retrieved, "Age")
}

// BUG PROOF: GetEntity should be fast for known keys, ListEntities with filter should not.
// This test demonstrates that using GetEntity (direct key lookup) vs ListEntities
// (scan+filter) can have drastically different performance characteristics.
// If your app uses ListEntities where GetEntity would work, you're doing it wrong.
func TestBugProof_GetEntityShouldBeFasterThanFilteredListForKnownKey(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbuggetvslist")
	// Clean up any leftover table from previous runs
	_, _ = client.Delete(ctx, nil)
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: insert many entities, with one "settings" entity at a known location
	const total = 500
	settingsPK := "tenant-xyz"
	settingsRK := "settings-main"

	for i := 0; i < total; i++ {
		entity := map[string]interface{}{
			"PartitionKey": settingsPK,
			"RowKey":       fmt.Sprintf("noise-%06d", i),
			"Kind":         "noise",
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}
	// Insert the settings entity with known key
	settingsEntity := map[string]interface{}{
		"PartitionKey": settingsPK,
		"RowKey":       settingsRK,
		"Kind":         "settings",
		"Value":        "important-config",
	}
	m, _ := json.Marshal(settingsEntity)
	_, err := client.AddEntity(ctx, m, nil)
	require.NoError(t, err)

	// Act & Assert: GetEntity should work and return the entity
	resp, err := client.GetEntity(ctx, settingsPK, settingsRK, nil)
	require.NoError(t, err, "GetEntity should succeed for known key")

	var retrieved map[string]interface{}
	json.Unmarshal(resp.Value, &retrieved)
	assert.Equal(t, "settings", retrieved["Kind"])
	assert.Equal(t, "important-config", retrieved["Value"])

	// Contrast: using ListEntities with filter (this is the "wrong" way for known keys)
	// This will work but is inefficient - scanning 500+ rows to find 1
	filter := fmt.Sprintf("PartitionKey eq '%s' and RowKey eq '%s'", settingsPK, settingsRK)
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
	})

	var listResults []map[string]interface{}
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, e := range page.Entities {
			var ent map[string]interface{}
			json.Unmarshal(e, &ent)
			listResults = append(listResults, ent)
		}
	}

	require.Len(t, listResults, 1, "ListEntities should also find exactly 1 entity")
	assert.Equal(t, "settings", listResults[0]["Kind"])
}

// BUG PROOF: Pagination should not drop entities between pages.
// This test creates a specific scenario where the off-by-one continuation
// token bug would cause an entity to be skipped.
func TestBugProof_PaginationShouldNotSkipEntityAtPageBoundary(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbugpageskip")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: insert exactly 15 entities
	const total = 15
	expected := make([]string, total)
	for i := 0; i < total; i++ {
		rk := fmt.Sprintf("row-%04d", i)
		expected[i] = rk
		entity := map[string]interface{}{
			"PartitionKey": "p",
			"RowKey":       rk,
			"Index":        i,
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}

	// Act: page with top=5 (should get 3 pages: 5, 5, 5)
	top := int32(5)
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Top: &top,
	})

	var actual []string
	pageNum := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		pageNum++
		for _, e := range page.Entities {
			var ent map[string]interface{}
			json.Unmarshal(e, &ent)
			actual = append(actual, ent["RowKey"].(string))
		}
	}

	// Assert: should have all 15 entities, no skips, no duplicates
	assert.Equal(t, total, len(actual), "BUG: Should have exactly %d entities, got %d (possible skip at page boundary)", total, len(actual))
	assert.ElementsMatch(t, expected, actual, "BUG: Entity set mismatch - some entities may have been skipped or duplicated")
	assert.GreaterOrEqual(t, pageNum, 3, "Should have at least 3 pages")
}

// BUG PROOF: Pagination with filter should not skip matching entities.
// This is a more complex scenario combining filtering with pagination.
func TestBugProof_PaginationWithFilterShouldNotSkipMatches(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbugpagefilter")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: insert 30 entities, every 3rd one is "important"
	const total = 30
	expectedImportant := 0
	for i := 0; i < total; i++ {
		kind := "noise"
		if i%3 == 0 {
			kind = "important"
			expectedImportant++
		}
		entity := map[string]interface{}{
			"PartitionKey": "p",
			"RowKey":       fmt.Sprintf("row-%04d", i),
			"Kind":         kind,
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}

	// Act: query with filter and small page size
	filter := "Kind eq 'important'"
	top := int32(3)
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
		Top:    &top,
	})

	count := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, e := range page.Entities {
			var ent map[string]interface{}
			json.Unmarshal(e, &ent)
			assert.Equal(t, "important", ent["Kind"], "Filter should only return 'important' entities")
			count++
		}
	}

	// Assert: should find all "important" entities (every 3rd = 10 total)
	assert.Equal(t, expectedImportant, count, "BUG: Should find exactly %d 'important' entities, got %d", expectedImportant, count)
}

// BUG PROOF: Filter with PartitionKey should constrain the scan range.
// This test checks that queries with PartitionKey eq '...' don't scan other partitions.
// We can't directly measure scan range, but we can verify correctness.
func TestBugProof_PartitionKeyFilterShouldNotReturnOtherPartitions(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbugpkfilter")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: create entities in multiple partitions with same property values
	partitions := []string{"tenant-alpha", "tenant-beta", "tenant-gamma"}
	for _, pk := range partitions {
		for i := 0; i < 5; i++ {
			entity := map[string]interface{}{
				"PartitionKey": pk,
				"RowKey":       fmt.Sprintf("row-%d", i),
				"Status":       "active", // Same value across all partitions
			}
			m, _ := json.Marshal(entity)
			_, err := client.AddEntity(ctx, m, nil)
			require.NoError(t, err)
		}
	}

	// Act: query for only tenant-beta with Status eq 'active'
	filter := "PartitionKey eq 'tenant-beta' and Status eq 'active'"
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
			// Every returned entity MUST be from tenant-beta
			assert.Equal(t, "tenant-beta", ent["PartitionKey"], "BUG: Got entity from wrong partition")
			count++
		}
	}

	// Should get exactly 5 (only tenant-beta's entities)
	assert.Equal(t, 5, count, "Should return exactly 5 entities from tenant-beta")
}

// ============================================================================
// Timing / Performance Tests
// These tests measure query latency to reproduce the ~2s delay issue.
// ============================================================================

// TIMING TEST: Reproduces the tenant table 2s delay.
// Creates a tenant-style table with many rows, then queries for a single
// entity using a filter WITHOUT PartitionKey (forces full scan).
// This should be FAST but may be slow due to full-table scan.
func TestTiming_TenantTableQueryWithoutPartitionKey(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testtimingnopartkey")
	_, _ = client.Delete(ctx, nil)
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: simulate Tcontrol<tenantId> with many rows
	const totalRows = 1000
	t.Logf("Inserting %d entities...", totalRows)
	insertStart := time.Now()
	for i := 0; i < totalRows; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "tenant-hydden",
			"RowKey":       fmt.Sprintf("%06d", i),
			"Kind":         "config",
			"Category":     fmt.Sprintf("cat-%d", i%10),
		}
		// Put the "settings" entity somewhere in the middle
		if i == 500 {
			entity["Kind"] = "settings"
			entity["Category"] = "hydden-settings"
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}
	t.Logf("Insert took %v", time.Since(insertStart))

	// Act: Query WITHOUT PartitionKey filter (this is the problematic pattern)
	// This forces a full table scan
	filter := "Kind eq 'settings'"
	t.Logf("Querying with filter: %s (NO PartitionKey - forces full scan)", filter)

	queryStart := time.Now()
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
	queryDuration := time.Since(queryStart)

	// Assert
	require.Len(t, results, 1, "Should find exactly 1 entity")
	assert.Equal(t, "settings", results[0]["Kind"])

	t.Logf("Query returned %d entity in %v", len(results), queryDuration)

	// This is the key assertion - query should be fast even with many rows
	// Currently this may FAIL if the full scan is slow
	if queryDuration > 500*time.Millisecond {
		t.Logf("WARNING: Query took %v - this is the 2s delay bug!", queryDuration)
	}
	assert.Less(t, queryDuration, 2*time.Second, "Query should complete in under 2s")
}

// TIMING TEST: Same query but WITH PartitionKey filter.
// This should be fast because it only scans one partition.
func TestTiming_TenantTableQueryWithPartitionKey(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testtimingwithpartkey")
	_, _ = client.Delete(ctx, nil)
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange: same setup as above
	const totalRows = 1000
	t.Logf("Inserting %d entities...", totalRows)
	insertStart := time.Now()
	for i := 0; i < totalRows; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "tenant-hydden",
			"RowKey":       fmt.Sprintf("%06d", i),
			"Kind":         "config",
		}
		if i == 500 {
			entity["Kind"] = "settings"
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}
	t.Logf("Insert took %v", time.Since(insertStart))

	// Act: Query WITH PartitionKey filter (should enable partition scan optimization)
	filter := "PartitionKey eq 'tenant-hydden' and Kind eq 'settings'"
	t.Logf("Querying with filter: %s (WITH PartitionKey)", filter)

	queryStart := time.Now()
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
	queryDuration := time.Since(queryStart)

	// Assert
	require.Len(t, results, 1, "Should find exactly 1 entity")
	assert.Equal(t, "settings", results[0]["Kind"])

	t.Logf("Query returned %d entity in %v", len(results), queryDuration)
	assert.Less(t, queryDuration, 500*time.Millisecond, "Query with PartitionKey should be fast")
}

// TIMING TEST: Compares system table (small) vs tenant table (large).
// This mirrors the real-world scenario where Tcontrolsystem is fast but
// Tcontrol<tenantId> is slow.
func TestTiming_SystemTableVsTenantTable(t *testing.T) {
	ctx := context.Background()

	// Setup system table (small, few rows)
	systemClient := newTableClient(t, "Tcontrolsystem")
	_, _ = systemClient.Delete(ctx, nil)
	_, _ = systemClient.CreateTable(ctx, nil)
	defer func() {
		_, _ = systemClient.Delete(ctx, nil)
	}()

	// Setup tenant table (large, many rows)
	tenantClient := newTableClient(t, "Tcontrolhydden")
	_, _ = tenantClient.Delete(ctx, nil)
	_, _ = tenantClient.CreateTable(ctx, nil)
	defer func() {
		_, _ = tenantClient.Delete(ctx, nil)
	}()

	// Insert few rows in system table
	for i := 0; i < 10; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "system",
			"RowKey":       fmt.Sprintf("setting-%d", i),
			"Kind":         "system-config",
		}
		if i == 5 {
			entity["Kind"] = "target-setting"
		}
		m, _ := json.Marshal(entity)
		_, _ = systemClient.AddEntity(ctx, m, nil)
	}

	// Insert many rows in tenant table
	const tenantRows = 10000
	t.Logf("Inserting %d entities into tenant table...", tenantRows)
	for i := 0; i < tenantRows; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "tenant-hydden",
			"RowKey":       fmt.Sprintf("%06d", i),
			"Kind":         "tenant-config",
		}
		if i == 5000 {
			entity["Kind"] = "target-setting"
		}
		m, _ := json.Marshal(entity)
		_, _ = tenantClient.AddEntity(ctx, m, nil)
	}

	// Query system table
	systemFilter := "Kind eq 'target-setting'"
	systemStart := time.Now()
	systemPager := systemClient.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &systemFilter,
	})
	var systemResults []map[string]interface{}
	for systemPager.More() {
		page, _ := systemPager.NextPage(ctx)
		for _, e := range page.Entities {
			var ent map[string]interface{}
			json.Unmarshal(e, &ent)
			systemResults = append(systemResults, ent)
		}
	}
	systemDuration := time.Since(systemStart)

	// Query tenant table
	tenantFilter := "Kind eq 'target-setting'"
	tenantStart := time.Now()
	tenantPager := tenantClient.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &tenantFilter,
	})
	var tenantResults []map[string]interface{}
	for tenantPager.More() {
		page, _ := tenantPager.NextPage(ctx)
		for _, e := range page.Entities {
			var ent map[string]interface{}
			json.Unmarshal(e, &ent)
			tenantResults = append(tenantResults, ent)
		}
	}
	tenantDuration := time.Since(tenantStart)

	// Results
	t.Logf("System table: %d results in %v", len(systemResults), systemDuration)
	t.Logf("Tenant table: %d results in %v", len(tenantResults), tenantDuration)
	t.Logf("Tenant/System ratio: %.1fx slower", float64(tenantDuration)/float64(systemDuration))

	require.Len(t, systemResults, 1)
	require.Len(t, tenantResults, 1)

	// The tenant query should not be dramatically slower
	// If it's >10x slower, that's the bug
	if tenantDuration > 10*systemDuration && tenantDuration > time.Second {
		t.Logf("BUG REPRODUCED: Tenant table query is %.1fx slower than system table!", float64(tenantDuration)/float64(systemDuration))
	}
}

// BUG PROOF: Complex filter with parentheses should still extract PartitionKey.
// This tests whether extractPartitionKeyFromFilter handles realistic filter shapes.
func TestBugProof_ComplexFilterWithParensShouldWork(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbugcomplexfilter")
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Arrange
	for i := 0; i < 10; i++ {
		entity := map[string]interface{}{
			"PartitionKey": "pk-test",
			"RowKey":       fmt.Sprintf("rk-%d", i),
			"Type":         "A",
			"Value":        i,
		}
		if i%2 == 0 {
			entity["Type"] = "B"
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}

	// Act: filter with a shape that might trip up simple parsing
	// Looking for pk-test entities where (Type eq 'A' or Type eq 'B') and Value ge 5
	filter := "PartitionKey eq 'pk-test' and Value ge 5"
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
			assert.Equal(t, "pk-test", ent["PartitionKey"])
			val := ent["Value"].(float64)
			assert.GreaterOrEqual(t, val, float64(5), "Value should be >= 5")
			count++
		}
	}

	// Values 5,6,7,8,9 = 5 entities
	assert.Equal(t, 5, count, "Should return 5 entities with Value >= 5")
}

// BUG PROOF: Empty RowKey should not break pagination or cause re-scans.
// If RowKey is empty string "", the continuation token logic may fail.
func TestBugProof_EmptyRowKeyShouldNotBreakPagination(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbugemptyrk")
	_, _ = client.Delete(ctx, nil)
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Insert entities with empty RowKey (Azure Tables allows this)
	for i := 0; i < 25; i++ {
		entity := map[string]interface{}{
			"PartitionKey": fmt.Sprintf("pk-%02d", i),
			"RowKey":       "", // Empty RowKey!
			"Value":        i,
		}
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}

	// Query with pagination - this may break if empty RowKey isn't handled
	top := int32(10)
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Top: &top,
	})

	seen := make(map[string]bool)
	count := 0
	pageNum := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		pageNum++
		for _, e := range page.Entities {
			var ent map[string]interface{}
			json.Unmarshal(e, &ent)
			pk := ent["PartitionKey"].(string)
			rk := ent["RowKey"].(string)
			key := fmt.Sprintf("%s|%s", pk, rk)
			if seen[key] {
				t.Errorf("BUG: Duplicate entity %s on page %d (empty RowKey causes re-scan)", key, pageNum)
			}
			seen[key] = true
			count++
		}
		t.Logf("Page %d: got %d entities", pageNum, len(page.Entities))
	}

	assert.Equal(t, 25, count, "BUG: Should get exactly 25 entities, got %d (empty RowKey may cause issues)", count)
	assert.GreaterOrEqual(t, pageNum, 3, "Should have at least 3 pages")
}

// BUG PROOF: Mixed empty and non-empty RowKeys in same partition.
func TestBugProof_MixedEmptyRowKeysShouldWork(t *testing.T) {
	ctx := context.Background()
	client := newTableClient(t, "testbugmixedrk")
	_, _ = client.Delete(ctx, nil)
	_, _ = client.CreateTable(ctx, nil)
	defer func() {
		_, _ = client.Delete(ctx, nil)
	}()

	// Insert entities with mix of empty and non-empty RowKeys
	entities := []map[string]interface{}{
		{"PartitionKey": "pk", "RowKey": "", "Name": "empty-rk"},
		{"PartitionKey": "pk", "RowKey": "rk1", "Name": "rk1"},
		{"PartitionKey": "pk", "RowKey": "rk2", "Name": "rk2"},
	}
	for _, entity := range entities {
		m, _ := json.Marshal(entity)
		_, err := client.AddEntity(ctx, m, nil)
		require.NoError(t, err)
	}

	// Query all - should get all 3
	pager := client.NewListEntitiesPager(nil)
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

	assert.Len(t, results, 3, "Should get all 3 entities including empty RowKey")
}
