package test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

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
