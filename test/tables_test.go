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
	tableEmulatorURL = "http://localhost:10000"
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
		IfMatch: &addResp.ETag,
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
