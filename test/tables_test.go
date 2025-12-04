package test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	tableEmulatorURL = "http://localhost:10000"
	tableAccountName = "devstoreaccount1"
)

func skipTablesIfNotRunning(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", tableEmulatorURL+"/health", nil)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)

	if err != nil || (resp != nil && resp.StatusCode != 200) {
		t.Skip("Table emulator not running. Start with: docker compose up -d")
	}

	if resp != nil {
		resp.Body.Close()
	}
}

func newServiceClient(t *testing.T) *aztables.ServiceClient {
	client, err := aztables.NewServiceClientWithNoCredential(
		tableEmulatorURL+"/"+tableAccountName, nil,
	)
	require.NoError(t, err, "Arrange: failed to create service client")
	return client
}

func newTableClient(t *testing.T, tableName string) *aztables.Client {
	svc := newServiceClient(t)
	return svc.NewClient(tableName)
}

// ============================================================================
// ShouldCreateTableGivenValidNameWhenCallingCreateTable
// ============================================================================
func TestShouldCreateTableGivenValidNameWhenCallingCreateTable(t *testing.T) {
	skipTablesIfNotRunning(t)

	// Arrange
	ctx := context.Background()
	client := newTableClient(t, "table-create-test")

	// Act
	_, err := client.CreateTable(ctx, nil)

	// Assert
	require.NoError(t, err, "Assert: CreateTable should succeed")

	// Cleanup
	defer client.DeleteTable(ctx, nil)
}

// ============================================================================
// ShouldInsertEntityGivenNewPKRKWhenCallingAddEntity
// ============================================================================
func TestShouldInsertEntityGivenNewPKRKWhenCallingAddEntity(t *testing.T) {
	skipTablesIfNotRunning(t)

	// Arrange
	ctx := context.Background()
	client := newTableClient(t, "table-insert-test")
	_, _ = client.CreateTable(ctx, nil)
	defer client.DeleteTable(ctx, nil)

	entity := map[string]any{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Name":         "Alice",
	}

	// Act
	_, err := client.AddEntity(ctx, entity, nil)

	// Assert
	require.NoError(t, err, "Assert: AddEntity should succeed")
}

// ============================================================================
// ShouldDetectConflictGivenExistingEntityWhenCallingAddEntity
// ============================================================================
func TestShouldDetectConflictGivenExistingEntityWhenCallingAddEntity(t *testing.T) {
	skipTablesIfNotRunning(t)

	// Arrange
	ctx := context.Background()
	client := newTableClient(t, "table-conflict-test")
	_, _ = client.CreateTable(ctx, nil)
	defer client.DeleteTable(ctx, nil)

	entity := map[string]any{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Value":        "initial",
	}

	_, err := client.AddEntity(ctx, entity, nil)
	require.NoError(t, err)

	// Act
	_, err = client.AddEntity(ctx, entity, nil)

	// Assert
	require.Error(t, err, "Assert: duplicate PK/RK should fail with conflict")
}

// ============================================================================
// ShouldReplaceEntityGivenMatchingETagWhenCallingUpdateEntity
// ============================================================================
func TestShouldReplaceEntityGivenMatchingETagWhenCallingUpdateEntity(t *testing.T) {
	skipTablesIfNotRunning(t)

	ctx := context.Background()
	client := newTableClient(t, "table-replace-test")
	_, _ = client.CreateTable(ctx, nil)
	defer client.DeleteTable(ctx, nil)

	// Arrange
	entity := map[string]any{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Value":        "initial",
	}
	addResp, err := client.AddEntity(ctx, entity, nil)
	require.NoError(t, err)

	etag := *addResp.ETag

	update := map[string]any{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Value":        "updated",
	}

	// Act
	_, err = client.UpdateEntity(ctx, update, &aztables.UpdateEntityOptions{
		ETag: &etag,
		Mode: aztables.UpdateModeReplace,
	})

	// Assert
	require.NoError(t, err, "Assert: Replace with correct ETag should succeed")
}

// ============================================================================
// ShouldMergeEntityGivenMatchingETagWhenCallingUpdateEntity
// ============================================================================
func TestShouldMergeEntityGivenMatchingETagWhenCallingUpdateEntity(t *testing.T) {
	skipTablesIfNotRunning(t)

	ctx := context.Background()
	client := newTableClient(t, "table-merge-test")
	_, _ = client.CreateTable(ctx, nil)
	defer client.DeleteTable(ctx, nil)

	// Arrange
	entity := map[string]any{
		"PartitionKey": "p",
		"RowKey":       "1",
		"A":            "foo",
	}
	addResp, err := client.AddEntity(ctx, entity, nil)
	require.NoError(t, err)
	etag := *addResp.ETag

	patch := map[string]any{
		"PartitionKey": "p",
		"RowKey":       "1",
		"B":            "bar",
	}

	// Act
	_, err = client.UpdateEntity(ctx, patch, &aztables.UpdateEntityOptions{
		ETag: &etag,
		Mode: aztables.UpdateModeMerge,
	})

	// Assert
	require.NoError(t, err)
}

// ============================================================================
// ShouldDeleteEntityGivenValidPKRKWhenCallingDeleteEntity
// ============================================================================
func TestShouldDeleteEntityGivenValidPKRKWhenCallingDeleteEntity(t *testing.T) {
	skipTablesIfNotRunning(t)

	ctx := context.Background()
	client := newTableClient(t, "table-delete-test")
	_, _ = client.CreateTable(ctx, nil)
	defer client.DeleteTable(ctx, nil)

	// Arrange
	entity := map[string]any{
		"PartitionKey": "p",
		"RowKey":       "r",
	}
	_, err := client.AddEntity(ctx, entity, nil)
	require.NoError(t, err)

	// Act
	_, err = client.DeleteEntity(ctx, "p", "r", nil)

	// Assert
	require.NoError(t, err)
}

// ============================================================================
// ShouldQueryEntitiesGivenFilterWhenCallingListEntities
// ============================================================================
func TestShouldQueryEntitiesGivenFilterWhenCallingListEntities(t *testing.T) {
	skipTablesIfNotRunning(t)

	ctx := context.Background()
	client := newTableClient(t, "table-query-test")
	_, _ = client.CreateTable(ctx, nil)
	defer client.DeleteTable(ctx, nil)

	// Arrange
	for i := 1; i <= 3; i++ {
		entity := map[string]any{
			"PartitionKey": "p",
			"RowKey":       fmt.Sprintf("%d", i),
			"Value":        i,
		}
		_, err := client.AddEntity(ctx, entity, nil)
		require.NoError(t, err)
	}

	// Act
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: "Value gt 1",
	})

	var results []map[string]any

	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)

		for _, e := range page.Entities {
			results = append(results, e)
		}
	}

	// Assert
	assert.Len(t, results, 2)
}

// ============================================================================
// ShouldReturnContinuationTokensGivenLargeResultSetWhenCallingListEntities
// ============================================================================
func TestShouldReturnContinuationTokensGivenLargeResultSetWhenCallingListEntities(t *testing.T) {
	skipTablesIfNotRunning(t)

	ctx := context.Background()
	client := newTableClient(t, "table-continuation-test")
	_, _ = client.CreateTable(ctx, nil)
	defer client.DeleteTable(ctx, nil)

	// Arrange: insert > 50 rows
	for i := 0; i < 60; i++ {
		entity := map[string]any{
			"PartitionKey": "p",
			"RowKey":       fmt.Sprintf("%04d", i),
		}
		_, err := client.AddEntity(ctx, entity, nil)
		require.NoError(t, err)
	}

	// Act
	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Top: 20,
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
	assert.Greater(t, pageCount, 1, "Should paginate across multiple continuation pages")
	assert.Equal(t, 60, count)
}

// ============================================================================
// BATCH SEMANTICS – TRUE AZURE TABLE STORAGE
// ============================================================================

// ShouldExecuteBatchGivenValidOpsWhenCallingSubmitTransaction
func TestShouldExecuteBatchGivenValidOpsWhenCallingSubmitTransaction(t *testing.T) {
	skipTablesIfNotRunning(t)

	ctx := context.Background()
	client := newTableClient(t, "table-batch-test")
	_, _ = client.CreateTable(ctx, nil)
	defer client.DeleteTable(ctx, nil)

	// Arrange
	actions := []aztables.TransactionAction{
		aztables.NewAddEntityAction(map[string]any{
			"PartitionKey": "p",
			"RowKey":       "1",
		}),
		aztables.NewAddEntityAction(map[string]any{
			"PartitionKey": "p",
			"RowKey":       "2",
		}),
	}

	// Act
	resp, err := client.SubmitTransaction(ctx, actions, nil)

	// Assert
	require.NoError(t, err)
	assert.Len(t, resp.TransactionResponses, 2)
}

// ShouldFailBatchGivenMixedPartitionKeysWhenCallingSubmitTransaction
func TestShouldFailBatchGivenMixedPartitionKeysWhenCallingSubmitTransaction(t *testing.T) {
	skipTablesIfNotRunning(t)

	ctx := context.Background()
	client := newTableClient(t, "table-batch-pk-test")
	_, _ = client.CreateTable(ctx, nil)
	defer client.DeleteTable(ctx, nil)

	// Arrange (INVALID: different PKs)
	batch := []aztables.TransactionAction{
		aztables.NewAddEntityAction(map[string]any{
			"PartitionKey": "p1",
			"RowKey":       "1",
		}),
		aztables.NewAddEntityAction(map[string]any{
			"PartitionKey": "p2",
			"RowKey":       "2",
		}),
	}

	// Act
	_, err := client.SubmitTransaction(ctx, batch, nil)

	// Assert
	require.Error(t, err, "Azure should reject mixed-PK batch")
}

// ShouldBeAtomicGivenOneOperationFailsWhenCallingSubmitTransaction
func TestShouldBeAtomicGivenOneOperationFailsWhenCallingSubmitTransaction(t *testing.T) {
	skipTablesIfNotRunning(t)

	ctx := context.Background()
	client := newTableClient(t, "table-batch-atomic-test")
	_, _ = client.CreateTable(ctx, nil)
	defer client.DeleteTable(ctx, nil)

	// Arrange
	_ = client.AddEntity(ctx, map[string]any{
		"PartitionKey": "p",
		"RowKey":       "dupe",
	}, nil)

	batch := []aztables.TransactionAction{
		aztables.NewAddEntityAction(map[string]any{
			"PartitionKey": "p",
			"RowKey":       "1",
		}),
		// This insert will fail because RowKey=dupe exists
		aztables.NewAddEntityAction(map[string]any{
			"PartitionKey": "p",
			"RowKey":       "dupe",
		}),
		aztables.NewAddEntityAction(map[string]any{
			"PartitionKey": "p",
			"RowKey":       "2",
		}),
	}

	// Act
	_, err := client.SubmitTransaction(ctx, batch, nil)

	// Assert
	require.Error(t, err, "Batch should fail atomically")

	// Verify atomicity (1 and 2 should NOT exist)
	for _, rk := range []string{"1", "2"} {
		get := client.NewGetEntityPager("p", rk, nil)
		page, _ := get.NextPage(ctx)
		assert.Equal(t, 0, len(page.Entities), "Entity should not exist due to batch rollback")
	}
}
