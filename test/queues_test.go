package test

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	queueEmulatorURL = "http://localhost:10001"
	queueAccountName = "devstoreaccount1"
)

func newQueueServiceClient(t *testing.T) *azqueue.ServiceClient {
	client, err := azqueue.NewServiceClientWithNoCredential(queueEmulatorURL+"/"+queueAccountName, nil)
	require.NoError(t, err)
	return client
}

func newQueueClient(t *testing.T, queueName string) *azqueue.QueueClient {
	service := newQueueServiceClient(t)
	return service.NewQueueClient(queueName)
}

// ============================================================================
// ShouldCreateQueueGivenValidNameWhenCallingCreateQueue
// ============================================================================
func TestShouldCreateQueueGivenValidNameWhenCallingCreateQueue(t *testing.T) {
	// Arrange
	ctx := context.Background()
	client, err := azqueue.NewServiceClientWithNoCredential(queueEmulatorURL+"/"+queueAccountName, nil)
	require.NoError(t, err, "Arrange: failed creating service client")

	qClient := client.NewQueueClient("create-queue-test")

	// Act
	_, err = qClient.Create(ctx, nil)

	// Assert
	require.NoError(t, err, "Assert: Create returned error")

	// Cleanup
	defer qClient.Delete(ctx, nil)
}

// ============================================================================
// ShouldEnqueueMessageGivenExistingQueueWhenCallingEnqueue
// ============================================================================
func TestShouldEnqueueMessageGivenExistingQueueWhenCallingEnqueue(t *testing.T) {
	// Arrange
	ctx := context.Background()
	service, err := azqueue.NewServiceClientWithNoCredential(queueEmulatorURL+"/"+queueAccountName, nil)
	require.NoError(t, err)

	qClient := service.NewQueueClient("enqueue-test")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	msg := "hello queue"

	// Act
	msgResp, err := qClient.EnqueueMessage(ctx, msg, nil)

	// Assert
	require.NoError(t, err, "Assert: enqueue failed")
	assert.NotNil(t, msgResp)
}

// ============================================================================
// ShouldDequeueMessageGivenExistingQueueWhenCallingDequeue
// ============================================================================
func TestShouldDequeueMessageGivenExistingQueueWhenCallingDequeue(t *testing.T) {
	// Arrange
	ctx := context.Background()
	service, err := azqueue.NewServiceClientWithNoCredential(queueEmulatorURL+"/"+queueAccountName, nil)
	require.NoError(t, err)

	qClient := service.NewQueueClient("dequeue-test")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	_, err = qClient.EnqueueMessage(ctx, "dequeue-message", nil)
	require.NoError(t, err)

	// Act
	resp, err := qClient.DequeueMessages(ctx, nil)

	// Assert
	require.NoError(t, err, "Assert: dequeue returned error")
	require.NotNil(t, resp.Messages, "Assert: no messages returned")
	require.Len(t, resp.Messages, 1, "Assert: expected exactly one message")

	msg := resp.Messages[0]
	assert.Equal(t, "dequeue-message", *msg.MessageText)
	assert.NotEmpty(t, *msg.PopReceipt, "Assert: pop receipt should not be empty")
}

// ============================================================================
// ShouldRespectVisibilityTimeoutGivenMessageDequeuedWhenCallingDequeueAgain
// ============================================================================
func TestShouldRespectVisibilityTimeoutGivenMessageDequeuedWhenCallingDequeueAgain(t *testing.T) {
	// Arrange
	ctx := context.Background()
	service, err := azqueue.NewServiceClientWithNoCredential(queueEmulatorURL+"/"+queueAccountName, nil)
	require.NoError(t, err)

	qClient := service.NewQueueClient("visibility-test")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	_, err = qClient.EnqueueMessage(ctx, "vis-msg", nil)
	require.NoError(t, err)

	// Act — first dequeue (sets visibility timeout)
	first, err := qClient.DequeueMessages(ctx, &azqueue.DequeueMessagesOptions{
		VisibilityTimeout: func() *int32 { v := int32(3); return &v }(), // seconds
	})
	require.NoError(t, err)
	require.Len(t, first.Messages, 1, "Assert: first dequeue should return a message")

	// Act — second immediate dequeue should return *zero* messages
	second, err := qClient.DequeueMessages(ctx, nil)
	require.NoError(t, err)

	// Assert
	assert.Len(t, second.Messages, 0, "Assert: message should be invisible during timeout")

	// Wait for visibility timeout to expire
	time.Sleep(3 * time.Second)

	// Act — third dequeue should reveal the message again
	third, err := qClient.DequeueMessages(ctx, nil)
	require.NoError(t, err)

	// Assert
	assert.Len(t, third.Messages, 1, "Assert: message should reappear after visibility timeout")
}

// ============================================================================
// ShouldDeleteMessageGivenValidPopReceiptWhenCallingDeleteMessage
// ============================================================================
func TestShouldDeleteMessageGivenValidPopReceiptWhenCallingDeleteMessage(t *testing.T) {
	// Arrange
	ctx := context.Background()
	service, err := azqueue.NewServiceClientWithNoCredential(queueEmulatorURL+"/"+queueAccountName, nil)
	require.NoError(t, err)

	qClient := service.NewQueueClient("delete-msg-test")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	enqueueResp, err := qClient.EnqueueMessage(ctx, "delete-me", nil)
	require.NoError(t, err)
	_ = enqueueResp

	deq, err := qClient.DequeueMessages(ctx, nil)
	require.NoError(t, err)
	require.Len(t, deq.Messages, 1)

	msg := deq.Messages[0]

	// Act
	_, err = qClient.DeleteMessage(ctx, *msg.MessageID, *msg.PopReceipt, nil)

	// Assert
	require.NoError(t, err, "Assert: DeleteMessage failed")

	// Verify queue is empty
	check, err := qClient.DequeueMessages(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, check.Messages, 0, "Assert: message was not deleted")
}

// ============================================================================
// ShouldClearMessagesGivenQueueWithMessagesWhenCallingClear
// ============================================================================
func TestShouldClearMessagesGivenQueueWithMessagesWhenCallingClear(t *testing.T) {
	// Arrange
	ctx := context.Background()
	service, err := azqueue.NewServiceClientWithNoCredential(queueEmulatorURL+"/"+queueAccountName, nil)
	require.NoError(t, err)

	qClient := service.NewQueueClient("clear-test")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	// Add multiple messages
	for i := 0; i < 3; i++ {
		_, err := qClient.EnqueueMessage(ctx, "msg", nil)
		require.NoError(t, err)
	}

	// Act
	_, err = qClient.ClearMessages(ctx, nil)

	// Assert
	require.NoError(t, err, "Assert: ClearMessages failed")

	after, err := qClient.DequeueMessages(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, after.Messages, 0, "Assert: queue should be empty after clear")
}

// ============================================================================
// Peek Messages Tests
// ============================================================================

func TestShouldPeekMessageGivenExistingMessageWhenCallingPeekMessages(t *testing.T) {
	ctx := context.Background()
	qClient := newQueueClient(t, "peek-test")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	// Add message
	_, err := qClient.EnqueueMessage(ctx, "peek-me", nil)
	require.NoError(t, err)

	// Peek (should not remove message)
	resp, err := qClient.PeekMessages(ctx, nil)
	require.NoError(t, err, "PeekMessages should succeed")
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "peek-me", *resp.Messages[0].MessageText)

	// Verify message still exists (peek doesn't dequeue)
	deq, err := qClient.DequeueMessages(ctx, nil)
	require.NoError(t, err)
	require.Len(t, deq.Messages, 1, "Message should still exist after peek")
}

func TestShouldPeekMultipleMessagesGivenQueueWithManyMessagesWhenCallingPeekMessages(t *testing.T) {
	ctx := context.Background()
	qClient := newQueueClient(t, "peek-multi")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	// Add multiple messages
	for i := 0; i < 5; i++ {
		_, _ = qClient.EnqueueMessage(ctx, "msg", nil)
	}

	// Peek up to 3
	numMessages := int32(3)
	resp, err := qClient.PeekMessages(ctx, &azqueue.PeekMessagesOptions{
		NumberOfMessages: &numMessages,
	})
	require.NoError(t, err, "PeekMessages should succeed")
	assert.Len(t, resp.Messages, 3, "Should peek exactly 3 messages")
}

// ============================================================================
// Update Message Tests
// ============================================================================

func TestShouldUpdateMessageGivenValidPopReceiptWhenCallingUpdateMessage(t *testing.T) {
	ctx := context.Background()
	qClient := newQueueClient(t, "update-msg")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	// Add and dequeue message
	_, _ = qClient.EnqueueMessage(ctx, "original", nil)

	deq, err := qClient.DequeueMessages(ctx, nil)
	require.NoError(t, err)
	require.Len(t, deq.Messages, 1)

	msg := deq.Messages[0]

	// Update message text
	_, err = qClient.UpdateMessage(ctx, *msg.MessageID, *msg.PopReceipt, "updated", &azqueue.UpdateMessageOptions{
		VisibilityTimeout: ptrInt32(0), // Make visible immediately
	})
	require.NoError(t, err, "UpdateMessage should succeed")

	// Dequeue again and verify update
	deq2, err := qClient.DequeueMessages(ctx, nil)
	require.NoError(t, err)
	require.Len(t, deq2.Messages, 1)
	assert.Equal(t, "updated", *deq2.Messages[0].MessageText)
}

func ptrInt32(i int32) *int32 { return &i }

// ============================================================================
// Queue Metadata Tests
// ============================================================================

func TestShouldSetQueueMetadataGivenExistingQueueWhenCallingSetMetadata(t *testing.T) {
	ctx := context.Background()
	qClient := newQueueClient(t, "queue-meta")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	// Set metadata
	metadata := map[string]*string{
		"key1": ptrQueueString("value1"),
	}
	_, err := qClient.SetMetadata(ctx, &azqueue.SetMetadataOptions{
		Metadata: metadata,
	})
	require.NoError(t, err, "SetMetadata should succeed")

	// Verify
	props, err := qClient.GetProperties(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, props.Metadata, "Metadata should not be nil")
	require.NotNil(t, props.Metadata["key1"], "key1 should exist in metadata")
	assert.Equal(t, "value1", *props.Metadata["key1"])
}

func ptrQueueString(s string) *string { return &s }

// ============================================================================
// List Queues Tests
// ============================================================================

func TestShouldListQueuesGivenMultipleQueuesWhenCallingListQueues(t *testing.T) {
	ctx := context.Background()
	service := newQueueServiceClient(t)

	// Create multiple queues
	queues := []string{"list-q1", "list-q2", "list-q3"}
	for _, name := range queues {
		qc := service.NewQueueClient(name)
		_, _ = qc.Create(ctx, nil)
		defer qc.Delete(ctx, nil)
	}

	// List queues
	pager := service.NewListQueuesPager(nil)
	found := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		found += len(page.Queues)
	}

	assert.GreaterOrEqual(t, found, 3, "Should find at least 3 queues")
}

func TestShouldListQueuesWithPrefixGivenMatchingQueuesWhenCallingListQueues(t *testing.T) {
	ctx := context.Background()
	service := newQueueServiceClient(t)

	// Create queues with different prefixes
	prefixQueues := []string{"prefix-a1", "prefix-a2"}
	otherQueues := []string{"other-b1"}

	for _, name := range append(prefixQueues, otherQueues...) {
		qc := service.NewQueueClient(name)
		_, _ = qc.Create(ctx, nil)
		defer qc.Delete(ctx, nil)
	}

	// List with prefix
	prefix := "prefix-"
	pager := service.NewListQueuesPager(&azqueue.ListQueuesOptions{
		Prefix: &prefix,
	})

	found := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		found += len(page.Queues)
	}

	assert.Equal(t, 2, found, "Should find exactly 2 queues with prefix")
}

// ============================================================================
// Message TTL Tests
// ============================================================================

func TestShouldRespectTTLGivenExpiredMessageWhenCallingDequeue(t *testing.T) {
	ctx := context.Background()
	qClient := newQueueClient(t, "ttl-test")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	// Add message with 1 second TTL
	ttl := int32(1)
	_, err := qClient.EnqueueMessage(ctx, "short-lived", &azqueue.EnqueueMessageOptions{
		TimeToLive: &ttl,
	})
	require.NoError(t, err)

	// Wait for TTL to expire
	time.Sleep(2 * time.Second)

	// Dequeue - message should be gone
	resp, err := qClient.DequeueMessages(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 0, "Expired message should not be returned")
}

// ============================================================================
// Queue Properties Tests
// ============================================================================

func TestShouldGetQueuePropertiesGivenExistingQueueWhenCallingGetProperties(t *testing.T) {
	ctx := context.Background()
	qClient := newQueueClient(t, "props-test")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	// Add some messages
	for i := 0; i < 3; i++ {
		_, _ = qClient.EnqueueMessage(ctx, "msg", nil)
	}

	// Get properties
	props, err := qClient.GetProperties(ctx, nil)
	require.NoError(t, err, "GetProperties should succeed")
	assert.NotNil(t, props.ApproximateMessagesCount)
	assert.GreaterOrEqual(t, *props.ApproximateMessagesCount, int32(3))
}

// ============================================================================
// Delete Queue Tests
// ============================================================================

func TestShouldDeleteQueueGivenExistingQueueWhenCallingDelete(t *testing.T) {
	ctx := context.Background()
	qClient := newQueueClient(t, "delete-queue")

	// Create queue
	_, err := qClient.Create(ctx, nil)
	require.NoError(t, err)

	// Delete queue
	_, err = qClient.Delete(ctx, nil)
	require.NoError(t, err, "Delete should succeed")

	// Verify queue is gone
	_, err = qClient.GetProperties(ctx, nil)
	assert.Error(t, err, "Should get error for deleted queue")
}

// ============================================================================
// Dequeue Count Tests
// ============================================================================

func TestShouldIncrementDequeueCountGivenMultipleDequeuedWhenCallingDequeue(t *testing.T) {
	ctx := context.Background()
	qClient := newQueueClient(t, "dequeue-count")
	_, _ = qClient.Create(ctx, nil)
	defer qClient.Delete(ctx, nil)

	// Add message
	_, _ = qClient.EnqueueMessage(ctx, "count-me", nil)

	// First dequeue with short visibility
	vis := int32(1)
	resp1, err := qClient.DequeueMessages(ctx, &azqueue.DequeueMessagesOptions{
		VisibilityTimeout: &vis,
	})
	require.NoError(t, err)
	require.Len(t, resp1.Messages, 1)
	assert.Equal(t, int64(1), *resp1.Messages[0].DequeueCount)

	// Wait and dequeue again
	time.Sleep(2 * time.Second)

	resp2, err := qClient.DequeueMessages(ctx, nil)
	require.NoError(t, err)
	require.Len(t, resp2.Messages, 1)
	assert.Equal(t, int64(2), *resp2.Messages[0].DequeueCount, "Dequeue count should increment")
}
