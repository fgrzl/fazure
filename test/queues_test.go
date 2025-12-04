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
