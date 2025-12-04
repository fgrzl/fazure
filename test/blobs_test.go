package test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	emulatorURL = "http://localhost:10000"
	accountName = "devstoreaccount1"
)

func skipIfNotRunning(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", emulatorURL+"/health", nil)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)

	if err != nil || (resp != nil && resp.StatusCode != 200) {
		t.Skip("Emulator not running. Start with: docker compose up -d")
	}

	if resp != nil {
		resp.Body.Close()
	}
}

// ============================================================================
// ShouldCreateContainerGivenValidNameWhenCallingCreateContainer
// ============================================================================
func TestShouldCreateContainerGivenValidNameWhenCallingCreateContainer(t *testing.T) {
	skipIfNotRunning(t)

	// Arrange
	ctx := context.Background()
	client, err := azblob.NewClientWithNoCredential(emulatorURL+"/"+accountName, nil)
	require.NoError(t, err, "Arrange: failed to create client")

	containerName := "create-container-test"

	// Act
	_, err = client.CreateContainer(ctx, containerName, nil)

	// Assert
	require.NoError(t, err, "Assert: CreateContainer returned error")

	// Cleanup
	defer client.DeleteContainer(ctx, containerName, nil)
}

// ============================================================================
// ShouldUploadBlobGivenExistingContainerWhenUploadingBuffer
// ============================================================================
func TestShouldUploadBlobGivenExistingContainerWhenUploadingBuffer(t *testing.T) {
	skipIfNotRunning(t)

	// Arrange
	ctx := context.Background()
	client, err := azblob.NewClientWithNoCredential(emulatorURL+"/"+accountName, nil)
	require.NoError(t, err, "Arrange: failed to create client")

	containerName := "upload-test"
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	testData := []byte("Hello Azure Storage!")

	// Act
	_, err = client.UploadBuffer(ctx, containerName, "test.txt", testData, nil)

	// Assert
	require.NoError(t, err, "Assert: UploadBuffer returned error")
}

// ============================================================================
// ShouldDownloadBlobGivenUploadedBlobWhenCallingDownloadStream
// ============================================================================
func TestShouldDownloadBlobGivenUploadedBlobWhenCallingDownloadStream(t *testing.T) {
	skipIfNotRunning(t)

	// Arrange
	ctx := context.Background()
	client, err := azblob.NewClientWithNoCredential(emulatorURL+"/"+accountName, nil)
	require.NoError(t, err, "Arrange: failed to create client")

	containerName := "download-test"
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	expected := []byte("Download test data")
	_, err = client.UploadBuffer(ctx, containerName, "download.txt", expected, nil)
	require.NoError(t, err, "Arrange: upload failed")

	// Act
	resp, err := client.DownloadStream(ctx, containerName, "download.txt", nil)

	// Assert
	require.NoError(t, err, "Assert: DownloadStream failed")
	defer resp.Body.Close()

	actual, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "Assert: reading body failed")

	assert.Equal(t, string(expected), string(actual), "Assert: blob content mismatch")
}

// ============================================================================
// ShouldDeleteBlobGivenExistingBlobWhenCallingDeleteBlob
// ============================================================================
func TestShouldDeleteBlobGivenExistingBlobWhenCallingDeleteBlob(t *testing.T) {
	skipIfNotRunning(t)

	// Arrange
	ctx := context.Background()
	client, err := azblob.NewClientWithNoCredential(emulatorURL+"/"+accountName, nil)
	require.NoError(t, err, "Arrange: failed to create client")

	containerName := "delete-test"
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	_, err = client.UploadBuffer(ctx, containerName, "todelete.txt", []byte("delete me"), nil)
	require.NoError(t, err, "Arrange: upload failed")

	// Act
	_, err = client.DeleteBlob(ctx, containerName, "todelete.txt", nil)

	// Assert
	require.NoError(t, err, "Assert: DeleteBlob returned error")
}

// ============================================================================
// ShouldListBlobsGivenContainerWithTwoBlobsWhenCallingListBlobsFlatPager
// ============================================================================
func TestShouldListBlobsGivenContainerWithTwoBlobsWhenCallingListBlobsFlatPager(t *testing.T) {
	skipIfNotRunning(t)

	// Arrange
	ctx := context.Background()
	client, err := azblob.NewClientWithNoCredential(emulatorURL+"/"+accountName, nil)
	require.NoError(t, err, "Arrange: failed to create client")

	containerName := "list-test"
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	// Upload multiple blobs
	_, err = client.UploadBuffer(ctx, containerName, "file1.txt", []byte("data1"), nil)
	require.NoError(t, err)
	_, err = client.UploadBuffer(ctx, containerName, "file2.txt", []byte("data2"), nil)
	require.NoError(t, err)

	// Act
	pager := client.NewListBlobsFlatPager(containerName, nil)
	total := 0

	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err, "Assert: ListBlobs pager failure")
		total += len(page.Segment.BlobItems)
	}

	// Assert
	assert.Equal(t, 2, total, "Assert: unexpected blob count")
}
