package test

import (
	"context"
	"io"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	blobEmulatorURL = "http://localhost:10000"
	blobAccountName = "devstoreaccount1"
)

func newBlobServiceClient(t *testing.T) *azblob.Client {
	client, err := azblob.NewClientWithNoCredential(
		blobEmulatorURL+"/"+blobAccountName, nil,
	)
	require.NoError(t, err, "Failed to create blob service client")
	return client
}

// ============================================================================
// Container Tests
// ============================================================================

func TestShouldCreateContainerGivenValidNameWhenCallingCreateContainer(t *testing.T) {

	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-container-create"

	_, err := client.CreateContainer(ctx, containerName, nil)
	require.NoError(t, err, "CreateContainer should succeed")

	// Cleanup
	defer client.DeleteContainer(ctx, containerName, nil)
}

func TestShouldListContainersGivenMultipleContainersWhenCallingListContainers(t *testing.T) {

	ctx := context.Background()
	client := newBlobServiceClient(t)

	// Create test containers
	containers := []string{"list-test-1", "list-test-2", "list-test-3"}
	for _, name := range containers {
		_, _ = client.CreateContainer(ctx, name, nil)
		defer client.DeleteContainer(ctx, name, nil)
	}

	// List containers
	pager := client.NewListContainersPager(nil)
	found := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		found += len(page.ContainerItems)
	}

	assert.GreaterOrEqual(t, found, 3, "Should find at least the 3 test containers")
}

// ============================================================================
// Blob Tests
// ============================================================================

func TestShouldUploadBlobGivenValidBlobDataWhenCallingUploadBlob(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-upload"
	blobName := "test.txt"

	// Create container
	_, err := client.CreateContainer(ctx, containerName, nil)
	require.NoError(t, err)
	defer client.DeleteContainer(ctx, containerName, nil)

	// Upload blob
	data := []byte("Hello, Blob Storage!")
	_, err = client.UploadBuffer(ctx, containerName, blobName, data, nil)
	require.NoError(t, err, "Upload should succeed")
}

func TestShouldDownloadBlobGivenExistingBlobWhenCallingDownloadBlob(t *testing.T) {

	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-download"
	blobName := "test.txt"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	data := []byte("Download this content")
	_, err := client.UploadBuffer(ctx, containerName, blobName, data, nil)
	require.NoError(t, err)

	// Download blob
	resp, err := client.DownloadStream(ctx, containerName, blobName, nil)
	require.NoError(t, err)

	downloaded, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, data, downloaded, "Downloaded content should match uploaded content")
}

func TestShouldDeleteBlobGivenExistingBlobWhenCallingDeleteBlob(t *testing.T) {

	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-delete"
	blobName := "deleteme.txt"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	data := []byte("Delete me")
	_, err := client.UploadBuffer(ctx, containerName, blobName, data, nil)
	require.NoError(t, err)

	// Delete blob
	_, err = client.DeleteBlob(ctx, containerName, blobName, nil)
	require.NoError(t, err, "Delete should succeed")
}

func TestShouldListBlobsGivenMultipleBlobsWhenCallingListBlobs(t *testing.T) {

	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-list-blobs"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	// Upload multiple blobs
	blobs := []string{"blob1.txt", "blob2.txt", "blob3.txt"}
	for _, name := range blobs {
		_, err := client.UploadBuffer(ctx, containerName, name, []byte("data"), nil)
		require.NoError(t, err)
	}

	// List blobs
	pager := client.NewListBlobsFlatPager(containerName, nil)
	found := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		found += len(page.Segment.BlobItems)
	}

	assert.Equal(t, 3, found, "Should find exactly 3 blobs")
}

func TestShouldGetBlobPropertiesGivenExistingBlobWhenCallingGetBlobProperties(t *testing.T) {

	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-properties"
	blobName := "test.txt"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	data := []byte("Test content")
	_, err := client.UploadBuffer(ctx, containerName, blobName, data, nil)
	require.NoError(t, err)

	// Get properties
	resp, err := client.DownloadStream(ctx, containerName, blobName, nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.NotNil(t, resp.ETag, "ETag should be present")
	assert.NotNil(t, resp.LastModified, "LastModified should be present")
	assert.Equal(t, int64(len(data)), *resp.ContentLength, "ContentLength should match")
}

func TestShouldSetBlobMetadataGivenExistingBlobWhenCallingSetBlobMetadata(t *testing.T) {

	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-metadata"
	blobName := "test.txt"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	data := []byte("Test")
	_, err := client.UploadBuffer(ctx, containerName, blobName, data, nil)
	require.NoError(t, err)

	// Note: Metadata operations require implementation in the emulator
	// This test is a placeholder for when metadata support is added
	t.Skip("Metadata operations not yet implemented in emulator")
}
