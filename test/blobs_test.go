package test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
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

func newContainerClient(t *testing.T, containerName string) *container.Client {
	client, err := container.NewClientWithNoCredential(
		blobEmulatorURL+"/"+blobAccountName+"/"+containerName, nil,
	)
	require.NoError(t, err)
	return client
}

func newBlockBlobClient(t *testing.T, containerName, blobName string) *blockblob.Client {
	client, err := blockblob.NewClientWithNoCredential(
		blobEmulatorURL+"/"+blobAccountName+"/"+containerName+"/"+blobName, nil,
	)
	require.NoError(t, err)
	return client
}

func newBlobClient(t *testing.T, containerName, blobName string) *blob.Client {
	client, err := blob.NewClientWithNoCredential(
		blobEmulatorURL+"/"+blobAccountName+"/"+containerName+"/"+blobName, nil,
	)
	require.NoError(t, err)
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

	// Set metadata
	blobClient := newBlobClient(t, containerName, blobName)
	metadata := map[string]*string{
		"key1": ptrString("value1"),
		"key2": ptrString("value2"),
	}
	_, err = blobClient.SetMetadata(ctx, metadata, nil)
	require.NoError(t, err, "SetMetadata should succeed")

	// Get properties to verify metadata
	props, err := blobClient.GetProperties(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "value1", *props.Metadata["key1"])
	assert.Equal(t, "value2", *props.Metadata["key2"])
}

func ptrString(s string) *string { return &s }

// ============================================================================
// Block Blob Tests
// ============================================================================

func TestShouldUploadBlockBlobGivenStagedBlocksWhenCallingCommitBlockList(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-block-blob"
	blobName := "staged.txt"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	blockClient := newBlockBlobClient(t, containerName, blobName)

	// Stage blocks
	block1ID := "AAAA"
	block2ID := "BBBB"

	_, err := blockClient.StageBlock(ctx, block1ID, streaming.NopCloser(bytes.NewReader([]byte("Hello, "))), nil)
	require.NoError(t, err, "StageBlock 1 should succeed")

	_, err = blockClient.StageBlock(ctx, block2ID, streaming.NopCloser(bytes.NewReader([]byte("World!"))), nil)
	require.NoError(t, err, "StageBlock 2 should succeed")

	// Commit block list
	_, err = blockClient.CommitBlockList(ctx, []string{block1ID, block2ID}, nil)
	require.NoError(t, err, "CommitBlockList should succeed")

	// Verify content
	resp, err := client.DownloadStream(ctx, containerName, blobName, nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	downloaded, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "Hello, World!", string(downloaded))
}

func TestShouldReturnBlockListGivenCommittedBlobWhenCallingGetBlockList(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-block-list"
	blobName := "blocks.txt"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	blockClient := newBlockBlobClient(t, containerName, blobName)

	// Stage and commit blocks
	blockID := "AAAA"
	_, err := blockClient.StageBlock(ctx, blockID, streaming.NopCloser(bytes.NewReader([]byte("content"))), nil)
	require.NoError(t, err)

	_, err = blockClient.CommitBlockList(ctx, []string{blockID}, nil)
	require.NoError(t, err)

	// Get block list
	resp, err := blockClient.GetBlockList(ctx, blockblob.BlockListTypeAll, nil)
	require.NoError(t, err, "GetBlockList should succeed")
	assert.NotNil(t, resp.BlockList)
}

// ============================================================================
// Copy Blob Tests
// ============================================================================

func TestShouldCopyBlobGivenExistingSourceWhenCallingStartCopyFromURL(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-copy"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	// Upload source blob
	sourceData := []byte("Copy this content")
	_, err := client.UploadBuffer(ctx, containerName, "source.txt", sourceData, nil)
	require.NoError(t, err)

	// Copy blob
	destClient := newBlobClient(t, containerName, "dest.txt")
	sourceURL := blobEmulatorURL + "/" + blobAccountName + "/" + containerName + "/source.txt"
	_, err = destClient.StartCopyFromURL(ctx, sourceURL, nil)
	require.NoError(t, err, "StartCopyFromURL should succeed")

	// Verify copy
	resp, err := client.DownloadStream(ctx, containerName, "dest.txt", nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	downloaded, _ := io.ReadAll(resp.Body)
	assert.Equal(t, sourceData, downloaded, "Copied content should match")
}

// ============================================================================
// Blob Snapshot Tests
// ============================================================================

func TestShouldCreateSnapshotGivenExistingBlobWhenCallingCreateSnapshot(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-snapshot"
	blobName := "snap.txt"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	_, err := client.UploadBuffer(ctx, containerName, blobName, []byte("original"), nil)
	require.NoError(t, err)

	// Create snapshot
	blobClient := newBlobClient(t, containerName, blobName)
	resp, err := blobClient.CreateSnapshot(ctx, nil)
	require.NoError(t, err, "CreateSnapshot should succeed")
	require.NotNil(t, resp, "Response should not be nil")
	require.NotNil(t, resp.Snapshot, "Snapshot should not be nil")
	assert.NotEmpty(t, *resp.Snapshot, "Snapshot ID should be returned")
}

// ============================================================================
// Blob Lease Tests
// ============================================================================

func TestShouldAcquireLeaseGivenExistingBlobWhenCallingAcquireLease(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-lease"
	blobName := "leased.txt"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	_, err := client.UploadBuffer(ctx, containerName, blobName, []byte("lease me"), nil)
	require.NoError(t, err)

	// Acquire lease
	blobClient := newBlobClient(t, containerName, blobName)
	leaseClient, err := lease.NewBlobClient(blobClient, nil)
	require.NoError(t, err)
	require.NotNil(t, leaseClient, "Lease client should not be nil")

	duration := int32(15) // seconds
	resp, err := leaseClient.AcquireLease(ctx, duration, nil)
	require.NoError(t, err, "AcquireLease should succeed")
	require.NotNil(t, resp, "Response should not be nil")
	require.NotNil(t, resp.LeaseID, "LeaseID should not be nil")
	assert.NotEmpty(t, *resp.LeaseID, "Lease ID should be returned")

	// Release lease for cleanup
	_, _ = leaseClient.ReleaseLease(ctx, nil)
}

func TestShouldPreventDeleteGivenLeasedBlobWhenCallingDeleteBlob(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-lease-protect"
	blobName := "protected.txt"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	_, err := client.UploadBuffer(ctx, containerName, blobName, []byte("protected"), nil)
	require.NoError(t, err)

	// Acquire lease
	blobClient := newBlobClient(t, containerName, blobName)
	leaseClient, err := lease.NewBlobClient(blobClient, nil)
	require.NoError(t, err)
	require.NotNil(t, leaseClient, "Lease client should not be nil")

	duration := int32(60)
	resp, err := leaseClient.AcquireLease(ctx, duration, nil)
	require.NoError(t, err, "AcquireLease should succeed")
	require.NotNil(t, resp, "Response should not be nil")
	defer leaseClient.ReleaseLease(ctx, nil)

	// Try to delete without lease (should fail)
	_, err = client.DeleteBlob(ctx, containerName, blobName, nil)
	assert.Error(t, err, "Delete should fail without providing lease ID")
}

// ============================================================================
// Range Download Tests
// ============================================================================

func TestShouldDownloadRangeGivenValidRangeWhenCallingDownloadStream(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-range"
	blobName := "range.txt"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	data := []byte("0123456789ABCDEF")
	_, err := client.UploadBuffer(ctx, containerName, blobName, data, nil)
	require.NoError(t, err)

	// Download range [5, 10)
	blobClient := newBlobClient(t, containerName, blobName)
	resp, err := blobClient.DownloadStream(ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: 5, Count: 5},
	})
	require.NoError(t, err, "Range download should succeed")
	defer resp.Body.Close()

	downloaded, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "56789", string(downloaded), "Should get bytes 5-9")
}

// ============================================================================
// Container Metadata Tests
// ============================================================================

func TestShouldSetContainerMetadataGivenExistingContainerWhenCallingSetMetadata(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-container-meta"

	// Setup
	_, err := client.CreateContainer(ctx, containerName, nil)
	require.NoError(t, err)
	defer client.DeleteContainer(ctx, containerName, nil)

	// Set metadata
	containerClient := newContainerClient(t, containerName)
	metadata := map[string]*string{
		"env":     ptrString("test"),
		"version": ptrString("1.0"),
	}
	_, err = containerClient.SetMetadata(ctx, &container.SetMetadataOptions{Metadata: metadata})
	require.NoError(t, err, "SetMetadata should succeed")

	// Verify
	props, err := containerClient.GetProperties(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "test", *props.Metadata["env"])
}

// ============================================================================
// Container ACL Tests
// ============================================================================

func TestShouldSetContainerACLGivenValidACLWhenCallingSetAccessPolicy(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-acl"

	// Setup
	_, err := client.CreateContainer(ctx, containerName, nil)
	require.NoError(t, err)
	defer client.DeleteContainer(ctx, containerName, nil)

	// Set public access level
	containerClient := newContainerClient(t, containerName)
	blobAccess := container.PublicAccessTypeBlob
	_, err = containerClient.SetAccessPolicy(ctx, &container.SetAccessPolicyOptions{
		Access: &blobAccess,
	})
	require.NoError(t, err, "SetAccessPolicy should succeed")

	// Verify
	props, err := containerClient.GetAccessPolicy(ctx, nil)
	require.NoError(t, err)
	assert.NotNil(t, props.BlobPublicAccess)
}

// ============================================================================
// Large Blob Tests
// ============================================================================

func TestShouldUploadLargeBlobGivenChunkedDataWhenCallingUploadBuffer(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-large"
	blobName := "large.bin"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	// Create 5MB blob (forces chunking in SDK)
	data := bytes.Repeat([]byte("X"), 5*1024*1024)

	_, err := client.UploadBuffer(ctx, containerName, blobName, data, nil)
	require.NoError(t, err, "Large blob upload should succeed")

	// Verify size
	blobClient := newBlobClient(t, containerName, blobName)
	props, err := blobClient.GetProperties(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(5*1024*1024), *props.ContentLength)
}

// ============================================================================
// Blob Prefix Listing Tests
// ============================================================================

func TestShouldListBlobsWithPrefixGivenHierarchicalBlobsWhenCallingListBlobsHierarchy(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-hierarchy"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	// Create hierarchical blobs
	blobs := []string{
		"folder1/file1.txt",
		"folder1/file2.txt",
		"folder2/file1.txt",
		"root.txt",
	}
	for _, b := range blobs {
		_, _ = client.UploadBuffer(ctx, containerName, b, []byte("data"), nil)
	}

	// List with prefix
	containerClient := newContainerClient(t, containerName)
	prefix := "folder1/"
	pager := containerClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
		Prefix: &prefix,
	})

	found := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		found += len(page.Segment.BlobItems)
	}

	assert.Equal(t, 2, found, "Should find 2 blobs in folder1/")
}

// ============================================================================
// If-Match / If-None-Match Tests
// ============================================================================

func TestShouldFailUpdateGivenStaleETagWhenCallingUploadWithIfMatch(t *testing.T) {
	ctx := context.Background()
	client := newBlobServiceClient(t)
	containerName := "test-etag"
	blobName := "etag.txt"

	// Setup
	_, _ = client.CreateContainer(ctx, containerName, nil)
	defer client.DeleteContainer(ctx, containerName, nil)

	_, err := client.UploadBuffer(ctx, containerName, blobName, []byte("v1"), nil)
	require.NoError(t, err)

	// Get current ETag
	blobClient := newBlobClient(t, containerName, blobName)
	props, err := blobClient.GetProperties(ctx, nil)
	require.NoError(t, err)
	originalETag := *props.ETag

	// Update blob (changes ETag)
	_, err = client.UploadBuffer(ctx, containerName, blobName, []byte("v2"), nil)
	require.NoError(t, err)

	// Try to update with stale ETag (should fail)
	blockClient := newBlockBlobClient(t, containerName, blobName)
	_, err = blockClient.Upload(ctx, streaming.NopCloser(bytes.NewReader([]byte("v3"))), &blockblob.UploadOptions{
		AccessConditions: &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfMatch: &originalETag,
			},
		},
	})
	assert.Error(t, err, "Upload with stale ETag should fail")
}
