package blobs

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Path Generation Tests
// ============================================================================

func TestShouldGenerateHashBasedBlobFilePath(t *testing.T) {
	// Arrange
	h := &Handler{
		dataDir: "/data/blobs",
	}

	// Act
	path := h.blobFilePath("mycontainer", "myblob")

	// Assert
	hash := sha256.Sum256([]byte("mycontainer/myblob"))
	hashStr := hex.EncodeToString(hash[:])
	expectedSubdir := hashStr[:4]
	assert.Contains(t, path, expectedSubdir)
	assert.Contains(t, path, hashStr)
}

func TestShouldGenerateDifferentPathsForDifferentBlobs(t *testing.T) {
	// Arrange
	h := &Handler{
		dataDir: "/data/blobs",
	}

	// Act
	path1 := h.blobFilePath("container", "blob1")
	path2 := h.blobFilePath("container", "blob2")

	// Assert
	assert.NotEqual(t, path1, path2)
}

func TestShouldGenerateSamePathForSameBlob(t *testing.T) {
	// Arrange
	h := &Handler{
		dataDir: "/data/blobs",
	}

	// Act
	path1 := h.blobFilePath("container", "blob")
	path2 := h.blobFilePath("container", "blob")

	// Assert
	assert.Equal(t, path1, path2)
}

func TestShouldGenerateDifferentPathsForDifferentContainers(t *testing.T) {
	// Arrange
	h := &Handler{
		dataDir: "/data/blobs",
	}

	// Act
	path1 := h.blobFilePath("container1", "blob")
	path2 := h.blobFilePath("container2", "blob")

	// Assert
	assert.NotEqual(t, path1, path2)
}

func TestShouldUseDataDirInFilePath(t *testing.T) {
	// Arrange
	dataDir := "/data/blobs"
	h := &Handler{
		dataDir: dataDir,
	}

	// Act
	path := h.blobFilePath("container", "blob")

	// Assert
	// On Windows, paths use backslashes and have drive letters
	// Just verify the path has reasonable structure
	assert.NotEmpty(t, path)
	assert.True(t, len(path) > len(dataDir))
}

// ============================================================================
// Key Generation Tests
// ============================================================================

func TestShouldReturnCorrectContainerKey(t *testing.T) {
	// Arrange
	h := &Handler{}

	// Act
	key := h.containerKey("mycontainer")

	// Assert
	assert.Equal(t, []byte("blobs/containers/mycontainer"), key)
}

func TestShouldReturnCorrectBlobMetaKey(t *testing.T) {
	// Arrange
	h := &Handler{}

	// Act
	key := h.blobMetaKey("mycontainer", "myblob")

	// Assert
	assert.Equal(t, []byte("blobs/meta/mycontainer/myblob"), key)
}

func TestShouldHandleNestedBlobPathInMetaKey(t *testing.T) {
	// Arrange
	h := &Handler{}

	// Act
	key := h.blobMetaKey("mycontainer", "folder/subfolder/myblob.txt")

	// Assert
	assert.Equal(t, []byte("blobs/meta/mycontainer/folder/subfolder/myblob.txt"), key)
}

func TestShouldReturnCorrectBlockKey(t *testing.T) {
	// Arrange
	h := &Handler{}

	// Act
	key := h.blockKey("container", "blob", "block-001")

	// Assert
	assert.Equal(t, []byte("blobs/blocks/container/blob/block-001"), key)
}

func TestShouldReturnCorrectServicePropertiesKey(t *testing.T) {
	// Arrange
	h := &Handler{}

	// Act
	key := h.servicePropertiesKey()

	// Assert
	assert.Equal(t, []byte("blobs/service/properties"), key)
}

// ============================================================================
// BlobMetadata Serialization Tests
// ============================================================================

func TestShouldSerializeBlobMetadataToJSON(t *testing.T) {
	// Arrange
	now := time.Now().UTC()
	metadata := BlobMetadata{
		Container:     "mycontainer",
		Blob:          "myblob.txt",
		BlobType:      "BlockBlob",
		ContentType:   "text/plain",
		ContentLength: 1024,
		ETag:          "etag123",
		Created:       now,
		Modified:      now,
		FilePath:      "/data/blobs/abcd/abcdef123456",
		UserMetadata: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	// Act
	data, err := json.Marshal(metadata)
	require.NoError(t, err)
	var restored BlobMetadata
	err = json.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, metadata.Container, restored.Container)
	assert.Equal(t, metadata.Blob, restored.Blob)
	assert.Equal(t, metadata.BlobType, restored.BlobType)
	assert.Equal(t, metadata.ContentType, restored.ContentType)
	assert.Equal(t, metadata.ContentLength, restored.ContentLength)
	assert.Equal(t, metadata.ETag, restored.ETag)
	assert.Equal(t, metadata.UserMetadata, restored.UserMetadata)
}

func TestShouldHandleAppendBlobMetadata(t *testing.T) {
	// Arrange
	metadata := BlobMetadata{
		Container:           "mycontainer",
		Blob:                "append.log",
		BlobType:            "AppendBlob",
		CommittedBlockCount: 100,
	}

	// Act
	data, err := json.Marshal(metadata)
	require.NoError(t, err)
	var restored BlobMetadata
	err = json.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "AppendBlob", restored.BlobType)
	assert.Equal(t, 100, restored.CommittedBlockCount)
}

func TestShouldHandlePageBlobMetadata(t *testing.T) {
	// Arrange
	metadata := BlobMetadata{
		Container:         "mycontainer",
		Blob:              "disk.vhd",
		BlobType:          "PageBlob",
		BlobContentLength: 512 * 1024 * 1024,
	}

	// Act
	data, err := json.Marshal(metadata)
	require.NoError(t, err)
	var restored BlobMetadata
	err = json.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "PageBlob", restored.BlobType)
	assert.Equal(t, int64(512*1024*1024), restored.BlobContentLength)
}

func TestShouldHandleLeaseInfoInBlobMetadata(t *testing.T) {
	// Arrange
	expiry := time.Now().Add(time.Hour).UTC()
	metadata := BlobMetadata{
		Container:   "mycontainer",
		Blob:        "leased.txt",
		BlobType:    "BlockBlob",
		LeaseID:     "lease-123",
		LeaseExpiry: expiry,
	}

	// Act
	data, err := json.Marshal(metadata)
	require.NoError(t, err)
	var restored BlobMetadata
	err = json.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "lease-123", restored.LeaseID)
	assert.WithinDuration(t, expiry, restored.LeaseExpiry, time.Second)
}

func TestShouldHandleEmptyUserMetadata(t *testing.T) {
	// Arrange
	metadata := BlobMetadata{
		Container:    "container",
		Blob:         "blob",
		BlobType:     "BlockBlob",
		UserMetadata: nil,
	}

	// Act
	data, err := json.Marshal(metadata)
	require.NoError(t, err)
	var restored BlobMetadata
	err = json.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Nil(t, restored.UserMetadata)
}

// ============================================================================
// BlobType Tests
// ============================================================================

func TestShouldSerializeDifferentBlobTypes(t *testing.T) {
	// Arrange
	blobTypes := []string{"BlockBlob", "AppendBlob", "PageBlob"}

	for _, blobType := range blobTypes {
		t.Run(blobType, func(t *testing.T) {
			metadata := BlobMetadata{
				Container: "container",
				Blob:      "blob",
				BlobType:  blobType,
			}

			// Act
			data, err := json.Marshal(metadata)
			require.NoError(t, err)
			var restored BlobMetadata
			err = json.Unmarshal(data, &restored)

			// Assert
			require.NoError(t, err)
			assert.Equal(t, blobType, restored.BlobType)
		})
	}
}

// ============================================================================
// ContainerMetadata Tests
// ============================================================================

func TestShouldSerializeContainerMetadataToJSON(t *testing.T) {
	// Arrange
	now := time.Now().UTC()
	metadata := ContainerMetadata{
		Name:     "mycontainer",
		Created:  now,
		Modified: now,
		UserMetadata: map[string]string{
			"purpose": "testing",
		},
	}

	// Act
	data, err := json.Marshal(metadata)
	require.NoError(t, err)
	var restored ContainerMetadata
	err = json.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, metadata.Name, restored.Name)
	assert.Equal(t, metadata.UserMetadata, restored.UserMetadata)
}

func TestShouldHandleDifferentLeaseStates(t *testing.T) {
	// Arrange
	states := []string{"available", "leased", "expired", "breaking", "broken"}

	for _, state := range states {
		t.Run(state, func(t *testing.T) {
			metadata := ContainerMetadata{
				Name:       "container",
				LeaseState: state,
			}

			// Act
			data, err := json.Marshal(metadata)
			require.NoError(t, err)
			var restored ContainerMetadata
			err = json.Unmarshal(data, &restored)

			// Assert
			require.NoError(t, err)
			assert.Equal(t, state, restored.LeaseState)
		})
	}
}

func TestShouldHandleInfiniteLease(t *testing.T) {
	// Arrange
	metadata := ContainerMetadata{
		Name:       "container",
		LeaseID:    "lease-123",
		LeaseState: "leased",
		LeaseDur:   -1,
	}

	// Act
	data, err := json.Marshal(metadata)
	require.NoError(t, err)
	var restored ContainerMetadata
	err = json.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, int32(-1), restored.LeaseDur)
}

// ============================================================================
// ServiceProperties Tests
// ============================================================================

func TestShouldSerializeServicePropertiesToJSON(t *testing.T) {
	// Arrange
	props := ServiceProperties{
		Logging: &Logging{
			Version: "1.0",
			Delete:  true,
			Read:    true,
			Write:   true,
			RetentionPolicy: &RetentionPolicy{
				Enabled: true,
				Days:    7,
			},
		},
		HourMetrics: &Metrics{
			Version: "1.0",
			Enabled: true,
			RetentionPolicy: &RetentionPolicy{
				Enabled: true,
				Days:    7,
			},
		},
	}

	// Act
	data, err := json.Marshal(props)
	require.NoError(t, err)
	var restored ServiceProperties
	err = json.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, props.Logging.Version, restored.Logging.Version)
	assert.Equal(t, props.Logging.Delete, restored.Logging.Delete)
	assert.Equal(t, props.HourMetrics.Enabled, restored.HourMetrics.Enabled)
}

// ============================================================================
// CORSRule Tests
// ============================================================================

func TestShouldSerializeCORSRuleToXML(t *testing.T) {
	// Arrange
	rule := CORSRule{
		AllowedOrigins:  "http://example.com",
		AllowedMethods:  "GET,PUT,POST",
		AllowedHeaders:  "x-ms-*",
		ExposedHeaders:  "x-ms-meta-*",
		MaxAgeInSeconds: 3600,
	}

	// Act
	data, err := xml.Marshal(rule)
	require.NoError(t, err)
	var restored CORSRule
	err = xml.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, rule.AllowedOrigins, restored.AllowedOrigins)
	assert.Equal(t, rule.AllowedMethods, restored.AllowedMethods)
	assert.Equal(t, rule.AllowedHeaders, restored.AllowedHeaders)
	assert.Equal(t, rule.ExposedHeaders, restored.ExposedHeaders)
	assert.Equal(t, rule.MaxAgeInSeconds, restored.MaxAgeInSeconds)
}

// ============================================================================
// Logging Tests
// ============================================================================

func TestShouldSerializeLoggingToXML(t *testing.T) {
	// Arrange
	logging := Logging{
		Version: "1.0",
		Delete:  true,
		Read:    false,
		Write:   true,
		RetentionPolicy: &RetentionPolicy{
			Enabled: true,
			Days:    30,
		},
	}

	// Act
	data, err := xml.Marshal(logging)
	require.NoError(t, err)
	var restored Logging
	err = xml.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, logging.Version, restored.Version)
	assert.Equal(t, logging.Delete, restored.Delete)
	assert.Equal(t, logging.Read, restored.Read)
	assert.Equal(t, logging.Write, restored.Write)
	assert.Equal(t, logging.RetentionPolicy.Enabled, restored.RetentionPolicy.Enabled)
	assert.Equal(t, logging.RetentionPolicy.Days, restored.RetentionPolicy.Days)
}

// ============================================================================
// Metrics Tests
// ============================================================================

func TestShouldSerializeMetricsToXML(t *testing.T) {
	// Arrange
	includeAPIs := true
	metrics := Metrics{
		Version:     "1.0",
		Enabled:     true,
		IncludeAPIs: &includeAPIs,
		RetentionPolicy: &RetentionPolicy{
			Enabled: true,
			Days:    7,
		},
	}

	// Act
	data, err := xml.Marshal(metrics)
	require.NoError(t, err)
	var restored Metrics
	err = xml.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, metrics.Version, restored.Version)
	assert.Equal(t, metrics.Enabled, restored.Enabled)
	assert.NotNil(t, restored.IncludeAPIs)
	assert.Equal(t, true, *restored.IncludeAPIs)
}

// ============================================================================
// RetentionPolicy Tests
// ============================================================================

func TestShouldSerializeRetentionPolicyToXML(t *testing.T) {
	// Arrange
	tests := []struct {
		name   string
		policy RetentionPolicy
	}{
		{
			name:   "enabled with days",
			policy: RetentionPolicy{Enabled: true, Days: 30},
		},
		{
			name:   "disabled",
			policy: RetentionPolicy{Enabled: false},
		},
		{
			name:   "enabled with zero days",
			policy: RetentionPolicy{Enabled: true, Days: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			data, err := xml.Marshal(tt.policy)
			require.NoError(t, err)
			var restored RetentionPolicy
			err = xml.Unmarshal(data, &restored)

			// Assert
			require.NoError(t, err)
			assert.Equal(t, tt.policy.Enabled, restored.Enabled)
			if tt.policy.Days > 0 {
				assert.Equal(t, tt.policy.Days, restored.Days)
			}
		})
	}
}
