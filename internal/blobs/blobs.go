package blobs

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/fgrzl/fazure/internal/common"
)

// Handler handles blob storage operations
type Handler struct {
	db      *pebble.DB
	dataDir string // Directory for blob file storage
	log     *slog.Logger
}

// NewHandler creates a new blob handler
func NewHandler(db *pebble.DB, dataDir string, logger *slog.Logger) *Handler {
	// Create blobs subdirectory
	blobDir := filepath.Join(dataDir, "blobs")
	if err := os.MkdirAll(blobDir, 0755); err != nil {
		logger.Error("failed to create blob data directory", "error", err)
	}

	return &Handler{
		db:      db,
		dataDir: blobDir,
		log:     logger.With("component", "blobs"),
	}
}

// RegisterRoutes registers blob storage routes
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	h.log.Info("blob routes registered")
}

// HandleRequest routes blob requests - exported for use by main dispatcher
func (h *Handler) HandleRequest(w http.ResponseWriter, r *http.Request) {
	h.handleRequest(w, r)
}

// handleRequest routes blob requests
func (h *Handler) handleRequest(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	query := r.URL.Query()

	if len(parts) < 1 {
		h.writeError(w, http.StatusBadRequest, "InvalidUri", "Invalid path")
		return
	}

	// List containers: /{account}?comp=list
	if len(parts) == 1 && query.Get("comp") == "list" && r.Method == http.MethodGet {
		h.ListContainers(w, r)
		return
	}

	if len(parts) < 2 {
		h.writeError(w, http.StatusBadRequest, "InvalidUri", "Container name required")
		return
	}

	container := parts[1]

	// Container operations: /{account}/{container}
	if len(parts) == 2 {
		switch r.Method {
		case http.MethodPut:
			if query.Get("restype") == "container" {
				if query.Get("comp") == "metadata" {
					h.SetContainerMetadata(w, r, container)
				} else if query.Get("comp") == "acl" {
					h.SetContainerACL(w, r, container)
				} else {
					h.CreateContainer(w, r, container)
				}
			} else {
				h.writeError(w, http.StatusBadRequest, "InvalidQueryParameterValue", "restype=container required")
			}
		case http.MethodGet:
			if query.Get("restype") == "container" && query.Get("comp") == "list" {
				h.ListBlobs(w, r, container)
			} else if query.Get("restype") == "container" && query.Get("comp") == "acl" {
				h.GetContainerACL(w, r, container)
			} else if query.Get("restype") == "container" || query.Get("comp") == "" {
				// Get container properties (similar to HEAD)
				h.GetContainerProperties(w, r, container)
			} else {
				h.writeError(w, http.StatusBadRequest, "InvalidQueryParameterValue", "Invalid query parameters")
			}
		case http.MethodDelete:
			if query.Get("restype") == "container" {
				h.DeleteContainer(w, r, container)
			} else {
				h.writeError(w, http.StatusBadRequest, "InvalidQueryParameterValue", "restype=container required")
			}
		case http.MethodHead:
			h.GetContainerProperties(w, r, container)
		default:
			h.writeError(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "Method not allowed")
		}
		return
	}

	// Blob operations: /{account}/{container}/{blob...}
	blob := strings.Join(parts[2:], "/")

	switch r.Method {
	case http.MethodPut:
		if query.Get("comp") == "block" {
			h.PutBlock(w, r, container, blob)
		} else if query.Get("comp") == "blocklist" {
			h.PutBlockList(w, r, container, blob)
		} else if query.Get("comp") == "metadata" {
			h.SetBlobMetadata(w, r, container, blob)
		} else if query.Get("comp") == "snapshot" {
			h.CreateSnapshot(w, r, container, blob)
		} else if query.Get("comp") == "lease" {
			h.LeaseBlob(w, r, container, blob)
		} else if r.Header.Get("x-ms-copy-source") != "" {
			h.CopyBlob(w, r, container, blob)
		} else {
			h.PutBlob(w, r, container, blob)
		}
	case http.MethodGet:
		if query.Get("comp") == "blocklist" {
			h.GetBlockList(w, r, container, blob)
		} else {
			h.GetBlob(w, r, container, blob)
		}
	case http.MethodHead:
		h.GetBlobProperties(w, r, container, blob)
	case http.MethodDelete:
		h.DeleteBlob(w, r, container, blob)
	default:
		h.writeError(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "Method not allowed")
	}
}

// writeError writes an Azure Storage error response
func (h *Handler) writeError(w http.ResponseWriter, statusCode int, errorCode, message string) {
	common.WriteErrorResponse(w, statusCode, errorCode, message)
}

// containerKey returns the Pebble key for a container
func (h *Handler) containerKey(container string) []byte {
	return []byte(fmt.Sprintf("blobs/containers/%s", container))
}

// blobMetaKey returns the Pebble key for blob metadata (index entry)
func (h *Handler) blobMetaKey(container, blob string) []byte {
	return []byte(fmt.Sprintf("blobs/meta/%s/%s", container, blob))
}

// blobFilePath returns the filesystem path for blob data
// Uses content-addressable storage with hash-based directories
func (h *Handler) blobFilePath(container, blob string) string {
	// Create a hash of container/blob to distribute files
	hash := sha256.Sum256([]byte(container + "/" + blob))
	hashStr := hex.EncodeToString(hash[:])

	// Use first 4 chars as subdirectory for distribution
	subdir := hashStr[:4]
	return filepath.Join(h.dataDir, subdir, hashStr)
}

// BlobMetadata stores blob metadata in Pebble
type BlobMetadata struct {
	Container     string            `json:"container"`
	Blob          string            `json:"blob"`
	ContentType   string            `json:"contentType"`
	ContentLength int64             `json:"contentLength"`
	ETag          string            `json:"etag"`
	Created       time.Time         `json:"created"`
	Modified      time.Time         `json:"modified"`
	FilePath      string            `json:"filePath"` // Path to actual blob data
	UserMetadata  map[string]string `json:"userMetadata,omitempty"`
	LeaseID       string            `json:"leaseId,omitempty"`
	LeaseExpiry   time.Time         `json:"leaseExpiry,omitempty"`
}

// CreateContainer creates a new blob container
func (h *Handler) CreateContainer(w http.ResponseWriter, r *http.Request, container string) {
	h.log.Info("creating container", "container", container, "method", r.Method)

	key := h.containerKey(container)

	// Check if container already exists
	_, closer, err := h.db.Get(key)
	if err == nil {
		closer.Close()
		h.log.Debug("container already exists", "container", container)
		h.writeError(w, http.StatusConflict, "ContainerAlreadyExists", "The specified container already exists")
		return
	}
	if err != pebble.ErrNotFound {
		h.log.Error("failed to check container existence", "container", container, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Create container metadata
	metadata := fmt.Sprintf(`{"name":"%s","created":"%s"}`, container, time.Now().UTC().Format(time.RFC3339))
	if err := h.db.Set(key, []byte(metadata), pebble.Sync); err != nil {
		h.log.Error("failed to create container", "container", container, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("container created", "container", container)
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusCreated)
}

// DeleteContainer deletes a blob container
func (h *Handler) DeleteContainer(w http.ResponseWriter, r *http.Request, container string) {
	h.log.Info("deleting container", "container", container)

	key := h.containerKey(container)

	// Check if container exists
	_, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.log.Debug("container not found", "container", container)
		h.writeError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist")
		return
	}
	if err != nil {
		h.log.Error("failed to check container", "container", container, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

	// Find and delete all blobs in container (both metadata and files)
	prefix := []byte(fmt.Sprintf("blobs/meta/%s/", container))
	iter, err := h.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		h.log.Error("failed to iterate blobs", "container", container, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	batch := h.db.NewBatch()
	batch.Delete(key, nil)

	for iter.First(); iter.Valid(); iter.Next() {
		// Parse metadata to get file path
		var meta BlobMetadata
		if err := json.Unmarshal(iter.Value(), &meta); err == nil {
			// Delete the blob file
			if meta.FilePath != "" {
				os.Remove(meta.FilePath)
			}
		}
		batch.Delete(iter.Key(), nil)
	}
	iter.Close()

	if err := batch.Commit(pebble.Sync); err != nil {
		h.log.Error("failed to delete container", "container", container, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("container deleted", "container", container)
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusAccepted)
}

// GetContainerProperties returns container properties
func (h *Handler) GetContainerProperties(w http.ResponseWriter, r *http.Request, container string) {
	h.log.Debug("getting container properties", "container", container)

	key := h.containerKey(container)
	data, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	// Parse container metadata to return user metadata headers
	var meta map[string]interface{}
	if err := json.Unmarshal(dataCopy, &meta); err == nil {
		if userMeta, ok := meta["userMetadata"].(map[string]interface{}); ok {
			for key, value := range userMeta {
				if strVal, ok := value.(string); ok {
					w.Header().Set("x-ms-meta-"+key, strVal)
				}
			}
		}
	}

	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusOK)
}

// ListContainers lists all containers
func (h *Handler) ListContainers(w http.ResponseWriter, r *http.Request) {
	h.log.Info("listing containers")

	prefix := []byte("blobs/containers/")
	iter, err := h.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		h.log.Error("failed to iterate containers", "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer iter.Close()

	type Container struct {
		Name string `xml:"Name"`
	}
	type EnumerationResults struct {
		XMLName    xml.Name    `xml:"EnumerationResults"`
		Containers []Container `xml:"Containers>Container"`
	}

	result := EnumerationResults{}
	for iter.First(); iter.Valid(); iter.Next() {
		name := string(iter.Key()[len(prefix):])
		result.Containers = append(result.Containers, Container{Name: name})
	}

	h.log.Info("containers listed", "count", len(result.Containers))
	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result)
}

// PutBlob uploads a blob
func (h *Handler) PutBlob(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Info("uploading blob", "container", container, "blob", blob, "contentLength", r.ContentLength)

	// Check if container exists
	containerKey := h.containerKey(container)
	_, closer, err := h.db.Get(containerKey)
	if err == pebble.ErrNotFound {
		h.log.Debug("container not found for blob upload", "container", container)
		h.writeError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

	// Check If-Match header for conditional update
	ifMatch := r.Header.Get("If-Match")
	if ifMatch != "" && ifMatch != "*" {
		metaKey := h.blobMetaKey(container, blob)
		existingData, existingCloser, err := h.db.Get(metaKey)
		if err == nil {
			existingDataCopy := make([]byte, len(existingData))
			copy(existingDataCopy, existingData)
			existingCloser.Close()

			var existingMeta BlobMetadata
			if err := json.Unmarshal(existingDataCopy, &existingMeta); err == nil {
				// Strip quotes from If-Match header if present
				cleanIfMatch := strings.Trim(ifMatch, "\"")
				cleanETag := strings.Trim(existingMeta.ETag, "\"")
				if cleanIfMatch != cleanETag {
					h.writeError(w, http.StatusPreconditionFailed, "ConditionNotMet", "The condition specified using HTTP conditional header(s) is not met")
					return
				}
			}
		} else if err != pebble.ErrNotFound {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	}

	// Read blob data
	data, err := io.ReadAll(r.Body)
	if err != nil {
		h.log.Error("failed to read blob data", "container", container, "blob", blob, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Write blob data to filesystem
	blobPath := h.blobFilePath(container, blob)
	if err := os.MkdirAll(filepath.Dir(blobPath), 0755); err != nil {
		h.log.Error("failed to create blob directory", "container", container, "blob", blob, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if err := os.WriteFile(blobPath, data, 0644); err != nil {
		h.log.Error("failed to write blob file", "container", container, "blob", blob, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Store blob metadata in Pebble
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	etag := common.GenerateETag(data)

	metadata := BlobMetadata{
		Container:     container,
		Blob:          blob,
		ContentType:   contentType,
		ContentLength: int64(len(data)),
		ETag:          etag,
		Created:       time.Now().UTC(),
		Modified:      time.Now().UTC(),
		FilePath:      blobPath,
	}
	metadataBytes, _ := json.Marshal(metadata)
	metaKey := h.blobMetaKey(container, blob)
	if err := h.db.Set(metaKey, metadataBytes, pebble.Sync); err != nil {
		h.log.Error("failed to store blob metadata", "container", container, "blob", blob, "error", err)
		// Clean up the file we wrote
		os.Remove(blobPath)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("blob uploaded", "container", container, "blob", blob, "size", len(data), "path", blobPath)
	common.SetResponseHeaders(w, etag)
	w.WriteHeader(http.StatusCreated)
}

// GetBlob downloads a blob
func (h *Handler) GetBlob(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Debug("downloading blob", "container", container, "blob", blob)

	// Get metadata from Pebble
	metaKey := h.blobMetaKey(container, blob)
	metaData, metaCloser, err := h.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		h.log.Debug("blob not found", "container", container, "blob", blob)
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.log.Error("failed to get blob metadata", "container", container, "blob", blob, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Make a copy of metadata before closing
	metaDataCopy := make([]byte, len(metaData))
	copy(metaDataCopy, metaData)
	metaCloser.Close()

	var meta BlobMetadata
	if err := json.Unmarshal(metaDataCopy, &meta); err != nil {
		h.log.Error("failed to parse blob metadata", "container", container, "blob", blob, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", "Invalid metadata")
		return
	}

	// Read blob data from filesystem
	blobData, err := os.ReadFile(meta.FilePath)
	if err != nil {
		h.log.Error("failed to read blob file", "container", container, "blob", blob, "path", meta.FilePath, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Handle Range header for partial content
	rangeHeader := r.Header.Get("Range")
	xmsRange := r.Header.Get("x-ms-range")
	if xmsRange != "" {
		rangeHeader = xmsRange
	}

	if rangeHeader != "" {
		// Parse range header: "bytes=start-end"
		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(rangeHeader, "-")
		if len(parts) == 2 {
			start, _ := strconv.ParseInt(parts[0], 10, 64)
			end := int64(len(blobData) - 1)
			if parts[1] != "" {
				end, _ = strconv.ParseInt(parts[1], 10, 64)
			}

			// Clamp to valid range
			if start < 0 {
				start = 0
			}
			if end >= int64(len(blobData)) {
				end = int64(len(blobData)) - 1
			}
			if start <= end {
				blobData = blobData[start : end+1]
				h.log.Debug("serving partial content", "container", container, "blob", blob, "start", start, "end", end)
				common.SetResponseHeaders(w, meta.ETag)
				common.SetBlobHeaders(w, meta.ContentType, int64(len(blobData)))
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, meta.ContentLength))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(blobData)
				return
			}
		}
	}

	h.log.Debug("blob downloaded", "container", container, "blob", blob, "size", len(blobData))
	common.SetResponseHeaders(w, meta.ETag)
	common.SetBlobHeaders(w, meta.ContentType, int64(len(blobData)))
	w.WriteHeader(http.StatusOK)
	w.Write(blobData)
}

// GetBlobProperties returns blob properties
func (h *Handler) GetBlobProperties(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Debug("getting blob properties", "container", container, "blob", blob)

	// Get metadata from Pebble
	metaKey := h.blobMetaKey(container, blob)
	metaData, metaCloser, err := h.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Make a copy of metadata before closing
	metaDataCopy := make([]byte, len(metaData))
	copy(metaDataCopy, metaData)
	metaCloser.Close()

	var meta BlobMetadata
	if err := json.Unmarshal(metaDataCopy, &meta); err != nil {
		h.log.Error("failed to parse blob metadata", "container", container, "blob", blob, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", "Invalid metadata")
		return
	}

	common.SetResponseHeaders(w, meta.ETag)
	common.SetBlobHeaders(w, meta.ContentType, meta.ContentLength)
	w.Header().Set("x-ms-blob-type", "BlockBlob")

	// Return user metadata as x-ms-meta-* headers
	for key, value := range meta.UserMetadata {
		w.Header().Set("x-ms-meta-"+key, value)
	}

	w.WriteHeader(http.StatusOK)
}

// DeleteBlob deletes a blob
func (h *Handler) DeleteBlob(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Info("deleting blob", "container", container, "blob", blob)

	metaKey := h.blobMetaKey(container, blob)

	// Get metadata to find the file path
	metaData, closer, err := h.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		h.log.Debug("blob not found for deletion", "container", container, "blob", blob)
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Make a copy of metadata before closing
	metaDataCopy := make([]byte, len(metaData))
	copy(metaDataCopy, metaData)
	closer.Close()

	var meta BlobMetadata
	if err := json.Unmarshal(metaDataCopy, &meta); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Check for active lease
	if meta.LeaseID != "" && time.Now().Before(meta.LeaseExpiry) {
		// Lease is active - check if correct lease ID was provided
		providedLeaseID := r.Header.Get("x-ms-lease-id")
		if providedLeaseID != meta.LeaseID {
			h.writeError(w, http.StatusPreconditionFailed, "LeaseIdMismatchWithBlobOperation", "The lease ID specified did not match the lease ID for the blob")
			return
		}
	}

	// Delete the blob file from filesystem
	if meta.FilePath != "" {
		if err := os.Remove(meta.FilePath); err != nil && !os.IsNotExist(err) {
			h.log.Error("failed to delete blob file", "path", meta.FilePath, "error", err)
		}
	}

	// Delete metadata from Pebble
	if err := h.db.Delete(metaKey, pebble.Sync); err != nil {
		h.log.Error("failed to delete blob metadata", "container", container, "blob", blob, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("blob deleted", "container", container, "blob", blob)
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusAccepted)
}

// ListBlobs lists blobs in a container
func (h *Handler) ListBlobs(w http.ResponseWriter, r *http.Request, container string) {
	h.log.Debug("listing blobs", "container", container)

	prefixFilter := r.URL.Query().Get("prefix")

	// Check if container exists
	containerKey := h.containerKey(container)
	_, closer, err := h.db.Get(containerKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

	// Use metadata keys prefix instead of data keys
	prefix := []byte(fmt.Sprintf("blobs/meta/%s/", container))
	iter, err := h.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer iter.Close()

	type Blob struct {
		Name string `xml:"Name"`
	}
	type EnumerationResults struct {
		XMLName       xml.Name `xml:"EnumerationResults"`
		ContainerName string   `xml:"ContainerName,attr"`
		Blobs         []Blob   `xml:"Blobs>Blob"`
	}

	result := EnumerationResults{ContainerName: container}
	for iter.First(); iter.Valid(); iter.Next() {
		name := string(iter.Key()[len(prefix):])
		// Apply prefix filter if specified
		if prefixFilter != "" && !strings.HasPrefix(name, prefixFilter) {
			continue
		}
		result.Blobs = append(result.Blobs, Blob{Name: name})
	}

	h.log.Debug("blobs listed", "container", container, "count", len(result.Blobs))
	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result)
}

// blockKey returns the Pebble key for a staged block
func (h *Handler) blockKey(container, blob, blockID string) []byte {
	return []byte(fmt.Sprintf("blobs/blocks/%s/%s/%s", container, blob, blockID))
}

// PutBlock stages a block for later commit
func (h *Handler) PutBlock(w http.ResponseWriter, r *http.Request, container, blob string) {
	blockID := r.URL.Query().Get("blockid")
	h.log.Info("putting block", "container", container, "blob", blob, "blockId", blockID)

	// Check if container exists
	containerKey := h.containerKey(container)
	_, closer, err := h.db.Get(containerKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

	// Read block data
	data, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Store block
	key := h.blockKey(container, blob, blockID)
	if err := h.db.Set(key, data, pebble.Sync); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("block staged", "container", container, "blob", blob, "blockId", blockID, "size", len(data))
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusCreated)
}

// PutBlockList commits staged blocks to create a blob
func (h *Handler) PutBlockList(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Info("putting block list", "container", container, "blob", blob)

	// Parse block list from request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Failed to read request body")
		return
	}

	type BlockList struct {
		Latest []string `xml:"Latest"`
	}
	var blockList BlockList
	if err := xml.Unmarshal(body, &blockList); err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Invalid XML format")
		return
	}

	// Assemble blob data from blocks
	var blobData []byte
	for _, blockID := range blockList.Latest {
		key := h.blockKey(container, blob, blockID)
		data, closer, err := h.db.Get(key)
		if err != nil {
			h.log.Error("block not found", "blockId", blockID, "error", err)
			h.writeError(w, http.StatusBadRequest, "InvalidBlockList", "Block not found: "+blockID)
			return
		}
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		closer.Close()
		blobData = append(blobData, dataCopy...)
	}

	// Write assembled blob to filesystem
	blobPath := h.blobFilePath(container, blob)
	if err := os.MkdirAll(filepath.Dir(blobPath), 0755); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if err := os.WriteFile(blobPath, blobData, 0644); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Store metadata
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	etag := common.GenerateETag(blobData)

	metadata := BlobMetadata{
		Container:     container,
		Blob:          blob,
		ContentType:   contentType,
		ContentLength: int64(len(blobData)),
		ETag:          etag,
		Created:       time.Now().UTC(),
		Modified:      time.Now().UTC(),
		FilePath:      blobPath,
	}
	metadataBytes, _ := json.Marshal(metadata)
	metaKey := h.blobMetaKey(container, blob)
	if err := h.db.Set(metaKey, metadataBytes, pebble.Sync); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Delete staged blocks
	for _, blockID := range blockList.Latest {
		h.db.Delete(h.blockKey(container, blob, blockID), pebble.Sync)
	}

	h.log.Info("block list committed", "container", container, "blob", blob, "size", len(blobData))
	common.SetResponseHeaders(w, etag)
	w.WriteHeader(http.StatusCreated)
}

// GetBlockList returns the list of committed blocks
func (h *Handler) GetBlockList(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Debug("getting block list", "container", container, "blob", blob)

	type Block struct {
		Name string `xml:"Name"`
		Size int64  `xml:"Size"`
	}
	type BlockList struct {
		XMLName         xml.Name `xml:"BlockList"`
		CommittedBlocks []Block  `xml:"CommittedBlocks>Block"`
	}

	// For simplicity, return empty list - blocks are only transiently stored
	result := BlockList{}
	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result)
}

// SetBlobMetadata sets user-defined metadata on a blob
func (h *Handler) SetBlobMetadata(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Info("setting blob metadata", "container", container, "blob", blob)

	// Get existing metadata
	metaKey := h.blobMetaKey(container, blob)
	data, closer, err := h.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	var meta BlobMetadata
	if err := json.Unmarshal(dataCopy, &meta); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Extract x-ms-meta-* headers
	if meta.UserMetadata == nil {
		meta.UserMetadata = make(map[string]string)
	}
	for key, values := range r.Header {
		lowerKey := strings.ToLower(key)
		if strings.HasPrefix(lowerKey, "x-ms-meta-") {
			// Get the metadata key - key is canonicalized so "x-ms-meta-key1" becomes "X-Ms-Meta-Key1"
			// We need to extract after the prefix (10 characters)
			metaKey := strings.ToLower(key[len("X-Ms-Meta-"):])
			if len(values) > 0 {
				meta.UserMetadata[metaKey] = values[0]
			}
		}
	}

	// Save updated metadata
	metadataBytes, _ := json.Marshal(meta)
	if err := h.db.Set(h.blobMetaKey(container, blob), metadataBytes, pebble.Sync); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("blob metadata set", "container", container, "blob", blob)
	common.SetResponseHeaders(w, meta.ETag)
	w.WriteHeader(http.StatusOK)
}

// SetContainerMetadata sets metadata on a container
func (h *Handler) SetContainerMetadata(w http.ResponseWriter, r *http.Request, container string) {
	h.log.Info("setting container metadata", "container", container)

	key := h.containerKey(container)
	data, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	var meta map[string]interface{}
	if err := json.Unmarshal(dataCopy, &meta); err != nil {
		meta = make(map[string]interface{})
	}

	// Extract x-ms-meta-* headers
	userMeta := make(map[string]string)
	for key, values := range r.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-ms-meta-") {
			metaKey := strings.TrimPrefix(strings.ToLower(key), "x-ms-meta-")
			if len(values) > 0 {
				userMeta[metaKey] = values[0]
			}
		}
	}
	meta["userMetadata"] = userMeta

	metadataBytes, _ := json.Marshal(meta)
	if err := h.db.Set(key, metadataBytes, pebble.Sync); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("container metadata set", "container", container)
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusOK)
}

// SetContainerACL sets the access control list for a container
func (h *Handler) SetContainerACL(w http.ResponseWriter, r *http.Request, container string) {
	h.log.Info("setting container ACL", "container", container)

	key := h.containerKey(container)
	data, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	// Parse the existing container metadata
	var meta map[string]interface{}
	if err := json.Unmarshal(dataCopy, &meta); err != nil {
		meta = make(map[string]interface{})
	}

	// Store the public access level from the header
	accessLevel := r.Header.Get("x-ms-blob-public-access")
	if accessLevel != "" {
		meta["publicAccess"] = accessLevel
	}

	// Save updated metadata
	metadataBytes, _ := json.Marshal(meta)
	if err := h.db.Set(key, metadataBytes, pebble.Sync); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusOK)
}

// GetContainerACL gets the access control list for a container
func (h *Handler) GetContainerACL(w http.ResponseWriter, r *http.Request, container string) {
	h.log.Debug("getting container ACL", "container", container)

	key := h.containerKey(container)
	data, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	// Parse container metadata to get public access level
	var meta map[string]interface{}
	if err := json.Unmarshal(dataCopy, &meta); err == nil {
		if access, ok := meta["publicAccess"].(string); ok && access != "" {
			w.Header().Set("x-ms-blob-public-access", access)
		}
	}

	type SignedIdentifiers struct {
		XMLName xml.Name `xml:"SignedIdentifiers"`
	}

	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(SignedIdentifiers{})
}

// CreateSnapshot creates a snapshot of a blob
func (h *Handler) CreateSnapshot(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Info("creating snapshot", "container", container, "blob", blob)

	// Get blob metadata
	metaKey := h.blobMetaKey(container, blob)
	data, closer, err := h.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	// Generate snapshot timestamp
	snapshot := time.Now().UTC().Format("2006-01-02T15:04:05.0000000Z")

	common.SetResponseHeaders(w, "")
	w.Header().Set("x-ms-snapshot", snapshot)
	w.WriteHeader(http.StatusCreated)
}

// LeaseBlob handles lease operations on a blob
func (h *Handler) LeaseBlob(w http.ResponseWriter, r *http.Request, container, blob string) {
	leaseAction := r.Header.Get("x-ms-lease-action")
	leaseDurationHeader := r.Header.Get("x-ms-lease-duration")
	h.log.Info("lease blob", "container", container, "blob", blob, "action", leaseAction, "duration", leaseDurationHeader)

	// Get blob metadata
	metaKey := h.blobMetaKey(container, blob)
	data, closer, err := h.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	var meta BlobMetadata
	json.Unmarshal(dataCopy, &meta)

	switch leaseAction {
	case "acquire":
		// Check if there's an active lease
		if meta.LeaseID != "" && time.Now().Before(meta.LeaseExpiry) {
			h.writeError(w, http.StatusConflict, "LeaseAlreadyPresent", "There is already a lease present")
			return
		}

		// Parse lease duration (default 60s, -1 means infinite)
		leaseDuration := 60 * time.Second
		if leaseDurationHeader != "" && leaseDurationHeader != "-1" {
			if d, err := strconv.Atoi(leaseDurationHeader); err == nil {
				leaseDuration = time.Duration(d) * time.Second
			}
		} else if leaseDurationHeader == "-1" {
			leaseDuration = 365 * 24 * time.Hour // "infinite" lease
		}

		// Generate and store lease ID
		leaseID := fmt.Sprintf("lease-%d", time.Now().UnixNano())
		meta.LeaseID = leaseID
		meta.LeaseExpiry = time.Now().Add(leaseDuration)

		// Save updated metadata
		metadataBytes, _ := json.Marshal(meta)
		if err := h.db.Set(metaKey, metadataBytes, pebble.Sync); err != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		common.SetResponseHeaders(w, meta.ETag)
		w.Header().Set("x-ms-lease-id", leaseID)
		w.WriteHeader(http.StatusCreated)

	case "release":
		// Clear the lease
		meta.LeaseID = ""
		meta.LeaseExpiry = time.Time{}
		metadataBytes, _ := json.Marshal(meta)
		h.db.Set(metaKey, metadataBytes, pebble.Sync)

		common.SetResponseHeaders(w, meta.ETag)
		w.WriteHeader(http.StatusOK)

	case "break":
		// Clear the lease
		meta.LeaseID = ""
		meta.LeaseExpiry = time.Time{}
		metadataBytes, _ := json.Marshal(meta)
		h.db.Set(metaKey, metadataBytes, pebble.Sync)

		common.SetResponseHeaders(w, meta.ETag)
		w.WriteHeader(http.StatusOK)

	case "renew":
		leaseID := r.Header.Get("x-ms-lease-id")
		if meta.LeaseID != leaseID {
			h.writeError(w, http.StatusPreconditionFailed, "LeaseIdMismatchWithLeaseOperation", "The lease ID did not match")
			return
		}
		meta.LeaseExpiry = time.Now().Add(60 * time.Second)
		metadataBytes, _ := json.Marshal(meta)
		h.db.Set(metaKey, metadataBytes, pebble.Sync)

		common.SetResponseHeaders(w, meta.ETag)
		w.Header().Set("x-ms-lease-id", leaseID)
		w.WriteHeader(http.StatusOK)

	case "change":
		leaseID := r.Header.Get("x-ms-lease-id")
		common.SetResponseHeaders(w, meta.ETag)
		w.Header().Set("x-ms-lease-id", leaseID)
		w.WriteHeader(http.StatusOK)

	default:
		h.writeError(w, http.StatusBadRequest, "InvalidHeaderValue", "Invalid lease action")
	}
}

// CopyBlob copies a blob from a source URL
func (h *Handler) CopyBlob(w http.ResponseWriter, r *http.Request, container, blob string) {
	copySource := r.Header.Get("x-ms-copy-source")
	h.log.Info("copying blob", "container", container, "blob", blob, "source", copySource)

	// Check if container exists
	containerKey := h.containerKey(container)
	_, closer, err := h.db.Get(containerKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

	// Parse source URL to extract container/blob
	// Source URL format: http://host:port/account/container/blob
	sourceParts := strings.Split(strings.TrimPrefix(copySource, "http://"), "/")
	if len(sourceParts) < 3 {
		sourceParts = strings.Split(strings.TrimPrefix(copySource, "https://"), "/")
	}
	if len(sourceParts) < 4 {
		h.writeError(w, http.StatusBadRequest, "InvalidHeaderValue", "Invalid copy source URL")
		return
	}
	// Extract container and blob from source (skip host and account)
	// sourceParts[0] = host:port, sourceParts[1] = account, sourceParts[2] = container, sourceParts[3:] = blob
	srcContainer := sourceParts[2]
	srcBlob := strings.Join(sourceParts[3:], "/")

	// Read source blob
	srcMetaKey := h.blobMetaKey(srcContainer, srcBlob)
	srcMetaData, srcCloser, err := h.db.Get(srcMetaKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The source blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	srcMetaCopy := make([]byte, len(srcMetaData))
	copy(srcMetaCopy, srcMetaData)
	srcCloser.Close()

	var srcMeta BlobMetadata
	if err := json.Unmarshal(srcMetaCopy, &srcMeta); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Read source blob data
	srcData, err := os.ReadFile(srcMeta.FilePath)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Write to destination
	blobPath := h.blobFilePath(container, blob)
	if err := os.MkdirAll(filepath.Dir(blobPath), 0755); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if err := os.WriteFile(blobPath, srcData, 0644); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Create destination metadata
	etag := common.GenerateETag(srcData)
	destMeta := BlobMetadata{
		Container:     container,
		Blob:          blob,
		ContentType:   srcMeta.ContentType,
		ContentLength: srcMeta.ContentLength,
		ETag:          etag,
		Created:       time.Now().UTC(),
		Modified:      time.Now().UTC(),
		FilePath:      blobPath,
	}
	metadataBytes, _ := json.Marshal(destMeta)
	if err := h.db.Set(h.blobMetaKey(container, blob), metadataBytes, pebble.Sync); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	copyID := fmt.Sprintf("copy-%d", time.Now().UnixNano())
	h.log.Info("blob copied", "container", container, "blob", blob)
	common.SetResponseHeaders(w, etag)
	w.Header().Set("x-ms-copy-id", copyID)
	w.Header().Set("x-ms-copy-status", "success")
	w.WriteHeader(http.StatusAccepted)
}
