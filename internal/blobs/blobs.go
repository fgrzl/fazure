package blobs

import (
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/fgrzl/fazure/internal/common"
)

// Handler handles blob storage operations
type Handler struct {
	db  *pebble.DB
	log *slog.Logger
}

// NewHandler creates a new blob handler
func NewHandler(db *pebble.DB, logger *slog.Logger) *Handler {
	return &Handler{
		db:  db,
		log: logger.With("component", "blobs"),
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
				h.CreateContainer(w, r, container)
			} else {
				h.writeError(w, http.StatusBadRequest, "InvalidQueryParameterValue", "restype=container required")
			}
		case http.MethodGet:
			if query.Get("restype") == "container" && query.Get("comp") == "list" {
				h.ListBlobs(w, r, container)
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
		h.PutBlob(w, r, container, blob)
	case http.MethodGet:
		h.GetBlob(w, r, container, blob)
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

// blobKey returns the Pebble key for a blob
func (h *Handler) blobKey(container, blob string) []byte {
	return []byte(fmt.Sprintf("blobs/data/%s/%s", container, blob))
}

// blobMetaKey returns the Pebble key for blob metadata
func (h *Handler) blobMetaKey(container, blob string) []byte {
	return []byte(fmt.Sprintf("blobs/meta/%s/%s", container, blob))
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

	// Delete container and all blobs
	batch := h.db.NewBatch()
	batch.Delete(key, nil)

	// Delete all blobs in container
	prefix := []byte(fmt.Sprintf("blobs/data/%s/", container))
	iter, err := h.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		h.log.Error("failed to iterate blobs", "container", container, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		batch.Delete(iter.Key(), nil)
	}

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
	_, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

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

	// Read blob data
	data, err := io.ReadAll(r.Body)
	if err != nil {
		h.log.Error("failed to read blob data", "container", container, "blob", blob, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Store blob
	key := h.blobKey(container, blob)
	if err := h.db.Set(key, data, pebble.Sync); err != nil {
		h.log.Error("failed to store blob", "container", container, "blob", blob, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Store blob metadata
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	etag := common.GenerateETag(data)
	metadata := fmt.Sprintf(`{"contentType":"%s","contentLength":%d,"etag":"%s","created":"%s"}`,
		contentType, len(data), etag, time.Now().UTC().Format(time.RFC3339))
	metaKey := h.blobMetaKey(container, blob)
	if err := h.db.Set(metaKey, []byte(metadata), pebble.Sync); err != nil {
		h.log.Error("failed to store blob metadata", "container", container, "blob", blob, "error", err)
	}

	h.log.Info("blob uploaded", "container", container, "blob", blob, "size", len(data))
	common.SetResponseHeaders(w, etag)
	w.WriteHeader(http.StatusCreated)
}

// GetBlob downloads a blob
func (h *Handler) GetBlob(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Debug("downloading blob", "container", container, "blob", blob)

	key := h.blobKey(container, blob)
	data, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.log.Debug("blob not found", "container", container, "blob", blob)
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.log.Error("failed to get blob", "container", container, "blob", blob, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer closer.Close()

	// Make a copy of data before closer.Close()
	blobData := make([]byte, len(data))
	copy(blobData, data)

	h.log.Debug("blob downloaded", "container", container, "blob", blob, "size", len(blobData))
	common.SetResponseHeaders(w, "")
	common.SetBlobHeaders(w, "application/octet-stream", int64(len(blobData)))
	w.WriteHeader(http.StatusOK)
	w.Write(blobData)
}

// GetBlobProperties returns blob properties
func (h *Handler) GetBlobProperties(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Debug("getting blob properties", "container", container, "blob", blob)

	key := h.blobKey(container, blob)
	data, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer closer.Close()

	common.SetResponseHeaders(w, "")
	common.SetBlobHeaders(w, "application/octet-stream", int64(len(data)))
	w.WriteHeader(http.StatusOK)
}

// DeleteBlob deletes a blob
func (h *Handler) DeleteBlob(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Info("deleting blob", "container", container, "blob", blob)

	key := h.blobKey(container, blob)

	// Check if blob exists
	_, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.log.Debug("blob not found for deletion", "container", container, "blob", blob)
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

	// Delete blob and metadata
	batch := h.db.NewBatch()
	batch.Delete(key, nil)
	batch.Delete(h.blobMetaKey(container, blob), nil)

	if err := batch.Commit(pebble.Sync); err != nil {
		h.log.Error("failed to delete blob", "container", container, "blob", blob, "error", err)
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

	prefix := []byte(fmt.Sprintf("blobs/data/%s/", container))
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
		result.Blobs = append(result.Blobs, Blob{Name: name})
	}

	h.log.Debug("blobs listed", "container", container, "count", len(result.Blobs))
	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result)
}
