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

	// Service-level operations: /{account}?restype=service&comp=properties
	if len(parts) == 1 && query.Get("restype") == "service" && query.Get("comp") == "properties" {
		switch r.Method {
		case http.MethodGet:
			h.GetServiceProperties(w, r)
		case http.MethodPut:
			h.SetServiceProperties(w, r)
		default:
			h.writeError(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "Method not allowed")
		}
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
				} else if query.Get("comp") == "lease" {
					h.LeaseContainer(w, r, container)
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
		} else if query.Get("comp") == "appendblock" {
			h.AppendBlock(w, r, container, blob)
		} else if query.Get("comp") == "page" {
			h.PutPage(w, r, container, blob)
		} else if query.Get("comp") == "metadata" {
			h.SetBlobMetadata(w, r, container, blob)
		} else if query.Get("comp") == "snapshot" {
			h.CreateSnapshot(w, r, container, blob)
		} else if query.Get("comp") == "lease" {
			h.LeaseBlob(w, r, container, blob)
		} else if query.Get("comp") == "properties" {
			h.SetBlobProperties(w, r, container, blob)
		} else if r.Header.Get("x-ms-copy-source") != "" {
			h.CopyBlob(w, r, container, blob)
		} else {
			h.PutBlob(w, r, container, blob)
		}
	case http.MethodGet:
		if query.Get("comp") == "blocklist" {
			h.GetBlockList(w, r, container, blob)
		} else if query.Get("comp") == "pagelist" {
			h.GetPageRanges(w, r, container, blob)
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
	h.log.Warn("error response",
		"statusCode", statusCode,
		"errorCode", errorCode,
		"message", message,
	)
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
	Container           string            `json:"container"`
	Blob                string            `json:"blob"`
	BlobType            string            `json:"blobType"` // BlockBlob, AppendBlob, PageBlob
	ContentType         string            `json:"contentType"`
	ContentLength       int64             `json:"contentLength"`
	ETag                string            `json:"etag"`
	Created             time.Time         `json:"created"`
	Modified            time.Time         `json:"modified"`
	FilePath            string            `json:"filePath"` // Path to actual blob data
	UserMetadata        map[string]string `json:"userMetadata,omitempty"`
	LeaseID             string            `json:"leaseId,omitempty"`
	LeaseExpiry         time.Time         `json:"leaseExpiry,omitempty"`
	CommittedBlockCount int               `json:"committedBlockCount,omitempty"` // For append blobs
	BlobContentLength   int64             `json:"blobContentLength,omitempty"`   // For page blobs (x-ms-blob-content-length)
}

// ContainerMetadata stores container metadata in Pebble
type ContainerMetadata struct {
	Name         string            `json:"name"`
	Created      time.Time         `json:"created"`
	Modified     time.Time         `json:"modified"`
	UserMetadata map[string]string `json:"userMetadata,omitempty"`
	LeaseID      string            `json:"leaseId,omitempty"`
	LeaseExpiry  time.Time         `json:"leaseExpiry,omitempty"`
	LeaseState   string            `json:"leaseState,omitempty"`    // available, leased, expired, breaking, broken
	LeaseDur     int32             `json:"leaseDuration,omitempty"` // -1 for infinite, or seconds
}

// ServiceProperties stores blob service properties
type ServiceProperties struct {
	Logging       *Logging   `xml:"Logging" json:"logging,omitempty"`
	HourMetrics   *Metrics   `xml:"HourMetrics" json:"hourMetrics,omitempty"`
	MinuteMetrics *Metrics   `xml:"MinuteMetrics" json:"minuteMetrics,omitempty"`
	CORS          []CORSRule `xml:"Cors>CorsRule" json:"cors,omitempty"`
}

type Logging struct {
	Version         string           `xml:"Version" json:"version"`
	Delete          bool             `xml:"Delete" json:"delete"`
	Read            bool             `xml:"Read" json:"read"`
	Write           bool             `xml:"Write" json:"write"`
	RetentionPolicy *RetentionPolicy `xml:"RetentionPolicy" json:"retentionPolicy,omitempty"`
}

type Metrics struct {
	Version         string           `xml:"Version" json:"version"`
	Enabled         bool             `xml:"Enabled" json:"enabled"`
	IncludeAPIs     *bool            `xml:"IncludeAPIs,omitempty" json:"includeAPIs,omitempty"`
	RetentionPolicy *RetentionPolicy `xml:"RetentionPolicy" json:"retentionPolicy,omitempty"`
}

type RetentionPolicy struct {
	Enabled bool  `xml:"Enabled" json:"enabled"`
	Days    int32 `xml:"Days,omitempty" json:"days,omitempty"`
}

type CORSRule struct {
	AllowedOrigins  string `xml:"AllowedOrigins" json:"allowedOrigins"`
	AllowedMethods  string `xml:"AllowedMethods" json:"allowedMethods"`
	AllowedHeaders  string `xml:"AllowedHeaders" json:"allowedHeaders"`
	ExposedHeaders  string `xml:"ExposedHeaders" json:"exposedHeaders"`
	MaxAgeInSeconds int32  `xml:"MaxAgeInSeconds" json:"maxAgeInSeconds"`
}

// servicePropertiesKey returns the key for service properties
func (h *Handler) servicePropertiesKey() []byte {
	return []byte("blobs/service/properties")
}

// GetServiceProperties returns blob service properties
func (h *Handler) GetServiceProperties(w http.ResponseWriter, r *http.Request) {
	h.log.Debug("getting blob service properties")

	key := h.servicePropertiesKey()
	data, closer, err := h.db.Get(key)

	var props ServiceProperties
	if err == nil {
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		closer.Close()
		json.Unmarshal(dataCopy, &props)
	} else if err == pebble.ErrNotFound {
		// Return default properties
		props = ServiceProperties{
			Logging: &Logging{
				Version: "1.0",
				Delete:  false,
				Read:    false,
				Write:   false,
				RetentionPolicy: &RetentionPolicy{
					Enabled: false,
				},
			},
			HourMetrics: &Metrics{
				Version: "1.0",
				Enabled: false,
				RetentionPolicy: &RetentionPolicy{
					Enabled: false,
				},
			},
			MinuteMetrics: &Metrics{
				Version: "1.0",
				Enabled: false,
				RetentionPolicy: &RetentionPolicy{
					Enabled: false,
				},
			},
		}
	} else {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Wrap in StorageServiceProperties for XML
	type StorageServiceProperties struct {
		XMLName       xml.Name `xml:"StorageServiceProperties"`
		Logging       *Logging `xml:"Logging,omitempty"`
		HourMetrics   *Metrics `xml:"HourMetrics,omitempty"`
		MinuteMetrics *Metrics `xml:"MinuteMetrics,omitempty"`
		CORS          *struct {
			CORSRules []CORSRule `xml:"CorsRule"`
		} `xml:"Cors,omitempty"`
	}

	response := StorageServiceProperties{
		Logging:       props.Logging,
		HourMetrics:   props.HourMetrics,
		MinuteMetrics: props.MinuteMetrics,
	}
	if len(props.CORS) > 0 {
		response.CORS = &struct {
			CORSRules []CORSRule `xml:"CorsRule"`
		}{CORSRules: props.CORS}
	}

	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

// SetServiceProperties sets blob service properties
func (h *Handler) SetServiceProperties(w http.ResponseWriter, r *http.Request) {
	h.log.Debug("setting blob service properties")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Failed to read request body")
		return
	}

	// Parse incoming XML
	type StorageServicePropertiesInput struct {
		XMLName       xml.Name `xml:"StorageServiceProperties"`
		Logging       *Logging `xml:"Logging"`
		HourMetrics   *Metrics `xml:"HourMetrics"`
		MinuteMetrics *Metrics `xml:"MinuteMetrics"`
		CORS          struct {
			CORSRules []CORSRule `xml:"CorsRule"`
		} `xml:"Cors"`
	}

	var input StorageServicePropertiesInput
	if err := xml.Unmarshal(body, &input); err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidXmlDocument", "Invalid XML format")
		return
	}

	// Get existing properties to merge
	key := h.servicePropertiesKey()
	var props ServiceProperties

	data, closer, err := h.db.Get(key)
	if err == nil {
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		closer.Close()
		json.Unmarshal(dataCopy, &props)
	}

	// Update properties
	if input.Logging != nil {
		props.Logging = input.Logging
	}
	if input.HourMetrics != nil {
		props.HourMetrics = input.HourMetrics
	}
	if input.MinuteMetrics != nil {
		props.MinuteMetrics = input.MinuteMetrics
	}
	if len(input.CORS.CORSRules) > 0 {
		props.CORS = input.CORS.CORSRules
	}

	// Save
	propsBytes, _ := json.Marshal(props)
	if err := h.db.Set(key, propsBytes, pebble.Sync); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusAccepted)
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
	now := time.Now().UTC()
	containerMeta := ContainerMetadata{
		Name:       container,
		Created:    now,
		Modified:   now,
		LeaseState: "available",
	}
	metadataBytes, _ := json.Marshal(containerMeta)
	if err := h.db.Set(key, metadataBytes, pebble.Sync); err != nil {
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

	// Check if container exists and get metadata
	data, closer, err := h.db.Get(key)
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
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	// Check for active lease
	var containerMeta ContainerMetadata
	if err := json.Unmarshal(dataCopy, &containerMeta); err == nil {
		if containerMeta.LeaseState == "leased" && time.Now().Before(containerMeta.LeaseExpiry) {
			// Container is leased - check if lease ID is provided
			leaseID := r.Header.Get("x-ms-lease-id")
			if leaseID == "" || leaseID != containerMeta.LeaseID {
				h.writeError(w, http.StatusPreconditionFailed, "LeaseIdMismatchWithContainerOperation", "There is currently a lease on the container and no matching lease ID was specified")
				return
			}
		}
	}

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

// LeaseContainer handles container lease operations
func (h *Handler) LeaseContainer(w http.ResponseWriter, r *http.Request, container string) {
	action := r.Header.Get("x-ms-lease-action")
	h.log.Info("lease container", "container", container, "action", action)

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

	var containerMeta ContainerMetadata
	if err := json.Unmarshal(dataCopy, &containerMeta); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Check if lease is expired
	if containerMeta.LeaseState == "leased" && time.Now().After(containerMeta.LeaseExpiry) {
		containerMeta.LeaseState = "expired"
		containerMeta.LeaseID = ""
	}

	switch action {
	case "acquire":
		// Check if container already has active lease
		if containerMeta.LeaseState == "leased" {
			h.writeError(w, http.StatusConflict, "LeaseAlreadyPresent", "There is already a lease present")
			return
		}

		// Get lease duration (default 15s, can be 15-60 or -1 for infinite)
		durationStr := r.Header.Get("x-ms-lease-duration")
		duration := int32(15)
		if durationStr != "" {
			fmt.Sscanf(durationStr, "%d", &duration)
		}
		if duration != -1 && (duration < 15 || duration > 60) {
			h.writeError(w, http.StatusBadRequest, "InvalidHeaderValue", "Lease duration must be 15-60 seconds or -1 for infinite")
			return
		}

		// Generate lease ID
		leaseID := r.Header.Get("x-ms-proposed-lease-id")
		if leaseID == "" {
			leaseID = fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
				time.Now().UnixNano()&0xFFFFFFFF,
				(time.Now().UnixNano()>>32)&0xFFFF,
				(time.Now().UnixNano()>>48)&0xFFFF,
				(time.Now().UnixNano()>>32)&0xFFFF,
				time.Now().UnixNano()&0xFFFFFFFFFFFF)
		}

		containerMeta.LeaseID = leaseID
		containerMeta.LeaseDur = duration
		containerMeta.LeaseState = "leased"
		if duration == -1 {
			containerMeta.LeaseExpiry = time.Now().Add(100 * 365 * 24 * time.Hour) // Far future for infinite
		} else {
			containerMeta.LeaseExpiry = time.Now().Add(time.Duration(duration) * time.Second)
		}

		metadataBytes, _ := json.Marshal(containerMeta)
		if err := h.db.Set(key, metadataBytes, pebble.Sync); err != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		w.Header().Set("x-ms-lease-id", leaseID)
		common.SetResponseHeaders(w, "")
		w.WriteHeader(http.StatusCreated)

	case "renew":
		// Check if lease ID matches
		leaseID := r.Header.Get("x-ms-lease-id")
		if leaseID == "" || leaseID != containerMeta.LeaseID {
			h.writeError(w, http.StatusConflict, "LeaseIdMismatchWithLeaseOperation", "The lease ID specified did not match the lease ID for the container")
			return
		}
		if containerMeta.LeaseState != "leased" {
			h.writeError(w, http.StatusConflict, "LeaseNotPresentWithLeaseOperation", "There is currently no lease on the container")
			return
		}

		// Renew lease
		if containerMeta.LeaseDur == -1 {
			containerMeta.LeaseExpiry = time.Now().Add(100 * 365 * 24 * time.Hour)
		} else {
			containerMeta.LeaseExpiry = time.Now().Add(time.Duration(containerMeta.LeaseDur) * time.Second)
		}

		metadataBytes, _ := json.Marshal(containerMeta)
		if err := h.db.Set(key, metadataBytes, pebble.Sync); err != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		w.Header().Set("x-ms-lease-id", leaseID)
		common.SetResponseHeaders(w, "")
		w.WriteHeader(http.StatusOK)

	case "release":
		// Check if lease ID matches
		leaseID := r.Header.Get("x-ms-lease-id")
		if leaseID == "" || leaseID != containerMeta.LeaseID {
			h.writeError(w, http.StatusConflict, "LeaseIdMismatchWithLeaseOperation", "The lease ID specified did not match the lease ID for the container")
			return
		}

		containerMeta.LeaseID = ""
		containerMeta.LeaseState = "available"
		containerMeta.LeaseExpiry = time.Time{}
		containerMeta.LeaseDur = 0

		metadataBytes, _ := json.Marshal(containerMeta)
		if err := h.db.Set(key, metadataBytes, pebble.Sync); err != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		common.SetResponseHeaders(w, "")
		w.WriteHeader(http.StatusOK)

	case "break":
		if containerMeta.LeaseState != "leased" {
			h.writeError(w, http.StatusConflict, "LeaseNotPresentWithLeaseOperation", "There is currently no lease on the container")
			return
		}

		// Break lease immediately (simplified - real Azure has break period)
		containerMeta.LeaseID = ""
		containerMeta.LeaseState = "broken"
		containerMeta.LeaseExpiry = time.Time{}

		metadataBytes, _ := json.Marshal(containerMeta)
		if err := h.db.Set(key, metadataBytes, pebble.Sync); err != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		w.Header().Set("x-ms-lease-time", "0")
		common.SetResponseHeaders(w, "")
		w.WriteHeader(http.StatusAccepted)

	case "change":
		// Check if lease ID matches
		leaseID := r.Header.Get("x-ms-lease-id")
		if leaseID == "" || leaseID != containerMeta.LeaseID {
			h.writeError(w, http.StatusConflict, "LeaseIdMismatchWithLeaseOperation", "The lease ID specified did not match the lease ID for the container")
			return
		}

		proposedLeaseID := r.Header.Get("x-ms-proposed-lease-id")
		if proposedLeaseID == "" {
			h.writeError(w, http.StatusBadRequest, "MissingRequiredHeader", "x-ms-proposed-lease-id header is required")
			return
		}

		containerMeta.LeaseID = proposedLeaseID

		metadataBytes, _ := json.Marshal(containerMeta)
		if err := h.db.Set(key, metadataBytes, pebble.Sync); err != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		w.Header().Set("x-ms-lease-id", proposedLeaseID)
		common.SetResponseHeaders(w, "")
		w.WriteHeader(http.StatusOK)

	default:
		h.writeError(w, http.StatusBadRequest, "InvalidHeaderValue", "Invalid x-ms-lease-action")
	}
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
	blobType := r.Header.Get("x-ms-blob-type")
	if blobType == "" {
		blobType = "BlockBlob"
	}

	h.log.Info("uploading blob", "container", container, "blob", blob, "blobType", blobType, "contentLength", r.ContentLength)

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

	// Handle different blob types
	switch blobType {
	case "AppendBlob":
		h.createAppendBlob(w, r, container, blob)
		return
	case "PageBlob":
		h.createPageBlob(w, r, container, blob)
		return
	}

	// Default: BlockBlob - read blob data
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
		BlobType:      "BlockBlob",
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
	// Set blob type header
	blobType := meta.BlobType
	if blobType == "" {
		blobType = "BlockBlob"
	}
	w.Header().Set("x-ms-blob-type", blobType)
	// Add append blob specific headers
	if blobType == "AppendBlob" {
		w.Header().Set("x-ms-blob-committed-block-count", strconv.Itoa(meta.CommittedBlockCount))
	}
	// Add page blob specific headers
	if blobType == "PageBlob" {
		w.Header().Set("x-ms-blob-content-length", strconv.FormatInt(meta.BlobContentLength, 10))
	}
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
	// Set blob type header
	blobType := meta.BlobType
	if blobType == "" {
		blobType = "BlockBlob"
	}
	w.Header().Set("x-ms-blob-type", blobType)
	// Add append blob specific headers
	if blobType == "AppendBlob" {
		w.Header().Set("x-ms-blob-committed-block-count", strconv.Itoa(meta.CommittedBlockCount))
	}
	// Add page blob specific headers
	if blobType == "PageBlob" {
		w.Header().Set("x-ms-blob-content-length", strconv.FormatInt(meta.BlobContentLength, 10))
	}

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

// ============================================================================
// Append Blob Operations
// ============================================================================

// createAppendBlob creates a new append blob (empty)
func (h *Handler) createAppendBlob(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Info("creating append blob", "container", container, "blob", blob)

	// Create empty file for the append blob
	blobPath := h.blobFilePath(container, blob)
	if err := os.MkdirAll(filepath.Dir(blobPath), 0755); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if err := os.WriteFile(blobPath, []byte{}, 0644); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	etag := common.GenerateETag([]byte{})

	metadata := BlobMetadata{
		Container:           container,
		Blob:                blob,
		BlobType:            "AppendBlob",
		ContentType:         contentType,
		ContentLength:       0,
		ETag:                etag,
		Created:             time.Now().UTC(),
		Modified:            time.Now().UTC(),
		FilePath:            blobPath,
		CommittedBlockCount: 0,
	}
	metadataBytes, _ := json.Marshal(metadata)
	if err := h.db.Set(h.blobMetaKey(container, blob), metadataBytes, pebble.Sync); err != nil {
		os.Remove(blobPath)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("append blob created", "container", container, "blob", blob)
	common.SetResponseHeaders(w, etag)
	w.WriteHeader(http.StatusCreated)
}

// AppendBlock appends a block to an append blob
func (h *Handler) AppendBlock(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Info("appending block", "container", container, "blob", blob)

	// Get existing blob metadata
	metaKey := h.blobMetaKey(container, blob)
	metaData, closer, err := h.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	metaCopy := make([]byte, len(metaData))
	copy(metaCopy, metaData)
	closer.Close()

	var metadata BlobMetadata
	if err := json.Unmarshal(metaCopy, &metadata); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Verify it's an append blob
	if metadata.BlobType != "AppendBlob" {
		h.writeError(w, http.StatusBadRequest, "InvalidBlobType", "The blob type is invalid for this operation")
		return
	}

	// Read block data
	blockData, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Open file and append data
	f, err := os.OpenFile(metadata.FilePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	appendOffset := metadata.ContentLength
	_, err = f.Write(blockData)
	f.Close()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Update metadata
	metadata.ContentLength += int64(len(blockData))
	metadata.CommittedBlockCount++
	metadata.Modified = time.Now().UTC()

	// Read full file to calculate new ETag
	fullData, err := os.ReadFile(metadata.FilePath)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	metadata.ETag = common.GenerateETag(fullData)

	metadataBytes, _ := json.Marshal(metadata)
	if err := h.db.Set(metaKey, metadataBytes, pebble.Sync); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("block appended", "container", container, "blob", blob, "offset", appendOffset, "blockSize", len(blockData))
	common.SetResponseHeaders(w, metadata.ETag)
	w.Header().Set("x-ms-blob-append-offset", strconv.FormatInt(appendOffset, 10))
	w.Header().Set("x-ms-blob-committed-block-count", strconv.Itoa(metadata.CommittedBlockCount))
	w.WriteHeader(http.StatusCreated)
}

// ============================================================================
// Page Blob Operations
// ============================================================================

// createPageBlob creates a new page blob with the specified size
func (h *Handler) createPageBlob(w http.ResponseWriter, r *http.Request, container, blob string) {
	// Get the blob content length from header
	blobContentLengthStr := r.Header.Get("x-ms-blob-content-length")
	if blobContentLengthStr == "" {
		h.writeError(w, http.StatusBadRequest, "MissingRequiredHeader", "x-ms-blob-content-length header is required for page blobs")
		return
	}

	blobContentLength, err := strconv.ParseInt(blobContentLengthStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidHeaderValue", "Invalid x-ms-blob-content-length value")
		return
	}

	// Page blob size must be a multiple of 512
	if blobContentLength%512 != 0 {
		h.writeError(w, http.StatusBadRequest, "InvalidHeaderValue", "Page blob size must be a multiple of 512 bytes")
		return
	}

	h.log.Info("creating page blob", "container", container, "blob", blob, "size", blobContentLength)

	// Create file with zeros
	blobPath := h.blobFilePath(container, blob)
	if err := os.MkdirAll(filepath.Dir(blobPath), 0755); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Create sparse file (or file filled with zeros)
	f, err := os.Create(blobPath)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if err := f.Truncate(blobContentLength); err != nil {
		f.Close()
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	f.Close()

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	etag := common.GenerateETag([]byte(fmt.Sprintf("pageblob-%s-%d", blob, time.Now().UnixNano())))

	metadata := BlobMetadata{
		Container:         container,
		Blob:              blob,
		BlobType:          "PageBlob",
		ContentType:       contentType,
		ContentLength:     blobContentLength,
		BlobContentLength: blobContentLength,
		ETag:              etag,
		Created:           time.Now().UTC(),
		Modified:          time.Now().UTC(),
		FilePath:          blobPath,
	}
	metadataBytes, _ := json.Marshal(metadata)
	if err := h.db.Set(h.blobMetaKey(container, blob), metadataBytes, pebble.Sync); err != nil {
		os.Remove(blobPath)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("page blob created", "container", container, "blob", blob, "size", blobContentLength)
	common.SetResponseHeaders(w, etag)
	w.WriteHeader(http.StatusCreated)
}

// PutPage writes pages to a page blob
func (h *Handler) PutPage(w http.ResponseWriter, r *http.Request, container, blob string) {
	pageWrite := r.Header.Get("x-ms-page-write")
	rangeHeader := r.Header.Get("x-ms-range")
	if rangeHeader == "" {
		rangeHeader = r.Header.Get("Range")
	}

	h.log.Info("put page", "container", container, "blob", blob, "action", pageWrite, "range", rangeHeader)

	// Get existing blob metadata
	metaKey := h.blobMetaKey(container, blob)
	metaData, closer, err := h.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	metaCopy := make([]byte, len(metaData))
	copy(metaCopy, metaData)
	closer.Close()

	var metadata BlobMetadata
	if err := json.Unmarshal(metaCopy, &metadata); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Parse range (format: "bytes=start-end")
	start, end, err := parseRange(rangeHeader)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidHeaderValue", "Invalid range header")
		return
	}

	f, err := os.OpenFile(metadata.FilePath, os.O_RDWR, 0644)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer f.Close()

	if pageWrite == "clear" {
		// Clear pages - write zeros
		zeros := make([]byte, end-start+1)
		if _, err := f.WriteAt(zeros, start); err != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	} else {
		// Update pages - write data
		pageData, err := io.ReadAll(r.Body)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
		if _, err := f.WriteAt(pageData, start); err != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	}

	// Update ETag
	metadata.Modified = time.Now().UTC()
	metadata.ETag = common.GenerateETag([]byte(fmt.Sprintf("page-%s-%d", blob, time.Now().UnixNano())))

	metadataBytes, _ := json.Marshal(metadata)
	if err := h.db.Set(metaKey, metadataBytes, pebble.Sync); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	common.SetResponseHeaders(w, metadata.ETag)
	w.Header().Set("x-ms-blob-sequence-number", "0")
	w.WriteHeader(http.StatusCreated)
}

// SetBlobProperties sets properties on a blob (used for page blob resize)
func (h *Handler) SetBlobProperties(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Info("setting blob properties", "container", container, "blob", blob)

	// Get existing blob metadata
	metaKey := h.blobMetaKey(container, blob)
	metaData, closer, err := h.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	metaCopy := make([]byte, len(metaData))
	copy(metaCopy, metaData)
	closer.Close()

	var metadata BlobMetadata
	if err := json.Unmarshal(metaCopy, &metadata); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Handle page blob resize
	newSizeStr := r.Header.Get("x-ms-blob-content-length")
	if newSizeStr != "" && metadata.BlobType == "PageBlob" {
		newSize, err := strconv.ParseInt(newSizeStr, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "InvalidHeaderValue", "Invalid x-ms-blob-content-length")
			return
		}
		if newSize%512 != 0 {
			h.writeError(w, http.StatusBadRequest, "InvalidHeaderValue", "Size must be a multiple of 512")
			return
		}

		// Resize the file
		if err := os.Truncate(metadata.FilePath, newSize); err != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		metadata.ContentLength = newSize
		metadata.BlobContentLength = newSize
	}

	// Update content type if provided
	if ct := r.Header.Get("x-ms-blob-content-type"); ct != "" {
		metadata.ContentType = ct
	}

	metadata.Modified = time.Now().UTC()
	metadata.ETag = common.GenerateETag([]byte(fmt.Sprintf("props-%s-%d", blob, time.Now().UnixNano())))

	metadataBytes, _ := json.Marshal(metadata)
	if err := h.db.Set(metaKey, metadataBytes, pebble.Sync); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	common.SetResponseHeaders(w, metadata.ETag)
	w.WriteHeader(http.StatusOK)
}

// GetPageRanges returns the list of valid page ranges for a page blob
func (h *Handler) GetPageRanges(w http.ResponseWriter, r *http.Request, container, blob string) {
	h.log.Info("getting page ranges", "container", container, "blob", blob)

	// Get existing blob metadata
	metaKey := h.blobMetaKey(container, blob)
	metaData, closer, err := h.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	metaCopy := make([]byte, len(metaData))
	copy(metaCopy, metaData)
	closer.Close()

	var metadata BlobMetadata
	if err := json.Unmarshal(metaCopy, &metadata); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Read the file and find non-zero page ranges
	data, err := os.ReadFile(metadata.FilePath)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	type PageRange struct {
		Start int64 `xml:"Start"`
		End   int64 `xml:"End"`
	}
	type PageList struct {
		XMLName   xml.Name    `xml:"PageList"`
		PageRange []PageRange `xml:"PageRange"`
	}

	pageList := PageList{}

	// Scan for non-zero 512-byte pages
	pageSize := int64(512)
	var inRange bool
	var rangeStart int64

	for i := int64(0); i < int64(len(data)); i += pageSize {
		end := i + pageSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}

		page := data[i:end]
		hasData := false
		for _, b := range page {
			if b != 0 {
				hasData = true
				break
			}
		}

		if hasData && !inRange {
			inRange = true
			rangeStart = i
		} else if !hasData && inRange {
			pageList.PageRange = append(pageList.PageRange, PageRange{
				Start: rangeStart,
				End:   i - 1,
			})
			inRange = false
		}
	}

	// Close any open range
	if inRange {
		pageList.PageRange = append(pageList.PageRange, PageRange{
			Start: rangeStart,
			End:   int64(len(data)) - 1,
		})
	}

	common.SetResponseHeaders(w, metadata.ETag)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-ms-blob-content-length", strconv.FormatInt(metadata.ContentLength, 10))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(pageList)
}

// parseRange parses a range header like "bytes=0-511"
func parseRange(rangeHeader string) (int64, int64, error) {
	rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeHeader, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format")
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}
