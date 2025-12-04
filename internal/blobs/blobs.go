package blobs

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/cockroachdb/pebble"
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
	// Routes are registered at the root level with a dispatcher
	h.log.Info("blob routes registered")
}

// HandleRequest routes blob requests
func (h *Handler) HandleRequest(w http.ResponseWriter, r *http.Request) {
	h.handleRequest(w, r)
}

// handleRequest routes blob requests
func (h *Handler) handleRequest(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	if len(parts) < 1 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	// Check if this is a blob/container request by looking at query params or path structure
	query := r.URL.Query()

	// List containers: /{account}?comp=list
	if len(parts) == 1 && query.Get("comp") == "list" && r.Method == "GET" {
		h.ListContainers(w, r)
		return
	}

	if len(parts) < 2 {
		return // Not a blob request
	}

	// Create container: PUT /{account}/{container}
	if len(parts) == 2 && r.Method == "PUT" && query.Get("restype") != "container" {
		h.CreateContainer(w, r)
		return
	}

	// List blobs: GET /{account}/{container}?restype=container&comp=list
	if len(parts) == 2 && r.Method == "GET" && query.Get("restype") == "container" && query.Get("comp") == "list" {
		h.ListBlobs(w, r)
		return
	}

	// Blob operations: /{account}/{container}/{blob}
	if len(parts) >= 3 {
		switch r.Method {
		case "PUT":
			h.PutBlob(w, r)
		case "GET":
			h.GetBlob(w, r)
		case "DELETE":
			h.DeleteBlob(w, r)
		}
	}
} // CreateContainer creates a new blob container
func (h *Handler) CreateContainer(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	container := parts[1]

	h.log.Debug("create container", "container", container) // TODO: Implement container creation
	w.WriteHeader(http.StatusCreated)
}

// ListContainers lists all containers
func (h *Handler) ListContainers(w http.ResponseWriter, r *http.Request) {
	h.log.Debug("list containers")

	// TODO: Implement container listing
	w.WriteHeader(http.StatusOK)
}

// PutBlob uploads a blob
func (h *Handler) PutBlob(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	container := parts[1]
	blob := strings.Join(parts[2:], "/")

	h.log.Debug("put blob", "container", container, "blob", blob)

	// TODO: Implement blob upload
	w.WriteHeader(http.StatusCreated)
}

// GetBlob downloads a blob
func (h *Handler) GetBlob(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	container := parts[1]
	blob := strings.Join(parts[2:], "/")

	h.log.Debug("get blob", "container", container, "blob", blob)

	// TODO: Implement blob download
	w.WriteHeader(http.StatusOK)
}

// DeleteBlob deletes a blob
func (h *Handler) DeleteBlob(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	container := parts[1]
	blob := strings.Join(parts[2:], "/")

	h.log.Debug("delete blob", "container", container, "blob", blob)

	// TODO: Implement blob deletion
	w.WriteHeader(http.StatusAccepted)
}

// ListBlobs lists blobs in a container
func (h *Handler) ListBlobs(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	container := parts[1]

	h.log.Debug("list blobs", "container", container)

	// TODO: Implement blob listing
	w.WriteHeader(http.StatusOK)
}
