package queues

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/cockroachdb/pebble"
)

// Handler handles queue storage operations
type Handler struct {
	db  *pebble.DB
	log *slog.Logger
}

// NewHandler creates a new queue handler
func NewHandler(db *pebble.DB, logger *slog.Logger) *Handler {
	return &Handler{
		db:  db,
		log: logger.With("component", "queues"),
	}
}

// RegisterRoutes registers queue storage routes
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// Routes are registered at the root level with a dispatcher
	h.log.Info("queue routes registered")
}

// HandleRequest routes queue requests
func (h *Handler) HandleRequest(w http.ResponseWriter, r *http.Request) {
	h.handleRequest(w, r)
}

// handleRequest routes queue requests
func (h *Handler) handleRequest(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	query := r.URL.Query()

	if len(parts) < 1 {
		return
	}

	// List queues: /{account}?comp=list
	if len(parts) == 1 && query.Get("comp") == "list" && r.Method == "GET" {
		h.ListQueues(w, r)
		return
	}

	if len(parts) < 2 {
		return
	}

	// Queue operations: /{account}/{queue}
	if len(parts) == 2 {
		switch r.Method {
		case "PUT":
			h.CreateQueue(w, r)
		case "DELETE":
			h.DeleteQueue(w, r)
		}
		return
	}

	// Message operations: /{account}/{queue}/messages
	if len(parts) >= 3 && parts[2] == "messages" {
		if len(parts) == 3 {
			switch r.Method {
			case "POST":
				h.PutMessage(w, r)
			case "GET":
				h.GetMessages(w, r)
			case "DELETE":
				h.ClearMessages(w, r)
			}
		} else if len(parts) == 4 && r.Method == "DELETE" {
			h.DeleteMessage(w, r)
		}
	}
} // CreateQueue creates a new queue
func (h *Handler) CreateQueue(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	queue := parts[1]

	h.log.Debug("create queue", "queue", queue) // TODO: Implement queue creation
	w.WriteHeader(http.StatusCreated)
}

// ListQueues lists all queues
func (h *Handler) ListQueues(w http.ResponseWriter, r *http.Request) {
	h.log.Debug("list queues")

	// TODO: Implement queue listing
	w.WriteHeader(http.StatusOK)
}

// DeleteQueue deletes a queue
func (h *Handler) DeleteQueue(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	queue := parts[1]

	h.log.Debug("delete queue", "queue", queue) // TODO: Implement queue deletion
	w.WriteHeader(http.StatusNoContent)
}

// PutMessage adds a message to the queue
func (h *Handler) PutMessage(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	queue := parts[1]

	h.log.Debug("put message", "queue", queue) // TODO: Implement message enqueue
	w.WriteHeader(http.StatusCreated)
}

// GetMessages retrieves messages from the queue
func (h *Handler) GetMessages(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	queue := parts[1]

	h.log.Debug("get messages", "queue", queue) // TODO: Implement message dequeue
	w.WriteHeader(http.StatusOK)
}

// DeleteMessage deletes a specific message
func (h *Handler) DeleteMessage(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	queue := parts[1]
	messageID := parts[3]

	h.log.Debug("delete message", "queue", queue, "messageId", messageID) // TODO: Implement message deletion
	w.WriteHeader(http.StatusNoContent)
}

// ClearMessages clears all messages from the queue
func (h *Handler) ClearMessages(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	queue := parts[1]

	h.log.Debug("clear messages", "queue", queue) // TODO: Implement message clearing
	w.WriteHeader(http.StatusNoContent)
}
