package queues

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/fgrzl/fazure/internal/common"
)

// Handler handles queue storage operations
type Handler struct {
	db  *pebble.DB
	log *slog.Logger
}

// Message represents a queue message
type Message struct {
	MessageID       string    `xml:"MessageId"`
	InsertionTime   time.Time `xml:"InsertionTime"`
	ExpirationTime  time.Time `xml:"ExpirationTime"`
	PopReceipt      string    `xml:"PopReceipt"`
	TimeNextVisible time.Time `xml:"TimeNextVisible"`
	DequeueCount    int       `xml:"DequeueCount"`
	MessageText     string    `xml:"MessageText"`
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
	h.log.Info("queue routes registered")
}

// HandleRequest routes queue requests - exported for use by main dispatcher
func (h *Handler) HandleRequest(w http.ResponseWriter, r *http.Request) {
	h.handleRequest(w, r)
}

// handleRequest routes queue requests
func (h *Handler) handleRequest(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	query := r.URL.Query()

	if len(parts) < 1 {
		h.writeError(w, http.StatusBadRequest, "InvalidUri", "Invalid path")
		return
	}

	// List queues: /{account}?comp=list
	if len(parts) == 1 && query.Get("comp") == "list" && r.Method == http.MethodGet {
		h.ListQueues(w, r)
		return
	}

	if len(parts) < 2 {
		h.writeError(w, http.StatusBadRequest, "InvalidUri", "Queue name required")
		return
	}

	queue := parts[1]

	// Queue operations: /{account}/{queue}
	if len(parts) == 2 {
		switch r.Method {
		case http.MethodPut:
			h.CreateQueue(w, r, queue)
		case http.MethodDelete:
			h.DeleteQueue(w, r, queue)
		case http.MethodGet:
			h.GetQueueMetadata(w, r, queue)
		default:
			h.writeError(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "Method not allowed")
		}
		return
	}

	// Message operations: /{account}/{queue}/messages
	if len(parts) >= 3 && parts[2] == "messages" {
		if len(parts) == 3 {
			switch r.Method {
			case http.MethodPost:
				h.PutMessage(w, r, queue)
			case http.MethodGet:
				h.GetMessages(w, r, queue)
			case http.MethodDelete:
				h.ClearMessages(w, r, queue)
			default:
				h.writeError(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "Method not allowed")
			}
		} else if len(parts) == 4 && r.Method == http.MethodDelete {
			messageID := parts[3]
			h.DeleteMessage(w, r, queue, messageID)
		}
		return
	}

	h.writeError(w, http.StatusBadRequest, "InvalidUri", "Invalid queue operation")
}

// writeError writes an Azure Storage error response
func (h *Handler) writeError(w http.ResponseWriter, statusCode int, errorCode, message string) {
	common.WriteErrorResponse(w, statusCode, errorCode, message)
}

// queueKey returns the Pebble key for queue metadata
func (h *Handler) queueKey(queue string) []byte {
	return []byte(fmt.Sprintf("queues/meta/%s", queue))
}

// messageKey returns the Pebble key for a message
func (h *Handler) messageKey(queue, messageID string) []byte {
	return []byte(fmt.Sprintf("queues/data/%s/%s", queue, messageID))
}

// generateMessageID generates a unique message ID
func (h *Handler) generateMessageID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// generatePopReceipt generates a unique pop receipt
func (h *Handler) generatePopReceipt() string {
	return fmt.Sprintf("pop-%d", time.Now().UnixNano())
}

// CreateQueue creates a new queue
func (h *Handler) CreateQueue(w http.ResponseWriter, r *http.Request, queue string) {
	h.log.Info("creating queue", "queue", queue)

	key := h.queueKey(queue)

	// Check if queue already exists
	_, closer, err := h.db.Get(key)
	if err == nil {
		closer.Close()
		h.log.Debug("queue already exists", "queue", queue)
		// Azure returns 204 if queue exists with same metadata
		common.SetResponseHeaders(w, "")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != pebble.ErrNotFound {
		h.log.Error("failed to check queue existence", "queue", queue, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Create queue metadata
	metadata := fmt.Sprintf(`{"name":"%s","created":"%s"}`, queue, time.Now().UTC().Format(time.RFC3339))
	if err := h.db.Set(key, []byte(metadata), pebble.Sync); err != nil {
		h.log.Error("failed to create queue", "queue", queue, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("queue created", "queue", queue)
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusCreated)
}

// DeleteQueue deletes a queue
func (h *Handler) DeleteQueue(w http.ResponseWriter, r *http.Request, queue string) {
	h.log.Info("deleting queue", "queue", queue)

	key := h.queueKey(queue)

	// Check if queue exists
	_, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.log.Debug("queue not found", "queue", queue)
		h.writeError(w, http.StatusNotFound, "QueueNotFound", "The specified queue does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

	// Delete queue metadata and all messages
	batch := h.db.NewBatch()
	batch.Delete(key, nil)

	// Delete all messages in queue
	prefix := []byte(fmt.Sprintf("queues/data/%s/", queue))
	iter, err := h.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		batch.Delete(iter.Key(), nil)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		h.log.Error("failed to delete queue", "queue", queue, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("queue deleted", "queue", queue)
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusNoContent)
}

// GetQueueMetadata returns queue metadata
func (h *Handler) GetQueueMetadata(w http.ResponseWriter, r *http.Request, queue string) {
	h.log.Debug("getting queue metadata", "queue", queue)

	key := h.queueKey(queue)
	_, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "QueueNotFound", "The specified queue does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

	// Count messages
	prefix := []byte(fmt.Sprintf("queues/data/%s/", queue))
	iter, err := h.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	common.SetResponseHeaders(w, "")
	w.Header().Set("x-ms-approximate-messages-count", strconv.Itoa(count))
	w.WriteHeader(http.StatusOK)
}

// ListQueues lists all queues
func (h *Handler) ListQueues(w http.ResponseWriter, r *http.Request) {
	h.log.Info("listing queues")

	prefix := []byte("queues/meta/")
	iter, err := h.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		h.log.Error("failed to iterate queues", "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer iter.Close()

	type Queue struct {
		Name string `xml:"Name"`
	}
	type EnumerationResults struct {
		XMLName xml.Name `xml:"EnumerationResults"`
		Queues  []Queue  `xml:"Queues>Queue"`
	}

	result := EnumerationResults{}
	for iter.First(); iter.Valid(); iter.Next() {
		name := string(iter.Key()[len(prefix):])
		result.Queues = append(result.Queues, Queue{Name: name})
	}

	h.log.Info("queues listed", "count", len(result.Queues))
	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result)
}

// PutMessage adds a message to the queue
func (h *Handler) PutMessage(w http.ResponseWriter, r *http.Request, queue string) {
	h.log.Info("putting message", "queue", queue)

	// Check if queue exists
	queueKey := h.queueKey(queue)
	_, closer, err := h.db.Get(queueKey)
	if err == pebble.ErrNotFound {
		h.log.Debug("queue not found for message", "queue", queue)
		h.writeError(w, http.StatusNotFound, "QueueNotFound", "The specified queue does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

	// Parse request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Failed to read request body")
		return
	}

	// Extract message text from XML
	type QueueMessage struct {
		MessageText string `xml:"MessageText"`
	}
	var msg QueueMessage
	if err := xml.Unmarshal(body, &msg); err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Invalid XML format")
		return
	}

	// Create message
	messageID := h.generateMessageID()
	now := time.Now().UTC()
	message := Message{
		MessageID:       messageID,
		InsertionTime:   now,
		ExpirationTime:  now.Add(7 * 24 * time.Hour), // Default 7 days
		PopReceipt:      h.generatePopReceipt(),
		TimeNextVisible: now,
		DequeueCount:    0,
		MessageText:     base64.StdEncoding.EncodeToString([]byte(msg.MessageText)),
	}

	// Store message
	key := h.messageKey(queue, messageID)
	data, _ := xml.Marshal(message)
	if err := h.db.Set(key, data, pebble.Sync); err != nil {
		h.log.Error("failed to store message", "queue", queue, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("message added", "queue", queue, "messageId", messageID)

	// Return response
	type QueueMessagesList struct {
		XMLName      xml.Name  `xml:"QueueMessagesList"`
		QueueMessage []Message `xml:"QueueMessage"`
	}
	response := QueueMessagesList{QueueMessage: []Message{message}}

	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusCreated)
	xml.NewEncoder(w).Encode(response)
}

// GetMessages retrieves messages from the queue
func (h *Handler) GetMessages(w http.ResponseWriter, r *http.Request, queue string) {
	query := r.URL.Query()
	numMessages := 1
	if n := query.Get("numofmessages"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed > 0 {
			numMessages = parsed
		}
	}

	visibilityTimeout := 30 * time.Second
	if vt := query.Get("visibilitytimeout"); vt != "" {
		if parsed, err := strconv.Atoi(vt); err == nil && parsed > 0 {
			visibilityTimeout = time.Duration(parsed) * time.Second
		}
	}

	h.log.Debug("getting messages", "queue", queue, "numMessages", numMessages, "visibilityTimeout", visibilityTimeout)

	// Check if queue exists
	queueKey := h.queueKey(queue)
	_, closer, err := h.db.Get(queueKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "QueueNotFound", "The specified queue does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

	// Get visible messages
	prefix := []byte(fmt.Sprintf("queues/data/%s/", queue))
	iter, err := h.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer iter.Close()

	now := time.Now().UTC()
	var messages []Message

	for iter.First(); iter.Valid() && len(messages) < numMessages; iter.Next() {
		var msg Message
		if err := xml.Unmarshal(iter.Value(), &msg); err != nil {
			continue
		}

		// Check if message is visible
		if msg.TimeNextVisible.After(now) {
			continue
		}

		// Check if message has expired
		if msg.ExpirationTime.Before(now) {
			h.db.Delete(iter.Key(), pebble.Sync)
			continue
		}

		// Update message visibility
		msg.DequeueCount++
		msg.PopReceipt = h.generatePopReceipt()
		msg.TimeNextVisible = now.Add(visibilityTimeout)

		// Save updated message
		data, _ := xml.Marshal(msg)
		h.db.Set(iter.Key(), data, pebble.Sync)

		messages = append(messages, msg)
	}

	h.log.Debug("messages retrieved", "queue", queue, "count", len(messages))

	type QueueMessagesList struct {
		XMLName      xml.Name  `xml:"QueueMessagesList"`
		QueueMessage []Message `xml:"QueueMessage"`
	}
	response := QueueMessagesList{QueueMessage: messages}

	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

// DeleteMessage deletes a specific message
func (h *Handler) DeleteMessage(w http.ResponseWriter, r *http.Request, queue, messageID string) {
	popReceipt := r.URL.Query().Get("popreceipt")
	h.log.Info("deleting message", "queue", queue, "messageId", messageID, "popReceipt", popReceipt)

	key := h.messageKey(queue, messageID)

	// Check if message exists
	data, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.log.Debug("message not found", "queue", queue, "messageId", messageID)
		h.writeError(w, http.StatusNotFound, "MessageNotFound", "The specified message does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer closer.Close()

	// Verify pop receipt
	var msg Message
	if err := xml.Unmarshal(data, &msg); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	if popReceipt != "" && msg.PopReceipt != popReceipt {
		h.writeError(w, http.StatusBadRequest, "PopReceiptMismatch", "The specified pop receipt did not match")
		return
	}

	// Delete message
	if err := h.db.Delete(key, pebble.Sync); err != nil {
		h.log.Error("failed to delete message", "queue", queue, "messageId", messageID, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("message deleted", "queue", queue, "messageId", messageID)
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusNoContent)
}

// ClearMessages clears all messages from the queue
func (h *Handler) ClearMessages(w http.ResponseWriter, r *http.Request, queue string) {
	h.log.Info("clearing messages", "queue", queue)

	// Check if queue exists
	queueKey := h.queueKey(queue)
	_, closer, err := h.db.Get(queueKey)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "QueueNotFound", "The specified queue does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	closer.Close()

	// Delete all messages
	prefix := []byte(fmt.Sprintf("queues/data/%s/", queue))
	iter, err := h.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer iter.Close()

	batch := h.db.NewBatch()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		batch.Delete(iter.Key(), nil)
		count++
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		h.log.Error("failed to clear messages", "queue", queue, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("messages cleared", "queue", queue, "count", count)
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusNoContent)
}
