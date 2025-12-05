package queues

import (
	"encoding/json"
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
	"github.com/google/uuid"
)

// Handler handles queue storage operations
type Handler struct {
	db  *pebble.DB
	log *slog.Logger
}

// RFC1123Time wraps time.Time to marshal/unmarshal using RFC1123 format (Azure requirement)
type RFC1123Time time.Time

func (t RFC1123Time) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return e.EncodeElement(time.Time(t).UTC().Format(time.RFC1123), start)
}

func (t *RFC1123Time) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string
	if err := d.DecodeElement(&s, &start); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC1123, s)
	if err != nil {
		return err
	}
	*t = RFC1123Time(parsed)
	return nil
}

// Message represents a queue message
type Message struct {
	MessageID       string      `xml:"MessageId"`
	InsertionTime   RFC1123Time `xml:"InsertionTime"`
	ExpirationTime  RFC1123Time `xml:"ExpirationTime"`
	PopReceipt      string      `xml:"PopReceipt"`
	TimeNextVisible RFC1123Time `xml:"TimeNextVisible"`
	DequeueCount    int         `xml:"DequeueCount"`
	MessageText     string      `xml:"MessageText"`
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
			if query.Get("comp") == "metadata" {
				h.SetQueueMetadata(w, r, queue)
			} else {
				h.CreateQueue(w, r, queue)
			}
		case http.MethodDelete:
			h.DeleteQueue(w, r, queue)
		case http.MethodGet, http.MethodHead:
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
				if query.Get("peekonly") == "true" {
					h.PeekMessages(w, r, queue)
				} else {
					h.GetMessages(w, r, queue)
				}
			case http.MethodDelete:
				h.ClearMessages(w, r, queue)
			default:
				h.writeError(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "Method not allowed")
			}
		} else if len(parts) == 4 {
			messageID := parts[3]
			switch r.Method {
			case http.MethodDelete:
				h.DeleteMessage(w, r, queue, messageID)
			case http.MethodPut:
				h.UpdateMessage(w, r, queue, messageID)
			default:
				h.writeError(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "Method not allowed")
			}
		}
		return
	}

	h.writeError(w, http.StatusBadRequest, "InvalidUri", "Invalid queue operation")
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
	return uuid.NewString()
}

// generatePopReceipt generates a unique pop receipt
func (h *Handler) generatePopReceipt() string {
	return uuid.NewString()
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
	data, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "QueueNotFound", "The specified queue does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Make copy before closing
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	// Parse metadata to extract user metadata
	var metadata map[string]interface{}
	if err := json.Unmarshal(dataCopy, &metadata); err == nil {
		if userMeta, ok := metadata["userMetadata"].(map[string]interface{}); ok {
			for k, v := range userMeta {
				if vs, ok := v.(string); ok {
					w.Header().Set("x-ms-meta-"+k, vs)
				}
			}
		}
	}

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

	prefixFilter := r.URL.Query().Get("prefix")

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
		// Apply prefix filter if specified
		if prefixFilter != "" && !strings.HasPrefix(name, prefixFilter) {
			continue
		}
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

	// Parse TTL from query parameter (messagettl in seconds)
	ttl := 7 * 24 * time.Hour // Default 7 days
	if ttlStr := r.URL.Query().Get("messagettl"); ttlStr != "" {
		if parsed, err := strconv.Atoi(ttlStr); err == nil && parsed > 0 {
			ttl = time.Duration(parsed) * time.Second
		}
	}

	// Parse visibility timeout from query parameter
	visibilityTimeout := time.Duration(0)
	if vt := r.URL.Query().Get("visibilitytimeout"); vt != "" {
		if parsed, err := strconv.Atoi(vt); err == nil && parsed > 0 {
			visibilityTimeout = time.Duration(parsed) * time.Second
		}
	}

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
		InsertionTime:   RFC1123Time(now),
		ExpirationTime:  RFC1123Time(now.Add(ttl)),
		PopReceipt:      h.generatePopReceipt(),
		TimeNextVisible: RFC1123Time(now.Add(visibilityTimeout)),
		DequeueCount:    0,
		MessageText:     msg.MessageText, // SDK already base64 encodes, don't double encode
	}

	// Store message
	key := h.messageKey(queue, messageID)
	data, _ := xml.Marshal(message)
	if err := h.db.Set(key, data, pebble.Sync); err != nil {
		h.log.Error("failed to store message", "queue", queue, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("message added", "queue", queue, "messageId", messageID, "ttl", ttl)

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
		if time.Time(msg.TimeNextVisible).After(now) {
			continue
		}

		// Check if message has expired
		if time.Time(msg.ExpirationTime).Before(now) {
			h.db.Delete(iter.Key(), pebble.Sync)
			continue
		}

		// Update message visibility
		msg.DequeueCount++
		msg.PopReceipt = h.generatePopReceipt()
		msg.TimeNextVisible = RFC1123Time(now.Add(visibilityTimeout))

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

// PeekMessages peeks at messages without dequeuing them
func (h *Handler) PeekMessages(w http.ResponseWriter, r *http.Request, queue string) {
	query := r.URL.Query()
	numMessages := 1
	if n := query.Get("numofmessages"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed > 0 {
			numMessages = parsed
		}
	}

	h.log.Debug("peeking messages", "queue", queue, "numMessages", numMessages)

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

	// Get visible messages without modifying them
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

		// Check if message is visible (for peek, we still check visibility)
		if time.Time(msg.TimeNextVisible).After(now) {
			continue
		}

		// Check if message has expired
		if time.Time(msg.ExpirationTime).Before(now) {
			h.db.Delete(iter.Key(), pebble.Sync)
			continue
		}

		// Do NOT update visibility or dequeue count for peek
		messages = append(messages, msg)
	}

	h.log.Debug("messages peeked", "queue", queue, "count", len(messages))

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

// UpdateMessage updates a message's visibility timeout and/or content
func (h *Handler) UpdateMessage(w http.ResponseWriter, r *http.Request, queue, messageID string) {
	popReceipt := r.URL.Query().Get("popreceipt")
	visibilityTimeout := 30 * time.Second
	if vt := r.URL.Query().Get("visibilitytimeout"); vt != "" {
		if parsed, err := strconv.Atoi(vt); err == nil && parsed >= 0 {
			visibilityTimeout = time.Duration(parsed) * time.Second
		}
	}

	h.log.Info("updating message", "queue", queue, "messageId", messageID, "visibilityTimeout", visibilityTimeout)

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

	// Make copy of data before closing
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	// Parse existing message
	var msg Message
	if err := xml.Unmarshal(dataCopy, &msg); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Verify pop receipt
	if popReceipt != "" && msg.PopReceipt != popReceipt {
		h.writeError(w, http.StatusBadRequest, "PopReceiptMismatch", "The specified pop receipt did not match")
		return
	}

	// Parse request body for new message text
	body, err := io.ReadAll(r.Body)
	if err == nil && len(body) > 0 {
		type QueueMessage struct {
			MessageText string `xml:"MessageText"`
		}
		var newMsg QueueMessage
		if err := xml.Unmarshal(body, &newMsg); err == nil && newMsg.MessageText != "" {
			msg.MessageText = newMsg.MessageText
		}
	}

	// Update visibility and pop receipt
	now := time.Now().UTC()
	msg.TimeNextVisible = RFC1123Time(now.Add(visibilityTimeout))
	msg.PopReceipt = h.generatePopReceipt()

	// Save updated message
	updatedData, _ := xml.Marshal(msg)
	if err := h.db.Set(key, updatedData, pebble.Sync); err != nil {
		h.log.Error("failed to update message", "queue", queue, "messageId", messageID, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("message updated", "queue", queue, "messageId", messageID)
	common.SetResponseHeaders(w, "")
	w.Header().Set("x-ms-popreceipt", msg.PopReceipt)
	w.Header().Set("x-ms-time-next-visible", time.Time(msg.TimeNextVisible).UTC().Format(time.RFC1123))
	w.WriteHeader(http.StatusNoContent)
}

// SetQueueMetadata sets metadata on a queue
func (h *Handler) SetQueueMetadata(w http.ResponseWriter, r *http.Request, queue string) {
	h.log.Info("setting queue metadata", "queue", queue)

	key := h.queueKey(queue)

	// Check if queue exists
	data, closer, err := h.db.Get(key)
	if err == pebble.ErrNotFound {
		h.writeError(w, http.StatusNotFound, "QueueNotFound", "The specified queue does not exist")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Make copy before closing
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	// Parse existing metadata
	var metadata map[string]interface{}
	if err := json.Unmarshal(dataCopy, &metadata); err != nil {
		metadata = make(map[string]interface{})
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
	metadata["userMetadata"] = userMeta

	// Save updated metadata
	updatedData, _ := json.Marshal(metadata)
	if err := h.db.Set(key, updatedData, pebble.Sync); err != nil {
		h.log.Error("failed to set queue metadata", "queue", queue, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	h.log.Info("queue metadata set", "queue", queue)
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusNoContent)
}

// ============================================================================
// Service Properties
// ============================================================================

// ServiceProperties stores queue service properties
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
	return []byte("queues/service/properties")
}

// GetServiceProperties returns queue service properties
func (h *Handler) GetServiceProperties(w http.ResponseWriter, r *http.Request) {
	h.log.Debug("getting queue service properties")

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

// SetServiceProperties sets queue service properties
func (h *Handler) SetServiceProperties(w http.ResponseWriter, r *http.Request) {
	h.log.Debug("setting queue service properties")

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
