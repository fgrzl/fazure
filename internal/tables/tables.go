package tables

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"regexp"
	"strings"

	"github.com/fgrzl/fazure/internal/common"
)

// Handler handles table storage operations
type Handler struct {
	store *TableStore
	log   *slog.Logger
}

// NewHandler creates a new table handler
func NewHandler(store *TableStore, logger *slog.Logger) *Handler {
	return &Handler{
		store: store,
		log:   logger.With("component", "tables"),
	}
}

// RegisterRoutes registers table storage routes
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	h.log.Info("table routes registered")
}

// HandleRequest routes table requests - exported for use by main dispatcher
func (h *Handler) HandleRequest(w http.ResponseWriter, r *http.Request) {
	h.handleRequest(w, r)
}

// handleRequest routes table requests
func (h *Handler) handleRequest(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		h.writeError(w, http.StatusBadRequest, "InvalidUri", "Invalid path")
		return
	}

	// Tables endpoint: /{account}/Tables
	if parts[1] == "Tables" {
		switch r.Method {
		case http.MethodGet:
			h.ListTables(w, r)
		case http.MethodPost:
			h.CreateTable(w, r)
		default:
			h.writeError(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "Method not allowed")
		}
		return
	}

	// Delete table: /{account}/Tables('{table}')
	if strings.HasPrefix(parts[1], "Tables(") {
		if r.Method == http.MethodDelete {
			h.DeleteTable(w, r)
		} else {
			h.writeError(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "Method not allowed")
		}
		return
	}

	// Batch operations: /{account}/$batch
	if parts[1] == "$batch" && r.Method == http.MethodPost {
		h.HandleBatchOperation(w, r)
		return
	}

	table := parts[1]

	// Entity with keys: /{account}/{table}(PartitionKey='pk',RowKey='rk')
	entityPattern := regexp.MustCompile(`^([^(]+)\(PartitionKey='([^']+)',RowKey='([^']+)'\)$`)
	if matches := entityPattern.FindStringSubmatch(table); len(matches) == 4 {
		tableName := matches[1]
		pk := matches[2]
		rk := matches[3]

		switch r.Method {
		case http.MethodGet:
			h.GetEntity(w, r, tableName, pk, rk)
		case http.MethodPut:
			h.UpdateEntity(w, r, tableName, pk, rk)
		case "PATCH", "MERGE":
			h.MergeEntity(w, r, tableName, pk, rk)
		case http.MethodDelete:
			h.DeleteEntity(w, r, tableName, pk, rk)
		default:
			h.writeError(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "Method not allowed")
		}
		return
	}

	// Query entities: /{account}/{table}() or GET /{account}/{table}
	tableName := strings.TrimSuffix(table, "()")
	if r.Method == http.MethodGet {
		h.QueryEntities(w, r, tableName)
		return
	}

	// Insert entity: POST /{account}/{table}
	if r.Method == http.MethodPost {
		h.InsertEntity(w, r, tableName)
		return
	}

	h.writeError(w, http.StatusBadRequest, "InvalidUri", "Invalid table operation")
}

// writeError writes an Azure Storage error response
func (h *Handler) writeError(w http.ResponseWriter, statusCode int, errorCode, message string) {
	common.WriteErrorResponse(w, statusCode, errorCode, message)
}

// ListTables lists all tables
func (h *Handler) ListTables(w http.ResponseWriter, r *http.Request) {
	h.log.Info("listing tables")

	tables, err := h.store.ListTables(context.Background())
	if err != nil {
		h.log.Error("failed to list tables", "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	h.log.Info("tables listed", "count", len(tables))
	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"value": tables,
	}
	json.NewEncoder(w).Encode(response)
}

// CreateTable creates a new table
func (h *Handler) CreateTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TableName string `json:"TableName"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Invalid request body")
		return
	}

	if req.TableName == "" {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "TableName required")
		return
	}

	h.log.Info("creating table", "table", req.TableName)

	err := h.store.CreateTable(context.Background(), req.TableName)
	if err != nil {
		if err == ErrTableExists {
			h.log.Debug("table already exists", "table", req.TableName)
			h.writeError(w, http.StatusConflict, "TableAlreadyExists", "Table already exists")
			return
		}
		h.log.Error("failed to create table", "table", req.TableName, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	h.log.Info("table created", "table", req.TableName)
	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"TableName": req.TableName,
	})
}

// DeleteTable deletes a table
func (h *Handler) DeleteTable(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		h.writeError(w, http.StatusBadRequest, "InvalidUri", "Invalid path")
		return
	}

	re := regexp.MustCompile(`Tables\('([^']+)'\)`)
	matches := re.FindStringSubmatch(parts[1])
	if len(matches) < 2 {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Invalid table name")
		return
	}
	tableName := matches[1]

	h.log.Info("deleting table", "table", tableName)

	err := h.store.DeleteTable(context.Background(), tableName)
	if err != nil {
		if err == ErrTableNotFound {
			h.log.Debug("table not found", "table", tableName)
			h.writeError(w, http.StatusNotFound, "TableNotFound", "The specified table does not exist")
			return
		}
		h.log.Error("failed to delete table", "table", tableName, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	h.log.Info("table deleted", "table", tableName)
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusNoContent)
}

// GetEntity retrieves a single entity
func (h *Handler) GetEntity(w http.ResponseWriter, r *http.Request, tableName, pk, rk string) {
	h.log.Debug("getting entity", "table", tableName, "partitionKey", pk, "rowKey", rk)

	table, err := h.store.GetTable(context.Background(), tableName)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "TableNotFound", "Table not found")
		return
	}

	entity, err := table.GetEntity(context.Background(), pk, rk)
	if err != nil {
		if err == ErrEntityNotFound {
			h.writeError(w, http.StatusNotFound, "ResourceNotFound", "Entity not found")
			return
		}
		h.log.Error("failed to get entity", "table", tableName, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	common.SetResponseHeaders(w, entity.ETag)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entity)
}

// InsertEntity inserts a new entity
func (h *Handler) InsertEntity(w http.ResponseWriter, r *http.Request, tableName string) {
	table, err := h.store.GetTable(context.Background(), tableName)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "TableNotFound", "Table not found")
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Invalid request body")
		return
	}

	pk, okPK := data["PartitionKey"].(string)
	rk, okRK := data["RowKey"].(string)

	if !okPK || !okRK {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "PartitionKey and RowKey required")
		return
	}

	h.log.Info("inserting entity", "table", tableName, "partitionKey", pk, "rowKey", rk)

	entity, err := table.InsertEntity(context.Background(), pk, rk, data)
	if err != nil {
		if err == ErrEntityExists {
			h.log.Debug("entity already exists", "table", tableName, "partitionKey", pk, "rowKey", rk)
			h.writeError(w, http.StatusConflict, "EntityAlreadyExists", "Entity already exists")
			return
		}
		h.log.Error("failed to insert entity", "table", tableName, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	h.log.Info("entity inserted", "table", tableName, "partitionKey", pk, "rowKey", rk)
	common.SetResponseHeaders(w, entity.ETag)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(entity)
}

// UpdateEntity replaces an entity (PUT) - also supports upsert
func (h *Handler) UpdateEntity(w http.ResponseWriter, r *http.Request, tableName, pk, rk string) {
	h.log.Info("updating entity", "table", tableName, "partitionKey", pk, "rowKey", rk)

	table, err := h.store.GetTable(context.Background(), tableName)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "TableNotFound", "Table not found")
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Invalid request body")
		return
	}

	// Get If-Match header for ETag validation
	ifMatch := r.Header.Get("If-Match")

	var entity *Entity

	// Check if entity exists
	_, getErr := table.GetEntity(context.Background(), pk, rk)
	if getErr == ErrEntityNotFound && (ifMatch == "" || ifMatch == "*") {
		// Entity doesn't exist and no specific ETag required - upsert (insert)
		entity, err = table.InsertEntity(context.Background(), pk, rk, data)
		if err != nil {
			h.log.Error("failed to insert entity for upsert", "table", tableName, "error", err)
			h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}
		h.log.Info("entity upserted (inserted)", "table", tableName, "partitionKey", pk, "rowKey", rk)
	} else if getErr == ErrEntityNotFound {
		// Entity doesn't exist but specific ETag required - fail
		h.writeError(w, http.StatusNotFound, "ResourceNotFound", "Entity not found")
		return
	} else if getErr != nil {
		h.log.Error("failed to get entity", "table", tableName, "error", getErr)
		h.writeError(w, http.StatusInternalServerError, "InternalServerError", getErr.Error())
		return
	} else {
		// Entity exists - update it
		entity, err = table.UpdateEntityWithETag(context.Background(), pk, rk, data, false, ifMatch)
		if err != nil {
			if err == ErrEntityNotFound {
				h.writeError(w, http.StatusNotFound, "ResourceNotFound", "Entity not found")
				return
			}
			if err == ErrPreconditionFailed {
				h.writeError(w, http.StatusPreconditionFailed, "UpdateConditionNotSatisfied", "The update condition specified in the request was not satisfied")
				return
			}
			h.log.Error("failed to update entity", "table", tableName, "error", err)
			h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}
		h.log.Info("entity upserted (updated)", "table", tableName, "partitionKey", pk, "rowKey", rk)
	}

	common.SetResponseHeaders(w, entity.ETag)
	w.WriteHeader(http.StatusNoContent)
}

// MergeEntity merges an entity (MERGE/PATCH) - also supports upsert
func (h *Handler) MergeEntity(w http.ResponseWriter, r *http.Request, tableName, pk, rk string) {
	h.log.Info("merging entity", "table", tableName, "partitionKey", pk, "rowKey", rk)

	table, err := h.store.GetTable(context.Background(), tableName)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "TableNotFound", "Table not found")
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Invalid request body")
		return
	}

	// Get If-Match header for ETag validation
	ifMatch := r.Header.Get("If-Match")

	var entity *Entity
	if ifMatch == "" || ifMatch == "*" {
		// No ETag or wildcard - do upsert (insert or merge)
		entity, err = table.UpsertEntity(context.Background(), pk, rk, data, true)
	} else {
		// ETag specified - must match existing entity
		entity, err = table.UpdateEntityWithETag(context.Background(), pk, rk, data, true, ifMatch)
	}

	if err != nil {
		if err == ErrEntityNotFound {
			h.writeError(w, http.StatusNotFound, "ResourceNotFound", "Entity not found")
			return
		}
		if err == ErrPreconditionFailed {
			h.writeError(w, http.StatusPreconditionFailed, "UpdateConditionNotSatisfied", "The update condition specified in the request was not satisfied")
			return
		}
		h.log.Error("failed to merge entity", "table", tableName, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	h.log.Info("entity merged", "table", tableName, "partitionKey", pk, "rowKey", rk)
	common.SetResponseHeaders(w, entity.ETag)
	w.WriteHeader(http.StatusNoContent)
}

// DeleteEntity deletes an entity
func (h *Handler) DeleteEntity(w http.ResponseWriter, r *http.Request, tableName, pk, rk string) {
	h.log.Info("deleting entity", "table", tableName, "partitionKey", pk, "rowKey", rk)

	table, err := h.store.GetTable(context.Background(), tableName)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "TableNotFound", "Table not found")
		return
	}

	err = table.DeleteEntity(context.Background(), pk, rk)
	if err != nil {
		if err == ErrEntityNotFound {
			h.writeError(w, http.StatusNotFound, "ResourceNotFound", "Entity not found")
			return
		}
		h.log.Error("failed to delete entity", "table", tableName, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	h.log.Info("entity deleted", "table", tableName, "partitionKey", pk, "rowKey", rk)
	common.SetResponseHeaders(w, "")
	w.WriteHeader(http.StatusNoContent)
}

// QueryEntities queries entities with filter and select
func (h *Handler) QueryEntities(w http.ResponseWriter, r *http.Request, tableName string) {
	filter := r.URL.Query().Get("$filter")
	selectStr := r.URL.Query().Get("$select")
	top := r.URL.Query().Get("$top")
	nextPK := r.URL.Query().Get("NextPartitionKey")
	nextRK := r.URL.Query().Get("NextRowKey")

	h.log.Debug("querying entities", "table", tableName, "filter", filter, "select", selectStr, "top", top, "nextPK", nextPK, "nextRK", nextRK)

	table, err := h.store.GetTable(context.Background(), tableName)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "TableNotFound", "Table not found")
		return
	}

	var selectFields []string
	if selectStr != "" {
		selectFields = parseSelect(selectStr)
	}

	var limit int
	if top != "" {
		fmt.Sscanf(top, "%d", &limit)
	}

	entities, contPK, contRK, err := table.QueryEntities(context.Background(), filter, limit, selectFields, nextPK, nextRK)
	if err != nil {
		h.log.Error("failed to query entities", "table", tableName, "error", err)
		h.writeError(w, http.StatusBadRequest, "InvalidInput", err.Error())
		return
	}

	h.log.Debug("entities queried", "table", tableName, "count", len(entities), "hasMore", contPK != "")
	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/json")

	// Set continuation tokens if there are more results
	if contPK != "" {
		w.Header().Set("x-ms-continuation-NextPartitionKey", contPK)
		w.Header().Set("x-ms-continuation-NextRowKey", contRK)
	}

	response := map[string]interface{}{
		"value": entities,
	}
	json.NewEncoder(w).Encode(response)
}

func parseSelect(selectStr string) []string {
	fields := strings.Split(selectStr, ",")
	for i, f := range fields {
		fields[i] = strings.TrimSpace(f)
	}
	return fields
}

// HandleBatchOperation handles batch operations
func (h *Handler) HandleBatchOperation(w http.ResponseWriter, r *http.Request) {
	h.log.Info("handling batch operation")

	batchReq, err := ParseBatchRequest(r)
	if err != nil {
		h.log.Error("failed to parse batch request", "error", err)
		h.writeError(w, http.StatusBadRequest, "InvalidInput", err.Error())
		return
	}

	h.log.Debug("batch request parsed", "operations", len(batchReq.Operations))

	// Validate that all operations target the same partition
	if err := ValidateBatchRequest(batchReq); err != nil {
		h.log.Error("batch validation failed", "error", err)
		h.writeBatchError(w, http.StatusBadRequest, "CommandsInBatchActOnDifferentPartitions", err.Error())
		return
	}

	// Execute the batch operations atomically
	responses, failedIndex, err := h.executeBatch(batchReq)
	if err != nil {
		h.log.Error("batch execution failed", "error", err, "failedIndex", failedIndex)
		h.writeBatchErrorWithIndex(w, failedIndex, err)
		return
	}

	// Generate boundaries matching Azure format exactly
	batchBoundary := fmt.Sprintf("batchresponse_%s", generateUUID())
	changesetBoundary := fmt.Sprintf("changesetresponse_%s", generateUUID())

	// Build the changeset using multipart.Writer
	changesetBuf := new(bytes.Buffer)
	changesetWriter := multipart.NewWriter(changesetBuf)
	changesetWriter.SetBoundary(changesetBoundary)

	for i, resp := range responses {
		h.log.Debug("writing batch response", "index", i, "status", resp.StatusCode)

		// Create headers for this part
		partHeaders := make(textproto.MIMEHeader)
		partHeaders.Set("Content-Type", "application/http")
		partHeaders.Set("Content-Transfer-Encoding", "binary")

		partWriter, err := changesetWriter.CreatePart(partHeaders)
		if err != nil {
			h.log.Error("failed to create batch part", "error", err)
			continue
		}

		// Write the HTTP response for this operation
		var httpResp strings.Builder
		httpResp.WriteString(fmt.Sprintf("HTTP/1.1 %d %s\r\n", resp.StatusCode, http.StatusText(resp.StatusCode)))
		httpResp.WriteString(fmt.Sprintf("Content-ID: %d\r\n", i+1))
		httpResp.WriteString("X-Content-Type-Options: nosniff\r\n")
		httpResp.WriteString("Cache-Control: no-cache\r\n")
		httpResp.WriteString("DataServiceVersion: 3.0;\r\n")
		httpResp.WriteString("\r\n")

		partWriter.Write([]byte(httpResp.String()))
	}
	changesetWriter.Close()

	// Build the outer batch response using multipart.Writer
	batchBuf := new(bytes.Buffer)
	batchWriter := multipart.NewWriter(batchBuf)
	batchWriter.SetBoundary(batchBoundary)

	// Create the changeset part
	batchPartHeaders := make(textproto.MIMEHeader)
	batchPartHeaders.Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", changesetBoundary))

	batchPart, err := batchWriter.CreatePart(batchPartHeaders)
	if err != nil {
		h.log.Error("failed to create batch part", "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	batchPart.Write(changesetBuf.Bytes())
	batchWriter.Close()

	w.Header().Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", batchBoundary))
	w.WriteHeader(http.StatusAccepted)
	w.Write(batchBuf.Bytes())
}

// generateUUID generates a proper UUID v4 string for batch boundaries
func generateUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	// Set version (4) and variant bits
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// executeBatch executes batch operations atomically
// Returns the responses, the index of the failed operation (if any), and an error
func (h *Handler) executeBatch(batchReq *BatchRequest) ([]BatchOperationResponse, int, error) {
	ctx := context.Background()
	responses := make([]BatchOperationResponse, len(batchReq.Operations))

	// Get the table
	table, err := h.store.GetTable(ctx, batchReq.TableName)
	if err != nil {
		return nil, 0, err
	}

	// First pass: validate all operations will succeed
	for i, op := range batchReq.Operations {
		pk := op.PartitionKey
		rk := op.RowKey

		h.log.Debug("validating batch operation", "index", i, "method", op.Method, "pk", pk, "rk", rk)

		switch op.Method {
		case http.MethodPost:
			// Insert - check entity doesn't exist
			_, err := table.GetEntity(ctx, pk, rk)
			if err == nil {
				// Entity already exists - conflict
				return nil, i, ErrEntityExists
			}
		case http.MethodPut:
			// Update/Replace - in Azure, PUT requires entity to exist
			// However, for compatibility with many clients that expect upsert,
			// we treat PUT as upsert (insert or replace)
			h.log.Debug("treating PUT as upsert for compatibility", "index", i, "method", op.Method)
		case "PATCH", "MERGE":
			// Merge/Upsert - entity may or may not exist, no validation needed
			// Azure Tables upsert creates if not exists
			h.log.Debug("skipping validation for upsert operation", "index", i, "method", op.Method)
		case http.MethodDelete:
			// Delete - check entity exists
			_, err := table.GetEntity(ctx, pk, rk)
			if err == ErrEntityNotFound {
				return nil, i, ErrEntityNotFound
			}
		default:
			h.log.Debug("unrecognized batch method", "index", i, "method", op.Method)
		}
	}

	// Second pass: execute all operations
	for i, op := range batchReq.Operations {
		pk := op.PartitionKey
		rk := op.RowKey
		props := op.EntityData

		h.log.Debug("executing batch operation", "index", i, "method", op.Method, "pk", pk, "rk", rk)

		switch op.Method {
		case http.MethodPost:
			// Insert
			_, err := table.InsertEntity(ctx, pk, rk, props)
			if err != nil {
				return nil, i, err
			}
			responses[i] = BatchOperationResponse{StatusCode: http.StatusNoContent}

		case http.MethodPut:
			// Replace/Upsert - for compatibility, insert if not exists, replace if exists
			_, err := table.GetEntity(ctx, pk, rk)
			if err == ErrEntityNotFound {
				// Entity doesn't exist - insert it
				_, err = table.InsertEntity(ctx, pk, rk, props)
				if err != nil {
					return nil, i, err
				}
			} else if err != nil {
				return nil, i, err
			} else {
				// Entity exists - replace it
				_, err = table.UpdateEntity(ctx, pk, rk, props, false)
				if err != nil {
					return nil, i, err
				}
			}
			responses[i] = BatchOperationResponse{StatusCode: http.StatusNoContent}

		case "PATCH", "MERGE":
			// Merge/Upsert - insert if not exists, merge if exists
			_, err := table.GetEntity(ctx, pk, rk)
			if err == ErrEntityNotFound {
				// Entity doesn't exist - insert it
				_, err = table.InsertEntity(ctx, pk, rk, props)
				if err != nil {
					return nil, i, err
				}
			} else if err != nil {
				return nil, i, err
			} else {
				// Entity exists - merge it
				_, err = table.UpdateEntity(ctx, pk, rk, props, true)
				if err != nil {
					return nil, i, err
				}
			}
			responses[i] = BatchOperationResponse{StatusCode: http.StatusNoContent}

		case http.MethodDelete:
			// Delete
			err := table.DeleteEntity(ctx, pk, rk)
			if err != nil {
				return nil, i, err
			}
			responses[i] = BatchOperationResponse{StatusCode: http.StatusNoContent}

		default:
			responses[i] = BatchOperationResponse{StatusCode: http.StatusNoContent}
		}
	}

	return responses, -1, nil
}

// writeBatchError writes a batch error response
func (h *Handler) writeBatchError(w http.ResponseWriter, statusCode int, code, message string) {
	batchBoundary := fmt.Sprintf("batchresponse_%s", generateUUID())
	changesetBoundary := fmt.Sprintf("changesetresponse_%s", generateUUID())

	// Build error response in changeset
	changesetBuf := new(bytes.Buffer)
	changesetWriter := multipart.NewWriter(changesetBuf)
	changesetWriter.SetBoundary(changesetBoundary)

	partHeaders := make(textproto.MIMEHeader)
	partHeaders.Set("Content-Type", "application/http")
	partHeaders.Set("Content-Transfer-Encoding", "binary")

	partWriter, _ := changesetWriter.CreatePart(partHeaders)

	errorBody := fmt.Sprintf(`{"odata.error":{"code":"%s","message":{"lang":"en-US","value":"%s"}}}`, code, message)

	var httpResp strings.Builder
	httpResp.WriteString(fmt.Sprintf("HTTP/1.1 %d %s\r\n", statusCode, http.StatusText(statusCode)))
	httpResp.WriteString("Content-Type: application/json;odata=minimalmetadata\r\n")
	httpResp.WriteString(fmt.Sprintf("Content-Length: %d\r\n", len(errorBody)))
	httpResp.WriteString("\r\n")
	httpResp.WriteString(errorBody)

	partWriter.Write([]byte(httpResp.String()))
	changesetWriter.Close()

	// Build outer batch response
	batchBuf := new(bytes.Buffer)
	batchWriter := multipart.NewWriter(batchBuf)
	batchWriter.SetBoundary(batchBoundary)

	batchPartHeaders := make(textproto.MIMEHeader)
	batchPartHeaders.Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", changesetBoundary))

	batchPart, _ := batchWriter.CreatePart(batchPartHeaders)
	batchPart.Write(changesetBuf.Bytes())
	batchWriter.Close()

	w.Header().Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", batchBoundary))
	w.WriteHeader(http.StatusAccepted)
	w.Write(batchBuf.Bytes())
}

// writeBatchErrorWithIndex writes a batch error response for a specific operation
func (h *Handler) writeBatchErrorWithIndex(w http.ResponseWriter, index int, err error) {
	code := "InvalidInput"
	statusCode := http.StatusBadRequest

	switch err {
	case ErrEntityExists:
		code = "EntityAlreadyExists"
		statusCode = http.StatusConflict
	case ErrEntityNotFound:
		code = "ResourceNotFound"
		statusCode = http.StatusNotFound
	}

	h.writeBatchError(w, statusCode, code, fmt.Sprintf("Operation %d failed: %s", index, err.Error()))
}
