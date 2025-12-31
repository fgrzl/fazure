package tables

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/fgrzl/fazure/internal/common"
)

// Precompiled patterns for entity and table URIs.
var (
	entityPathRe  = regexp.MustCompile(`^([^(]+)\(PartitionKey='([^']+)',RowKey='([^']+)'\)$`)
	deleteTableRe = regexp.MustCompile(`Tables\('([^']+)'\)`)
)

// Handler handles table storage operations.
type Handler struct {
	store *TableStore
	log   *slog.Logger
}

// NewHandler creates a new table handler.
func NewHandler(store *TableStore, logger *slog.Logger) *Handler {
	return &Handler{
		store: store,
		log:   logger.With("component", "tables"),
	}
}

// RegisterRoutes registers table storage routes (main dispatcher uses HandleRequest).
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	h.log.Info("table routes registered")
}

// HandleRequest routes table requests - exported for use by main dispatcher.
func (h *Handler) HandleRequest(w http.ResponseWriter, r *http.Request) {
	h.handleRequest(w, r)
}

// handleRequest routes table requests.
func (h *Handler) handleRequest(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		h.writeError(w, http.StatusBadRequest, "InvalidUri", "Invalid path")
		return
	}

	accountSegment := parts[0] // reserved for account, not currently used
	_ = accountSegment

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

	tableSegment := parts[1]

	// Entity with keys: /{account}/{table}(PartitionKey='pk',RowKey='rk')
	if matches := entityPathRe.FindStringSubmatch(tableSegment); len(matches) == 4 {
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
	tableName := strings.TrimSuffix(tableSegment, "()")
	switch r.Method {
	case http.MethodGet:
		h.QueryEntities(w, r, tableName)
		return
	case http.MethodPost:
		// Insert entity: POST /{account}/{table}
		h.InsertEntity(w, r, tableName)
		return
	default:
		h.writeError(w, http.StatusBadRequest, "InvalidUri", "Invalid table operation")
		return
	}
}

// writeError writes an Azure Storage error response.
func (h *Handler) writeError(w http.ResponseWriter, statusCode int, errorCode, message string) {
	h.log.Warn("error response",
		"statusCode", statusCode,
		"errorCode", errorCode,
		"message", message,
	)
	common.WriteErrorResponse(w, statusCode, errorCode, message)
}

// ListTables lists all tables.
func (h *Handler) ListTables(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.log.Debug("listing tables")

	tables, err := h.store.ListTables(ctx)
	if err != nil {
		h.log.Error("failed to list tables", "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	h.log.Debug("tables listed", "count", len(tables))
	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"value": tables,
	}
	_ = json.NewEncoder(w).Encode(response)
}

// CreateTable creates a new table.
func (h *Handler) CreateTable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

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

	if err := h.store.CreateTable(ctx, req.TableName); err != nil {
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
	_ = json.NewEncoder(w).Encode(map[string]string{
		"TableName": req.TableName,
	})
}

// DeleteTable deletes a table.
func (h *Handler) DeleteTable(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		h.writeError(w, http.StatusBadRequest, "InvalidUri", "Invalid path")
		return
	}

	matches := deleteTableRe.FindStringSubmatch(parts[1])
	if len(matches) < 2 {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Invalid table name")
		return
	}
	tableName := matches[1]

	ctx := r.Context()
	h.log.Info("deleting table", "table", tableName)

	if err := h.store.DeleteTable(ctx, tableName); err != nil {
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

// GetEntity retrieves a single entity.
func (h *Handler) GetEntity(w http.ResponseWriter, r *http.Request, tableName, pk, rk string) {
	ctx := r.Context()
	h.log.Debug("getting entity", "table", tableName, "partitionKey", pk, "rowKey", rk)

	table, err := h.store.GetTable(ctx, tableName)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "TableNotFound", "Table not found")
		return
	}

	entity, err := table.GetEntity(ctx, pk, rk)
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
	_ = json.NewEncoder(w).Encode(entity)
}

// InsertEntity inserts a new entity.
func (h *Handler) InsertEntity(w http.ResponseWriter, r *http.Request, tableName string) {
	ctx := r.Context()

	table, err := h.store.GetTable(ctx, tableName)
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

	entity, err := table.InsertEntity(ctx, pk, rk, data)
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
	_ = json.NewEncoder(w).Encode(entity)
}

// UpdateEntity replaces an entity (PUT) - also supports upsert for Azure SDK compatibility.
func (h *Handler) UpdateEntity(w http.ResponseWriter, r *http.Request, tableName, pk, rk string) {
	ctx := r.Context()
	h.log.Info("updating entity", "table", tableName, "partitionKey", pk, "rowKey", rk)

	table, err := h.store.GetTable(ctx, tableName)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "TableNotFound", "Table not found")
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Invalid request body")
		return
	}

	ifMatch := r.Header.Get("If-Match")

	var entity *Entity

	existing, getErr := table.GetEntity(ctx, pk, rk)
	switch {
	case getErr == ErrEntityNotFound && (ifMatch == "" || ifMatch == "*"):
		// Upsert: insert when not found and no specific ETag required.
		entity, err = table.InsertEntity(ctx, pk, rk, data)
		if err != nil {
			h.log.Error("failed to insert entity for upsert", "table", tableName, "error", err)
			h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}
		h.log.Info("entity upserted (inserted)", "table", tableName, "partitionKey", pk, "rowKey", rk)

	case getErr == ErrEntityNotFound:
		// Entity doesn't exist but specific ETag is required.
		h.writeError(w, http.StatusNotFound, "ResourceNotFound", "Entity not found")
		return

	case getErr != nil:
		h.log.Error("failed to get entity", "table", tableName, "error", getErr)
		h.writeError(w, http.StatusInternalServerError, "InternalServerError", getErr.Error())
		return

	default:
		_ = existing // we only needed to know that it exists
		entity, err = table.UpdateEntityWithETag(ctx, pk, rk, data, false, ifMatch)
		if err != nil {
			if err == ErrEntityNotFound {
				h.writeError(w, http.StatusNotFound, "ResourceNotFound", "Entity not found")
				return
			}
			if err == ErrPreconditionFailed {
				h.writeError(w, http.StatusPreconditionFailed, "UpdateConditionNotSatisfied",
					"The update condition specified in the request was not satisfied")
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

// MergeEntity merges an entity (MERGE/PATCH) - also supports upsert.
func (h *Handler) MergeEntity(w http.ResponseWriter, r *http.Request, tableName, pk, rk string) {
	ctx := r.Context()
	h.log.Info("merging entity", "table", tableName, "partitionKey", pk, "rowKey", rk)

	table, err := h.store.GetTable(ctx, tableName)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "TableNotFound", "Table not found")
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		h.writeError(w, http.StatusBadRequest, "InvalidInput", "Invalid request body")
		return
	}

	ifMatch := r.Header.Get("If-Match")

	var entity *Entity
	if ifMatch == "" || ifMatch == "*" {
		// Upsert merge semantics.
		entity, err = table.UpsertEntity(ctx, pk, rk, data, true)
	} else {
		entity, err = table.UpdateEntityWithETag(ctx, pk, rk, data, true, ifMatch)
	}

	if err != nil {
		if err == ErrEntityNotFound {
			h.writeError(w, http.StatusNotFound, "ResourceNotFound", "Entity not found")
			return
		}
		if err == ErrPreconditionFailed {
			h.writeError(w, http.StatusPreconditionFailed, "UpdateConditionNotSatisfied",
				"The update condition specified in the request was not satisfied")
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

// DeleteEntity deletes an entity.
func (h *Handler) DeleteEntity(w http.ResponseWriter, r *http.Request, tableName, pk, rk string) {
	ctx := r.Context()
	h.log.Info("deleting entity", "table", tableName, "partitionKey", pk, "rowKey", rk)

	table, err := h.store.GetTable(ctx, tableName)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "TableNotFound", "Table not found")
		return
	}

	ifMatch := r.Header.Get("If-Match")
	err = table.DeleteEntityWithETag(ctx, pk, rk, ifMatch)
	if err != nil {
		if err == ErrEntityNotFound {
			h.writeError(w, http.StatusNotFound, "ResourceNotFound", "Entity not found")
			return
		}
		if err == ErrPreconditionFailed {
			h.writeError(w, http.StatusPreconditionFailed, "UpdateConditionNotSatisfied",
				"The update condition specified in the request was not satisfied")
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

// QueryEntities queries entities with filter and select.
func (h *Handler) QueryEntities(w http.ResponseWriter, r *http.Request, tableName string) {
	ctx := r.Context()

	filter := r.URL.Query().Get("$filter")
	selectStr := r.URL.Query().Get("$select")
	top := r.URL.Query().Get("$top")
	nextPK := r.URL.Query().Get("NextPartitionKey")
	nextRK := r.URL.Query().Get("NextRowKey")

	h.log.Debug("querying entities",
		"table", tableName,
		"filter", filter,
		"select", selectStr,
		"top", top,
		"nextPK", nextPK,
		"nextRK", nextRK,
	)

	table, err := h.store.GetTable(ctx, tableName)
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
		if n, err := strconv.Atoi(top); err == nil && n > 0 {
			limit = n
		}
	}

	entities, contPK, contRK, err := table.QueryEntities(ctx, filter, limit, selectFields, nextPK, nextRK)
	if err != nil {
		if errors.Is(err, ErrInvalidFilter) {
			h.log.Warn("invalid $filter", "table", tableName, "filter", filter, "error", err)
			h.writeError(w, http.StatusBadRequest, "InvalidInput", "Unsupported or invalid $filter expression")
			return
		}
		h.log.Error("failed to query entities", "table", tableName, "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	h.log.Debug("entities queried",
		"table", tableName,
		"count", len(entities),
		"hasMore", contPK != "",
	)

	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/json")

	if contPK != "" {
		w.Header().Set("x-ms-continuation-NextPartitionKey", contPK)
		w.Header().Set("x-ms-continuation-NextRowKey", contRK)
	}

	var responseEntities []map[string]interface{}
	for _, entity := range entities {
		timestamp := entity.Timestamp.UTC().Format(time.RFC3339Nano)
		entityMap := map[string]interface{}{
			"PartitionKey": entity.PartitionKey,
			"RowKey":       entity.RowKey,
			"Timestamp":    timestamp,
			"ETag":         entity.ETag,
		}
		for k, v := range entity.Properties {
			entityMap[k] = v
		}

		if len(selectFields) > 0 {
			projected := map[string]interface{}{
				"PartitionKey": entity.PartitionKey,
				"RowKey":       entity.RowKey,
				"Timestamp":    timestamp,
				"ETag":         entity.ETag,
			}
			for _, field := range selectFields {
				if val, ok := entityMap[field]; ok {
					projected[field] = val
				}
			}
			responseEntities = append(responseEntities, projected)
		} else {
			responseEntities = append(responseEntities, entityMap)
		}
	}

	response := map[string]interface{}{
		"value": responseEntities,
	}
	_ = json.NewEncoder(w).Encode(response)
}

func parseSelect(selectStr string) []string {
	fields := strings.Split(selectStr, ",")
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

// HandleBatchOperation handles batch operations.
func (h *Handler) HandleBatchOperation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.log.Info("handling batch operation")

	batchReq, err := ParseBatchRequest(r)
	if err != nil {
		h.log.Error("failed to parse batch request", "error", err)
		h.writeError(w, http.StatusBadRequest, "InvalidInput", err.Error())
		return
	}

	h.log.Debug("batch request parsed", "operations", len(batchReq.Operations))

	if err := ValidateBatchRequest(batchReq); err != nil {
		h.log.Error("batch validation failed", "error", err)
		h.writeBatchError(w, http.StatusBadRequest, "CommandsInBatchActOnDifferentPartitions", err.Error())
		return
	}

	responses, failedIndex, err := h.executeBatch(ctx, batchReq)
	if err != nil {
		h.log.Error("batch execution failed", "error", err, "failedIndex", failedIndex)
		h.writeBatchErrorWithIndex(w, failedIndex, err)
		return
	}

	batchBoundary := fmt.Sprintf("batchresponse_%s", generateUUID())
	changesetBoundary := fmt.Sprintf("changesetresponse_%s", generateUUID())

	// Build the changeset using multipart.Writer.
	changesetBuf := new(bytes.Buffer)
	changesetWriter := multipart.NewWriter(changesetBuf)
	if err := changesetWriter.SetBoundary(changesetBoundary); err != nil {
		h.log.Error("failed to set changeset boundary", "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	for i, resp := range responses {
		h.log.Debug("writing batch response", "index", i, "status", resp.StatusCode)

		partHeaders := make(textproto.MIMEHeader)
		partHeaders.Set("Content-Type", "application/http")
		partHeaders.Set("Content-Transfer-Encoding", "binary")

		partWriter, err := changesetWriter.CreatePart(partHeaders)
		if err != nil {
			h.log.Error("failed to create batch part", "error", err)
			continue
		}

		var httpResp strings.Builder
		httpResp.WriteString(fmt.Sprintf("HTTP/1.1 %d %s\r\n", resp.StatusCode, http.StatusText(resp.StatusCode)))
		httpResp.WriteString(fmt.Sprintf("Content-ID: %d\r\n", i+1))
		httpResp.WriteString("X-Content-Type-Options: nosniff\r\n")
		httpResp.WriteString("Cache-Control: no-cache\r\n")
		httpResp.WriteString("DataServiceVersion: 3.0;\r\n")
		httpResp.WriteString("\r\n")

		if _, err := partWriter.Write([]byte(httpResp.String())); err != nil {
			h.log.Error("failed to write batch part", "error", err)
		}
	}
	if err := changesetWriter.Close(); err != nil {
		h.log.Error("failed to close changeset writer", "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Build the outer batch response.
	batchBuf := new(bytes.Buffer)
	batchWriter := multipart.NewWriter(batchBuf)
	if err := batchWriter.SetBoundary(batchBoundary); err != nil {
		h.log.Error("failed to set batch boundary", "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	batchPartHeaders := make(textproto.MIMEHeader)
	batchPartHeaders.Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", changesetBoundary))

	batchPart, err := batchWriter.CreatePart(batchPartHeaders)
	if err != nil {
		h.log.Error("failed to create batch part", "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if _, err := batchPart.Write(changesetBuf.Bytes()); err != nil {
		h.log.Error("failed to write batch changeset", "error", err)
	}
	if err := batchWriter.Close(); err != nil {
		h.log.Error("failed to close batch writer", "error", err)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", batchBoundary))
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write(batchBuf.Bytes())
}

// generateUUID generates a v4 UUID string for batch boundaries.
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// executeBatch executes batch operations atomically at the emulator level.
// (Note: without real transactions, this is best-effort "all-or-first-failure".)
func (h *Handler) executeBatch(ctx context.Context, batchReq *BatchRequest) ([]BatchOperationResponse, int, error) {
	responses := make([]BatchOperationResponse, len(batchReq.Operations))

	table, err := h.store.GetTable(ctx, batchReq.TableName)
	if err != nil {
		return nil, 0, err
	}

	// First pass: validate operations.
	for i, op := range batchReq.Operations {
		pk := op.PartitionKey
		rk := op.RowKey

		h.log.Debug("validating batch operation", "index", i, "method", op.Method, "pk", pk, "rk", rk)

		switch op.Method {
		case http.MethodPost:
			// Insert must fail if entity exists.
			if _, err := table.GetEntity(ctx, pk, rk); err == nil {
				return nil, i, ErrEntityExists
			}
		case http.MethodPut:
			// PUT: we treat as upsert for Azure SDK compatibility; no validation needed here.
			h.log.Debug("treating PUT as upsert for compatibility", "index", i)
		case "PATCH", "MERGE":
			// MERGE/PATCH: upsert semantics, no existence validation required.
		case http.MethodDelete:
			if _, err := table.GetEntity(ctx, pk, rk); err == ErrEntityNotFound {
				return nil, i, ErrEntityNotFound
			}
		default:
			h.log.Debug("unrecognized batch method", "index", i, "method", op.Method)
		}
	}

	// Second pass: execute operations.
	for i, op := range batchReq.Operations {
		select {
		case <-ctx.Done():
			return nil, i, ctx.Err()
		default:
		}

		pk := op.PartitionKey
		rk := op.RowKey
		props := op.EntityData

		h.log.Debug("executing batch operation", "index", i, "method", op.Method, "pk", pk, "rk", rk)

		switch op.Method {
		case http.MethodPost:
			if _, err := table.InsertEntity(ctx, pk, rk, props); err != nil {
				return nil, i, err
			}
			responses[i] = BatchOperationResponse{StatusCode: http.StatusNoContent}

		case http.MethodPut:
			if _, err := table.GetEntity(ctx, pk, rk); err == ErrEntityNotFound {
				if _, err := table.InsertEntity(ctx, pk, rk, props); err != nil {
					return nil, i, err
				}
			} else if err != nil {
				return nil, i, err
			} else {
				if _, err := table.UpdateEntity(ctx, pk, rk, props, false); err != nil {
					return nil, i, err
				}
			}
			responses[i] = BatchOperationResponse{StatusCode: http.StatusNoContent}

		case "PATCH", "MERGE":
			if _, err := table.GetEntity(ctx, pk, rk); err == ErrEntityNotFound {
				if _, err := table.InsertEntity(ctx, pk, rk, props); err != nil {
					return nil, i, err
				}
			} else if err != nil {
				return nil, i, err
			} else {
				if _, err := table.UpdateEntity(ctx, pk, rk, props, true); err != nil {
					return nil, i, err
				}
			}
			responses[i] = BatchOperationResponse{StatusCode: http.StatusNoContent}

		case http.MethodDelete:
			if err := table.DeleteEntity(ctx, pk, rk); err != nil {
				return nil, i, err
			}
			responses[i] = BatchOperationResponse{StatusCode: http.StatusNoContent}

		default:
			// Unknown verbs still get a 204 inside the batch, to keep batch structurally valid.
			responses[i] = BatchOperationResponse{StatusCode: http.StatusNoContent}
		}
	}

	return responses, -1, nil
}

// writeBatchError writes a batch error response.
func (h *Handler) writeBatchError(w http.ResponseWriter, statusCode int, code, message string) {
	batchBoundary := fmt.Sprintf("batchresponse_%s", generateUUID())
	changesetBoundary := fmt.Sprintf("changesetresponse_%s", generateUUID())

	changesetBuf := new(bytes.Buffer)
	changesetWriter := multipart.NewWriter(changesetBuf)
	_ = changesetWriter.SetBoundary(changesetBoundary)

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

	_, _ = partWriter.Write([]byte(httpResp.String()))
	_ = changesetWriter.Close()

	batchBuf := new(bytes.Buffer)
	batchWriter := multipart.NewWriter(batchBuf)
	_ = batchWriter.SetBoundary(batchBoundary)

	batchPartHeaders := make(textproto.MIMEHeader)
	batchPartHeaders.Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", changesetBoundary))

	batchPart, _ := batchWriter.CreatePart(batchPartHeaders)
	_, _ = batchPart.Write(changesetBuf.Bytes())
	_ = batchWriter.Close()

	w.Header().Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", batchBoundary))
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write(batchBuf.Bytes())
}

// writeBatchErrorWithIndex writes a batch error response for a specific operation.
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
