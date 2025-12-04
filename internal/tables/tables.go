package tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/fgrzl/fazure/internal/common"
)

var globalStore *TableStore

// SetStore sets the global storage instance
func SetStore(store *TableStore) {
	globalStore = store
}

// RegisterRoutes registers table storage routes
func RegisterRoutes(mux *http.ServeMux) {
	// Routes are registered at the root level with a dispatcher
}

// HandleRequest routes table requests - exported for use by main dispatcher
func HandleRequest(w http.ResponseWriter, r *http.Request) {
	handleRequest(w, r)
}

// handleRequest routes table requests
func handleRequest(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	if len(parts) < 1 {
		return
	}

	// Skip non-table requests (handled by blob/queue handlers)
	if len(parts) < 2 {
		return
	}

	// Tables endpoint: /{account}/Tables
	if parts[1] == "Tables" {
		switch r.Method {
		case "GET":
			ListTables(w, r)
		case "POST":
			CreateTable(w, r)
		}
		return
	}

	// Delete table: /{account}/Tables('{table}')
	if strings.HasPrefix(parts[1], "Tables(") {
		if r.Method == "DELETE" {
			DeleteTable(w, r)
		}
		return
	}

	// Batch operations: /{account}/$batch
	if parts[1] == "$batch" && r.Method == "POST" {
		HandleBatchOperation(w, r)
		return
	}

	if len(parts) < 2 {
		return
	}

	table := parts[1]

	// Entity with keys: /{account}/{table}(PartitionKey='pk',RowKey='rk')
	entityPattern := regexp.MustCompile(`\(PartitionKey='([^']+)',RowKey='([^']+)'\)`)
	if matches := entityPattern.FindStringSubmatch(table); len(matches) == 3 {
		switch r.Method {
		case "GET":
			GetEntity(w, r)
		case "PUT":
			UpdateEntity(w, r)
		case "PATCH", "MERGE":
			MergeEntity(w, r)
		case "DELETE":
			DeleteEntity(w, r)
		}
		return
	}

	// Query entities: /{account}/{table}() or /{account}/{table}
	if strings.HasSuffix(table, "()") || r.Method == "GET" {
		QueryEntities(w, r)
		return
	}

	// Insert entity: POST /{account}/{table}
	if r.Method == "POST" {
		InsertEntity(w, r)
	}
}

// ListTables lists all tables
func ListTables(w http.ResponseWriter, r *http.Request) {
	tables, err := globalStore.ListTables(context.Background())
	if err != nil {
		common.WriteErrorResponse(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"value": tables,
	}
	json.NewEncoder(w).Encode(response)
}

// CreateTable creates a new table
func CreateTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TableName string `json:"TableName"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		common.WriteErrorResponse(w, http.StatusBadRequest, "InvalidInput", "Invalid request body")
		return
	}

	if req.TableName == "" {
		common.WriteErrorResponse(w, http.StatusBadRequest, "InvalidInput", "TableName required")
		return
	}

	err := globalStore.CreateTable(context.Background(), req.TableName)
	if err != nil {
		if err == ErrTableExists {
			common.WriteErrorResponse(w, http.StatusConflict, "TableAlreadyExists", "Table already exists")
			return
		}
		common.WriteErrorResponse(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	common.SetResponseHeaders(w, "")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"TableName": req.TableName,
	})
}

// DeleteTable deletes a table
func DeleteTable(w http.ResponseWriter, r *http.Request) {
	// Extract table name from Tables('{table}') pattern
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	re := regexp.MustCompile(`Tables\('([^']+)'\)`)
	matches := re.FindStringSubmatch(parts[1])
	if len(matches) < 2 {
		http.Error(w, "Invalid table name", http.StatusBadRequest)
		return
	}
	tableName := matches[1]

	err := globalStore.DeleteTable(context.Background(), tableName)
	if err != nil {
		if err == ErrTableNotFound {
			http.Error(w, "TableNotFound", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("InternalServerError: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ListEntities returns all entities in a table (without filter)
func ListEntities(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	tableName := strings.TrimSuffix(parts[1], "()")

	table, err := globalStore.GetTable(context.Background(), tableName)
	if err != nil {
		http.Error(w, "TableNotFound", http.StatusNotFound)
		return
	}

	entities, err := table.ListEntities(context.Background())
	if err != nil {
		http.Error(w, fmt.Sprintf("InternalServerError: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"value": entities,
	}
	json.NewEncoder(w).Encode(response)
}

// GetEntity retrieves a single entity
func GetEntity(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		common.WriteErrorResponse(w, http.StatusBadRequest, "InvalidInput", "Invalid path")
		return
	}

	// Parse table(PartitionKey='pk',RowKey='rk')
	re := regexp.MustCompile(`([^(]+)\(PartitionKey='([^']+)',RowKey='([^']+)'\)`)
	matches := re.FindStringSubmatch(parts[1])
	if len(matches) < 4 {
		common.WriteErrorResponse(w, http.StatusBadRequest, "InvalidInput", "Invalid entity path")
		return
	}
	tableName := matches[1]
	pk := matches[2]
	rk := matches[3]

	table, err := globalStore.GetTable(context.Background(), tableName)
	if err != nil {
		common.WriteErrorResponse(w, http.StatusNotFound, "ResourceNotFound", "Table not found")
		return
	}

	entity, err := table.GetEntity(context.Background(), pk, rk)
	if err != nil {
		if err == ErrEntityNotFound {
			common.WriteErrorResponse(w, http.StatusNotFound, "ResourceNotFound", "Entity not found")
			return
		}
		common.WriteErrorResponse(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	common.SetResponseHeaders(w, entity.ETag)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entity)
}

// InsertEntity inserts a new entity
func InsertEntity(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	tableName := strings.TrimSuffix(parts[1], "()")

	table, err := globalStore.GetTable(context.Background(), tableName)
	if err != nil {
		http.Error(w, "TableNotFound", http.StatusNotFound)
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "InvalidInput", http.StatusBadRequest)
		return
	}

	pk, okPK := data["PartitionKey"].(string)
	rk, okRK := data["RowKey"].(string)

	if !okPK || !okRK {
		http.Error(w, "InvalidInput: PartitionKey and RowKey required", http.StatusBadRequest)
		return
	}

	entity, err := table.InsertEntity(context.Background(), pk, rk, data)
	if err != nil {
		if err == ErrEntityExists {
			http.Error(w, "EntityAlreadyExists", http.StatusConflict)
			return
		}
		http.Error(w, fmt.Sprintf("InternalServerError: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", entity.ETag)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(entity)
}

// UpdateEntity replaces an entity (PUT)
func UpdateEntity(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	re := regexp.MustCompile(`([^(]+)\(PartitionKey='([^']+)',RowKey='([^']+)'\)`)
	matches := re.FindStringSubmatch(parts[1])
	if len(matches) < 4 {
		http.Error(w, "Invalid entity path", http.StatusBadRequest)
		return
	}
	tableName := matches[1]
	pk := matches[2]
	rk := matches[3]

	table, err := globalStore.GetTable(context.Background(), tableName)
	if err != nil {
		http.Error(w, "TableNotFound", http.StatusNotFound)
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "InvalidInput", http.StatusBadRequest)
		return
	}

	entity, err := table.UpdateEntity(context.Background(), pk, rk, data, false)
	if err != nil {
		if err == ErrEntityNotFound {
			http.Error(w, "ResourceNotFound", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("InternalServerError: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", entity.ETag)
	w.WriteHeader(http.StatusNoContent)
}

// MergeEntity merges an entity (MERGE)
func MergeEntity(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	re := regexp.MustCompile(`([^(]+)\(PartitionKey='([^']+)',RowKey='([^']+)'\)`)
	matches := re.FindStringSubmatch(parts[1])
	if len(matches) < 4 {
		http.Error(w, "Invalid entity path", http.StatusBadRequest)
		return
	}
	tableName := matches[1]
	pk := matches[2]
	rk := matches[3]

	table, err := globalStore.GetTable(context.Background(), tableName)
	if err != nil {
		http.Error(w, "TableNotFound", http.StatusNotFound)
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "InvalidInput", http.StatusBadRequest)
		return
	}

	entity, err := table.UpdateEntity(context.Background(), pk, rk, data, true)
	if err != nil {
		if err == ErrEntityNotFound {
			http.Error(w, "ResourceNotFound", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("InternalServerError: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", entity.ETag)
	w.WriteHeader(http.StatusNoContent)
}

// DeleteEntity deletes an entity
func DeleteEntity(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	re := regexp.MustCompile(`([^(]+)\(PartitionKey='([^']+)',RowKey='([^']+)'\)`)
	matches := re.FindStringSubmatch(parts[1])
	if len(matches) < 4 {
		http.Error(w, "Invalid entity path", http.StatusBadRequest)
		return
	}
	tableName := matches[1]
	pk := matches[2]
	rk := matches[3]

	table, err := globalStore.GetTable(context.Background(), tableName)
	if err != nil {
		http.Error(w, "TableNotFound", http.StatusNotFound)
		return
	}

	err = table.DeleteEntity(context.Background(), pk, rk)
	if err != nil {
		if err == ErrEntityNotFound {
			http.Error(w, "ResourceNotFound", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("InternalServerError: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// QueryEntities queries entities with filter and select
func QueryEntities(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	tableName := strings.TrimSuffix(parts[1], "()")

	table, err := globalStore.GetTable(context.Background(), tableName)
	if err != nil {
		http.Error(w, "TableNotFound", http.StatusNotFound)
		return
	}

	filter := r.URL.Query().Get("$filter")
	selectStr := r.URL.Query().Get("$select")
	top := r.URL.Query().Get("$top")

	var selectFields []string
	if selectStr != "" {
		selectFields = parseSelect(selectStr)
	}

	// Parse top limit
	var limit int
	if top != "" {
		fmt.Sscanf(top, "%d", &limit)
	}

	entities, err := table.QueryEntities(context.Background(), filter, limit, selectFields)
	if err != nil {
		http.Error(w, fmt.Sprintf("InvalidInput: %v", err), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
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
func HandleBatchOperation(w http.ResponseWriter, r *http.Request) {
	// Parse batch request
	batchReq, err := ParseBatchRequest(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("InvalidInput: %v", err), http.StatusBadRequest)
		return
	}

	// For now, return not implemented
	// TODO: Implement proper batch execution with access to global store
	_ = batchReq

	// TODO: Format multipart response
	w.Header().Set("Content-Type", "multipart/mixed")
	w.WriteHeader(http.StatusNotImplemented)
}
