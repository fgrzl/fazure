package tables

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/fgrzl/fazure/internal/common"
)

// Entity represents a table entity with metadata
type Entity struct {
	PartitionKey string                 `json:"PartitionKey"`
	RowKey       string                 `json:"RowKey"`
	Timestamp    time.Time              `json:"Timestamp"`
	ETag         string                 `json:"ETag"`
	Properties   map[string]interface{} `json:"-"`
}

// MarshalJSON implements custom JSON marshaling
func (e *Entity) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"PartitionKey": e.PartitionKey,
		"RowKey":       e.RowKey,
		"Timestamp":    e.Timestamp.Unix(),
		"ETag":         e.ETag,
	}

	// Merge properties
	for k, v := range e.Properties {
		m[k] = v
	}

	return json.Marshal(m)
}

// UnmarshalJSON implements custom JSON unmarshaling
func (e *Entity) UnmarshalJSON(data []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	if pk, ok := m["PartitionKey"].(string); ok {
		e.PartitionKey = pk
	}
	if rk, ok := m["RowKey"].(string); ok {
		e.RowKey = rk
	}
	if etag, ok := m["ETag"].(string); ok {
		e.ETag = etag
	}

	// Parse timestamp if it's a Unix timestamp
	if ts, ok := m["Timestamp"].(float64); ok {
		e.Timestamp = time.Unix(int64(ts), 0).UTC()
	}

	// Store remaining properties
	e.Properties = make(map[string]interface{})
	for k, v := range m {
		if k != "PartitionKey" && k != "RowKey" && k != "Timestamp" && k != "ETag" {
			e.Properties[k] = v
		}
	}

	return nil
}

// TableStore manages tables and entities
type TableStore struct {
	store *common.Store
}

// NewTableStore creates a new table store
func NewTableStore(store *common.Store) (*TableStore, error) {
	return &TableStore{store: store}, nil
}

// ListTables returns all table names
func (ts *TableStore) ListTables(ctx context.Context) ([]map[string]string, error) {
	db := ts.store.DB()
	prefix := []byte("tables/meta/")

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	tables := make([]map[string]string, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		tableName := string(iter.Key()[len(prefix):])
		tables = append(tables, map[string]string{
			"TableName": tableName,
		})
	}

	return tables, iter.Error()
}

// CreateTable creates a new table
func (ts *TableStore) CreateTable(ctx context.Context, tableName string) error {
	db := ts.store.DB()
	key := []byte("tables/meta/" + tableName)

	// Check if table exists
	_, closer, err := db.Get(key)
	if err == nil {
		closer.Close()
		return ErrTableExists
	}
	if err != pebble.ErrNotFound {
		return err
	}

	// Create table metadata
	metadata := map[string]interface{}{
		"name":      tableName,
		"createdAt": time.Now().UTC(),
	}
	data, _ := json.Marshal(metadata)

	return db.Set(key, data, pebble.NoSync)
}

// DeleteTable deletes a table and all its entities
func (ts *TableStore) DeleteTable(ctx context.Context, tableName string) error {
	db := ts.store.DB()
	metaKey := []byte("tables/meta/" + tableName)

	// Check if table exists
	_, closer, err := db.Get(metaKey)
	if err == pebble.ErrNotFound {
		return ErrTableNotFound
	}
	if err != nil {
		return err
	}
	closer.Close()

	// Delete table metadata
	if err := db.Delete(metaKey, pebble.NoSync); err != nil {
		return err
	}

	// Delete all entities
	prefix := []byte(fmt.Sprintf("tables/data/%s/", tableName))
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := db.NewBatch()
	for iter.First(); iter.Valid(); iter.Next() {
		batch.Delete(iter.Key(), nil)
	}

	return batch.Commit(pebble.NoSync)
}

// GetTable returns a table handle
func (ts *TableStore) GetTable(ctx context.Context, tableName string) (*Table, error) {
	db := ts.store.DB()
	metaKey := []byte("tables/meta/" + tableName)

	// Check if table exists
	_, closer, err := db.Get(metaKey)
	if err == pebble.ErrNotFound {
		return nil, ErrTableNotFound
	}
	if err != nil {
		return nil, err
	}
	closer.Close()

	return &Table{
		store: ts.store,
		name:  tableName,
	}, nil
}

// Table represents a table handle
type Table struct {
	store *common.Store
	name  string
}

// InsertEntity inserts a new entity
func (t *Table) InsertEntity(ctx context.Context, partitionKey, rowKey string, properties map[string]interface{}) (*Entity, error) {
	db := t.store.DB()
	key := []byte(fmt.Sprintf("tables/data/%s/%s/%s", t.name, partitionKey, rowKey))

	// Check if entity exists
	_, closer, err := db.Get(key)
	if err == nil {
		closer.Close()
		return nil, ErrEntityExists
	}
	if err != pebble.ErrNotFound {
		return nil, err
	}

	// Create entity
	entity := &Entity{
		PartitionKey: partitionKey,
		RowKey:       rowKey,
		Timestamp:    time.Now().UTC(),
		Properties:   properties,
	}

	// Generate ETag
	data, _ := json.Marshal(entity)
	entity.ETag = common.GenerateETag(data)

	// Marshal with ETag - use pointer to ensure custom MarshalJSON is called
	data, err = json.Marshal(entity)
	if err != nil {
		return nil, err
	}

	if err := db.Set(key, data, pebble.NoSync); err != nil {
		return nil, err
	}

	return entity, nil
}

// GetEntity retrieves an entity
func (t *Table) GetEntity(ctx context.Context, partitionKey, rowKey string) (*Entity, error) {
	db := t.store.DB()
	key := []byte(fmt.Sprintf("tables/data/%s/%s/%s", t.name, partitionKey, rowKey))

	data, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, ErrEntityNotFound
	}
	if err != nil {
		return nil, err
	}
	// Copy data before closing (Pebble reuses buffers)
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	var entity Entity
	if err := json.Unmarshal(dataCopy, &entity); err != nil {
		return nil, err
	}

	return &entity, nil
}

// UpdateEntity updates an entity (merge if merge=true, replace if merge=false)
func (t *Table) UpdateEntity(ctx context.Context, partitionKey, rowKey string, properties map[string]interface{}, merge bool) (*Entity, error) {
	return t.UpdateEntityWithETag(ctx, partitionKey, rowKey, properties, merge, "")
}

// UpdateEntityWithETag updates an entity with optional ETag validation
func (t *Table) UpdateEntityWithETag(ctx context.Context, partitionKey, rowKey string, properties map[string]interface{}, merge bool, ifMatch string) (*Entity, error) {
	db := t.store.DB()
	key := []byte(fmt.Sprintf("tables/data/%s/%s/%s", t.name, partitionKey, rowKey))

	// Get existing entity
	data, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, ErrEntityNotFound
	}
	if err != nil {
		return nil, err
	}

	// Copy data before closing (Pebble reuses buffers)
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	var entity Entity
	if err := json.Unmarshal(dataCopy, &entity); err != nil {
		return nil, err
	}

	// Validate ETag if provided (and not wildcard)
	if ifMatch != "" && ifMatch != "*" {
		if entity.ETag != ifMatch {
			return nil, ErrPreconditionFailed
		}
	}

	// Update properties
	if merge {
		// Merge: add/update properties, keep existing ones
		if entity.Properties == nil {
			entity.Properties = make(map[string]interface{})
		}
		for k, v := range properties {
			if k != "PartitionKey" && k != "RowKey" && k != "Timestamp" && k != "odata.etag" {
				entity.Properties[k] = v
			}
		}
	} else {
		// Replace: replace all properties
		entity.Properties = make(map[string]interface{})
		for k, v := range properties {
			if k != "PartitionKey" && k != "RowKey" && k != "Timestamp" && k != "odata.etag" {
				entity.Properties[k] = v
			}
		}
	}

	// Update timestamp and ETag
	entity.Timestamp = time.Now().UTC()
	data, _ = json.Marshal(&entity)
	entity.ETag = common.GenerateETag(data)

	// Marshal with new ETag - use pointer to ensure custom MarshalJSON is called
	data, err = json.Marshal(&entity)
	if err != nil {
		return nil, err
	}

	if err := db.Set(key, data, pebble.NoSync); err != nil {
		return nil, err
	}

	return &entity, nil
}

// UpsertEntity inserts or updates an entity
func (t *Table) UpsertEntity(ctx context.Context, partitionKey, rowKey string, properties map[string]interface{}, merge bool) (*Entity, error) {
	db := t.store.DB()
	key := []byte(fmt.Sprintf("tables/data/%s/%s/%s", t.name, partitionKey, rowKey))

	// Try to get existing entity
	data, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		// Entity doesn't exist, insert new one
		return t.InsertEntity(ctx, partitionKey, rowKey, properties)
	}
	if err != nil {
		return nil, err
	}

	// Copy data before closing (Pebble reuses buffers)
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	var entity Entity
	if err := json.Unmarshal(dataCopy, &entity); err != nil {
		return nil, err
	}

	// Update properties
	if merge {
		// Merge: add/update properties, keep existing ones
		if entity.Properties == nil {
			entity.Properties = make(map[string]interface{})
		}
		for k, v := range properties {
			if k != "PartitionKey" && k != "RowKey" && k != "Timestamp" && k != "odata.etag" {
				entity.Properties[k] = v
			}
		}
	} else {
		// Replace: replace all properties
		entity.Properties = make(map[string]interface{})
		for k, v := range properties {
			if k != "PartitionKey" && k != "RowKey" && k != "Timestamp" && k != "odata.etag" {
				entity.Properties[k] = v
			}
		}
	}

	// Update timestamp and ETag
	entity.Timestamp = time.Now().UTC()
	marshaledData, _ := json.Marshal(&entity)
	entity.ETag = common.GenerateETag(marshaledData)

	// Marshal with new ETag
	finalData, err := json.Marshal(&entity)
	if err != nil {
		return nil, err
	}

	if err := db.Set(key, finalData, pebble.NoSync); err != nil {
		return nil, err
	}

	return &entity, nil
}

// DeleteEntity deletes an entity
func (t *Table) DeleteEntity(ctx context.Context, partitionKey, rowKey string) error {
	db := t.store.DB()
	key := []byte(fmt.Sprintf("tables/data/%s/%s/%s", t.name, partitionKey, rowKey))

	// Check if entity exists
	_, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return ErrEntityNotFound
	}
	if err != nil {
		return err
	}
	closer.Close()

	return db.Delete(key, pebble.NoSync)
}

// ListEntities lists all entities in the table
func (t *Table) ListEntities(ctx context.Context) ([]*Entity, error) {
	db := t.store.DB()
	prefix := []byte(fmt.Sprintf("tables/data/%s/", t.name))

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	entities := make([]*Entity, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		var entity Entity
		if err := json.Unmarshal(iter.Value(), &entity); err != nil {
			continue
		}
		entities = append(entities, &entity)
	}

	return entities, iter.Error()
}

// QueryEntities queries entities with filters and pagination
func (t *Table) QueryEntities(ctx context.Context, filter string, top int, selectFields []string, nextPK, nextRK string) ([]*Entity, string, string, error) {
	db := t.store.DB()

	// Extract partition key from filter to optimize the scan range
	partitionKeyFilter := extractPartitionKeyFromFilter(filter)

	// Build prefix based on partition key filter
	var prefix, upperBound []byte
	if partitionKeyFilter != "" {
		// If we have a partition key filter, scan only that partition
		prefix = []byte(fmt.Sprintf("tables/data/%s/%s/", t.name, partitionKeyFilter))
		upperBound = append(prefix, 0xff)
	} else {
		// Full table scan
		prefix = []byte(fmt.Sprintf("tables/data/%s/", t.name))
		upperBound = append(prefix, 0xff)
	}

	// Set lower bound based on continuation token
	lowerBound := prefix
	if nextPK != "" && nextRK != "" {
		lowerBound = []byte(fmt.Sprintf("tables/data/%s/%s/%s", t.name, nextPK, nextRK))
	}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, "", "", err
	}
	defer iter.Close()

	entities := make([]*Entity, 0)
	count := 0
	limit := top
	if limit <= 0 {
		limit = 1000 // Default page size
	}

	// Pre-parse the filter once for efficiency
	var filterFunc func(entity map[string]interface{}) bool
	if filter != "" {
		filterFunc = func(entity map[string]interface{}) bool {
			return MatchesFilter(filter, entity)
		}
	}

	for iter.First(); iter.Valid(); iter.Next() {
		var entity Entity
		if err := json.Unmarshal(iter.Value(), &entity); err != nil {
			continue
		}

		// Skip the continuation token entity itself (we want to start after it)
		if nextPK != "" && nextRK != "" && entity.PartitionKey == nextPK && entity.RowKey == nextRK {
			continue
		}

		// Apply filter if specified
		if filterFunc != nil {
			// Convert entity to map for filtering
			entityMap := map[string]interface{}{
				"PartitionKey": entity.PartitionKey,
				"RowKey":       entity.RowKey,
			}
			for k, v := range entity.Properties {
				entityMap[k] = v
			}
			if !filterFunc(entityMap) {
				continue
			}
		}

		entities = append(entities, &entity)
		count++

		if count >= limit {
			// Check if there are more entities
			if iter.Next() {
				// Return continuation token
				lastEntity := entities[len(entities)-1]
				return entities, lastEntity.PartitionKey, lastEntity.RowKey, nil
			}
			break
		}
	}

	return entities, "", "", iter.Error()
}
