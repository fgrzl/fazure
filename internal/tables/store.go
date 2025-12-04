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

	return db.Set(key, data, pebble.Sync)
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
	if err := db.Delete(metaKey, pebble.Sync); err != nil {
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

	return batch.Commit(pebble.Sync)
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

func (ts *TableStore) newTable(name string) *Table {
	return &Table{
		store: ts.store,
		name:  name,
	}
} // InsertEntity inserts a new entity
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

	// Marshal with ETag
	data, err = json.Marshal(entity)
	if err != nil {
		return nil, err
	}

	if err := db.Set(key, data, pebble.Sync); err != nil {
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
	defer closer.Close()

	var entity Entity
	if err := json.Unmarshal(data, &entity); err != nil {
		return nil, err
	}

	return &entity, nil
}

// UpdateEntity updates an entity (merge if merge=true, replace if merge=false)
func (t *Table) UpdateEntity(ctx context.Context, partitionKey, rowKey string, properties map[string]interface{}, merge bool) (*Entity, error) {
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

	var entity Entity
	if err := json.Unmarshal(data, &entity); err != nil {
		closer.Close()
		return nil, err
	}
	closer.Close()

	// Update properties
	if merge {
		// Merge: add/update properties
		for k, v := range properties {
			entity.Properties[k] = v
		}
	} else {
		// Replace: replace all properties
		entity.Properties = properties
	}

	// Update timestamp and ETag
	entity.Timestamp = time.Now().UTC()
	data, _ = json.Marshal(entity)
	entity.ETag = common.GenerateETag(data)

	// Marshal with new ETag
	data, err = json.Marshal(entity)
	if err != nil {
		return nil, err
	}

	if err := db.Set(key, data, pebble.Sync); err != nil {
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

	return db.Delete(key, pebble.Sync)
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

// QueryEntities queries entities with filters
func (t *Table) QueryEntities(ctx context.Context, filter string, top int, selectFields []string) ([]*Entity, error) {
	// For now, just list all and apply simple filtering
	entities, err := t.ListEntities(ctx)
	if err != nil {
		return nil, err
	}

	// Apply top limit
	if top > 0 && len(entities) > top {
		entities = entities[:top]
	}

	// TODO: Apply filter and select

	return entities, nil
}
