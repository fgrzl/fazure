package tables

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/fgrzl/fazure/internal/common"
)

// ───────────────────────────────────────────────────────────────────────────────
// Entity model
// ───────────────────────────────────────────────────────────────────────────────

// Entity represents a table entity with metadata.
type Entity struct {
	PartitionKey string                 `json:"PartitionKey"`
	RowKey       string                 `json:"RowKey"`
	Timestamp    time.Time              `json:"Timestamp"`
	ETag         string                 `json:"ETag"`
	Properties   map[string]interface{} `json:"-"`
}

// MarshalJSON implements custom JSON marshaling (Azure-style flat object).
func (e *Entity) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"PartitionKey": e.PartitionKey,
		"RowKey":       e.RowKey,
		"Timestamp":    e.Timestamp.Unix(),
		"ETag":         e.ETag,
	}

	for k, v := range e.Properties {
		m[k] = v
	}

	return json.Marshal(m)
}

// UnmarshalJSON implements custom JSON unmarshaling.
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
	if ts, ok := m["Timestamp"].(float64); ok {
		e.Timestamp = time.Unix(int64(ts), 0).UTC()
	}

	e.Properties = make(map[string]interface{}, len(m))
	for k, v := range m {
		if k != "PartitionKey" && k != "RowKey" && k != "Timestamp" && k != "ETag" {
			e.Properties[k] = v
		}
	}

	return nil
}

// ───────────────────────────────────────────────────────────────────────────────
// Pebble key encoding
// Layout: tables/data/<table>\x00<partitionKey>\x00<rowKey>
// This preserves table, PK, RK ordering and works well with prefix/range scans.
// ───────────────────────────────────────────────────────────────────────────────

const (
	metaPrefix      = "tables/meta/"
	dataPrefix      = "tables/data/"
	keySep     byte = 0x00
)

// metaKey(tableName) => tables/meta/<table>
func metaKey(tableName string) []byte {
	return []byte(metaPrefix + tableName)
}

// dataKey(table, pk, rk) => tables/data/<table>\x00<pk>\x00<rk>
func dataKey(table, pk, rk string) []byte {
	n := len(dataPrefix) + len(table) + 1 + len(pk) + 1 + len(rk)
	b := make([]byte, 0, n)
	b = append(b, dataPrefix...)
	b = append(b, table...)
	b = append(b, keySep)
	b = append(b, pk...)
	b = append(b, keySep)
	b = append(b, rk...)
	return b
}

// tablePrefix(table) => prefix for all entities in a table
// tables/data/<table>\x00
func tablePrefix(table string) []byte {
	n := len(dataPrefix) + len(table) + 1
	b := make([]byte, 0, n)
	b = append(b, dataPrefix...)
	b = append(b, table...)
	b = append(b, keySep)
	return b
}

// partitionPrefix(table, pk) => prefix for all entities in a partition
// tables/data/<table>\x00<pk>\x00
func partitionPrefix(table, pk string) []byte {
	n := len(dataPrefix) + len(table) + 1 + len(pk) + 1
	b := make([]byte, 0, n)
	b = append(b, dataPrefix...)
	b = append(b, table...)
	b = append(b, keySep)
	b = append(b, pk...)
	b = append(b, keySep)
	return b
}

// upperBoundForPrefix returns an upper bound suitable for Pebble range scans.
func upperBoundForPrefix(prefix []byte) []byte {
	upper := make([]byte, len(prefix)+1)
	copy(upper, prefix)
	upper[len(prefix)] = 0xff
	return upper
}

// ───────────────────────────────────────────────────────────────────────────────
// TableStore
// ───────────────────────────────────────────────────────────────────────────────

// TableStore manages tables and entities.
type TableStore struct {
	store   *common.Store
	log     *slog.Logger
	metrics *Metrics
}

// NewTableStore creates a new table store.
func NewTableStore(store *common.Store, logger *slog.Logger, metrics *Metrics) (*TableStore, error) {
	if logger == nil {
		logger = slog.Default()
	}

	return &TableStore{
		store:   store,
		log:     logger.With("component", "tables-store"),
		metrics: metrics,
	}, nil
}

// ListTables returns all table names.
func (ts *TableStore) ListTables(ctx context.Context) ([]map[string]string, error) {
	db := ts.store.DB()
	prefix := []byte(metaPrefix)
	upper := upperBoundForPrefix(prefix)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var tables []map[string]string
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		key := iter.Key()
		tableName := string(key[len(prefix):])
		tables = append(tables, map[string]string{
			"TableName": tableName,
		})
	}

	return tables, iter.Error()
}

// CreateTable creates a new table.
func (ts *TableStore) CreateTable(ctx context.Context, tableName string) error {
	db := ts.store.DB()
	key := metaKey(tableName)

	_, closer, err := db.Get(key)
	if err == nil {
		closer.Close()
		return ErrTableExists
	}
	if err != nil && err != pebble.ErrNotFound {
		return err
	}

	metadata := map[string]interface{}{
		"name":      tableName,
		"createdAt": time.Now().UTC(),
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	return db.Set(key, data, pebble.NoSync)
}

// DeleteTable deletes a table and all its entities.
func (ts *TableStore) DeleteTable(ctx context.Context, tableName string) error {
	db := ts.store.DB()
	mKey := metaKey(tableName)

	_, closer, err := db.Get(mKey)
	if err == pebble.ErrNotFound {
		return ErrTableNotFound
	}
	if err != nil {
		return err
	}
	closer.Close()

	if err := db.Delete(mKey, pebble.NoSync); err != nil {
		return err
	}

	prefix := tablePrefix(tableName)
	upper := upperBoundForPrefix(prefix)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := db.NewBatch()
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			_ = batch.Close()
			return ctx.Err()
		default:
		}
		if err := batch.Delete(iter.Key(), nil); err != nil {
			_ = batch.Close()
			return err
		}
	}

	if err := iter.Error(); err != nil {
		_ = batch.Close()
		return err
	}

	return batch.Commit(pebble.NoSync)
}

// ───────────────────────────────────────────────────────────────────────────────
// Table
// ───────────────────────────────────────────────────────────────────────────────

// Table represents a table handle.
type Table struct {
	store   *common.Store
	name    string
	log     *slog.Logger
	metrics *Metrics
}

// GetTable returns a table handle.
func (ts *TableStore) GetTable(ctx context.Context, tableName string) (*Table, error) {
	db := ts.store.DB()
	key := metaKey(tableName)

	_, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, ErrTableNotFound
	}
	if err != nil {
		return nil, err
	}
	closer.Close()

	logger := ts.log
	if logger == nil {
		logger = slog.Default()
	}

	return &Table{
		store:   ts.store,
		name:    tableName,
		log:     logger.With("table", tableName),
		metrics: ts.metrics,
	}, nil
}

// InsertEntity inserts a new entity (fails if exists).
func (t *Table) InsertEntity(ctx context.Context, partitionKey, rowKey string, properties map[string]interface{}) (*Entity, error) {
	db := t.store.DB()
	key := dataKey(t.name, partitionKey, rowKey)

	_, closer, err := db.Get(key)
	if err == nil {
		closer.Close()
		return nil, ErrEntityExists
	}
	if err != nil && err != pebble.ErrNotFound {
		return nil, err
	}

	entity := &Entity{
		PartitionKey: partitionKey,
		RowKey:       rowKey,
		Timestamp:    time.Now().UTC(),
		Properties:   properties,
	}

	data, err := json.Marshal(entity)
	if err != nil {
		return nil, err
	}
	entity.ETag = common.GenerateETag(data)

	data, err = json.Marshal(entity)
	if err != nil {
		return nil, err
	}

	if err := db.Set(key, data, pebble.NoSync); err != nil {
		return nil, err
	}

	return entity, nil
}

// GetEntity retrieves an entity.
func (t *Table) GetEntity(ctx context.Context, partitionKey, rowKey string) (*Entity, error) {
	db := t.store.DB()
	key := dataKey(t.name, partitionKey, rowKey)

	data, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, ErrEntityNotFound
	}
	if err != nil {
		return nil, err
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	var entity Entity
	if err := json.Unmarshal(dataCopy, &entity); err != nil {
		return nil, err
	}

	return &entity, nil
}

// UpdateEntity updates an entity (merge if merge=true, replace if merge=false).
func (t *Table) UpdateEntity(
	ctx context.Context,
	partitionKey, rowKey string,
	properties map[string]interface{},
	merge bool,
) (*Entity, error) {
	return t.UpdateEntityWithETag(ctx, partitionKey, rowKey, properties, merge, "")
}

// UpdateEntityWithETag updates an entity with optional ETag validation.
func (t *Table) UpdateEntityWithETag(
	ctx context.Context,
	partitionKey, rowKey string,
	properties map[string]interface{},
	merge bool,
	ifMatch string,
) (*Entity, error) {
	db := t.store.DB()
	key := dataKey(t.name, partitionKey, rowKey)

	data, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, ErrEntityNotFound
	}
	if err != nil {
		return nil, err
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	var entity Entity
	if err := json.Unmarshal(dataCopy, &entity); err != nil {
		return nil, err
	}

	if ifMatch != "" && ifMatch != "*" && entity.ETag != ifMatch {
		return nil, ErrPreconditionFailed
	}

	if merge {
		if entity.Properties == nil {
			entity.Properties = make(map[string]interface{})
		}
		for k, v := range properties {
			if k != "PartitionKey" && k != "RowKey" && k != "Timestamp" && k != "odata.etag" {
				entity.Properties[k] = v
			}
		}
	} else {
		entity.Properties = make(map[string]interface{}, len(properties))
		for k, v := range properties {
			if k != "PartitionKey" && k != "RowKey" && k != "Timestamp" && k != "odata.etag" {
				entity.Properties[k] = v
			}
		}
	}

	entity.Timestamp = time.Now().UTC()
	data, err = json.Marshal(&entity)
	if err != nil {
		return nil, err
	}
	entity.ETag = common.GenerateETag(data)

	data, err = json.Marshal(&entity)
	if err != nil {
		return nil, err
	}

	if err := db.Set(key, data, pebble.NoSync); err != nil {
		return nil, err
	}

	return &entity, nil
}

// UpsertEntity inserts or updates an entity.
func (t *Table) UpsertEntity(
	ctx context.Context,
	partitionKey, rowKey string,
	properties map[string]interface{},
	merge bool,
) (*Entity, error) {
	db := t.store.DB()
	key := dataKey(t.name, partitionKey, rowKey)

	data, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return t.InsertEntity(ctx, partitionKey, rowKey, properties)
	}
	if err != nil {
		return nil, err
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	var entity Entity
	if err := json.Unmarshal(dataCopy, &entity); err != nil {
		return nil, err
	}

	if merge {
		if entity.Properties == nil {
			entity.Properties = make(map[string]interface{})
		}
		for k, v := range properties {
			if k != "PartitionKey" && k != "RowKey" && k != "Timestamp" && k != "odata.etag" {
				entity.Properties[k] = v
			}
		}
	} else {
		entity.Properties = make(map[string]interface{}, len(properties))
		for k, v := range properties {
			if k != "PartitionKey" && k != "RowKey" && k != "Timestamp" && k != "odata.etag" {
				entity.Properties[k] = v
			}
		}
	}

	entity.Timestamp = time.Now().UTC()
	marshaledData, err := json.Marshal(&entity)
	if err != nil {
		return nil, err
	}
	entity.ETag = common.GenerateETag(marshaledData)

	finalData, err := json.Marshal(&entity)
	if err != nil {
		return nil, err
	}

	if err := db.Set(key, finalData, pebble.NoSync); err != nil {
		return nil, err
	}

	return &entity, nil
}

// DeleteEntity deletes an entity.
func (t *Table) DeleteEntity(ctx context.Context, partitionKey, rowKey string) error {
	db := t.store.DB()
	key := dataKey(t.name, partitionKey, rowKey)

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

// ListEntities lists all entities in the table.
func (t *Table) ListEntities(ctx context.Context) ([]*Entity, error) {
	db := t.store.DB()
	prefix := tablePrefix(t.name)
	upper := upperBoundForPrefix(prefix)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var entities []*Entity
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var entity Entity
		if err := json.Unmarshal(iter.Value(), &entity); err != nil {
			continue
		}
		entities = append(entities, &entity)
	}

	return entities, iter.Error()
}

// QueryEntities queries entities with filters and pagination.
//
// - filter: OData-like filter string (we delegate to MatchesFilter).
// - top: max number of results (0 => default 1000).
// - selectFields: currently ignored here (projection is done in handler).
// - nextPK/nextRK: continuation tokens (Azure-style).
func (t *Table) QueryEntities(
	ctx context.Context,
	filter string,
	top int,
	selectFields []string,
	nextPK, nextRK string,
) (entities []*Entity, contPK string, contRK string, err error) {
	start := time.Now()
	fullScan := false
	db := t.store.DB()
	scanned := 0
	pkHint := ""
	scanReason := ""

	defer func() {
		if t.metrics != nil {
			t.metrics.ObserveQuery(QueryMetric{
				Duration:     time.Since(start),
				Returned:     len(entities),
				Scanned:      scanned,
				FullScan:     fullScan,
				Err:          err != nil,
				FilterError:  errors.Is(err, ErrInvalidFilter),
				Continuation: contPK != "" || contRK != "",
			})
		}
	}()

	partitionKeyFilter := extractPartitionKeyFromFilter(filter)
	pkHint = partitionKeyFilter

	var (
		lowerBound []byte
		upperBound []byte
	)

	if partitionKeyFilter != "" {
		prefix := partitionPrefix(t.name, partitionKeyFilter)
		lowerBound = prefix
		upperBound = upperBoundForPrefix(prefix)

		t.log.Debug("query using partition prefix",
			"partitionKey", partitionKeyFilter,
			"prefix", string(prefix),
			"filter", filter,
			"top", top,
			"continuation", nextPK != "" || nextRK != "",
		)
	} else {
		fullScan = true
		prefix := tablePrefix(t.name)
		lowerBound = prefix
		upperBound = upperBoundForPrefix(prefix)

		if filter != "" {
			scanReason = "filter not partition-restricted"
			t.log.Warn("query falling back to full table scan",
				"reason", scanReason,
				"filter", filter,
				"top", top,
				"continuation", nextPK != "" || nextRK != "",
			)
		} else {
			scanReason = "no filter provided"
			t.log.Debug("query using full table scan (no filter)",
				"top", top,
				"continuation", nextPK != "" || nextRK != "",
			)
		}
	}

	if nextPK != "" && nextRK != "" {
		lowerBound = dataKey(t.name, nextPK, nextRK)
	}

	iterStart := time.Now()
	iter, newIterErr := db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if newIterErr != nil {
		err = newIterErr
		return
	}
	defer iter.Close()
	t.log.Debug("iterator created",
		"elapsed", time.Since(iterStart),
		"pkHint", pkHint,
		"fullScan", fullScan,
		"scanReason", scanReason,
	)

	limit := top
	if limit <= 0 {
		limit = 1000
	}

	var filterFunc func(entity map[string]interface{}) (bool, error)
	if filter != "" {
		filterFunc = func(entity map[string]interface{}) (bool, error) {
			return MatchesFilter(filter, entity)
		}
	}

	scanStart := time.Now()
	count := 0

	logCompletion := func(hasMore bool) {
		elapsed := time.Since(scanStart)
		total := time.Since(start)
		logger := t.log
		level := logger.Debug
		if elapsed >= slowQueryThreshold {
			level = logger.Warn
		}
		level("query complete",
			"returned", count,
			"scanned", scanned,
			"scanTime", elapsed,
			"totalTime", total,
			"hasMore", hasMore,
			"fullScan", fullScan,
			"pkHint", pkHint,
			"scanReason", scanReason,
			"slow", elapsed >= slowQueryThreshold,
		)
	}

	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}

		scanned++

		var entity Entity
		if unmarshalErr := json.Unmarshal(iter.Value(), &entity); unmarshalErr != nil {
			continue
		}

		if nextPK != "" && nextRK != "" &&
			entity.PartitionKey == nextPK &&
			entity.RowKey == nextRK {
			continue
		}

		if filterFunc != nil {
			entityMap := map[string]interface{}{
				"PartitionKey": entity.PartitionKey,
				"RowKey":       entity.RowKey,
			}
			for k, v := range entity.Properties {
				entityMap[k] = v
			}
			match, matchErr := filterFunc(entityMap)
			if matchErr != nil {
				if errors.Is(matchErr, ErrInvalidFilter) {
					t.log.Warn("invalid filter during evaluation", "filter", filter, "error", matchErr)
				}
				err = matchErr
				return
			}
			if !match {
				continue
			}
		}

		entities = append(entities, &entity)
		count++

		if count >= limit {
			hasMore := iter.Next()
			if hasMore {
				last := entities[len(entities)-1]
				contPK = last.PartitionKey
				contRK = last.RowKey
			}

			logCompletion(hasMore)
			return
		}
	}

	logCompletion(false)
	err = iter.Error()
	return
}
