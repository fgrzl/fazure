package tables

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQueryEntitiesMatchesBruteForceFilter is a conformance test for the compiled-filter
// fast path in QueryEntities (compile-once + the "PartitionKey eq" skip shortcut).
//
// For every filter it asserts that the result set returned by QueryEntities (paged, with
// key-range bounds and the per-row skip optimization) is identical to a brute-force
// reference: ListEntities() followed by evaluating MatchesFilter on every entity. Any
// divergence — e.g. the skip shortcut returning rows a filter would exclude, or loose
// key bounds leaking rows — fails here.
func TestQueryEntitiesMatchesBruteForceFilter(t *testing.T) {
	ts, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	const tableName = "conformancetable"
	require.NoError(t, ts.CreateTable(ctx, tableName))

	table, err := ts.GetTable(ctx, tableName)
	require.NoError(t, err)

	// Seed entities across a few partitions with zero-padded, lexicographically ordered
	// RowKeys (so RowKey ranges behave like the fixed-width keys mesh-core's merkle uses),
	// plus properties to exercise non-key predicates and string functions.
	partitions := []string{"p1", "p2", "p3"}
	const perPartition = 25
	for _, pk := range partitions {
		for i := 0; i < perPartition; i++ {
			rk := fmt.Sprintf("%04d", i)
			status := "active"
			if i%3 == 0 {
				status = "inactive"
			}
			props := map[string]interface{}{
				"Status": status,
				"Name":   fmt.Sprintf("%s-%04d", pk, i),
				"Seq":    int64(i),
			}
			_, insErr := table.InsertEntity(ctx, pk, rk, props)
			require.NoError(t, insErr)
		}
	}

	filters := []string{
		"",                                       // no filter (full scan)
		"PartitionKey eq 'p2'",                   // exercises the skip shortcut
		"PartitionKey eq 'nope'",                 // empty partition
		"PartitionKey eq 'p2' and RowKey ge '0005' and RowKey le '0010'", // merkle-like range
		"PartitionKey eq 'p1' and RowKey eq '0007'",                      // point lookup
		"PartitionKey eq 'p1' and Status eq 'active'",                    // compound, NOT covered by prefix
		"Status eq 'active'",                                             // non-key full scan
		"Status ne 'active'",                                             // ne
		"RowKey gt '0010'",                                               // key range, no partition
		"RowKey ge '0005' and RowKey le '0008'",                          // range across partitions
		"startswith(Name, 'p2-001')",                                     // string function
		"Status eq 'active' or PartitionKey eq 'p3'",                     // or
		"Seq ge 20",                                                      // numeric comparison
	}

	for _, filter := range filters {
		t.Run(fmt.Sprintf("filter=%q", filter), func(t *testing.T) {
			fast := drainQuery(t, table, filter)
			brute := bruteForce(t, table, filter)
			require.Equal(t, brute, fast, "fast path diverged from brute-force MatchesFilter for filter %q", filter)
		})
	}
}

// drainQuery pages through QueryEntities with a small page size to exercise continuation
// tokens, and returns the sorted set of "PK|RK" keys it yielded.
func drainQuery(t *testing.T, table *Table, filter string) []string {
	t.Helper()
	ctx := context.Background()

	const pageSize = 7
	var (
		keys           []string
		nextPK, nextRK string
		seen           = map[string]bool{}
	)
	for {
		page, cPK, cRK, err := table.QueryEntities(ctx, filter, pageSize, nil, nextPK, nextRK)
		require.NoError(t, err)
		for _, e := range page {
			key := e.PartitionKey + "|" + e.RowKey
			require.Falsef(t, seen[key], "entity %q returned more than once across pages", key)
			seen[key] = true
			keys = append(keys, key)
		}
		if cPK == "" && cRK == "" {
			break
		}
		nextPK, nextRK = cPK, cRK
	}
	sort.Strings(keys)
	return keys
}

// bruteForce computes the reference result: every entity in the table that MatchesFilter
// accepts, as a sorted set of "PK|RK" keys.
func bruteForce(t *testing.T, table *Table, filter string) []string {
	t.Helper()
	ctx := context.Background()

	all, err := table.ListEntities(ctx)
	require.NoError(t, err)

	var keys []string
	for _, e := range all {
		entityMap := map[string]interface{}{
			"PartitionKey": e.PartitionKey,
			"RowKey":       e.RowKey,
		}
		for k, v := range e.Properties {
			entityMap[k] = v
		}
		match, matchErr := MatchesFilter(filter, entityMap)
		require.NoError(t, matchErr)
		if match {
			keys = append(keys, e.PartitionKey+"|"+e.RowKey)
		}
	}
	sort.Strings(keys)
	return keys
}
