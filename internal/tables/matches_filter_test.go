package tables

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchesFilterWithHexRowKeys(t *testing.T) {
	tests := []struct {
		name     string
		filter   string
		rowKey   string
		expected bool
	}{
		{
			name:     "should match when rowkey greater than lower bound",
			filter:   "RowKey ge '00000000000000e9'",
			rowKey:   "00000000000000ea",
			expected: true,
		},
		{
			name:     "should match when rowkey equal to lower bound",
			filter:   "RowKey ge '00000000000000e9'",
			rowKey:   "00000000000000e9",
			expected: true,
		},
		{
			name:     "should not match when rowkey less than lower bound",
			filter:   "RowKey ge '00000000000000e9'",
			rowKey:   "0000000000000001",
			expected: false,
		},
		{
			name:     "should not match when rowkey less than lower bound (e8)",
			filter:   "RowKey ge '00000000000000e9'",
			rowKey:   "00000000000000e8",
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entity := map[string]interface{}{
				"PartitionKey": "testpk",
				"RowKey":       tc.rowKey,
			}

			result, err := MatchesFilter(tc.filter, entity)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result, "Filter: %s, RowKey: %s", tc.filter, tc.rowKey)
		})
	}
}
