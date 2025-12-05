package tables

import (
	"fmt"
	"strconv"
	"strings"
)

// ODataQuery represents parsed OData query parameters
type ODataQuery struct {
	Filter string
	Select []string
	Top    int
}

// ParseODataQuery parses OData query parameters
func ParseODataQuery(filterStr, selectStr string, top int) *ODataQuery {
	q := &ODataQuery{
		Filter: filterStr,
		Top:    top,
	}

	if selectStr != "" {
		q.Select = strings.Split(selectStr, ",")
		for i := range q.Select {
			q.Select[i] = strings.TrimSpace(q.Select[i])
		}
	}

	return q
}

// MatchesFilter checks if an entity matches the OData filter.
// Returns ErrInvalidFilter when the filter expression is unsupported or malformed,
// mirroring Azure Tables behavior of rejecting invalid $filter values.
func MatchesFilter(filter string, entity map[string]interface{}) (bool, error) {
	if filter == "" {
		return true, nil
	}

	// Simplified filter parser - handles basic equality and logical operators
	filter = strings.TrimSpace(filter)

	// Handle 'and' operator (case insensitive)
	andIndex := findOperatorIndex(filter, " and ")
	if andIndex >= 0 {
		left := strings.TrimSpace(filter[:andIndex])
		right := strings.TrimSpace(filter[andIndex+5:])
		l, err := MatchesFilter(left, entity)
		if err != nil {
			return false, err
		}
		if !l {
			return false, nil
		}
		return MatchesFilter(right, entity)
	}

	// Handle 'or' operator (case insensitive)
	orIndex := findOperatorIndex(filter, " or ")
	if orIndex >= 0 {
		left := strings.TrimSpace(filter[:orIndex])
		right := strings.TrimSpace(filter[orIndex+4:])
		l, err := MatchesFilter(left, entity)
		if err != nil {
			return false, err
		}
		if l {
			return true, nil
		}
		return MatchesFilter(right, entity)
	}

	return evaluateSimpleFilter(filter, entity)
}

// findOperatorIndex finds the index of an operator (case insensitive)
func findOperatorIndex(s, op string) int {
	lower := strings.ToLower(s)
	lowerOp := strings.ToLower(op)
	return strings.Index(lower, lowerOp)
}

// evaluateSimpleFilter evaluates a single filter expression.
// Returns ErrInvalidFilter when no supported operator is found.
func evaluateSimpleFilter(filter string, entity map[string]interface{}) (bool, error) {
	filter = strings.TrimSpace(filter)

	// Handle comparison operators: eq, ne, lt, le, gt, ge
	operators := []struct {
		op   string
		eval func(left, right interface{}) bool
	}{
		{"eq", func(l, r interface{}) bool { return compareValues(l, r) == 0 }},
		{"ne", func(l, r interface{}) bool { return compareValues(l, r) != 0 }},
		{"lt", func(l, r interface{}) bool { return compareValues(l, r) < 0 }},
		{"le", func(l, r interface{}) bool { return compareValues(l, r) <= 0 }},
		{"gt", func(l, r interface{}) bool { return compareValues(l, r) > 0 }},
		{"ge", func(l, r interface{}) bool { return compareValues(l, r) >= 0 }},
	}

	for _, op := range operators {
		idx := findOperatorIndex(filter, " "+op.op+" ")
		if idx >= 0 {
			left := strings.TrimSpace(filter[:idx])
			right := strings.TrimSpace(filter[idx+len(op.op)+2:])

			if strings.HasPrefix(right, "'") && strings.HasSuffix(right, "'") {
				right = right[1 : len(right)-1]
			}

			val, ok := entity[left]
			if !ok {
				return false, nil
			}

			return op.eval(val, right), nil
		}
	}

	return false, fmt.Errorf("unsupported or invalid filter expression: %q: %w", filter, ErrInvalidFilter)
}

// compareValues compares two values, handling type conversion
func compareValues(left, right interface{}) int {
	// Try numeric comparison first
	leftNum, leftIsNum := toFloat64(left)
	rightNum, rightIsNum := toFloat64(right)

	if leftIsNum && rightIsNum {
		if leftNum < rightNum {
			return -1
		} else if leftNum > rightNum {
			return 1
		}
		return 0
	}

	// Fall back to string comparison
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)

	if leftStr < rightStr {
		return -1
	} else if leftStr > rightStr {
		return 1
	}
	return 0
}

// toFloat64 attempts to convert a value to float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// SelectFields returns only selected fields from entity
func SelectFields(entity map[string]interface{}, fields []string) map[string]interface{} {
	if len(fields) == 0 {
		return entity
	}

	result := make(map[string]interface{})
	for _, field := range fields {
		if val, ok := entity[field]; ok {
			result[field] = val
		}
	}
	return result
}

// extractPartitionKeyFromFilter extracts PartitionKey equality value from OData filter
// This allows optimizing queries by scanning only the relevant partition
func extractPartitionKeyFromFilter(filter string) string {
	if filter == "" {
		return ""
	}

	filter = strings.TrimSpace(filter)

	// Look for PartitionKey eq 'value' pattern
	// Handle both standalone and AND-combined filters
	parts := strings.Split(strings.ToLower(filter), " and ")
	originalParts := splitPreservingCase(filter, " and ")

	for i, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "partitionkey eq ") {
			// Extract the value from the original (case-preserved) part
			origPart := strings.TrimSpace(originalParts[i])
			idx := findOperatorIndex(origPart, " eq ")
			if idx >= 0 {
				value := strings.TrimSpace(origPart[idx+4:])
				// Remove quotes
				if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
					return value[1 : len(value)-1]
				}
			}
		}
	}

	return ""
}

// splitPreservingCase splits a string by separator (case-insensitive) while preserving original case
func splitPreservingCase(s, sep string) []string {
	lowerS := strings.ToLower(s)
	lowerSep := strings.ToLower(sep)

	var result []string
	start := 0
	for {
		idx := strings.Index(lowerS[start:], lowerSep)
		if idx == -1 {
			result = append(result, s[start:])
			break
		}
		result = append(result, s[start:start+idx])
		start = start + idx + len(sep)
	}
	return result
}
