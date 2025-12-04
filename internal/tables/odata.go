package tables

import (
	"fmt"
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

// MatchesFilter checks if an entity matches the OData filter
func MatchesFilter(filter string, entity map[string]interface{}) bool {
	if filter == "" {
		return true
	}

	// Simplified filter parser - handles basic equality and logical operators
	// Real implementation would handle full OData syntax
	filter = strings.TrimSpace(filter)

	// Split by 'and' / 'or'
	if strings.Contains(strings.ToLower(filter), " and ") {
		parts := strings.Split(strings.ToLower(filter), " and ")
		for _, part := range parts {
			if !evaluateSimpleFilter(part, entity) {
				return false
			}
		}
		return true
	}

	if strings.Contains(strings.ToLower(filter), " or ") {
		parts := strings.Split(strings.ToLower(filter), " or ")
		for _, part := range parts {
			if evaluateSimpleFilter(part, entity) {
				return true
			}
		}
		return false
	}

	return evaluateSimpleFilter(filter, entity)
}

// evaluateSimpleFilter evaluates a single filter expression
func evaluateSimpleFilter(filter string, entity map[string]interface{}) bool {
	// Handle comparison operators: eq, ne, lt, le, gt, ge
	operators := []string{"eq", "ne", "lt", "le", "gt", "ge"}

	for _, op := range operators {
		if strings.Contains(filter, " "+op+" ") {
			parts := strings.Split(filter, " "+op+" ")
			if len(parts) != 2 {
				continue
			}

			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			// Remove quotes from right side if present
			if strings.HasPrefix(right, "'") && strings.HasSuffix(right, "'") {
				right = right[1 : len(right)-1]
			}

			// Get value from entity
			val, ok := entity[left]
			if !ok {
				return false
			}

			valStr := fmt.Sprintf("%v", val)

			switch op {
			case "eq":
				return valStr == right
			case "ne":
				return valStr != right
			case "lt":
				return valStr < right
			case "le":
				return valStr <= right
			case "gt":
				return valStr > right
			case "ge":
				return valStr >= right
			}
		}
	}

	return true
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
