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

	// Strip surrounding parentheses if they enclose the whole expression
	if strings.HasPrefix(filter, "(") && strings.HasSuffix(filter, ")") {
		// check balance
		depth := 0
		balanced := true
		for i := 0; i < len(filter); i++ {
			c := filter[i]
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
				if depth == 0 && i != len(filter)-1 {
					balanced = false
					break
				}
			}
		}
		if balanced {
			return MatchesFilter(strings.TrimSpace(filter[1:len(filter)-1]), entity)
		}
	}

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
// It skips operators that are inside parentheses.
// The operator can be provided with or without surrounding spaces; this function
// tolerates either and ensures the operator is matched as a standalone token.
func findOperatorIndex(s, op string) int {
	lower := strings.ToLower(s)
	lowerOp := strings.ToLower(op)
	opLen := len(lowerOp)
	depth := 0
	for i := 0; i <= len(lower)-opLen; i++ {
		c := lower[i]
		switch c {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && i+opLen <= len(lower) && lower[i:i+opLen] == lowerOp {
			// Determine surrounding character constraints.
			// If the operator string starts/ends with a space, skip checking that side
			beforeOK := true
			afterOK := true
			if !strings.HasPrefix(lowerOp, " ") {
				beforeOK = i == 0 || isSeparator(lower[i-1])
			}
			if !strings.HasSuffix(lowerOp, " ") {
				afterOK = i+opLen == len(lower) || isSeparator(lower[i+opLen])
			}
			if beforeOK && afterOK {
				return i
			}
		}
	}
	return -1
}

func isSeparator(c byte) bool {
	return c == ' ' || c == '(' || c == ')'
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

			// Validate left and right are not empty
			if left == "" || right == "" {
				return false, fmt.Errorf("incomplete filter expression: %q: %w", filter, ErrInvalidFilter)
			}

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

// PartitionKeyHint represents extracted partition key filter information
type PartitionKeyHint struct {
	Exact      string // Exact match value from "PartitionKey eq 'value'"
	RangeStart string // Start value from "PartitionKey ge 'value'" or "PartitionKey gt 'value'"
	RangeEnd   string // End value from "PartitionKey le 'value'" or "PartitionKey lt 'value'"
	UseRange   bool   // True if range operators were found
}

// RowKeyHint represents extracted row key filter information
type RowKeyHint struct {
	Exact      string // Exact match value from "RowKey eq 'value'"
	RangeStart string // Start value from "RowKey ge 'value'" or "RowKey gt 'value'"
	RangeEnd   string // End value from "RowKey le 'value'" or "RowKey lt 'value'"
	UseRange   bool   // True if range operators were found
}

// extractPartitionKeyFromFilter extracts PartitionKey filter information from OData filter
// This allows optimizing queries by scanning only the relevant partition or partition range
func extractPartitionKeyFromFilter(filter string) string {
	hint := extractPartitionKeyHint(filter)
	if hint.Exact != "" {
		return hint.Exact
	}
	// For now, if we only have range queries, return the range start
	// This allows prefix optimization for range queries like "PartitionKey ge 'X' and PartitionKey le 'Y'"
	if hint.UseRange && hint.RangeStart != "" {
		return hint.RangeStart
	}
	return ""
}

// extractPartitionKeyHint extracts detailed PartitionKey filter information
func extractPartitionKeyHint(filter string) PartitionKeyHint {
	hint := PartitionKeyHint{}

	if filter == "" {
		return hint
	}

	filter = strings.TrimSpace(filter)

	// Look for PartitionKey patterns
	// Handle both standalone and AND-combined filters
	parts := strings.Split(strings.ToLower(filter), " and ")
	originalParts := splitPreservingCase(filter, " and ")

	for i, part := range parts {
		part = strings.TrimSpace(part)
		origPart := strings.TrimSpace(originalParts[i])

		// Check for PartitionKey eq 'value'
		if strings.HasPrefix(part, "partitionkey eq ") {
			idx := findOperatorIndex(origPart, " eq ")
			if idx >= 0 {
				value := strings.TrimSpace(origPart[idx+4:])
				if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
					hint.Exact = value[1 : len(value)-1]
					return hint // Exact match takes precedence
				}
			}
		}

		// Check for PartitionKey ge 'value' or PartitionKey gt 'value'
		if strings.HasPrefix(part, "partitionkey ge ") || strings.HasPrefix(part, "partitionkey gt ") {
			var idx int
			if strings.HasPrefix(part, "partitionkey ge ") {
				idx = findOperatorIndex(origPart, " ge ")
			} else {
				idx = findOperatorIndex(origPart, " gt ")
			}
			if idx >= 0 {
				opLen := 4 // length of " ge " or " gt "
				value := strings.TrimSpace(origPart[idx+opLen:])
				if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
					hint.RangeStart = value[1 : len(value)-1]
					hint.UseRange = true
				}
			}
		}

		// Check for PartitionKey le 'value' or PartitionKey lt 'value'
		if strings.HasPrefix(part, "partitionkey le ") || strings.HasPrefix(part, "partitionkey lt ") {
			var idx int
			if strings.HasPrefix(part, "partitionkey le ") {
				idx = findOperatorIndex(origPart, " le ")
			} else {
				idx = findOperatorIndex(origPart, " lt ")
			}
			if idx >= 0 {
				opLen := 4 // length of " le " or " lt "
				value := strings.TrimSpace(origPart[idx+opLen:])
				if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
					hint.RangeEnd = value[1 : len(value)-1]
					hint.UseRange = true
				}
			}
		}
	}

	return hint
}

// extractRowKeyHint extracts detailed RowKey filter information
func extractRowKeyHint(filter string) RowKeyHint {
	hint := RowKeyHint{}

	if filter == "" {
		return hint
	}

	filter = strings.TrimSpace(filter)

	// Look for RowKey patterns
	parts := strings.Split(strings.ToLower(filter), " and ")
	originalParts := splitPreservingCase(filter, " and ")

	for i, part := range parts {
		part = strings.TrimSpace(part)
		origPart := strings.TrimSpace(originalParts[i])

		// Check for RowKey eq 'value'
		if strings.HasPrefix(part, "rowkey eq ") {
			idx := findOperatorIndex(origPart, " eq ")
			if idx >= 0 {
				value := strings.TrimSpace(origPart[idx+4:])
				if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
					hint.Exact = value[1 : len(value)-1]
					return hint // Exact match takes precedence
				}
			}
		}

		// Check for RowKey ge 'value' or RowKey gt 'value'
		if strings.HasPrefix(part, "rowkey ge ") || strings.HasPrefix(part, "rowkey gt ") {
			var idx int
			if strings.HasPrefix(part, "rowkey ge ") {
				idx = findOperatorIndex(origPart, " ge ")
			} else {
				idx = findOperatorIndex(origPart, " gt ")
			}
			if idx >= 0 {
				opLen := 4 // length of " ge " or " gt "
				value := strings.TrimSpace(origPart[idx+opLen:])
				if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
					hint.RangeStart = value[1 : len(value)-1]
					hint.UseRange = true
				}
			}
		}

		// Check for RowKey le 'value' or RowKey lt 'value'
		if strings.HasPrefix(part, "rowkey le ") || strings.HasPrefix(part, "rowkey lt ") {
			var idx int
			if strings.HasPrefix(part, "rowkey le ") {
				idx = findOperatorIndex(origPart, " le ")
			} else {
				idx = findOperatorIndex(origPart, " lt ")
			}
			if idx >= 0 {
				opLen := 4 // length of " le " or " lt "
				value := strings.TrimSpace(origPart[idx+opLen:])
				if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
					hint.RangeEnd = value[1 : len(value)-1]
					hint.UseRange = true
				}
			}
		}
	}

	return hint
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
