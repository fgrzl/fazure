package tables

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustMatch(t *testing.T, filter string, entity map[string]interface{}) bool {
	t.Helper()
	result, err := MatchesFilter(filter, entity)
	require.NoError(t, err)
	return result
}

func TestShouldReturnErrorGivenUnsupportedFilterFunction(t *testing.T) {
	entity := map[string]interface{}{
		"PartitionKey": "pk",
		"RowKey":       "rk",
		"Name":         "Alice",
	}

	_, err := MatchesFilter("contains(Name,'A')", entity)
	require.ErrorIs(t, err, ErrInvalidFilter)
}

// ============================================================================
// MatchesFilter Tests
// ============================================================================

func TestShouldMatchAllEntitiesGivenEmptyFilter(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
	}

	// Act
	result := mustMatch(t, "", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldMatchEntityGivenEqualStringFilter(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Status":       "active",
	}

	// Act
	result := mustMatch(t, "Status eq 'active'", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldNotMatchEntityGivenNonMatchingStringFilter(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Status":       "active",
	}

	// Act
	result := mustMatch(t, "Status eq 'inactive'", entity)

	// Assert
	assert.False(t, result)
}

func TestShouldMatchEntityGivenEqualNumericFilter(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Score":        float64(100),
	}

	// Act
	result := mustMatch(t, "Score eq 100", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldMatchEntityGivenNotEqualFilter(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Type":         "A",
	}

	// Act
	result := mustMatch(t, "Type ne 'B'", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldNotMatchEntityGivenNotEqualFilterWithSameValue(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Type":         "A",
	}

	// Act
	result := mustMatch(t, "Type ne 'A'", entity)

	// Assert
	assert.False(t, result)
}

func TestShouldMatchEntityGivenGreaterThanFilter(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Age":          float64(30),
	}

	// Act
	result := mustMatch(t, "Age gt 20", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldNotMatchEntityGivenGreaterThanFilterWithEqualValue(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Age":          float64(30),
	}

	// Act
	result := mustMatch(t, "Age gt 30", entity)

	// Assert
	assert.False(t, result)
}

func TestShouldMatchEntityGivenGreaterThanOrEqualFilter(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Age":          float64(30),
	}

	// Act
	result := mustMatch(t, "Age ge 30", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldMatchEntityGivenLessThanFilter(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Age":          float64(30),
	}

	// Act
	result := mustMatch(t, "Age lt 40", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldNotMatchEntityGivenLessThanFilterWithEqualValue(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Age":          float64(30),
	}

	// Act
	result := mustMatch(t, "Age lt 30", entity)

	// Assert
	assert.False(t, result)
}

func TestShouldMatchEntityGivenLessThanOrEqualFilter(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Age":          float64(30),
	}

	// Act
	result := mustMatch(t, "Age le 30", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldMatchEntityGivenAndOperatorWithBothConditionsTrue(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Status":       "active",
		"Priority":     float64(1),
	}

	// Act
	result := mustMatch(t, "Status eq 'active' and Priority eq 1", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldNotMatchEntityGivenAndOperatorWithOneConditionFalse(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Status":       "active",
		"Priority":     float64(1),
	}

	// Act
	result := mustMatch(t, "Status eq 'active' and Priority eq 2", entity)

	// Assert
	assert.False(t, result)
}

func TestShouldMatchEntityGivenOrOperatorWithOneConditionTrue(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Status":       "pending",
	}

	// Act
	result := mustMatch(t, "Status eq 'active' or Status eq 'pending'", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldNotMatchEntityGivenOrOperatorWithBothConditionsFalse(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Status":       "inactive",
	}

	// Act
	result := mustMatch(t, "Status eq 'active' or Status eq 'pending'", entity)

	// Assert
	assert.False(t, result)
}

func TestShouldMatchEntityGivenUppercaseAndOperator(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Status":       "active",
		"Priority":     float64(1),
	}

	// Act
	result := mustMatch(t, "Status eq 'active' AND Priority eq 1", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldNotMatchEntityGivenMissingProperty(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
	}

	// Act
	result := mustMatch(t, "Status eq 'active'", entity)

	// Assert
	assert.False(t, result)
}

func TestShouldMatchEntityGivenPartitionKeyFilter(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "partition1",
		"RowKey":       "row1",
	}

	// Act
	result := mustMatch(t, "PartitionKey eq 'partition1'", entity)

	// Assert
	assert.True(t, result)
}

func TestShouldMatchEntityGivenRowKeyFilter(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "partition1",
		"RowKey":       "row1",
	}

	// Act
	result := mustMatch(t, "RowKey eq 'row1'", entity)

	// Assert
	assert.True(t, result)
}

// ============================================================================
// extractPartitionKeyFromFilter Tests
// ============================================================================

func TestShouldExtractPartitionKeyGivenSimpleFilter(t *testing.T) {
	// Arrange
	filter := "PartitionKey eq 'mypartition'"

	// Act
	result := extractPartitionKeyFromFilter(filter)

	// Assert
	assert.Equal(t, "mypartition", result)
}

func TestShouldExtractPartitionKeyGivenCombinedFilter(t *testing.T) {
	// Arrange
	filter := "PartitionKey eq 'mypartition' and Status eq 'active'"

	// Act
	result := extractPartitionKeyFromFilter(filter)

	// Assert
	assert.Equal(t, "mypartition", result)
}

func TestShouldExtractPartitionKeyGivenFilterWithPartitionKeyNotFirst(t *testing.T) {
	// Arrange
	filter := "Status eq 'active' and PartitionKey eq 'mypartition'"

	// Act
	result := extractPartitionKeyFromFilter(filter)

	// Assert
	assert.Equal(t, "mypartition", result)
}

func TestShouldExtractPartitionKeyGivenHexEncodedValue(t *testing.T) {
	// Arrange - exact filter from production logs
	filter := "PartitionKey eq '53010069030068796464656e'"

	// Act
	result := extractPartitionKeyFromFilter(filter)

	// Assert
	assert.Equal(t, "53010069030068796464656e", result)
}

func TestShouldReturnEmptyStringGivenFilterWithoutPartitionKey(t *testing.T) {
	// Arrange
	filter := "Status eq 'active'"

	// Act
	result := extractPartitionKeyFromFilter(filter)

	// Assert
	assert.Equal(t, "", result)
}

func TestShouldReturnEmptyStringGivenEmptyFilter(t *testing.T) {
	// Arrange
	filter := ""

	// Act
	result := extractPartitionKeyFromFilter(filter)

	// Assert
	assert.Equal(t, "", result)
}

func TestShouldExtractPartitionKeyGivenLowercaseFilter(t *testing.T) {
	// Arrange
	filter := "partitionkey eq 'mypartition'"

	// Act
	result := extractPartitionKeyFromFilter(filter)

	// Assert
	assert.Equal(t, "mypartition", result)
}

func TestShouldPreservePartitionKeyValueCase(t *testing.T) {
	// Arrange
	filter := "PartitionKey eq 'MyPartition'"

	// Act
	result := extractPartitionKeyFromFilter(filter)

	// Assert
	assert.Equal(t, "MyPartition", result)
}

// ============================================================================
// SelectFields Tests
// ============================================================================

func TestShouldReturnAllFieldsGivenEmptyFieldsList(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Name":         "Alice",
	}

	// Act
	result := SelectFields(entity, []string{})

	// Assert
	assert.Equal(t, entity, result)
}

func TestShouldReturnOnlySelectedFields(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
		"Name":         "Alice",
		"Age":          30,
		"City":         "Seattle",
	}

	// Act
	result := SelectFields(entity, []string{"Name", "Age"})

	// Assert
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "Alice", result["Name"])
	assert.Equal(t, 30, result["Age"])
	assert.NotContains(t, result, "City")
}

func TestShouldReturnEmptyMapGivenNonExistentFields(t *testing.T) {
	// Arrange
	entity := map[string]interface{}{
		"PartitionKey": "pk1",
		"RowKey":       "rk1",
	}

	// Act
	result := SelectFields(entity, []string{"NonExistent"})

	// Assert
	assert.Equal(t, 0, len(result))
}

// ============================================================================
// ParseODataQuery Tests
// ============================================================================

func TestShouldParseAllQueryParameters(t *testing.T) {
	// Arrange
	filterStr := "Status eq 'active'"
	selectStr := "Name,Age"
	top := 10

	// Act
	query := ParseODataQuery(filterStr, selectStr, top)

	// Assert
	assert.Equal(t, "Status eq 'active'", query.Filter)
	assert.Equal(t, []string{"Name", "Age"}, query.Select)
	assert.Equal(t, 10, query.Top)
}

func TestShouldHandleEmptySelectString(t *testing.T) {
	// Arrange
	filterStr := "Status eq 'active'"
	selectStr := ""
	top := 10

	// Act
	query := ParseODataQuery(filterStr, selectStr, top)

	// Assert
	assert.Nil(t, query.Select)
}

func TestShouldTrimSpacesFromSelectFields(t *testing.T) {
	// Arrange
	filterStr := ""
	selectStr := "Name , Age , City"
	top := 0

	// Act
	query := ParseODataQuery(filterStr, selectStr, top)

	// Assert
	assert.Equal(t, []string{"Name", "Age", "City"}, query.Select)
}

// ============================================================================
// compareValues Tests
// ============================================================================

func TestShouldReturnZeroGivenEqualStrings(t *testing.T) {
	// Arrange
	left := "abc"
	right := "abc"

	// Act
	result := compareValues(left, right)

	// Assert
	assert.Equal(t, 0, result)
}

func TestShouldReturnNegativeGivenLeftStringLessThanRight(t *testing.T) {
	// Arrange
	left := "abc"
	right := "def"

	// Act
	result := compareValues(left, right)

	// Assert
	assert.Equal(t, -1, result)
}

func TestShouldReturnPositiveGivenLeftStringGreaterThanRight(t *testing.T) {
	// Arrange
	left := "def"
	right := "abc"

	// Act
	result := compareValues(left, right)

	// Assert
	assert.Equal(t, 1, result)
}

func TestShouldReturnZeroGivenEqualNumbers(t *testing.T) {
	// Arrange
	left := float64(10)
	right := float64(10)

	// Act
	result := compareValues(left, right)

	// Assert
	assert.Equal(t, 0, result)
}

func TestShouldCompareNumericStringToNumber(t *testing.T) {
	// Arrange
	left := float64(10)
	right := "10"

	// Act
	result := compareValues(left, right)

	// Assert
	assert.Equal(t, 0, result)
}

// ============================================================================
// toFloat64 Tests
// ============================================================================

func TestShouldConvertFloat64ToFloat64(t *testing.T) {
	// Arrange
	input := float64(3.14)

	// Act
	result, ok := toFloat64(input)

	// Assert
	assert.True(t, ok)
	assert.Equal(t, 3.14, result)
}

func TestShouldConvertIntToFloat64(t *testing.T) {
	// Arrange
	input := int(42)

	// Act
	result, ok := toFloat64(input)

	// Assert
	assert.True(t, ok)
	assert.Equal(t, float64(42), result)
}

func TestShouldNotConvertNumericStringToFloat64(t *testing.T) {
	// Arrange - strings should not be converted to numbers
	// This ensures hex-encoded strings like "00000000000000e9" aren't
	// incorrectly parsed as scientific notation (0e9)
	input := "3.14"

	// Act
	_, ok := toFloat64(input)

	// Assert - strings should NOT be converted to float64
	assert.False(t, ok, "String values should not be converted to float64")
}

func TestShouldReturnFalseGivenNonNumericString(t *testing.T) {
	// Arrange
	input := "not a number"

	// Act
	_, ok := toFloat64(input)

	// Assert
	assert.False(t, ok)
}

func TestShouldReturnFalseGivenNilValue(t *testing.T) {
	// Arrange
	var input interface{} = nil

	// Act
	_, ok := toFloat64(input)

	// Assert
	assert.False(t, ok)
}

// ============================================================================
// splitPreservingCase Tests
// ============================================================================

func TestShouldSplitStringPreservingCase(t *testing.T) {
	// Arrange
	input := "Hello AND World"
	separator := " and "

	// Act
	result := splitPreservingCase(input, separator)

	// Assert
	assert.Equal(t, []string{"Hello", "World"}, result)
}

func TestShouldSplitMultipleOccurrences(t *testing.T) {
	// Arrange
	input := "One And Two AND Three"
	separator := " and "

	// Act
	result := splitPreservingCase(input, separator)

	// Assert
	assert.Equal(t, []string{"One", "Two", "Three"}, result)
}

func TestShouldReturnSingleElementGivenNoSeparator(t *testing.T) {
	// Arrange
	input := "NoSeparator"
	separator := " and "

	// Act
	result := splitPreservingCase(input, separator)

	// Assert
	assert.Equal(t, []string{"NoSeparator"}, result)
}

// ============================================================================
// findOperatorIndex Tests
// ============================================================================

func TestShouldFindOperatorIndexCaseInsensitive(t *testing.T) {
	// Arrange
	input := "Status eq 'active'"
	operator := " eq "

	// Act
	result := findOperatorIndex(input, operator)

	// Assert
	assert.Equal(t, 6, result)
}

func TestShouldFindUppercaseOperator(t *testing.T) {
	// Arrange
	input := "Status EQ 'active'"
	operator := " eq "

	// Act
	result := findOperatorIndex(input, operator)

	// Assert
	assert.Equal(t, 6, result)
}

func TestShouldReturnNegativeOneGivenMissingOperator(t *testing.T) {
	// Arrange
	input := "Status active"
	operator := " eq "

	// Act
	result := findOperatorIndex(input, operator)

	// Assert
	assert.Equal(t, -1, result)
}
