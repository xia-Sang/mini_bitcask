package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenericCompareValues(t *testing.T) {
	// String comparison
	result, err := CompareValues(FieldTypeString, []byte("Alice"), []byte("Bob"), "<")
	assert.Nil(t, err)
	assert.True(t, result)

	// Integer comparison
	result, err = CompareValues(FieldTypeInt, []byte("10"), []byte("20"), "<=")
	assert.Nil(t, err)
	assert.True(t, result)

	// Float comparison
	result, err = CompareValues(FieldTypeFloat, []byte("15.5"), []byte("15.0"), ">")
	assert.Nil(t, err)
	assert.True(t, result)

	// Boolean comparison
	result, err = CompareValues(FieldTypeBool, []byte("true"), []byte("false"), "!=")
	assert.Nil(t, err)
	assert.True(t, result)

	// Date comparison
	result, err = CompareValues(FieldTypeDate, []byte("2023-01-01"), []byte("2023-01-02"), "<")
	assert.Nil(t, err)
	assert.True(t, result)

	// Invalid comparison
	result, err = CompareValues(FieldTypeInt, []byte("abc"), []byte("123"), ">")
	assert.NotNil(t, err)
}
