package sql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Field defines the structure of a column
type Field struct {
	Name string    `json:"name"` // Column name
	Type FieldType `json:"type"` // Column data type
}

// TableSchema defines the structure of a table
type TableSchema struct {
	Name       string         `json:"name"`         // Table name
	FieldTypes []FieldType    `json:"columns_type"` // Field types corresponding to columns
	Columns    []string       `json:"columns"`      // List of column names
	colMaps    map[string]int // Column name to position mapping (not serialized)
}

// FieldType represents the type of a field in a table schema
type FieldType byte

// Supported field types
const (
	FieldTypeString    FieldType = iota // VARCHAR or TEXT
	FieldTypeInt                        // INT
	FieldTypeBytes                      // BYTEA or BLOB
	FieldTypeFloat                      // FLOAT or DOUBLE
	FieldTypeBool                       // BOOLEAN
	FieldTypeDate                       // DATE
	FieldTypeTime                       // TIME
	FieldTypeTimestamp                  // TIMESTAMP
)

// FieldTypeNames maps field type constants to human-readable names
var FieldTypeNames = map[FieldType]string{
	FieldTypeString:    "STRING",
	FieldTypeInt:       "INTEGER",
	FieldTypeBytes:     "BYTES",
	FieldTypeFloat:     "FLOAT",
	FieldTypeBool:      "BOOLEAN",
	FieldTypeDate:      "DATE",
	FieldTypeTime:      "TIME",
	FieldTypeTimestamp: "TIMESTAMP",
}

// validateFieldType checks that the provided value matches the expected field type.
func validateFieldType(fieldType FieldType, value []byte) error {
	switch fieldType {
	case FieldTypeString:
		// Strings don't require additional validation
		return nil
	case FieldTypeInt:
		// Ensure the value can be parsed as an integer
		if _, err := strconv.Atoi(string(value)); err != nil {
			return fmt.Errorf("expected an integer, got '%s'", value)
		}
	case FieldTypeFloat:
		// Ensure the value can be parsed as a float
		if _, err := strconv.ParseFloat(string(value), 64); err != nil {
			return fmt.Errorf("expected a float, got '%s'", value)
		}
	case FieldTypeBool:
		// Ensure the value is "true" or "false"
		val := strings.ToLower(string(value))
		if val != "true" && val != "false" {
			return fmt.Errorf("expected a boolean ('true' or 'false'), got '%s'", value)
		}
	case FieldTypeDate:
		// Ensure the value matches the date format
		if _, err := time.Parse("2006-01-02", string(value)); err != nil {
			return fmt.Errorf("expected a date (YYYY-MM-DD), got '%s'", value)
		}
	case FieldTypeTime:
		// Ensure the value matches the time format
		if _, err := time.Parse("15:04:05", string(value)); err != nil {
			return fmt.Errorf("expected a time (HH:MM:SS), got '%s'", value)
		}
	case FieldTypeTimestamp:
		// Ensure the value matches the timestamp format
		if _, err := time.Parse("2006-01-02 15:04:05", string(value)); err != nil {
			return fmt.Errorf("expected a timestamp (YYYY-MM-DD HH:MM:SS), got '%s'", value)
		}
	case FieldTypeBytes:
		// Bytes don't require additional validation
		return nil
	default:
		return fmt.Errorf("unsupported field type: %d", fieldType)
	}
	return nil
}

// GetColumnPosition retrieves the position of a column by name
func (t *TableSchema) getColumnPosition(columnName string) (int, error) {
	pos, exists := t.colMaps[columnName]
	if !exists {
		return -1, fmt.Errorf("column %s does not exist in table %s", columnName, t.Name)
	}
	return pos, nil
}

// resolveFieldType resolves a string field type name to its numeric FieldType value.
func resolveFieldType(typeName string) (FieldType, bool) {
	for fieldType, name := range FieldTypeNames {
		if name == typeName {
			return fieldType, true
		}
	}
	return 0, false
}
