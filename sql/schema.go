package sql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// 字段定义 目前不添加任何限制条件
type Field struct {
	Name string
	Type FieldType // 字段类型，例如 "string", "int", "bytes"
}

// table schema
type TableSchema struct {
	Name    string               `json:"name"`
	Columns map[string]FieldType `json:"columns"` // Column names and their types
	Indexes []string             `json:"indexes"`
}

// FieldType represents the type of a field in a table schema
type FieldType = byte

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

// MarshalJSON converts numeric FieldType to string representations for JSON serialization.
func (ts TableSchema) MarshalJSON() ([]byte, error) {
	columns := make(map[string]string)
	for colName, fieldType := range ts.Columns {
		typeName, ok := FieldTypeNames[fieldType]
		if !ok {
			return nil, fmt.Errorf("unsupported FieldType: %d", fieldType)
		}
		columns[colName] = typeName
	}

	// Alias the TableSchema to avoid infinite recursion during Marshal
	type Alias TableSchema
	return json.Marshal(&struct {
		Columns map[string]string `json:"columns"`
		*Alias
	}{
		Columns: columns,
		Alias:   (*Alias)(&ts),
	})
}

// UnmarshalJSON converts string FieldType representations to numeric FieldType for JSON deserialization.
func (ts *TableSchema) UnmarshalJSON(data []byte) error {
	// Alias the TableSchema to avoid infinite recursion during Unmarshal
	type Alias TableSchema
	aux := &struct {
		Columns map[string]string `json:"columns"`
		*Alias
	}{
		Alias: (*Alias)(ts),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return fmt.Errorf("failed to unmarshal TableSchema: %v", err)
	}

	// Convert string field types back to numeric FieldType
	ts.Columns = make(map[string]FieldType)
	for colName, typeName := range aux.Columns {
		var found bool
		for key, val := range FieldTypeNames {
			if val == typeName {
				ts.Columns[colName] = key
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("invalid FieldType name: %s", typeName)
		}
	}

	return nil
}
