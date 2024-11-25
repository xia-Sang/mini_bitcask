package sql

import "fmt"

// CreateTable creates a table schema with the specified field names and types.
func (db *RDBMS) CreateTable(name string, fields []string, types []FieldType) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if the table already exists
	if _, exists := db.Tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	// Validate input lengths
	if len(fields) != len(types) {
		return fmt.Errorf("fields and types must have the same length")
	}

	// Build the column name-to-index mapping
	colMaps := make(map[string]int, len(fields))
	for i, field := range fields {
		colMaps[field] = i
	}

	// Create and store the table schema
	db.Tables[name] = &TableSchema{
		Name:       name,
		FieldTypes: types,   // Directly store the field types
		Columns:    fields,  // Store the column names
		colMaps:    colMaps, // Store the mapping for quick lookups
	}

	return nil
}

// CreateTableByMap creates a table schema with a map of field names and types.
func (db *RDBMS) CreateTableByMap(name string, columns map[string]FieldType) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if the table already exists
	if _, exists := db.Tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	// Initialize slices and colMaps in one pass
	fields := make([]string, 0, len(columns))
	types := make([]FieldType, 0, len(columns))
	colMaps := make(map[string]int, len(columns))

	i := 0
	for field, fieldType := range columns {
		fields = append(fields, field)
		types = append(types, fieldType)
		colMaps[field] = i
		i++
	}

	// Create and store the table schema
	db.Tables[name] = &TableSchema{
		Name:       name,
		FieldTypes: types,
		Columns:    fields,
		colMaps:    colMaps,
	}

	return nil
}
