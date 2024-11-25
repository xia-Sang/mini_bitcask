package sql

import (
	"fmt"
	"testing"
)

func TestMarshalTableSchema(t *testing.T) {
	// Initialize the database
	db := &RDBMS{
		Tables: make(map[string]*TableSchema),
	}

	// Define columns as a map
	columns := map[string]FieldType{
		"id":    FieldTypeInt,
		"name":  FieldTypeString,
		"email": FieldTypeString,
	}

	// Create a table using the map
	err := db.CreateTableByMap("users", columns)
	if err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		return
	}

	// Print the created table schema
	fmt.Printf("Created Table Schema: %+v\n", db.Tables["users"])
}
func TestUnmarshalTableSchemaWithTypes(t *testing.T) {
	t.Log("test")
}
