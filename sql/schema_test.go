package sql

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalTableSchema(t *testing.T) {
	// Create a table schema
	schema := TableSchema{
		Name: "users",
		Columns: map[string]FieldType{
			"id":    FieldTypeInt,
			"name":  FieldTypeString,
			"email": FieldTypeString,
			"age":   FieldTypeInt,
		},
		Indexes: []string{"id", "email"},
	}

	// Serialize to JSON
	jsonData, err := json.MarshalIndent(schema, "", "  ")
	assert.Nil(t, err)
	t.Logf("Serialized JSON:\n%s", string(jsonData))
}
func TestUnmarshalTableSchemaWithTypes(t *testing.T) {
	// JSON input
	jsonInput := `{
		"name": "users",
		"columns": {
			"age": "INTEGER",
			"email": "STRING",
			"id": "INTEGER",
			"name": "STRING"
		},
		"indexes": ["id", "email"]
	}`

	// Deserialize JSON
	var schema TableSchema
	err := json.Unmarshal([]byte(jsonInput), &schema)
	assert.Nil(t, err)

	// Verify the structure
	assert.Equal(t, "users", schema.Name)
	assert.Equal(t, FieldTypeInt, schema.Columns["age"])
	assert.Equal(t, FieldTypeString, schema.Columns["email"])
	assert.Equal(t, []string{"id", "email"}, schema.Indexes)
	t.Log(schema)
}
