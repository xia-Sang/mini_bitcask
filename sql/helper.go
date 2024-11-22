package sql

import (
	"encoding/json"
	"fmt"
	"os"
)

// SerializeTables serializes the Tables map to JSON.
func SerializeTables(table map[string]*TableSchema) ([]byte, error) {
	data, err := json.Marshal(table)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize tables: %v", err)
	}
	return data, nil
}

// DeserializeTables deserializes JSON data back into the Tables map.
func DeserializeTables(data []byte) (map[string]*TableSchema, error) {
	var table map[string]*TableSchema
	err := json.Unmarshal(data, &table)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize tables: %v", err)
	}
	return table, nil
}

// WriteToFile writes serialized data to a file.
func WriteToFile(fileName string, table map[string]*TableSchema) error {
	data, err := SerializeTables(table)
	if err != nil {
		return fmt.Errorf("failed to serialize tables: %v", err)
	}

	err = os.WriteFile(fileName, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write to file %s: %v", fileName, err)
	}
	return nil
}

// ReadFromFile reads serialized data from a file and deserializes it.
// If the file does not exist, it returns an empty map.
func ReadFromFile(fileName string) (map[string]*TableSchema, error) {
	// Check if the file exists
	if _, err := os.Stat(fileName); err != nil {
		if os.IsNotExist(err) {
			// File does not exist, return an empty map
			return make(map[string]*TableSchema), nil
		}
		// Other errors while accessing the file
		return nil, fmt.Errorf("failed to access file %s: %v", fileName, err)
	}

	// Read file contents
	data, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read from file %s: %v", fileName, err)
	}

	// Deserialize the file contents into a map
	table, err := DeserializeTables(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize tables from file %s: %v", fileName, err)
	}
	return table, nil
}
