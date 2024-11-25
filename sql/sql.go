package sql

import (
	"bitcask/bitcask"
	"bitcask/conf"
	"bitcask/utils"
	"fmt"
	"sync"
)

type KVStore interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Fold(func(key, value []byte) bool) error
}

type RDBMS struct {
	Store  KVStore                 // Bitcask 底层存储
	Tables map[string]*TableSchema // 表定义存储
	mu     sync.RWMutex            // 并发操作
}

func NewRDBMS() (*RDBMS, error) {
	config := conf.DefaultConfig()
	db, err := bitcask.NewDb(config)
	if err != nil {
		return nil, err
	}
	table, err := ReadFromFile("data.info")
	if err != nil {
		return nil, err
	}
	return &RDBMS{Store: db, Tables: table}, nil
}

// Insert adds a new row to the specified table.
func (db *RDBMS) Insert(tableName string, primaryKey []byte, rowData map[string][]byte) error {
	// Validate the input data against the table schema
	if err := db.validateFields(tableName, rowData); err != nil {
		return fmt.Errorf("field validation failed: %v", err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if the primary key already exists
	key := append([]byte(tableName+":"), primaryKey...)
	if _, err := db.Store.Get(key); err == nil {
		return fmt.Errorf("primary key already exists: %s", primaryKey)
	}

	// Serialize the row data for storage
	serializedData, err := utils.SerializeRow(rowData)
	if err != nil {
		return fmt.Errorf("failed to serialize row data: %v", err)
	}

	// Store the serialized row in the database
	if err := db.Store.Put(key, serializedData); err != nil {
		return fmt.Errorf("failed to store row: %v", err)
	}

	// Update indexes
	for column, value := range rowData {
		indexKey := append([]byte("index:"+tableName+":"+column+":"), value...)
		existingKeys, _ := db.Store.Get(indexKey)

		// Append the primary key to the index
		updatedKeys := append(existingKeys, primaryKey...)
		if err := db.Store.Put(indexKey, updatedKeys); err != nil {
			return fmt.Errorf("failed to update index for column %s: %v", column, err)
		}
	}

	return nil
}

// Update modifies a row in the specified table based on its primary key.
func (db *RDBMS) Update(tableName string, primaryKey []byte, updates map[string][]byte) error {
	db.mu.RLock()
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()

	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Fetch the existing row
	oldData, err := db.QueryByPrimaryKey(tableName, primaryKey)
	if err != nil {
		return fmt.Errorf("failed to fetch existing row for primary key %s: %v", primaryKey, err)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate update fields and their types
	for columnName, newValue := range updates {
		// Check if the column exists using colMaps
		columnIndex, ok := table.colMaps[columnName]
		if !ok {
			return fmt.Errorf("invalid column: %s for table %s", columnName, tableName)
		}

		// Validate the type of the new value using FieldTypes
		columnType := table.FieldTypes[columnIndex]
		if err := validateFieldType(columnType, newValue); err != nil {
			return fmt.Errorf("invalid value for column %s: %v", columnName, err)
		}
	}

	// Remove old indexes for the updated fields
	for columnName := range updates {
		if oldValue, ok := oldData[columnName]; ok {
			indexKey := append([]byte("index:"+tableName+":"+columnName+":"), oldValue...)
			if err := db.Store.Delete(indexKey); err != nil {
				return fmt.Errorf("failed to delete old index for column %s: %v", columnName, err)
			}
		}
	}

	// Apply the updates to the row
	for columnName, newValue := range updates {
		oldData[columnName] = newValue
	}

	// Serialize and store the updated row
	serializedData, err := utils.SerializeRow(oldData)
	if err != nil {
		return fmt.Errorf("failed to serialize updated row: %v", err)
	}

	key := append([]byte(tableName+":"), primaryKey...)
	if err := db.Store.Put(key, serializedData); err != nil {
		return fmt.Errorf("failed to update row: %v", err)
	}

	// Add new indexes for the updated fields
	for columnName, newValue := range updates {
		indexKey := append([]byte("index:"+tableName+":"+columnName+":"), newValue...)
		existingKeys, _ := db.Store.Get(indexKey)

		// Append the primary key to the index
		updatedKeys := append(existingKeys, primaryKey...)
		if err := db.Store.Put(indexKey, updatedKeys); err != nil {
			return fmt.Errorf("failed to update index for column %s: %v", columnName, err)
		}
	}

	return nil
}

// validateFields checks that the provided data matches the schema of the table.
func (db *RDBMS) validateFields(tableName string, data map[string][]byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Check if the table exists
	table, exists := db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate each field in the input data
	for fieldName, fieldValue := range data {
		// Check if the field exists using colMaps
		fieldIndex, ok := table.colMaps[fieldName]
		if !ok {
			return fmt.Errorf("invalid field: %s in table %s", fieldName, tableName)
		}

		// Validate the field value against its type
		fieldType := table.FieldTypes[fieldIndex]
		if err := validateFieldType(fieldType, fieldValue); err != nil {
			return fmt.Errorf("validation failed for field '%s': %v", fieldName, err)
		}
	}

	// Check for missing required fields (if all fields are required)
	for _, fieldName := range table.Columns {
		if _, exists := data[fieldName]; !exists {
			return fmt.Errorf("missing required field: %s in table %s", fieldName, tableName)
		}
	}

	return nil
}

// Delete removes a row from the specified table and its associated indexes.
func (db *RDBMS) Delete(tableName string, primaryKey []byte) error {

	// Fetch row data
	row, err := db.QueryByPrimaryKey(tableName, primaryKey)
	if err != nil {
		db.mu.Unlock()
		return fmt.Errorf("failed to fetch row for deletion: %v", err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	// Remove indexes
	for column, value := range row {
		indexKey := append([]byte("index:"+tableName+":"+column+":"), value...)
		if err := db.Store.Delete(indexKey); err != nil {
			return fmt.Errorf("failed to delete index for column %s: %v", column, err)
		}
	}

	// Remove primary key record
	key := append([]byte(tableName+":"), primaryKey...)
	if err := db.Store.Delete(key); err != nil {
		return fmt.Errorf("failed to delete row: %v", err)
	}

	return nil
}

func (db *RDBMS) Close() error {
	return WriteToFile("data.info", db.Tables)
}
