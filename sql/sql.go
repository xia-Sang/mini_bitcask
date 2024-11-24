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

// CreateTable creates a table schema with the specified columns.
func (db *RDBMS) CreateTable(name string, columns map[string]FieldType) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}
	db.Tables[name] = &TableSchema{
		Name:    name,
		Columns: columns,
		Indexes: []string{}, // Initialize with no indexes
	}
	return nil
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
	// Validate that the table exists
	table, exists := db.Tables[tableName]
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
	// Validate update fields and types
	for columnName, newValue := range updates {
		// Check if the column exists in the schema
		columnType, ok := table.Columns[columnName]
		if !ok {
			return fmt.Errorf("invalid column: %s for table %s", columnName, tableName)
		}

		// Validate the type of the new value
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

	// Create a map of column definitions for fast lookup
	columnDefinitions := make(map[string]FieldType)
	for col, fieldType := range table.Columns {
		columnDefinitions[col] = fieldType
	}

	// Validate each field in the input data
	for fieldName, fieldValue := range data {
		// Check if the field exists in the schema
		fieldType, ok := columnDefinitions[fieldName]
		if !ok {
			return fmt.Errorf("invalid field: %s in table %s", fieldName, tableName)
		}

		// Validate the field value against its type
		if err := validateFieldType(fieldType, fieldValue); err != nil {
			return fmt.Errorf("validation failed for field '%s': %v", fieldName, err)
		}
	}

	// Check for missing required fields (optional)
	for fieldName := range columnDefinitions {
		if _, exists := data[fieldName]; !exists {
			return fmt.Errorf("missing required field: %s in table %s", fieldName, tableName)
		}
	}

	return nil
}

// QueryByPrimaryKey retrieves a row by its primary key from the specified table.
func (db *RDBMS) QueryByPrimaryKey(tableName string, primaryKey []byte) (map[string][]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Validate that the table exists
	table, exists := db.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Construct the primary key-based storage key
	key := append([]byte(tableName+":"), primaryKey...)
	serializedRow, err := db.Store.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve row: %v", err)
	}

	// Deserialize the stored row data
	rowData, err := utils.DeserializeRow(serializedRow)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize row data: %v", err)
	}

	// Validate the result against the table's column definitions
	for column := range rowData {
		if _, ok := table.Columns[column]; !ok {
			return nil, fmt.Errorf("unexpected column '%s' found in row data", column)
		}
	}

	return rowData, nil
}

// // SelectAndDisplay retrieves rows from the specified table, matches the condition, and displays formatted output.
// func (db *RDBMS) SelectAndDisplay(tableName string, columns []string, conditionColumn string, value []byte) error {
// 	// Reuse Select logic
// 	results, err := db.Select(tableName, columns, conditionColumn, value)
// 	if err != nil {
// 		return err
// 	}

// 	if len(results) == 0 {
// 		fmt.Println("No results found.")
// 		return nil
// 	}

// 	fmt.Println("Select data:")
// 	// Set up a tab writer for output
// 	var buffer bytes.Buffer
// 	writer := tabwriter.NewWriter(&buffer, 0, 0, 2, ' ', tabwriter.Debug)

// 	// Write column headers
// 	fmt.Fprintln(writer, strings.Join(columns, "\t"))

// 	// Write rows
// 	for _, row := range results {
// 		var line []string
// 		for _, col := range columns {
// 			line = append(line, string(row[col])) // Convert []byte to string
// 		}
// 		fmt.Fprintln(writer, strings.Join(line, "\t"))
// 	}

// 	// Flush and display the output
// 	writer.Flush()
// 	fmt.Println(buffer.String())
// 	return nil
// }

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
