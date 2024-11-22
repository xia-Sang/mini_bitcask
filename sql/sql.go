package sql

import (
	"bitcask/bitcask"
	"bitcask/conf"
	"bitcask/utils"
	"bytes"
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"
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
	mu     sync.RWMutex            //并发操作
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
func (db *RDBMS) CreateTable(name string, columns []string) error {
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

// validateFields validates the fields in the input data against the table's column definitions.
func (db *RDBMS) validateFields(tableName string, data map[string][]byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Create a set of valid column names for fast lookup
	validColumns := make(map[string]struct{})
	for _, column := range table.Columns {
		validColumns[column] = struct{}{}
	}

	// Check each key in the input data
	for field := range data {
		if _, ok := validColumns[field]; !ok {
			return fmt.Errorf("invalid field: %s", field)
		}
	}
	return nil
}

// Insert adds a new row to the specified table.
func (db *RDBMS) Insert(tableName string, primaryKey []byte, rowData map[string][]byte) error {
	// Validate fields
	if err := db.validateFields(tableName, rowData); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	// Serialize row data
	serializedData, err := utils.SerializeRow(rowData)
	if err != nil {
		return fmt.Errorf("failed to serialize row data: %v", err)
	}

	// Construct key and store data
	key := append([]byte(tableName+":"), primaryKey...)
	if err := db.Store.Put(key, serializedData); err != nil {
		return fmt.Errorf("failed to store row: %v", err)
	}

	// Update indexes
	for column, value := range rowData {
		indexKey := append([]byte("index:"+tableName+":"+column+":"), value...)
		existing, _ := db.Store.Get(indexKey)
		updatedIndex := append(existing, primaryKey...)
		if err := db.Store.Put(indexKey, updatedIndex); err != nil {
			return fmt.Errorf("failed to update index: %v", err)
		}
	}
	return nil
}

// Update modifies a row in the specified table.
func (db *RDBMS) Update(tableName string, primaryKey []byte, updates map[string][]byte) error {

	// Validate fields
	if err := db.validateFields(tableName, updates); err != nil {
		return err
	}

	// Fetch existing data
	oldData, err := db.QueryByPrimaryKey(tableName, primaryKey)
	if err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	// Remove old indexes
	for column, value := range oldData {
		indexKey := append([]byte("index:"+tableName+":"+column+":"), value...)
		if err := db.Store.Delete(indexKey); err != nil {
			return fmt.Errorf("failed to delete old index: %v", err)
		}
	}

	// Apply updates
	for k, v := range updates {
		oldData[k] = v
	}

	// Serialize and store updated data
	newSerializedData, err := utils.SerializeRow(oldData)
	if err != nil {
		return fmt.Errorf("failed to serialize updated row: %v", err)
	}
	key := append([]byte(tableName+":"), primaryKey...)
	if err := db.Store.Put(key, newSerializedData); err != nil {
		return fmt.Errorf("failed to update row: %v", err)
	}

	// Add new indexes
	for column, value := range oldData {
		indexKey := append([]byte("index:"+tableName+":"+column+":"), value...)
		if err := db.Store.Put(indexKey, primaryKey); err != nil {
			return fmt.Errorf("failed to update new index: %v", err)
		}
	}
	return nil
}

// Delete removes a row from the specified table and its associated indexes.
func (db *RDBMS) Delete(tableName string, primaryKey []byte) error {

	// Fetch row data
	row, err := db.QueryByPrimaryKey(tableName, primaryKey)
	if err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	// Remove indexes
	for column, value := range row {
		indexKey := append([]byte("index:"+tableName+":"+column+":"), value...)
		if err := db.Store.Delete(indexKey); err != nil {
			return fmt.Errorf("failed to delete index: %v", err)
		}
	}

	// Remove primary key record
	key := append([]byte(tableName+":"), primaryKey...)
	if err := db.Store.Delete(key); err != nil {
		return fmt.Errorf("failed to delete row: %v", err)
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
		if !columnExists(column, table.Columns) {
			return nil, fmt.Errorf("unexpected column '%s' found in row data", column)
		}
	}

	return rowData, nil
}

// Helper function to check if a column exists in the table schema
func columnExists(column string, columns []string) bool {
	for _, col := range columns {
		if col == column {
			return true
		}
	}
	return false
}
func (db *RDBMS) Close() error {
	return WriteToFile("data.info", db.Tables)
}

// Select retrieves rows from the specified table where the given column matches the specified value.
// It supports selecting specific columns or all columns (denoted by "*").
func (db *RDBMS) Select(tableName string, columns []string, conditionColumn string, value []byte) ([]map[string][]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Check if the table exists
	table, exists := db.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// If columns are "*", select all columns
	if len(columns) == 1 && columns[0] == "*" {
		columns = table.Columns
	}

	// Validate selected columns
	columnSet := make(map[string]struct{})
	for _, col := range table.Columns {
		columnSet[col] = struct{}{}
	}

	for _, col := range columns {
		if _, ok := columnSet[col]; !ok {
			return nil, fmt.Errorf("column %s does not exist in table %s", col, tableName)
		}
	}

	// Validate the condition column
	if _, ok := columnSet[conditionColumn]; !ok {
		return nil, fmt.Errorf("condition column %s does not exist in table %s", conditionColumn, tableName)
	}

	// Query the index for matching primary keys
	indexPrefix := append([]byte("index:"+tableName+":"+conditionColumn+":"), value...)
	primaryKeys, err := db.Store.Get(indexPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to get index for column %s in table %s: %v", conditionColumn, tableName, err)
	}

	// Fetch rows based on the primary keys
	var results []map[string][]byte
	primaryKeyLength := len(primaryKeys) / len(value) // Approximate primary key length
	if primaryKeyLength == 0 {
		return nil, nil // No matching rows
	}

	for i := 0; i < len(primaryKeys); i += primaryKeyLength {
		primaryKey := primaryKeys[i : i+primaryKeyLength]
		row, err := db.QueryByPrimaryKey(tableName, primaryKey)
		if err != nil {
			return nil, fmt.Errorf("failed to query row for primary key %s: %v", primaryKey, err)
		}

		// Filter the row to include only the requested columns
		selectedRow := make(map[string][]byte)
		for _, col := range columns {
			if val, ok := row[col]; ok {
				selectedRow[col] = val
			}
		}

		results = append(results, selectedRow)
	}

	return results, nil
}

// Select retrieves rows from the specified table where the given column matches the specified value.
// It supports selecting specific columns or all columns (denoted by "*") and directly displays the formatted output.
func (db *RDBMS) SelectAndDisplay(tableName string, columns []string, conditionColumn string, value []byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Check if the table exists
	table, exists := db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// If columns are "*", select all columns
	if len(columns) == 1 && columns[0] == "*" {
		columns = table.Columns
	}

	// Validate selected columns
	columnSet := make(map[string]struct{})
	for _, col := range table.Columns {
		columnSet[col] = struct{}{}
	}

	for _, col := range columns {
		if _, ok := columnSet[col]; !ok {
			return fmt.Errorf("column %s does not exist in table %s", col, tableName)
		}
	}

	// Validate the condition column
	if _, ok := columnSet[conditionColumn]; !ok {
		return fmt.Errorf("condition column %s does not exist in table %s", conditionColumn, tableName)
	}

	// Query the index for matching primary keys
	indexPrefix := append([]byte("index:"+tableName+":"+conditionColumn+":"), value...)
	primaryKeys, err := db.Store.Get(indexPrefix)
	if err != nil {
		return fmt.Errorf("failed to get index for column %s in table %s: %v", conditionColumn, tableName, err)
	}

	// Set up a tab writer for output
	var buffer bytes.Buffer
	writer := tabwriter.NewWriter(&buffer, 0, 0, 2, ' ', tabwriter.Debug)

	// Write column headers
	fmt.Fprintln(writer, strings.Join(columns, "\t"))

	// Fetch rows based on the primary keys and write them incrementally
	primaryKeyLength := len(primaryKeys) / len(value) // Approximate primary key length
	if primaryKeyLength == 0 {
		fmt.Println("No results found.")
		return nil
	}
	fmt.Println("Results found:")
	for i := 0; i < len(primaryKeys); i += primaryKeyLength {
		primaryKey := primaryKeys[i : i+primaryKeyLength]
		row, err := db.QueryByPrimaryKey(tableName, primaryKey)
		if err != nil {
			return fmt.Errorf("failed to query row for primary key %s: %v", primaryKey, err)
		}

		// Format and write the row to the writer
		var line []string
		for _, col := range columns {
			if val, ok := row[col]; ok {
				line = append(line, string(val))
			} else {
				line = append(line, "") // Handle missing columns
			}
		}
		fmt.Fprintln(writer, strings.Join(line, "\t"))
	}

	// Flush and display the output
	writer.Flush()
	fmt.Println(buffer.String())
	return nil
}
