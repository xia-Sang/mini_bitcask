package sql

import (
	"bitcask/utils"
	"fmt"
)

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
		if _, ok := table.colMaps[column]; !ok {
			return nil, fmt.Errorf("unexpected column '%s' found in row data", column)
		}
	}

	return rowData, nil
}
