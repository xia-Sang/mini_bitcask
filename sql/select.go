package sql

import (
	"bitcask/utils"
	"fmt"
)

// preprocessColumns resolves "*" to all columns and validates the requested columns.
func (db *RDBMS) preprocessColumns(tableName string, columns []string) ([]string, error) {
	// Check if the table exists
	table, exists := db.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Resolve "*" to all columns
	if len(columns) == 1 && columns[0] == "*" {
		allColumns := make([]string, 0, len(table.Columns))
		for col := range table.Columns {
			allColumns = append(allColumns, col)
		}
		return allColumns, nil
	}

	// Validate requested columns
	for _, col := range columns {
		if _, ok := table.Columns[col]; !ok {
			return nil, fmt.Errorf("column %s does not exist in table %s", col, tableName)
		}
	}

	return columns, nil
}

// fetchAndFilterRows retrieves all rows for a table and filters them based on the provided columns and conditions.
func (db *RDBMS) fetchAndFilterRows(tableName string, columns []string, conditions map[string]Condition) ([]map[string][]byte, error) {
	table := db.Tables[tableName] // Assume existence checked before calling

	// Fetch all rows for the table
	prefix := []byte(tableName + ":")
	rows, err := db.GetKeyValuesWithPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve rows for table %s: %v", tableName, err)
	}

	var results []map[string][]byte

	for _, value := range rows {
		// Deserialize the row
		row, err := utils.DeserializeRow(value)
		if err != nil {
			return nil, fmt.Errorf("error deserializing row: %v", err)
		}

		// Check conditions if provided
		if conditions != nil {
			match := true
			for condCol, cond := range conditions {
				fieldType := table.Columns[condCol]
				cellValue, exists := row[condCol]
				if !exists {
					match = false
					break
				}
				ok, err := CompareValues(fieldType, cellValue, cond.Value, cond.Operator)
				if err != nil || !ok {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		// Filter the row to include only the requested columns
		filteredRow := make(map[string][]byte)
		for _, col := range columns {
			if val, ok := row[col]; ok {
				filteredRow[col] = val
			}
		}
		results = append(results, filteredRow)
	}

	return results, nil
}

// Select retrieves all rows from the specified table and filters the specified columns.
// If "*" is passed as columns, it selects all available columns.
func (db *RDBMS) Select(tableName string, columns []string) ([]map[string][]byte, []string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Preprocess columns
	resolvedColumns, err := db.preprocessColumns(tableName, columns)
	if err != nil {
		return nil, nil, err
	}

	// Fetch and filter rows
	results, err := db.fetchAndFilterRows(tableName, resolvedColumns, nil)
	if err != nil {
		return nil, nil, err
	}

	return results, resolvedColumns, nil
}

// SelectWithWhere retrieves rows from the specified table, filters the specified columns,
// and applies the given conditions to filter results.
func (db *RDBMS) SelectWithWhere(tableName string, columns []string, conditions map[string]Condition) ([]map[string][]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Preprocess columns
	resolvedColumns, err := db.preprocessColumns(tableName, columns)
	if err != nil {
		return nil, err
	}

	// Fetch and filter rows with conditions
	return db.fetchAndFilterRows(tableName, resolvedColumns, conditions)
}

// SelectAndDisplay retrieves rows from the specified table, filters the specified columns,
// and displays the results in a tabular format.
func (db *RDBMS) SelectAndDisplay(tableName string, columns []string) error {
	// Use Select to retrieve data
	results, resolvedColumns, err := db.Select(tableName, columns)
	if err != nil {
		return err
	}

	// Display the results
	return DisplayResults(resolvedColumns, results)
}

// SelectWhereAndDisplay retrieves rows from the specified table, applies conditions,
// filters the specified columns, and displays the results in a tabular format.
func (db *RDBMS) SelectWhereAndDisplay(tableName string, columns []string, conditions map[string]Condition) error {
	// Fetch filtered rows with conditions
	results, err := db.SelectWithWhere(tableName, columns, conditions)
	if err != nil {
		return err
	}

	// Preprocess columns to ensure resolved column names are used for display
	resolvedColumns, err := db.preprocessColumns(tableName, columns)
	if err != nil {
		return err
	}

	// Display the results
	return DisplayResults(resolvedColumns, results)
}
