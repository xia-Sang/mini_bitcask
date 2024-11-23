package sql

import (
	"bitcask/utils"
	"bytes"
	"fmt"
	"strings"
	"text/tabwriter"
)

// View displays the data of the specified table in a tabular format.
func (db *RDBMS) View(tableName string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Check if the table exists
	table, exists := db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Fetch all rows for the specified table
	prefix := []byte(tableName + ":")
	rows, err := db.GetKeyValuesWithPrefix(prefix)
	if err != nil {
		return fmt.Errorf("failed to retrieve rows for table %s: %v", tableName, err)
	}

	// Prepare table headers from the schema
	headers := make([]string, 0, len(table.Columns))
	for col := range table.Columns {
		headers = append(headers, col)
	}

	// Use a bytes.Buffer to construct the tabular output
	var buffer bytes.Buffer
	writer := tabwriter.NewWriter(&buffer, 0, 0, 2, ' ', tabwriter.Debug)

	// Write the table name and headers
	fmt.Fprintf(writer, "Table: %s\n", tableName)
	fmt.Fprintln(writer, strings.Join(headers, "\t"))

	// Write the table rows
	for _, value := range rows {
		// Deserialize the row data
		rowData, err := utils.DeserializeRow(value)
		if err != nil {
			return fmt.Errorf("error deserializing row: %v", err)
		}

		// Extract data for each column in order
		row := make([]string, len(headers))
		for i, column := range headers {
			row[i] = string(rowData[column]) // Convert to string for display
		}

		// Write the row to the tabular output
		fmt.Fprintln(writer, strings.Join(row, "\t"))
	}

	// Flush the writer and print the buffer
	writer.Flush()
	fmt.Println(buffer.String())

	return nil
}

// GetKeyValuesWithPrefix fetches all key-value pairs from the KVStore where the keys start with the given prefix.
func (db *RDBMS) GetKeyValuesWithPrefix(prefix []byte) (map[string][]byte, error) {
	result := make(map[string][]byte)

	// Use Fold to iterate through all keys and values in the KVStore
	err := db.Store.Fold(func(key, value []byte) bool {
		// Check if the key starts with the specified prefix
		if len(key) >= len(prefix) && string(key[:len(prefix)]) == string(prefix) {
			// Add the matching key-value pair to the result map
			result[string(key)] = value
		}
		// Continue folding
		return true
	})

	// Return an error if Fold encounters any issues
	if err != nil {
		return nil, fmt.Errorf("failed to get key-value pairs with prefix: %v", err)
	}

	return result, nil
}

// ViewAllTables displays all tables and their data in a tabular format.
func (db *RDBMS) ViewAllTables() error {
	db.mu.RLock()

	// Check if there are any tables
	if len(db.Tables) == 0 {
		fmt.Println("No tables found.")
		db.mu.RUnlock()
		return nil
	}
	db.mu.RUnlock()
	// Iterate through all tables and call the View function
	for tableName := range db.Tables {
		fmt.Printf("===== Table: %s =====\n", tableName)
		if err := db.View(tableName); err != nil {
			return fmt.Errorf("failed to view table %s: %v", tableName, err)
		}
		fmt.Println() // Add spacing between tables
	}

	return nil
}

// FormatResults formats the query results into a tabular string based on the given column order.
func FormatResults(results []map[string][]byte, columns []string) (string, error) {
	if len(results) == 0 {
		return "No results found.", nil
	}

	// Initialize a buffer and tab writer for formatting
	var buffer bytes.Buffer
	writer := tabwriter.NewWriter(&buffer, 0, 0, 2, ' ', tabwriter.Debug)

	// Write the column headers
	fmt.Fprintln(writer, strings.Join(columns, "\t"))

	// Write each row, respecting the column order
	for _, row := range results {
		var line []string
		for _, col := range columns {
			if value, exists := row[col]; exists {
				line = append(line, string(value)) // Convert value to string for output
			} else {
				line = append(line, "") // Handle missing columns gracefully
			}
		}
		fmt.Fprintln(writer, strings.Join(line, "\t"))
	}

	// Flush the writer to the buffer
	writer.Flush()
	return buffer.String(), nil
}
