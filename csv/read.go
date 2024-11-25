package csv

import (
	"bitcask/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// ReadCSVAsBytes reads a CSV file and returns the headers and rows as slices of []byte.
// Headers are returned as a slice of []byte, and rows are slices of [][]byte.
func ReadCSVAsBytes(filename string) ([][]byte, [][][]byte, error) {
	// Open the CSV file
	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open CSV file: %v", err)
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read CSV file: %v", err)
	}

	// Ensure the CSV has content
	if len(records) < 1 {
		return nil, nil, fmt.Errorf("CSV file is empty or has no headers")
	}

	// Convert headers to []byte
	headers := make([][]byte, len(records[0]))
	for i, header := range records[0] {
		headers[i] = []byte(header)
	}

	// Convert rows to [][]byte
	rows := make([][][]byte, len(records)-1)
	for i, record := range records[1:] {
		row := make([][]byte, len(record))
		for j, value := range record {
			row[j] = []byte(value)
		}
		rows[i] = row
	}

	return headers, rows, nil
}

// ToMySql reads a CSV file, infers or uses provided column types, creates a table, and populates it with rows.
func ToMySql(filename string) (*sql.RDBMS, error) {
	// Read the CSV file
	headers, rows, err := ReadCSVAsBytes(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV file: %v", err)
	}

	// Initialize the sql
	mySql, err := sql.NewRDBMS()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sql: %v", err)
	}
	defer mySql.Close()

	col, colTypes, err := inferColumnTypesFromBytes(headers, rows)
	if err != nil {
		return nil, fmt.Errorf("failed to infer column types: %v", err)
	}

	// Use the file name (without extension) as the table name
	tableName := getTableName(filename)

	// Create the table
	if err := mySql.CreateTable(tableName, col, colTypes); err != nil {
		return nil, fmt.Errorf("failed to create table %s: %v", tableName, err)
	}

	// Insert rows into the table
	for i, row := range rows {
		rowData := make(map[string][]byte)
		for j, value := range row {
			rowData[string(headers[j])] = value
		}
		primaryKey := []byte(strconv.Itoa(i + 1)) // Use row number as primary key
		if err := mySql.Insert(tableName, primaryKey, rowData); err != nil {
			return nil, fmt.Errorf("failed to insert row %d into table %s: %v", i+1, tableName, err)
		}
	}

	return mySql, nil
}

// inferColumnTypesFromBytes infers column types based on CSV headers and rows.
func inferColumnTypesFromBytes(headers [][]byte, rows [][][]byte) ([]string, []sql.FieldType, error) {
	if len(headers) == 0 || len(rows) == 0 {
		return nil, nil, fmt.Errorf("cannot infer column types from empty headers or rows")
	}

	columnNames := make([]string, len(headers))
	columnTypes := make([]sql.FieldType, len(headers))

	// Initialize defaults
	for i, header := range headers {
		columnNames[i] = string(header)
		columnTypes[i] = sql.FieldTypeString // Default to string
	}

	// Infer column types based on row values
	for _, row := range rows {
		for i := 0; i < len(headers) && i < len(row); i++ {
			value := row[i]
			if _, err := strconv.Atoi(string(value)); err == nil {
				columnTypes[i] = sql.FieldTypeInt
			} else if _, err := strconv.ParseFloat(string(value), 64); err == nil {
				columnTypes[i] = sql.FieldTypeFloat
			} else if string(value) == "true" || string(value) == "false" {
				columnTypes[i] = sql.FieldTypeBool
			}
		}
	}

	return columnNames, columnTypes, nil
}

// ToMySql reads a CSV file, infers or uses provided column types, creates a table, and populates it with rows.
func ReadMySql(filename string) (*sql.RDBMS, error) {

	// Initialize the sql
	mySql, err := sql.NewRDBMS()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sql: %v", err)
	}
	defer mySql.Close()

	return mySql, nil
}
func getTableName(filename string) string {
	tableName := filepath.Base(filename)
	return tableName[:len(tableName)-len(filepath.Ext(tableName))]
}
