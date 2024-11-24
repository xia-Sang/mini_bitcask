package sql

import (
	"encoding/csv"
	"fmt"
	"os"
)

func (db *RDBMS) ToCSV(tableName, filename string) error {
	// Open the CSV file for writing
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %v", err)
	}
	defer file.Close()

	// Query all rows and columns from the table
	rows, columns, err := db.Select(tableName, []string{"*"})
	if err != nil {
		return fmt.Errorf("failed to query data from table %s: %v", tableName, err)
	}

	if len(rows) == 0 {
		return fmt.Errorf("no data found in table %s", tableName)
	}

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the headers (columns are already strings)
	if err := writer.Write(columns); err != nil {
		return fmt.Errorf("failed to write CSV headers: %v", err)
	}

	// Write the rows
	for _, row := range rows {
		csvRow := make([]string, len(columns))
		for i, col := range columns {
			csvRow[i] = string(row[col]) // Convert []byte to string
		}
		if err := writer.Write(csvRow); err != nil {
			return fmt.Errorf("failed to write CSV row: %v", err)
		}
	}

	return nil
}
