package csv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadCSV(t *testing.T) {
	fileName := "../test/test.csv"
	headers, rows, err := ReadCSVAsBytes(fileName)
	assert.Nil(t, err)

	// Format and print headers
	fmt.Println("Headers:")
	for _, header := range headers {
		fmt.Printf("%s\t", string(header))
	}
	fmt.Println()

	// Format and print rows
	fmt.Println("Rows:")
	for i, row := range rows {
		fmt.Printf("Row %d: ", i+1)
		for _, value := range row {
			fmt.Printf("%s\t", string(value))
		}
		fmt.Println()
	}
}
func TestToDb(t *testing.T) {
	fileName := "../test/test.csv"
	db, err := ToMySql(fileName)
	t.Log(err)
	db.ViewAllTables()
}
func TestReadToDb(t *testing.T) {
	fileName := "../test/test.csv"
	db, err := ReadMySql(fileName)
	t.Log(err)
	db.ViewAllTables()
}
func TestReadToDb1(t *testing.T) {
	fileName := "../test/test.csv"
	db, err := ReadMySql(fileName)
	t.Log(err)
	db.SelectAndDisplay(getTableName(fileName), []string{"*"})
	db.SelectAndDisplay(getTableName(fileName), []string{"id", "name", "age", "email", "is_active"})
}
func TestReadToDb2(t *testing.T) {
	fileName := "../test/test.csv"
	db, err := ReadMySql(fileName)
	t.Log(err)
	db.SelectAndDisplay(getTableName(fileName), []string{"*"})
	db.SelectAndDisplay(getTableName(fileName), []string{"id", "name", "age", "email", "is_active"})
}
func TestReadToDb3(t *testing.T) {
	fileName := "../test/test.csv"
	db, err := ReadMySql(fileName)
	t.Log(err)
	db.SelectAndDisplay(getTableName(fileName), []string{"*"})
	db.SelectAndDisplay(getTableName(fileName), []string{"id", "name", "age", "email", "is_active"})
	// writeToCSV(db, getTableName(fileName), "test1.csv")
}
func TestReadToDb4(t *testing.T) {
	fileName := "../test/test.csv"
	db, err := ToMySql(fileName)
	t.Log(err)
	db.ToCSV(getTableName(fileName), "test1.csv")
}
