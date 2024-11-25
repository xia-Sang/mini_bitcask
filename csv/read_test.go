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
	tableName := getTableName(fileName)
	t.Log(err)
	rowData := []map[string][]byte{
		{
			"id":        []byte("1"),
			"name":      []byte("Alice"),
			"age":       []byte("30"),
			"email":     []byte("alice@example.com"),
			"is_active": []byte("true"),
		},
		{
			"id":        []byte("2"),
			"name":      []byte("Bob"),
			"age":       []byte("25"),
			"email":     []byte("bob@example.com"),
			"is_active": []byte("false"),
		},
		{
			"id":        []byte("3"),
			"name":      []byte("Charlie"),
			"age":       []byte("35"),
			"email":     []byte("charlie@example.com"),
			"is_active": []byte("true"),
		},
		{
			"id":        []byte("4"),
			"name":      []byte("Daisy"),
			"age":       []byte("28"),
			"email":     []byte("daisy@example.com"),
			"is_active": []byte("false"),
		},
	}

	db.Insert(tableName, []byte("4"), rowData[0])
	db.Insert(tableName, []byte("4"), rowData[3])
	db.ToCSV(tableName, "test1.csv")
}
