package parse

import (
	"fmt"
	"testing"
)

func TestParse(t *testing.T) {
	// 示例 SQL 语句
	sqlInsert := "INSERT INTO users (id, name) VALUES (1, 'Alice')"

	fmt.Println("Parsing INSERT:")
	parseSQL(sqlInsert)

	sqlSelect := "SELECT (id, name) FROM users WHERE id = 1"

	fmt.Println("\nParsing SELECT:")
	parseSQL(sqlSelect)

	sql1 := "INSERT INTO my_table (id, name, balance, birthdate) VALUES (3, 'sang', 112.50, '19000401')"

	fmt.Println("\nParsing insert:")
	parseSQL(sql1)
	sql2 := "SELECT  * FROM my_table where name = 'xia' "
	parseSQL(sql2)
	sql3 := "SELECT (name,id,user) FROM my_table where name = 'xia' "
	parseSQL(sql3)
}

// TestParse 封装解析 SQL 测试
func TestParse1(t *testing.T) {
	// 示例 SQL 语句
	sqlInsert := "INSERT INTO users (id, name) VALUES (1, 'Alice')"
	fmt.Println("Parsing INSERT:")
	parseSQL(sqlInsert)

	sqlSelect := "SELECT id, name FROM users WHERE id = 1"
	fmt.Println("\nParsing SELECT:")
	parseSQL(sqlSelect)

	sqlDelete := "DELETE FROM users WHERE name = 'Alice'"
	fmt.Println("\nParsing DELETE:")
	parseSQL(sqlDelete)

	sql1 := "INSERT INTO my_table (id, name, balance, birthdate) VALUES (3, 'sang', 112.50, '19000401')"
	fmt.Println("\nParsing INSERT:")
	parseSQL(sql1)

	sql2 := "SELECT * FROM my_table WHERE name = 'xia'"
	fmt.Println("\nParsing SELECT:")
	parseSQL(sql2)

	sql3 := "DELETE FROM my_table WHERE balance > 1000"
	fmt.Println("\nParsing DELETE:")
	parseSQL(sql3)
}
