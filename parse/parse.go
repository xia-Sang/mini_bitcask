package parse

import (
	"fmt"
	"log"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

// parseSQL 解析并处理 SQL 语句
func parseSQL(sql string) {
	// 创建解析器
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		log.Printf("Error parsing SQL: %v\n", err)
		return
	}

	// 遍历 AST 节点
	for _, stmtNode := range stmtNodes {
		switch stmt := stmtNode.(type) {
		case *ast.SelectStmt:
			fmt.Println("Detected a SELECT statement")
			processSelect(stmt)
		case *ast.InsertStmt:
			fmt.Println("Detected an INSERT statement")
			processInsert(stmt)
		case *ast.DeleteStmt:
			fmt.Println("Detected a DELETE statement")
			processDelete(stmt)
		default:
			fmt.Printf("Unsupported statement type: %T\n", stmtNode)
		}
	}
}

// processSelect 处理 SELECT 语句
func processSelect(stmt *ast.SelectStmt) {
	fmt.Println("SELECT Fields:")
	for _, field := range stmt.Fields.Fields {
		if field.AsName.String() != "" {
			fmt.Printf("- Field: %s AS %s\n", field.Text(), field.AsName)
		} else {
			fmt.Printf("- Field: %s\n", field.Text())
		}
	}

	if stmt.Where != nil {
		fmt.Println("WHERE Clause Found")
	}
}

// processInsert 处理 INSERT 语句
func processInsert(stmt *ast.InsertStmt) {
	fmt.Println("Table Name:", stmt.Table.TableRefs.Left.(*ast.TableName).Name.String())
	fmt.Println("Columns:")
	for _, col := range stmt.Columns {
		fmt.Printf("- %s\n", col.Name.String())
	}

	fmt.Println("Values:")
	for _, list := range stmt.Lists {
		for _, value := range list {
			fmt.Printf("- %s\n", value.Text())
		}
	}
}

// processDelete 处理 DELETE 语句
func processDelete(stmt *ast.DeleteStmt) {
	fmt.Println("Table Name:", stmt.TableRefs.TableRefs.Left.(*ast.TableName).Name.String())
	if stmt.Where != nil {
		fmt.Println("WHERE Clause Found")
	}
}
