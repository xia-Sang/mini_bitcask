package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRDBMS_View(t *testing.T) {
	// 初始化 RDBMS
	rdbms, err := NewRDBMS()
	assert.NoError(t, err, "failed to initialize RDBMS")

	// 创建表
	fields := []Field{
		{"id", FieldTypeString},
		{"name", FieldTypeString},
		{"email", FieldTypeString},
	}
	err = rdbms.CreateTable("users", fields)
	assert.NoError(t, err, "failed to create table")

	// 插入数据
	rowData1 := map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
	}
	rowData2 := map[string][]byte{
		"id":    []byte("2"),
		"name":  []byte("Bob"),
		"email": []byte("bob@example.com"),
	}
	_ = rdbms.Insert("users", []byte("1"), rowData1)
	_ = rdbms.Insert("users", []byte("2"), rowData2)

	// 测试 View
	err = rdbms.View("users")
	assert.NoError(t, err, "view operation failed")
}
func TestRDBMS_ViewMultipleTables(t *testing.T) {
	// 初始化 RDBMS
	rdbms, err := NewRDBMS()
	assert.NoError(t, err, "failed to initialize RDBMS")

	// 创建表 users
	fieldsUsers := []Field{
		{"id", FieldTypeString},
		{"name", FieldTypeString},
		{"email", FieldTypeString},
	}
	err = rdbms.CreateTable("users", fieldsUsers)
	assert.NoError(t, err, "failed to create table users")

	// 创建表 products
	fieldsProducts := []Field{
		{"id", FieldTypeString},
		{"name", FieldTypeString},
		{"price", FieldTypeString},
	}
	err = rdbms.CreateTable("products", fieldsProducts)
	assert.NoError(t, err, "failed to create table products")

	// 插入数据到 users 表
	rowData1 := map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
	}
	rowData2 := map[string][]byte{
		"id":    []byte("2"),
		"name":  []byte("Bob"),
		"email": []byte("bob@example.com"),
	}
	_ = rdbms.Insert("users", []byte("1"), rowData1)
	_ = rdbms.Insert("users", []byte("2"), rowData2)

	// 插入数据到 products 表
	rowProduct1 := map[string][]byte{
		"id":    []byte("101"),
		"name":  []byte("Laptop"),
		"price": []byte("1000"),
	}
	rowProduct2 := map[string][]byte{
		"id":    []byte("102"),
		"name":  []byte("Phone"),
		"price": []byte("500"),
	}
	_ = rdbms.Insert("products", []byte("101"), rowProduct1)
	_ = rdbms.Insert("products", []byte("102"), rowProduct2)

	// 测试 View users 表
	fmt.Println("=== View Users Table ===")
	err = rdbms.View("users")
	assert.NoError(t, err, "view operation failed for users table")

	// 测试 View products 表
	fmt.Println("=== View Products Table ===")
	err = rdbms.View("products")
	assert.NoError(t, err, "view operation failed for products table")
}
