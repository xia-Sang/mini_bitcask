package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRDBMS_CreateTableAndInsert(t *testing.T) {
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

	// 插入数据（合法）
	rowData := map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
	}
	err = rdbms.Insert("users", []byte("1"), rowData)
	assert.NoError(t, err, "failed to insert valid row")

	// 插入数据（非法字段）
	invalidRowData := map[string][]byte{
		"id":       []byte("2"),
		"nickname": []byte("Bob"), // 非法字段
	}
	err = rdbms.Insert("users", []byte("2"), invalidRowData)
	assert.Error(t, err, "expected error for invalid field")
}

func TestRDBMS_UpdateWithValidation(t *testing.T) {
	// 初始化 RDBMS
	rdbms, err := NewRDBMS()
	assert.NoError(t, err, "failed to initialize RDBMS")

	// 创建表
	fields := []Field{
		{"id", FieldTypeString},
		{"name", FieldTypeString},
		{"email", FieldTypeString},
	}
	_ = rdbms.CreateTable("users", fields)

	// 插入数据
	rowData := map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
	}
	_ = rdbms.Insert("users", []byte("1"), rowData)

	// 更新数据（合法）
	updates := map[string][]byte{
		"name":  []byte("Alice Updated"),
		"email": []byte("alice.updated@example.com"),
	}
	err = rdbms.Update("users", []byte("1"), updates)
	assert.NoError(t, err, "failed to update with valid fields")

	// 更新数据（非法字段）
	invalidUpdates := map[string][]byte{
		"nickname": []byte("Updated Alice"), // 非法字段
	}
	err = rdbms.Update("users", []byte("1"), invalidUpdates)
	assert.Error(t, err, "expected error for invalid field update")
}
