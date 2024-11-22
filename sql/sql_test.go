package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRDBMS_InsertAndQueryByPrimaryKey(t *testing.T) {
	// 初始化 RDBMS
	rdbms, err := NewRDBMS()
	assert.NoError(t, err, "failed to initialize RDBMS")

	// 测试数据
	tableName := "users"
	primaryKey := []byte("1")
	rowData := map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
	}

	// 插入数据
	err = rdbms.Insert(tableName, primaryKey, rowData)
	assert.NoError(t, err, "insert operation failed")

	// 按主键查询
	result, err := rdbms.QueryByPrimaryKey(tableName, primaryKey)
	assert.NoError(t, err, "query by primary key failed")
	assert.Equal(t, rowData, result, "queried data does not match inserted data")
}

func TestRDBMS_QueryByCondition(t *testing.T) {
	// 初始化 RDBMS
	rdbms, err := NewRDBMS()
	assert.NoError(t, err, "failed to initialize RDBMS")

	// 测试数据
	tableName := "users"
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

	// 插入数据
	_ = rdbms.Insert(tableName, []byte("1"), rowData1)
	_ = rdbms.Insert(tableName, []byte("2"), rowData2)

	// 按条件查询
	results, err := rdbms.QueryByCondition(tableName, "email", []byte("alice@example.com"))
	t.Log(results, err)
	assert.NoError(t, err, "query by condition failed")
	assert.Len(t, results, 1, "query should return one result")
	assert.Equal(t, rowData1, results[0], "queried data does not match expected data")
}

func TestRDBMS_Update(t *testing.T) {
	// 初始化 RDBMS
	rdbms, err := NewRDBMS()
	assert.NoError(t, err, "failed to initialize RDBMS")

	// 测试数据
	tableName := "users"
	primaryKey := []byte("1")
	rowData := map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
	}

	// 插入数据
	_ = rdbms.Insert(tableName, primaryKey, rowData)

	// 更新数据
	updates := map[string][]byte{
		"name":  []byte("Alice Updated"),
		"email": []byte("alice.updated@example.com"),
	}
	err = rdbms.Update(tableName, primaryKey, updates)
	assert.NoError(t, err, "update operation failed")

	// 查询更新后的数据
	result, err := rdbms.QueryByPrimaryKey(tableName, primaryKey)
	assert.NoError(t, err, "query after update failed")
	expected := map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice Updated"),
		"email": []byte("alice.updated@example.com"),
	}
	assert.Equal(t, expected, result, "updated data does not match expected data")
}

func TestRDBMS_Delete(t *testing.T) {
	// 初始化 RDBMS
	rdbms, err := NewRDBMS()
	assert.NoError(t, err, "failed to initialize RDBMS")

	// 测试数据
	tableName := "users"
	primaryKey := []byte("1")
	rowData := map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
	}

	// 插入数据
	_ = rdbms.Insert(tableName, primaryKey, rowData)

	// 删除数据
	err = rdbms.Delete(tableName, primaryKey)
	assert.NoError(t, err, "delete operation failed")

	// 验证删除
	_, err = rdbms.QueryByPrimaryKey(tableName, primaryKey)
	assert.Error(t, err, "query after delete should return an error")
}
