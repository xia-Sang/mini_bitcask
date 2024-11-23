package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// func TestRDBMS_InsertAndQueryByPrimaryKey(t *testing.T) {
// 	// 初始化 RDBMS
// 	rdbms, err := NewRDBMS()
// 	assert.NoError(t, err, "failed to initialize RDBMS")

// 	// 测试数据
// 	tableName := "users"
// 	primaryKey := []byte("1")
// 	rowData := map[string][]byte{
// 		"id":    []byte("1"),
// 		"name":  []byte("Alice"),
// 		"email": []byte("alice@example.com"),
// 	}
// 	err = rdbms.CreateTable(tableName, []string{"id", "name", "email"})
// 	assert.Nil(t, err)
// 	// 插入数据
// 	err = rdbms.Insert(tableName, primaryKey, rowData)
// 	assert.NoError(t, err, "insert operation failed")

// 	// 按主键查询
// 	result, err := rdbms.QueryByPrimaryKey(tableName, primaryKey)
// 	assert.NoError(t, err, "query by primary key failed")
// 	assert.Equal(t, rowData, result, "queried data does not match inserted data")
// }

func TestRDBMS_Update(t *testing.T) {
	// 初始化 RDBMS
	rdbms, err := NewRDBMS()
	assert.NoError(t, err, "failed to initialize RDBMS")

	// 测试数据
	tableName := "users"
	primaryKey := []byte("1")
	rowData := map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("huawei"),
		"email": []byte("alice@example.com"),
	}

	// 插入数据
	_ = rdbms.Insert(tableName, primaryKey, rowData)

	// 更新数据
	updates := map[string][]byte{
		"name":  []byte("huawei"),
		"email": []byte("alice.updated@example.com"),
	}
	err = rdbms.Update(tableName, primaryKey, updates)
	assert.NoError(t, err, "update operation failed")

	// 查询更新后的数据
	result, err := rdbms.QueryByPrimaryKey(tableName, primaryKey)
	assert.NoError(t, err, "query after update failed")
	expected := map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("huawei"),
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

// func TestSerilize(t *testing.T) {
// 	rdbms := &RDBMS{
// 		Tables: map[string]*TableSchema{
// 			"users": {
// 				Name:    "users",
// 				Columns: []string{"id", "name", "email"},
// 				Indexes: []string{"id", "email"},
// 			},
// 			"orders": {
// 				Name:    "orders",
// 				Columns: []string{"order_id", "user_id", "amount"},
// 				Indexes: []string{"order_id"},
// 			},
// 		},
// 	}

//		// Serialize Tables
//		serialized, err := SerializeTables(rdbms.Tables)
//		if err != nil {
//			fmt.Printf("Error serializing tables: %v\n", err)
//		} else {
//			fmt.Printf("Serialized Tables: %s\n", serialized)
//		}
//	}
func TestSelectWithMultipleColumns(t *testing.T) {
	// Initialize RDBMS
	db, err := NewRDBMS()
	assert.Nil(t, err, "RDBMS initialization failed")

	// Create a table
	// err = db.CreateTable("users", []string{"id", "name", "email", "age"})
	// assert.Nil(t, err, "CreateTable failed")
	// t.Log("Created table 'users'")

	// Insert data
	_ = db.Insert("users", []byte("3"), map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
		"age":   []byte("30"),
	})
	_ = db.Insert("users", []byte("4"), map[string][]byte{
		"id":    []byte("2"),
		"name":  []byte("Bob"),
		"email": []byte("bob@example.com"),
		"age":   []byte("25"),
	})
	_ = db.Insert("users", []byte("5"), map[string][]byte{
		"id":    []byte("3"),
		"name":  []byte("Charlie"),
		"email": []byte("charlie@example.com"),
		"age":   []byte("30"),
	})

	// Select specific columns
	err = db.SelectAndDisplay("users", []string{"id", "email"}, "age", []byte("30"))
	assert.Nil(t, err, "Select failed")
	// assert.Equal(t, 2, len(results), "Expected 2 rows with age = 30")

	// Select all columns
	err = db.SelectAndDisplay("users", []string{"*"}, "age", []byte("30"))
	// ans, err = FormatResults(results, db.Tables["user"].Columns)
	// t.Log(ans, err)
	assert.Nil(t, err, "Select failed for all columns")

	t.Log("Select operation with multiple columns tested successfully")

	db.Close()
}
func TestSelectWithMultipleColumns1(t *testing.T) {
	// Initialize RDBMS
	db, err := NewRDBMS()
	assert.Nil(t, err, "RDBMS initialization failed")

	// Insert data
	_ = db.Insert("users", []byte("6"), map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
		"age":   []byte("30"),
	})
	_ = db.Insert("users", []byte("7"), map[string][]byte{
		"id":    []byte("2"),
		"name":  []byte("Bob"),
		"email": []byte("bob@example.com"),
		"age":   []byte("25"),
	})
	_ = db.Insert("users", []byte("8"), map[string][]byte{
		"id":    []byte("3"),
		"name":  []byte("Charlie"),
		"email": []byte("charlie@example.com"),
		"age":   []byte("30"),
	})

	// Select specific columns
	err = db.SelectAndDisplay("users", []string{"id", "email"}, "email", []byte("bob@example.com"))
	assert.Nil(t, err, "Select failed")
	// assert.Equal(t, 2, len(results), "Expected 2 rows with age = 30")

	// Select all columns
	err = db.SelectAndDisplay("users", []string{"*"}, "email", []byte("bob@example.com"))
	// ans, err = FormatResults(results, db.Tables["user"].Columns)
	// t.Log(ans, err)
	assert.Nil(t, err, "Select failed for all columns")

	t.Log("Select operation with multiple columns tested successfully")

	db.Close()
}
