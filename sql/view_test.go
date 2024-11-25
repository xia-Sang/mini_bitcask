package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestView(t *testing.T) {
	// Initialize RDBMS
	db, err := NewRDBMS()
	assert.Nil(t, err, "RDBMS initialization failed")
	defer db.Close()

	// Create tables
	err = db.CreateTableByMap("users", map[string]FieldType{
		"id":    FieldTypeInt,
		"name":  FieldTypeString,
		"email": FieldTypeString,
	})
	assert.Nil(t, err, "CreateTable for 'users' failed")

	err = db.CreateTableByMap("orders", map[string]FieldType{
		"order_id": FieldTypeInt,
		"user_id":  FieldTypeInt,
		"amount":   FieldTypeFloat,
	})
	assert.Nil(t, err, "CreateTable for 'orders' failed")

	// Add elements to 'users'
	err = db.Insert("users", []byte("1"), map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
	})
	assert.Nil(t, err, "Insert into 'users' failed")

	err = db.Insert("users", []byte("2"), map[string][]byte{
		"id":    []byte("2"),
		"name":  []byte("Bob"),
		"email": []byte("bob@example.com"),
	})
	assert.Nil(t, err, "Insert into 'users' failed")

	err = db.Insert("users", []byte("3"), map[string][]byte{
		"id":    []byte("3"),
		"name":  []byte("Charlie"),
		"email": []byte("charlie@example.com"),
	})
	assert.Nil(t, err, "Insert into 'users' failed")

	// Add elements to 'orders'
	err = db.Insert("orders", []byte("1001"), map[string][]byte{
		"order_id": []byte("1001"),
		"user_id":  []byte("1"),
		"amount":   []byte("250.75"),
	})
	assert.Nil(t, err, "Insert into 'orders' failed")

	err = db.Insert("orders", []byte("1002"), map[string][]byte{
		"order_id": []byte("1002"),
		"user_id":  []byte("2"),
		"amount":   []byte("125.50"),
	})
	assert.Nil(t, err, "Insert into 'orders' failed")

	err = db.Insert("orders", []byte("1003"), map[string][]byte{
		"order_id": []byte("1003"),
		"user_id":  []byte("3"),
		"amount":   []byte("375.00"),
	})
	assert.Nil(t, err, "Insert into 'orders' failed")

	// View the 'users' table
	t.Log("Viewing 'users' table:")
	err = db.View("users")
	assert.Nil(t, err, "View for 'users' failed")

	// View the 'orders' table
	t.Log("Viewing 'orders' table:")
	err = db.View("orders")
	assert.Nil(t, err, "View for 'orders' failed")

	// Update a record in 'users'
	err = db.Update("users", []byte("1"), map[string][]byte{
		"email": []byte("alice.new@example.com"),
	})
	assert.Nil(t, err, "Update in 'users' failed")

	// Update a record in 'orders'
	err = db.Update("orders", []byte("1002"), map[string][]byte{
		"amount": []byte("150.00"),
	})
	assert.Nil(t, err, "Update in 'orders' failed")

	// Verify the update in 'users'
	row, err := db.QueryByPrimaryKey("users", []byte("1"))
	assert.Nil(t, err, "QueryByPrimaryKey in 'users' failed")
	assert.Equal(t, []byte("alice.new@example.com"), row["email"], "Email update in 'users' not applied correctly")

	// Verify the update in 'orders'
	row, err = db.QueryByPrimaryKey("orders", []byte("1002"))
	assert.Nil(t, err, "QueryByPrimaryKey in 'orders' failed")
	assert.Equal(t, []byte("150.00"), row["amount"], "Amount update in 'orders' not applied correctly")

	// Delete a record from 'users'
	err = db.Delete("users", []byte("3"))
	assert.Nil(t, err, "Delete from 'users' failed")

	// Verify the delete in 'users'
	_, err = db.QueryByPrimaryKey("users", []byte("3"))
	assert.NotNil(t, err, "QueryByPrimaryKey should fail for deleted record in 'users'")

	// Delete a record from 'orders'
	err = db.Delete("orders", []byte("1003"))
	assert.Nil(t, err, "Delete from 'orders' failed")

	// Verify the delete in 'orders'
	_, err = db.QueryByPrimaryKey("orders", []byte("1003"))
	assert.NotNil(t, err, "QueryByPrimaryKey should fail for deleted record in 'orders'")

	// View all tables after updates and deletions
	t.Log("Viewing all tables after updates and deletions:")
	err = db.ViewAllTables()
	assert.Nil(t, err, "ViewAllTables failed")
}

func TestView1(t *testing.T) {
	// Initialize RDBMS
	db, err := NewRDBMS()
	assert.Nil(t, err)
	db.ViewAllTables()
}
func TestView2(t *testing.T) {
	// Initialize RDBMS
	db, err := NewRDBMS()
	assert.Nil(t, err)

	// db.SelectAndDisplay("orders", []string{"*"}, "*")
	// db.ViewAllTables()
	err = db.SelectAndDisplay("orders", []string{"*"})
	t.Log(err)
	err = db.SelectWhereAndDisplay("orders", []string{"*"}, map[string]Condition{
		"user_id": {Operator: "<=", Value: []byte("1")},
	})
	t.Log(err)
	err = db.SelectWhereAndDisplay("orders", []string{"*"}, map[string]Condition{
		"amount":  {Operator: "<=", Value: []byte("260")},
		"user_id": {Operator: "<=", Value: []byte("1")},
	})
	t.Log(err)

}
