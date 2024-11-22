package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestView(t *testing.T) {
	// Initialize RDBMS
	db, err := NewRDBMS()
	assert.Nil(t, err, "RDBMS initialization failed")

	// Create tables
	err = db.CreateTable("users", []string{"id", "name", "email"})
	assert.Nil(t, err, "CreateTable for 'users' failed")
	t.Log("Created table 'users'")

	err = db.CreateTable("orders", []string{"order_id", "user_id", "amount"})
	assert.Nil(t, err, "CreateTable for 'orders' failed")
	t.Log("Created table 'orders'")

	// Add elements to 'users'
	err = db.Insert("users", []byte("1"), map[string][]byte{
		"id":    []byte("1"),
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
	})
	assert.Nil(t, err, "Insert into 'users' failed")
	t.Log("Inserted record 1 into 'users'")

	err = db.Insert("users", []byte("2"), map[string][]byte{
		"id":    []byte("2"),
		"name":  []byte("Bob"),
		"email": []byte("bob@example.com"),
	})
	assert.Nil(t, err, "Insert into 'users' failed")
	t.Log("Inserted record 2 into 'users'")

	err = db.Insert("users", []byte("3"), map[string][]byte{
		"id":    []byte("3"),
		"name":  []byte("Charlie"),
		"email": []byte("charlie@example.com"),
	})
	assert.Nil(t, err, "Insert into 'users' failed")
	t.Log("Inserted record 3 into 'users'")

	// Add elements to 'orders'
	err = db.Insert("orders", []byte("1001"), map[string][]byte{
		"order_id": []byte("1001"),
		"user_id":  []byte("1"),
		"amount":   []byte("250.75"),
	})
	assert.Nil(t, err, "Insert into 'orders' failed")
	t.Log("Inserted record 1001 into 'orders'")

	err = db.Insert("orders", []byte("1002"), map[string][]byte{
		"order_id": []byte("1002"),
		"user_id":  []byte("2"),
		"amount":   []byte("125.50"),
	})
	assert.Nil(t, err, "Insert into 'orders' failed")
	t.Log("Inserted record 1002 into 'orders'")

	err = db.Insert("orders", []byte("1003"), map[string][]byte{
		"order_id": []byte("1003"),
		"user_id":  []byte("3"),
		"amount":   []byte("375.00"),
	})
	assert.Nil(t, err, "Insert into 'orders' failed")
	t.Log("Inserted record 1003 into 'orders'")

	// View the 'users' table
	t.Log("Viewing 'users' table")
	err = db.View("users")
	assert.Nil(t, err, "View for 'users' failed")

	// View the 'orders' table
	t.Log("Viewing 'orders' table")
	err = db.View("orders")
	assert.Nil(t, err, "View for 'orders' failed")

	// Update a record in 'users'
	err = db.Update("users", []byte("1"), map[string][]byte{
		"email": []byte("alice.new@example.com"),
	})
	assert.Nil(t, err, "Update in 'users' failed")
	t.Log("Updated record 1 in 'users'")

	// Update a record in 'orders'
	err = db.Update("orders", []byte("1002"), map[string][]byte{
		"amount": []byte("150.00"),
	})
	assert.Nil(t, err, "Update in 'orders' failed")
	t.Log("Updated record 1002 in 'orders'")

	// Verify the update in 'users'
	row, err := db.QueryByPrimaryKey("users", []byte("1"))
	assert.Nil(t, err, "QueryByPrimaryKey in 'users' failed")
	assert.Equal(t, []byte("alice.new@example.com"), row["email"], "Email update in 'users' not applied correctly")
	t.Log("Verified update for record 1 in 'users'")

	// Verify the update in 'orders'
	row, err = db.QueryByPrimaryKey("orders", []byte("1002"))
	assert.Nil(t, err, "QueryByPrimaryKey in 'orders' failed")
	assert.Equal(t, []byte("150.00"), row["amount"], "Amount update in 'orders' not applied correctly")
	t.Log("Verified update for record 1002 in 'orders'")

	// Delete a record from 'users'
	err = db.Delete("users", []byte("3"))
	assert.Nil(t, err, "Delete from 'users' failed")
	t.Log("Deleted record 3 from 'users'")

	// Verify the delete in 'users'
	_, err = db.QueryByPrimaryKey("users", []byte("3"))
	assert.NotNil(t, err, "QueryByPrimaryKey should fail for deleted record in 'users'")
	t.Log("Verified deletion of record 3 from 'users'")

	// Delete a record from 'orders'
	err = db.Delete("orders", []byte("1003"))
	assert.Nil(t, err, "Delete from 'orders' failed")
	t.Log("Deleted record 1003 from 'orders'")

	// Verify the delete in 'orders'
	_, err = db.QueryByPrimaryKey("orders", []byte("1003"))
	assert.NotNil(t, err, "QueryByPrimaryKey should fail for deleted record in 'orders'")
	t.Log("Verified deletion of record 1003 from 'orders'")

	// View all tables after updates and deletions
	t.Log("Viewing all tables after updates and deletions")
	err = db.ViewAllTables()
	assert.Nil(t, err, "ViewAllTables failed")

	db.Close()
}

func TestView1(t *testing.T) {
	// Initialize RDBMS
	db, err := NewRDBMS()
	assert.Nil(t, err)
	db.ViewAllTables()
}
