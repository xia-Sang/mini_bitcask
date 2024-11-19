package bitcask

import (
	"bitcask/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB(t *testing.T) {
	db, err := NewDb(DefaultConfig())
	assert.Nil(t, err)

	for i := range 400 {
		key, value := utils.GetKey(i), utils.GetValue(12)
		db.Put(key, value)
	}
	db.memtable.Debug()
	// for i := range 400 {
	// 	key, _ := utils.GetKey(i), utils.GetValue(12)
	// 	value, err := db.Get(key)
	// 	assert.Nil(t, err)
	// 	t.Logf("%s   %s\n", key, value)
	// }
}
func TestDB1(t *testing.T) {
	db, err := NewDb(DefaultConfig())
	assert.Nil(t, err)
	t.Log(db)

	db.memtable.Debug()
	for i := range 50 {
		key, _ := utils.GetKey(i), utils.GetValue(12)
		value, err := db.Get(key)
		t.Logf("%s-%s-%v\n", key, value, err)
	}
}
func TestDB2(t *testing.T) {
	db, err := NewDb(DefaultConfig())
	assert.Nil(t, err)
	t.Log(db)

	db.Flush()
}
