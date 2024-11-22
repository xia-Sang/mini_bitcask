package sql

import (
	"bitcask/bitcask"
	"bitcask/conf"
	"bitcask/utils"
)

type KVStore interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}

type RDBMS struct {
	Store KVStore // Bitcask 底层存储
}

func NewRDBMS() (*RDBMS, error) {
	config := conf.DefaultConfig()
	db, err := bitcask.NewDb(config)
	if err != nil {
		return nil, err
	}
	return &RDBMS{db}, nil
}
func (db *RDBMS) Insert(tableName string, primaryKey []byte, rowData map[string][]byte) error {
	// 1. 序列化行数据
	serializedData, err := utils.SerializeRow(rowData)
	if err != nil {
		return err
	}

	// 2. 存储主键记录
	key := append([]byte(tableName+":"), primaryKey...)
	if err := db.Store.Put(key, serializedData); err != nil {
		return err
	}

	// 3. 更新索引（可选）
	for column, value := range rowData {
		indexKey := append([]byte("index:"+tableName+":"+column+":"), value...)
		existing, _ := db.Store.Get(indexKey)

		updatedIndex := append(existing, primaryKey...) // 将主键追加到索引值中
		if err := db.Store.Put(indexKey, updatedIndex); err != nil {
			return err
		}
	}
	return nil
}
func (db *RDBMS) QueryByPrimaryKey(tableName string, primaryKey []byte) (map[string][]byte, error) {
	// 构造主键键
	key := append([]byte(tableName+":"), primaryKey...)
	value, err := db.Store.Get(key)
	if err != nil {
		return nil, err
	}

	// 反序列化行数据
	return utils.DeserializeRow(value)
}

func (db *RDBMS) QueryByCondition(tableName, column string, value []byte) ([]map[string][]byte, error) {
	// 1. 查找索引键
	indexKey := append([]byte("index:"+tableName+":"+column+":"), value...)
	primaryKeys, err := db.Store.Get(indexKey)
	if err != nil {
		return nil, err
	}
	primaryKeyLength := len(primaryKeys)
	// 2. 根据主键逐条查询数据
	var results []map[string][]byte
	for i := 0; i < len(primaryKeys); i += primaryKeyLength {
		primaryKey := primaryKeys[i : i+primaryKeyLength]
		row, err := db.QueryByPrimaryKey(tableName, primaryKey)
		if err != nil {
			return nil, err
		}
		results = append(results, row)
	}
	return results, nil
}
func (db *RDBMS) Update(tableName string, primaryKey []byte, updates map[string][]byte) error {
	// 1. 获取旧记录
	oldData, err := db.QueryByPrimaryKey(tableName, primaryKey)
	if err != nil {
		return err
	}

	// 2. 删除旧索引
	for column, value := range oldData {
		if column == "primary_key" {
			continue
		}
		indexKey := append([]byte("index:"+tableName+":"+column+":"), value...)
		db.Store.Delete(indexKey)
	}

	// 3. 合并新数据
	for k, v := range updates {
		oldData[k] = v
	}

	// 4. 写入新值
	newSerializedData, _ := utils.SerializeRow(oldData)
	key := append([]byte(tableName+":"), primaryKey...)
	db.Store.Put(key, newSerializedData)

	// 5. 写入新索引
	for column, value := range oldData {
		if column == "primary_key" {
			continue
		}
		indexKey := append([]byte("index:"+tableName+":"+column+":"), value...)
		db.Store.Put(indexKey, primaryKey)
	}
	return nil
}
func (db *RDBMS) Delete(tableName string, primaryKey []byte) error {
	// 1. 获取旧记录
	row, err := db.QueryByPrimaryKey(tableName, primaryKey)
	if err != nil {
		return err
	}

	// 2. 删除所有相关索引
	for column, value := range row {
		indexKey := append([]byte("index:"+tableName+":"+column+":"), value...)
		db.Store.Delete(indexKey)
	}

	// 3. 删除主键记录
	key := append([]byte(tableName+":"), primaryKey...)
	return db.Store.Delete(key)
}
