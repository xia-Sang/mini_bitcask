package bitcask

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

// Record数据结构
type Record struct {
	expireTime uint32     //过期时间--这个存在个问题 时间一致性
	Key        []byte     //key
	Value      []byte     //value
	RecordType recordType //record类型
}
type recordType uint8

const (
	recordSet    recordType = iota //设置记录
	recordDelete                   //删除记录
	// recordTxn                      //事务记录 后续补充
)
const timeForever = ^uint32(0) // Maximum uint32 value, signifies "forever"

// NewRecordTimeForever creates a record with an infinite expiration time
func NewRecordTimeForever(key, value []byte) *Record {
	return &Record{
		expireTime: timeForever,
		Key:        key,
		Value:      value,
		RecordType: recordSet,
	}
}
func NewRecordTimeForeverDel(key []byte) *Record {
	return &Record{
		expireTime: timeForever,
		Key:        key,
		RecordType: recordDelete,
	}
}

// NewRecord creates a record with a specific expiration duration from now
func NewRecord(key, value []byte, duration time.Duration) *Record {
	expireTime := uint32(time.Now().Add(duration).Unix()) // Convert to UNIX timestamp (seconds)
	return &Record{
		expireTime: expireTime,
		Key:        key,
		Value:      value,
		RecordType: recordSet,
	}
}

// 存储格式
// timeout recordType keyLength  valueLength key value crc32 --recordSet
// timeout recordType keyLength  valueLength key value crc32  --recordTxn
// timeout recordType keyLength  key crc32 --recordDelete
// timeout calculateCRC32 calculates the CRC32 checksum for a record

// ToBytes serializes the Record to []byte with CRC32
func (r *Record) ToBytes() ([]byte, error) {
	var buffer bytes.Buffer

	// Write expire time (4 bytes)
	if err := binary.Write(&buffer, binary.LittleEndian, r.expireTime); err != nil {
		return nil, fmt.Errorf("failed to write expire time: %w", err)
	}

	// Write record type (1 byte)
	if err := binary.Write(&buffer, binary.LittleEndian, r.RecordType); err != nil {
		return nil, fmt.Errorf("failed to write record type: %w", err)
	}

	// Write key length (4 bytes)
	keyLength := uint32(len(r.Key))
	if err := binary.Write(&buffer, binary.LittleEndian, keyLength); err != nil {
		return nil, fmt.Errorf("failed to write key length: %w", err)
	}
	// Write value length (4 bytes)
	valueLength := uint32(len(r.Value))
	if err := binary.Write(&buffer, binary.LittleEndian, valueLength); err != nil {
		return nil, fmt.Errorf("failed to write key length: %w", err)
	}
	// Write key (variable length)
	if _, err := buffer.Write(r.Key); err != nil {
		return nil, fmt.Errorf("failed to write key: %w", err)
	}
	// Write value (variable length)
	if _, err := buffer.Write(r.Value); err != nil {
		return nil, fmt.Errorf("failed to write value: %w", err)
	}

	// Calculate CRC32 on all serialized data except CRC itself
	data := buffer.Bytes()
	crc := crc32.ChecksumIEEE(data)

	// Append CRC32 (4 bytes)
	if err := binary.Write(&buffer, binary.LittleEndian, crc); err != nil {
		return nil, fmt.Errorf("failed to write CRC32: %w", err)
	}

	return buffer.Bytes(), nil
}

// Pos位置信息存储
type Pos struct {
	Fid    uint32
	Offset uint32
	Length uint32
}
