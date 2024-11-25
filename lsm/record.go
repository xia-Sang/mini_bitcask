package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type Record struct {
	Key   []byte
	Value []byte
	RType recordType
}

type recordType uint8

const (
	recordSet    recordType = iota //设置记录
	recordDelete                   //删除记录
	// recordTxn                      //事务记录 后续补充
)

// record数据存储
// recordtype KeyLength ValueLength Key Value Crc32 | recordSet
// recordtype KeyLength ValueLength key Crc32 | recordDelete
func (rec *Record) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write recordType
	if err := buf.WriteByte(byte(rec.RType)); err != nil {
		return nil, fmt.Errorf("failed to write recordType: %v", err)
	}

	// Write KeyLength and ValueLength
	keyLength := uint32(len(rec.Key))
	valueLength := uint32(len(rec.Value))

	if err := binary.Write(buf, binary.BigEndian, keyLength); err != nil {
		return nil, fmt.Errorf("failed to write keyLength: %v", err)
	}

	if rec.RType == recordSet {
		if err := binary.Write(buf, binary.BigEndian, valueLength); err != nil {
			return nil, fmt.Errorf("failed to write valueLength: %v", err)
		}
	} else {
		// For recordDelete, ValueLength is always 0
		if err := binary.Write(buf, binary.BigEndian, uint32(0)); err != nil {
			return nil, fmt.Errorf("failed to write valueLength for recordDelete: %v", err)
		}
	}

	// Write Key
	if _, err := buf.Write(rec.Key); err != nil {
		return nil, fmt.Errorf("failed to write key: %v", err)
	}

	// Write Value (only for recordSet)
	if rec.RType == recordSet {
		if _, err := buf.Write(rec.Value); err != nil {
			return nil, fmt.Errorf("failed to write value: %v", err)
		}
	}

	// Compute and write CRC32
	crc := crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(buf, binary.BigEndian, crc); err != nil {
		return nil, fmt.Errorf("failed to write CRC32: %v", err)
	}

	return buf.Bytes(), nil
}

// 读取时候 先预读 recordtype keylength  valuelength
// 根据recordType进行判断
func ReadRecord(data []byte) (*Record, error) {
	buf := bytes.NewReader(data)

	// Read recordType
	recordTypeByte, err := buf.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read recordType: %v", err)
	}
	rType := recordType(recordTypeByte)

	// Read KeyLength and ValueLength
	var keyLength, valueLength uint32
	if err := binary.Read(buf, binary.BigEndian, &keyLength); err != nil {
		return nil, fmt.Errorf("failed to read keyLength: %v", err)
	}

	if err := binary.Read(buf, binary.BigEndian, &valueLength); err != nil {
		return nil, fmt.Errorf("failed to read valueLength: %v", err)
	}

	// Read Key
	key := make([]byte, keyLength)
	if _, err := buf.Read(key); err != nil {
		return nil, fmt.Errorf("failed to read key: %v", err)
	}

	// Read Value (only for recordSet)
	var value []byte
	if rType == recordSet {
		value = make([]byte, valueLength)
		if _, err := buf.Read(value); err != nil {
			return nil, fmt.Errorf("failed to read value: %v", err)
		}
	}

	// Read and verify CRC32
	var crc uint32
	if err := binary.Read(buf, binary.BigEndian, &crc); err != nil {
		return nil, fmt.Errorf("failed to read CRC32: %v", err)
	}

	expectedCrc := crc32.ChecksumIEEE(data[:len(data)-4])
	if crc != expectedCrc {
		return nil, fmt.Errorf("CRC32 mismatch: expected %d, got %d", expectedCrc, crc)
	}

	// Return the parsed Record
	return &Record{
		Key:   key,
		Value: value,
		RType: rType,
	}, nil
}
