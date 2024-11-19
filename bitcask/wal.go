package bitcask

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"time"
)

// WAL represents the Write-Ahead Log
type WAL struct {
	Fid         uint32
	Offset      uint32
	fileHandler FileHandler
}

// NewWAL initializes a new WAL
func NewWAL(fileHandler FileHandler) (*WAL, error) {
	return &WAL{fileHandler: fileHandler}, nil
}

// getWalFileName generates a WAL file name given a directory path and file ID.
func getWalFileName(dirPath string, fid uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("wal_%05d.log", fid))
}
func CreateNewWAL(dirPath string, fid uint32) (*WAL, error) {
	filename := getWalFileName(dirPath, fid)
	filehander, err := NewOSFileHandler(filename, false)
	if err != nil {
		return nil, err
	}
	return &WAL{Fid: fid, fileHandler: filehander, Offset: 0}, nil
}
func ReadNewWAL(dirPath string, fid uint32) (*WAL, error) {
	filename := getWalFileName(dirPath, fid)
	filehander, err := NewOSFileHandler(filename, true)
	if err != nil {
		return nil, err
	}
	return &WAL{Fid: fid, fileHandler: filehander, Offset: 0}, nil
}

// AppendPut appends a PUT operation to the WAL
func (wal *WAL) AppendPut(record *Record) error {
	data, err := record.ToBytes()
	if err != nil {
		return err
	}
	length, err := wal.fileHandler.Write(data)
	wal.Offset += uint32(length)
	return err
}
func (wal *WAL) Recover(memtable *Memtable) error {
	offset := int64(0)

	// Get the file size
	fileSize, err := wal.fileHandler.Size()
	if err != nil {
		return fmt.Errorf("failed to get file size: %w", err)
	}

	timeNow := uint32(time.Now().Unix()) // Current time for expiration checks

	for offset < fileSize {
		startOffset := offset   // Save the starting offset for this record
		var buffer bytes.Buffer // Buffer to accumulate data for CRC calculation

		// Step 1: Read fixed-size header (4 bytes expireTime, 1 byte recordType, 4 bytes keyLength, 4 bytes valueLength)
		headerLength := 4 + 1 + 4 + 4
		headerBuf, err := wal.fileHandler.ReadAt(offset, headerLength)
		if err != nil {
			return fmt.Errorf("failed to read header at offset %d: %w", offset, err)
		}
		buffer.Write(headerBuf) // Add header to buffer

		recordExpireTime := binary.LittleEndian.Uint32(headerBuf[:4])
		recordType := recordType(headerBuf[4])
		keyLength := binary.LittleEndian.Uint32(headerBuf[5:9])
		valueLength := binary.LittleEndian.Uint32(headerBuf[9:13])
		offset += int64(headerLength) // Advance offset by header size

		expireFlag := recordExpireTime <= timeNow

		// Step 2: Read key
		key, err := wal.fileHandler.ReadAt(offset, int(keyLength))
		if err != nil {
			return fmt.Errorf("failed to read key at offset %d: %w", offset, err)
		}
		buffer.Write(key)          // Add key to buffer
		offset += int64(keyLength) // Advance offset by key length

		// Step 3: Read value (if valueLength > 0)
		var value []byte
		if valueLength > 0 {
			value, err = wal.fileHandler.ReadAt(offset, int(valueLength))
			if err != nil {
				return fmt.Errorf("failed to read value at offset %d: %w", offset, err)
			}
			buffer.Write(value)          // Add value to buffer
			offset += int64(valueLength) // Advance offset by value length
		}

		// Step 4: Read CRC32 (4 bytes)
		crcBuf, err := wal.fileHandler.ReadAt(offset, 4)
		if err != nil {
			return fmt.Errorf("failed to read CRC32 at offset %d: %w", offset, err)
		}
		expectedCRC := binary.LittleEndian.Uint32(crcBuf)
		offset += 4 // Advance offset by 4 bytes

		// Step 5: If expired, skip CRC check and processing
		if expireFlag {
			continue
		}

		// Step 6: Calculate and verify CRC32 from buffer
		calculatedCRC := crc32.ChecksumIEEE(buffer.Bytes())
		if calculatedCRC != expectedCRC {
			return fmt.Errorf("CRC32 verification failed at offset %d: expected %x, got %x", startOffset, expectedCRC, calculatedCRC)
		}

		// Step 7: Update memtable based on record type
		length := uint32(offset - startOffset) // Total length of this record
		if recordType == recordSet {
			memtable.Put(key, &Pos{Fid: wal.Fid, Offset: uint32(startOffset), Length: length})
		} else if recordType == recordDelete {
			memtable.Delete(key)
		}
	}
	// Update WAL offset after recovery
	wal.Offset = uint32(offset)
	return nil
}

// readRecord reads a record from the WAL at the given offset and known length.
func (wal *WAL) readRecord(offset, length uint32) (*Record, error) {
	// Step 1: Read the entire record data
	data, err := wal.fileHandler.ReadAt(int64(offset), int(length))
	if err != nil {
		return nil, fmt.Errorf("failed to read record at offset %d: %w", offset, err)
	}

	// Step 2: Parse the fixed-size header directly from slices
	if len(data) < 13 { // Minimum size for header (4+1+4+4 bytes)
		return nil, fmt.Errorf("record at offset %d is too small", offset)
	}

	recordExpireTime := binary.LittleEndian.Uint32(data[:4])
	timeNow := uint32(time.Now().Unix())
	if recordExpireTime <= timeNow {
		return nil, fmt.Errorf("record at offset %d has expired (expireTime: %d, currentTime: %d)", offset, recordExpireTime, timeNow)
	}

	recordType := recordType(data[4])
	keyLength := binary.LittleEndian.Uint32(data[5:9])
	valueLength := binary.LittleEndian.Uint32(data[9:13])

	// Step 3: Calculate the total expected size and validate
	expectedSize := 13 + int(keyLength) + int(valueLength) + 4 // Header + key + value + CRC32
	if len(data) != expectedSize {
		return nil, fmt.Errorf("record at offset %d has unexpected size: got %d, expected %d", offset, len(data), expectedSize)
	}

	// Step 4: Parse key and value
	key := data[13 : 13+keyLength]
	value := data[13+keyLength : 13+keyLength+valueLength]

	// Step 5: Extract and validate CRC32
	crcOffset := len(data) - 4
	expectedCRC := binary.LittleEndian.Uint32(data[crcOffset:])
	calculatedCRC := crc32.ChecksumIEEE(data[:crcOffset])
	if calculatedCRC != expectedCRC {
		return nil, fmt.Errorf("CRC32 mismatch at offset %d: expected %x, got %x", offset, expectedCRC, calculatedCRC)
	}

	// Step 6: Create and return the Record object
	return &Record{
		expireTime: recordExpireTime,
		RecordType: recordType,
		Key:        key,
		Value:      value,
	}, nil
}

// Write writes data to the WAL
func (wal *WAL) Write(data []byte) (*Pos, error) {
	length, err := wal.fileHandler.Write(data)
	if err != nil {
		return nil, err
	}

	pos := &Pos{wal.Fid, wal.Offset, uint32(length)}
	wal.Offset += uint32(length)
	return pos, nil
}

// ReadAt reads data from the WAL at a specific offset
func (wal *WAL) ReadAt(offset int64, length int) ([]byte, error) {
	return wal.fileHandler.ReadAt(offset, length)
}

// Size returns the size of the WAL file
func (wal *WAL) Size() (int64, error) {
	return wal.fileHandler.Size()
}

// Sync flushes the WAL data to disk (if FileHandler supports it)
func (wal *WAL) Sync() error {
	return wal.fileHandler.Sync()
}

// Close closes the WAL file
func (wal *WAL) Close() error {
	if err := wal.Sync(); err != nil {
		return err
	}
	return wal.fileHandler.Close()
}

// Close closes the WAL file
func (wal *WAL) ToReadOnly() error {
	return wal.fileHandler.ToReadOnly()
}
func (wal *WAL) delete() error {
	_ = wal.Close()
	return wal.fileHandler.Delete()
}
