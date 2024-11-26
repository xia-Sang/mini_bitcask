package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"path/filepath"
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

// Recover replays the WAL to restore the memtable state.
func (wal *WAL) Recover(memtable *Memtable) error {
	offset := int64(0)

	// Get the file size
	fileSize, err := wal.fileHandler.Size()
	if err != nil {
		return fmt.Errorf("failed to get file size: %w", err)
	}

	// Loop through the WAL file until we reach the end
	for offset < fileSize {
		startOffset := offset // Save the starting offset for this record

		// Step 1: Read the fixed-size header (1 byte recordType, 4 bytes keyLength, 4 bytes valueLength)
		header, err := wal.fileHandler.ReadAt(offset, 9) // 9 bytes header
		if err != nil {
			return fmt.Errorf("failed to read record header at offset %d: %w", offset, err)
		}
		offset += int64(len(header))

		// Extract the recordType, keyLength, and valueLength from the header
		recordType := recordType(header[0])
		keyLength := binary.BigEndian.Uint32(header[1:5])
		valueLength := binary.BigEndian.Uint32(header[5:9])

		// Step 2: Read key and value + CRC32 in a single read
		recordSize := int(keyLength + valueLength + 4) // keyLength + valueLength + 4 bytes for CRC32
		recordData, err := wal.fileHandler.ReadAt(offset, recordSize)
		if err != nil {
			return fmt.Errorf("failed to read record data at offset %d: %w", offset, err)
		}
		offset += int64(len(recordData))

		// Extract key and value from the recordData
		key := recordData[:keyLength]
		var value []byte
		if recordType == recordSet {
			value = recordData[keyLength : keyLength+valueLength]
		}

		// Read CRC32 from the last 4 bytes
		expectedCRC := binary.BigEndian.Uint32(recordData[len(recordData)-4:])

		// Step 3: Calculate and verify CRC32
		var buffer bytes.Buffer
		buffer.WriteByte(byte(recordType))                   // Write recordType
		binary.Write(&buffer, binary.BigEndian, keyLength)   // Write keyLength
		binary.Write(&buffer, binary.BigEndian, valueLength) // Write valueLength
		buffer.Write(key)                                    // Write key
		if recordType == recordSet {
			buffer.Write(value) // Write value if it's a recordSet
		}

		// Calculate CRC32 over the buffer (without the last 4 bytes, since that's CRC)
		calculatedCRC := crc32.ChecksumIEEE(buffer.Bytes())
		if calculatedCRC != expectedCRC {
			return fmt.Errorf("CRC32 mismatch at offset %d: expected %d, got %d", startOffset, expectedCRC, calculatedCRC)
		}

		// Step 4: Update the memtable based on the record type
		if recordType == recordSet {
			memtable.Put(key, value)
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
	// Step 1: Read the raw data from the file at the specified offset and length
	data, err := wal.fileHandler.ReadAt(int64(offset), int(length))
	if err != nil {
		return nil, fmt.Errorf("failed to read record at offset %d: %w", offset, err)
	}

	// Step 2: Parse the raw data into a Record using the ReadRecord function
	record, err := ReadRecord(data)
	if err != nil {
		return nil, fmt.Errorf("failed to read record at offset %d: %w", offset, err)
	}

	// Step 3: Return the parsed record
	return record, nil
}

// Write writes data to the WAL
func (wal *WAL) Write(data []byte) error {
	length, err := wal.fileHandler.Write(data)
	if err != nil {
		return err
	}

	wal.Offset += uint32(length)
	return nil
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
