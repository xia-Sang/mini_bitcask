package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// SSTable represents a sorted string table (SSTable) structure
type SSTable struct {
	fileHandler FileHandler
}

// WriteToSSTable writes the contents of a Memtable to an SSTable file
func (sst *SSTable) WriteSSTable(mem *Memtable, kvCount int, bloomFilter BloomFilter, prefixLength int) error {
	var indexEntries []IndexEntry
	// var dataBlockBuffer bytes.Buffer
	// var currentBlockSize int
	var currentBlockData []byte
	var currentBlockCount int

	// Use Memtable's fold function to iterate over key-value pairs
	mem.Fold(func(key []byte, value []byte) bool {
		// Add key to Bloom filter
		bloomFilter.Add(key)

		// Serialize key-value pair
		rec := Record{Key: key, Value: value} //serializeKeyValue(key, value)
		kvData, err := rec.ToBytes()
		if err != nil {
			return false // stop folding if there's an error
		}

		// Check if we've reached the kvCount threshold for the block
		if currentBlockCount >= kvCount {
			// Write current data block to SSTable and get the offset
			blockOffset, err := sst.writeDataBlock(currentBlockData)
			if err != nil {
				return false
			}

			// Add an index entry for the new data block
			indexEntries = append(indexEntries, IndexEntry{
				Key:    currentBlockData[:len(key)], // use the first key in the block as the index key
				Offset: blockOffset,
			})

			// Reset the current block data and size
			currentBlockData = nil
			currentBlockCount = 0
		}

		// Add key-value data to the current block
		currentBlockData = append(currentBlockData, kvData...)
		currentBlockCount++

		return true // continue folding
	})

	// Write the last data block if any data exists
	if currentBlockCount > 0 {
		blockOffset, err := sst.writeDataBlock(currentBlockData)
		if err != nil {
			return fmt.Errorf("failed to write last data block: %w", err)
		}
		indexEntries = append(indexEntries, IndexEntry{
			Key:    currentBlockData[:len(currentBlockData)], // first key in the last block
			Offset: blockOffset,
		})
	}

	// Write the index block
	indexData, err := sst.serializeIndexBlock(indexEntries, prefixLength)
	if err != nil {
		return fmt.Errorf("failed to serialize index block: %w", err)
	}

	// Write the Bloom filter (as a binary slice)
	bloomFilterData, err := bloomFilter.Store()
	if err != nil {
		return fmt.Errorf("failed to serialize bloom filter: %w", err)
	}

	// Write the Bloom filter and index data, followed by the data blocks to the SSTable file
	if err := sst.writeSSTable(bloomFilterData, indexData); err != nil {
		return fmt.Errorf("failed to write SSTable: %w", err)
	}

	return nil
}

// serializeKeyValue serializes a key-value pair into a byte slice
func serializeKeyValue(key, value []byte) ([]byte, error) {
	var buffer bytes.Buffer
	// Write the key length and key
	binary.Write(&buffer, binary.BigEndian, uint32(len(key)))
	buffer.Write(key)
	// Write the value length and value
	binary.Write(&buffer, binary.BigEndian, uint32(len(value)))
	buffer.Write(value)
	// Write the CRC32 of the key-value data
	data := buffer.Bytes()
	crc := crc32.ChecksumIEEE(data)
	binary.Write(&buffer, binary.BigEndian, crc)

	return buffer.Bytes(), nil
}

// writeDataBlock writes a data block to the file and returns the offset of the block
func (sst *SSTable) writeDataBlock(blockData []byte) (int64, error) {
	// Assuming fileHandler has a Write method that appends data to the file
	offset, err := sst.fileHandler.Write(blockData)
	if err != nil {
		return 0, fmt.Errorf("failed to write data block to file: %w", err)
	}
	return int64(offset), nil
}

// serializeIndexBlock serializes the index block as a byte slice, optionally using prefix indexing
func (sst *SSTable) serializeIndexBlock(entries []IndexEntry, prefixLength int) ([]byte, error) {
	var buffer bytes.Buffer
	for _, entry := range entries {
		var indexKey []byte
		// Use prefix indexing if prefixLength > 0
		if prefixLength > 0 && len(entry.Key) > prefixLength {
			indexKey = entry.Key[:prefixLength]
		} else {
			indexKey = entry.Key
		}

		// Write the key and offset of each index entry
		binary.Write(&buffer, binary.BigEndian, uint32(len(indexKey)))
		buffer.Write(indexKey)
		binary.Write(&buffer, binary.BigEndian, entry.Offset)
	}
	return buffer.Bytes(), nil
}

// writeSSTable writes the Bloom filter, index block, and data blocks to the SSTable file
func (sst *SSTable) writeSSTable(bloomFilterData, indexData []byte) error {
	// Write the Bloom filter data to the file
	_, err := sst.fileHandler.Write(bloomFilterData)
	if err != nil {
		return fmt.Errorf("failed to write Bloom filter: %w", err)
	}

	// Write the index block to the file
	_, err = sst.fileHandler.Write(indexData)
	if err != nil {
		return fmt.Errorf("failed to write index block: %w", err)
	}

	return nil
}

// IndexEntry represents an index entry in the SSTable
type IndexEntry struct {
	Key    []byte
	Offset int64
}
