package lsm

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash"

	"github.com/twmb/murmur3"
)

type BloomFilter interface {
	Add(key []byte) error
	Check(key []byte) bool
	Load(data []byte) error
	Store() ([]byte, error)
}

// MurmurBloomFilter is a Bloom filter implementation using MurmurHash3
type MurmurBloomFilter struct {
	bitArray  []bool
	hashFuncs []hash.Hash32
	arraySize uint
}

// NewMurmurBloomFilter creates a new MurmurBloomFilter with the given parameters.
func NewMurmurBloomFilter(size uint, numHashes uint) (*MurmurBloomFilter, error) {
	bf := &MurmurBloomFilter{
		bitArray:  make([]bool, size),
		arraySize: size,
		hashFuncs: make([]hash.Hash32, numHashes),
	}

	// Initialize fixed MurmurHash3 hash functions
	for i := uint(0); i < numHashes; i++ {
		bf.hashFuncs[i] = murmur3.New32() // Fixed MurmurHash3
	}

	return bf, nil
}

// Add inserts an element (key) into the Bloom filter.
func (bf *MurmurBloomFilter) Add(key []byte) error {
	for _, h := range bf.hashFuncs {
		h.Reset()
		h.Write(key)
		hashValue := h.Sum32()

		// Set the corresponding bit in the bitmap
		index := int(hashValue % uint32(bf.arraySize))
		bf.bitArray[index] = true
	}

	return nil
}

// Check checks if an element (key) might be in the Bloom filter.
func (bf *MurmurBloomFilter) Check(key []byte) bool {
	for _, h := range bf.hashFuncs {
		h.Reset()
		h.Write(key)
		hashValue := h.Sum32()

		// Check if the corresponding bit is set
		index := int(hashValue % uint32(bf.arraySize))
		if !bf.bitArray[index] {
			// If any bit is not set, the element is definitely not in the filter
			return false
		}
	}
	// If all bits are set, the element might be in the filter
	return true
}

// Load loads the Bloom filter state from the given byte slice.
func (bf *MurmurBloomFilter) Load(data []byte) error {
	// Decode the bitmap from the byte slice
	var bitArray []bool
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&bitArray); err != nil {
		return fmt.Errorf("failed to decode bitmap: %w", err)
	}

	// Assign the loaded bit array to the filter
	bf.bitArray = bitArray
	return nil
}

// Store returns the current Bloom filter state as a byte slice.
func (bf *MurmurBloomFilter) Store() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// Encode the bit array to the buffer
	if err := enc.Encode(bf.bitArray); err != nil {
		return nil, fmt.Errorf("failed to encode bitmap: %w", err)
	}

	return buf.Bytes(), nil
}
