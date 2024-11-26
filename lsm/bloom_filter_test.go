package lsm

import (
	"bitcask/utils"
	"testing"
)

func TestMurmurBloomFilter(t *testing.T) {
	// Test parameters
	arraySize := uint(1000) // Size of the Bloom filter bit array
	numHashes := uint(3)    // Number of hash functions

	// Create a new Bloom filter
	bf, err := NewMurmurBloomFilter(arraySize, numHashes)
	if err != nil {
		t.Fatalf("Error creating Bloom filter: %v", err)
	}

	// Test case 1: Add keys and check if they exist
	keysToAdd := utils.GenerateKeys([]byte("lsm"), []byte("-"), 9, 199)

	// Add keys to the Bloom filter
	for _, key := range keysToAdd {
		err := bf.Add(key)
		if err != nil {
			t.Errorf("Error adding key %s: %v", key, err)
		}
	}

	// Check if added keys are present
	for _, key := range keysToAdd {
		if !bf.Check(key) {
			t.Errorf("Key %s should be in the Bloom filter", key)
		}
	}

	// Test case 2: Check a key that was not added
	missingKey := []byte("key4")
	if bf.Check(missingKey) {
		t.Errorf("Key %s should not be in the Bloom filter", missingKey)
	}

	// Test case 3: Serialize the Bloom filter state to []byte
	storedData, err := bf.Store()
	if err != nil {
		t.Fatalf("Error storing Bloom filter: %v", err)
	}

	// Test case 4: Create a new Bloom filter and load the state from the serialized []byte
	newBF, err := NewMurmurBloomFilter(arraySize, numHashes)
	if err != nil {
		t.Fatalf("Error creating new Bloom filter: %v", err)
	}

	// Load the state from the serialized data
	err = newBF.Load(storedData)
	if err != nil {
		t.Fatalf("Error loading Bloom filter: %v", err)
	}

	// Check if the loaded Bloom filter has the same state
	for _, key := range keysToAdd {
		if !newBF.Check(key) {
			t.Errorf("Key %s should be in the loaded Bloom filter", key)
		}
	}

	// Check for a missing key in the loaded Bloom filter
	if newBF.Check(missingKey) {
		t.Errorf("Key %s should not be in the loaded Bloom filter", missingKey)
	}
}
