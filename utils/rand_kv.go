package utils

import (
	"fmt"
	"math/rand"
	"time"
)

// generateKeys generates a list of random keys based on the specified parameters
func generateKeys(prefix []byte, split []byte, splitCount int, count int) [][]byte {
	rand.Seed(time.Now().UnixNano()) // Seed the random number generator

	var result [][]byte

	for i := 0; i < count; i++ {
		result = append(result, generateKey(prefix, split, splitCount, 8)) // Length of each random segment is 8
	}

	return result
}

// generateKey generates a single random key
func generateKey(prefix []byte, split []byte, splitCount int, randomSegmentLength int) []byte {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	key := make([]byte, len(prefix))
	copy(key, prefix) // Start with the prefix

	for i := 0; i < splitCount; i++ {
		key = append(key, split...)
		key = append(key, randomBytes(randomSegmentLength, charset)...)
	}

	return key
}

// randomBytes generates a random byte slice of a given length from the charset
func randomBytes(length int, charset string) []byte {
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return result
}

// GetKey generates a key based on pre-defined parameters
func GetValue(count int) []byte {
	return generateKey([]byte("bitcask-test-key"), []byte("-"), 2, count)
}
func GetKey(count int) []byte {
	return []byte(fmt.Sprintf("bitcask-test-key:%09d", count))
}
