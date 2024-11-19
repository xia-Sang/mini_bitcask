package utils

import (
	"bytes"
	"strings"
	"testing"
)

func TestGenerateKey(t *testing.T) {
	for i := range 20 {
		t.Logf("%s:%s", GetKey(i), GetValue(12))
	}
}

// Helper function to check if the key has the correct format
func keyHasCorrectFormat(key, prefix, split []byte, splitCount int) bool {
	parts := strings.Split(string(key), string(split))
	return len(parts) == splitCount+1 && bytes.Equal([]byte(parts[0]), prefix)
}
