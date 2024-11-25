package utils

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
)

// CompressToJSON compresses a Go object into a compressed JSON format.
func CompressToJSON(obj interface{}) ([]byte, error) {
	// Serialize the object to JSON
	jsonData, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal object to JSON: %v", err)
	}

	// Compress the JSON data
	var compressedData bytes.Buffer
	writer := gzip.NewWriter(&compressedData)
	if _, err := writer.Write(jsonData); err != nil {
		return nil, fmt.Errorf("failed to compress JSON: %v", err)
	}
	writer.Close() // Ensure all data is flushed and the gzip stream is closed

	return compressedData.Bytes(), nil
}

// DecompressFromJSON decompresses compressed JSON data and unmarshals it into a Go object.
func DecompressFromJSON(compressedData []byte, obj interface{}) error {
	// Decompress the JSON data
	reader, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %v", err)
	}
	defer reader.Close()

	// Read decompressed JSON data
	var jsonData bytes.Buffer
	if _, err := io.Copy(&jsonData, reader); err != nil {
		return fmt.Errorf("failed to read decompressed JSON: %v", err)
	}

	// Deserialize the JSON data into the Go object
	if err := json.Unmarshal(jsonData.Bytes(), obj); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	return nil
}
