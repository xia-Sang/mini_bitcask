package bitcask

import (
	"fmt"
	"io"
	"os"
)

// FileHandler interface for file operations
type FileHandler interface {
	Write(data []byte) (int, error)                  // File write
	ReadAt(offset int64, length int) ([]byte, error) // File read
	Size() (int64, error)                            // File size
	Close() error                                    // Close file
	Sync() error                                     // Synchronize data to disk
	ToReadOnly() error                               // Convert file to read-only mode
	Delete() error                                   // Delete file
}

// OSFileHandler implements FileHandler using os.File
type OSFileHandler struct {
	file *os.File
}

// NewOSFileHandler creates a new OSFileHandler
// The `readOnly` parameter determines if the file should be opened in read-only mode.
func NewOSFileHandler(filePath string, readOnly bool) (*OSFileHandler, error) {
	var file *os.File
	var err error

	if readOnly {
		// 打开文件为只读模式
		file, err = os.OpenFile(filePath, os.O_RDONLY, 0644)
	} else {
		// 打开文件为读写模式
		file, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	return &OSFileHandler{file: file}, nil
}

// Write writes data to the file
func (h *OSFileHandler) Write(data []byte) (int, error) {
	return h.file.Write(data)
}

// ReadAt reads data from a specific offset
func (h *OSFileHandler) ReadAt(offset int64, length int) ([]byte, error) {
	buffer := make([]byte, length)
	_, err := h.file.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buffer, nil
}

// Size returns the size of the file
func (h *OSFileHandler) Size() (int64, error) {
	info, err := h.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Sync synchronizes the file's content to disk
func (h *OSFileHandler) Sync() error {
	return h.file.Sync()
}

// Close closes the file
func (h *OSFileHandler) Close() error {
	return h.file.Close()
}

// ToReadOnly converts the file to read-only mode
func (h *OSFileHandler) ToReadOnly() error {
	// 如果文件已关闭，重新打开为只读
	if h.file == nil {
		return fmt.Errorf("file is already closed")
	}

	// 获取当前文件路径
	filePath := h.file.Name()

	// 关闭当前文件
	if err := h.file.Close(); err != nil {
		return fmt.Errorf("failed to close file before reopening as read-only: %w", err)
	}

	// 重新以只读模式打开文件
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen file as read-only: %w", err)
	}

	h.file = file
	return nil
}

// Delete deletes the file associated with the handler
func (h *OSFileHandler) Delete() error {
	if h.file == nil {
		return fmt.Errorf("file handler is not initialized")
	}

	// 获取文件路径
	filePath := h.file.Name()

	// Step 1: 关闭文件
	if err := h.Close(); err != nil {
		return fmt.Errorf("failed to close file before deleting: %w", err)
	}

	// Step 2: 删除文件
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete file %s: %w", filePath, err)
	}

	return nil
}
