package conf

import (
	"fmt"
	"os"
)

// Config holds the configuration for the storage system.
type Config struct {
	DirPath         string // Directory path for storage files
	MemtableOrder   int    // Order of the B-tree used in the memtable
	WalSize         uint32 // Maximum size of the memtable (in bytes)
	KeyValueMaxSize uint32 // Maximum size of a single key-value pair (in bytes)
	FidMaxSize      uint32 // Maximum size of a single file ID (in bytes)
}

// ApplyDefaults ensures all fields in Config have reasonable default values.
func (c *Config) ApplyDefaults() {
	if c.DirPath == "" {
		c.DirPath = "./data" // Default storage directory
	}
	if c.MemtableOrder <= 0 {
		c.MemtableOrder = 9 // Default B-tree order
	}
	if c.WalSize <= 0 {
		c.WalSize = 64 * 1024 // Default memtable size: 64 KB
	}
	if c.KeyValueMaxSize == 0 {
		c.KeyValueMaxSize = 1024 // Default max key-value size: 1 KB
	}
	if c.FidMaxSize == 0 {
		c.FidMaxSize = 10 * 1024 * 1024 // Default max file size: 10 MB
	}
}

// Validate checks if the Config values are valid.
func (c *Config) Validate() error {
	if c.DirPath == "" {
		return fmt.Errorf("DirPath cannot be empty")
	}
	if err := checkDirPath(c.DirPath); err != nil {
		return fmt.Errorf("DirPath cannot be create")
	}
	if c.MemtableOrder < 3 {
		return fmt.Errorf("MemtableOrder must be at least 3")
	}
	if c.WalSize <= 0 {
		return fmt.Errorf("WalSize must be greater than 0")
	}
	if c.KeyValueMaxSize == 0 || c.KeyValueMaxSize > 10*1024*1024 {
		return fmt.Errorf("KeyValueMaxSize must be between 1 and 10 MB")
	}
	if c.FidMaxSize == 0 || c.FidMaxSize > 1024*1024*1024 {
		return fmt.Errorf("FidMaxSize must be between 1 MB and 1 GB")
	}
	return nil
}

// DefaultConfig returns a Config instance with default values.
func DefaultConfig() *Config {
	return &Config{
		DirPath:         "./data", // Default directory for storage files
		MemtableOrder:   4,        // Default B-tree order
		WalSize:         4 * 1024, // Default WAL size (4 KB)
		KeyValueMaxSize: 1024,     // Default max key-value size (1 KB)
		FidMaxSize:      64,       // Default max file ID size (64 bytes)
	}
}
func checkDirPath(dirPath string) error {
	// 存在就不创建 不存在就创建 可能多级目录
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return err
		}
		return nil
	}
	return nil
}
