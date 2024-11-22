package bitcask

import (
	"bitcask/conf"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Db represents the database structure.
type Db struct {
	conf     *conf.Config    // Configuration for the database
	dbMu     sync.RWMutex    // Lock for safe concurrent access
	memtable *Memtable       // In-memory indexing table
	olderWal map[uint32]*WAL // Map of older WAL files
	newWal   *WAL            // Current WAL file
	fid      uint32          // Current file ID
	fileIds  []uint32        // List of file IDs
}

func (db *Db) recover() error {
	if err := db.loadWalFiles(); err != nil {
		return err
	}
	return db.loadWalByIds()
}
func (db *Db) loadWalFiles() error {
	dirPath := db.conf.DirPath
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %w", dirPath, err)
	}

	var fileIds []uint32
	var invalidFiles []string // 存储非 .log 文件或解析错误的文件名

	for _, file := range files {
		// 只处理 .log 文件
		if filepath.Ext(file.Name()) != ".log" {
			// 非法文件，记录下来
			invalidFiles = append(invalidFiles, file.Name())
			continue
		}

		// 提取文件 ID（文件名格式为 wal_<fid>.log）
		fileName := file.Name()
		baseName := strings.TrimSuffix(fileName, ".log")
		parts := strings.Split(baseName, "_")
		if len(parts) != 2 {
			// 文件名格式错误，记录下来
			invalidFiles = append(invalidFiles, fileName)
			continue
		}

		fid, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			// 文件名解析错误，记录下来
			invalidFiles = append(invalidFiles, fileName)
			continue
		}

		// 添加到 fileIds 列表
		fileIds = append(fileIds, uint32(fid))
	}

	// 处理非法文件
	if len(invalidFiles) > 0 {
		return fmt.Errorf("directory %s contains invalid files: %v", dirPath, invalidFiles)
	}

	// 按升序排序 fileIds
	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i] < fileIds[j]
	})

	db.fileIds = fileIds
	// 处理fid 存储当前最大的fid即可
	if len(fileIds) == 0 {
		db.fid = 0
	} else {
		db.fid = fileIds[len(fileIds)-1]
	}
	return nil
}
func (db *Db) loadWalByIds() error {

	// 检查 fileIds 是否存在
	if len(db.fileIds) == 0 {
		return db.freshWal()
	}
	db.dbMu.Lock()
	defer db.dbMu.Unlock()
	// 遍历 fileIds，读取对应的 WAL 文件
	for k, fid := range db.fileIds {

		if len(db.fileIds)-1 != k {
			// 将 WAL 文件加载到 olderWal
			wal, err := ReadNewWAL(db.conf.DirPath, fid)
			if err != nil {
				return err
			}
			db.olderWal[uint32(fid)] = wal
			// 将 WAL 文件数据恢复到 Memtable
			if err := wal.Recover(db.memtable); err != nil {
				return fmt.Errorf("failed to recover data from WAL : %w", err)
			}
		} else {
			// 加载 WAL 文件
			wal, err := CreateNewWAL(db.conf.DirPath, fid)
			if err != nil {
				return err
			}
			db.newWal = wal
			// 将 WAL 文件数据恢复到 Memtable
			if err := wal.Recover(db.memtable); err != nil {
				return fmt.Errorf("failed to recover data from WAL : %w", err)
			}
		}
	}

	return nil
}
func (db *Db) Fold(fn func(key, value []byte) bool) error {
	// 加锁，确保并发安全
	// db.dbMu.RLock()
	// defer db.dbMu.RUnlock()

	// Step 1: 遍历 memtable 中的数据
	errOccurred := false
	db.memtable.Fold(func(key []byte, pos *Pos) bool {
		// 从 WAL 中读取记录
		var wal *WAL
		if db.newWal.Fid == pos.Fid {
			wal = db.newWal
		} else {
			if v, ok := db.olderWal[pos.Fid]; ok {
				wal = v
			} else {
				panic("存在错误！！")
			}
		}
		record, err := wal.readRecord(pos.Offset, pos.Length)
		if err != nil {
			errOccurred = true
			fmt.Printf("error reading record from WAL Fid %d: %v\n", pos.Fid, err)
			return false
		}

		// 调用回调函数
		if !fn(key, record.Value) {
			return false // 中断遍历
		}
		return true // 继续遍历
	})

	if errOccurred {
		return fmt.Errorf("errors occurred during Fold")
	}
	return nil
}

// 刷新wal配置
func (db *Db) freshWal() error {
	db.dbMu.Lock() // 对整个操作加写锁，保证并发安全
	defer db.dbMu.Unlock()

	// 初始化检查：如果 newWal 为空，直接创建一个新 WAL
	if db.newWal == nil {
		newWal, err := CreateNewWAL(db.conf.DirPath, db.fid)
		if err != nil {
			return fmt.Errorf("failed to create initial WAL with fid %d: %w", db.fid, err)
		}
		db.newWal = newWal
		return nil
	}

	// 如果当前 WAL 的 Fid 等于 db 的 fid，将其归档到 olderWal
	if db.newWal.Fid == db.fid {
		// 模式转换
		if err := db.newWal.ToReadOnly(); err != nil {
			return err
		}
		db.olderWal[db.fid] = db.newWal
		db.fid += 1 // 更新 fid
	}

	// 创建新的 WAL 文件
	newWal, err := CreateNewWAL(db.conf.DirPath, db.fid)
	if err != nil {
		return fmt.Errorf("failed to create new WAL with fid %d: %w", db.fid, err)
	}

	// 更新当前 WAL
	db.newWal = newWal
	return nil
}

// NewDb creates a new database instance.
func NewDb(conf *conf.Config) (*Db, error) {
	// Step 1: Validate the configuration.
	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("failed to use config: %w", err)
	}

	// Step 2: Initialize the Memtable.
	memtable := NewMemtable(conf.MemtableOrder)

	// Step 3: Create the database instance.
	db := &Db{
		conf:     conf,                  // Assign configuration
		memtable: memtable,              // Initialize Memtable
		olderWal: make(map[uint32]*WAL), // Initialize map for older WAL files
		fid:      0,                     // Init fid
		fileIds:  []uint32{},            // Initialize empty file ID list
	}

	// Step 4: Recover database state from WAL or persistent storage.
	if err := db.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover database: %w", err)
	}
	// Step 5: Return the initialized database instance.
	return db, nil
}

// 读取conf文件下的 .log文件并且记录文件fid
func (db *Db) Put(key, value []byte) error {
	// 将记录写入到当前的 WAL（Write-Ahead Log）
	record := NewRecordTimeForever(key, value)
	return db.putRecord(record)
}
func (db *Db) PutWithData(key, value []byte, duration time.Duration) error {
	// 将记录写入到当前的 WAL（Write-Ahead Log）
	record := NewRecord(key, value, duration)
	return db.putRecord(record)
}
func (db *Db) putRecord(record *Record) error {
	pos, err := db.appendRecord(record)
	if err != nil {
		return err
	}
	// 将记录插入到 Memtable
	db.memtable.Put(record.Key, pos)
	return nil
}
func (db *Db) Delete(key []byte) error {
	// 将删除操作写入 WAL
	record := NewRecordTimeForeverDel(key)
	_, err := db.appendRecord(record)
	if err != nil {
		return err
	}
	// 从 Memtable 中删除
	db.memtable.Delete(key)
	return nil
}
func (db *Db) willOverflow(count int) bool {
	size, _ := db.newWal.Size() // 获取当前 WAL 大小
	return uint32(size)+uint32(count) > db.conf.WalSize
}

func (db *Db) appendRecord(record *Record) (*Pos, error) {
	// 序列化记录
	data, err := record.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize record: %w", err)
	}

	// 检查是否需要切换 WAL
	if db.willOverflow(len(data)) {
		if err := db.freshWal(); err != nil {
			return nil, fmt.Errorf("failed to rotate WAL: %w", err)
		}
	}
	db.dbMu.Lock() // 加锁保护共享资源
	defer db.dbMu.Unlock()
	// 将数据写入 WAL
	pos, err := db.newWal.Write(data)
	if err != nil {
		return nil, fmt.Errorf("failed to write record to WAL: %w", err)
	}
	return pos, nil
}

func (db *Db) Get(key []byte) ([]byte, error) {
	db.dbMu.RLock() // 加锁保护共享资源
	defer db.dbMu.RUnlock()
	// 优先从 Memtable 获取
	pos, found := db.memtable.Get(key)
	if found {
		var wal *WAL
		if db.newWal.Fid == pos.Fid {
			wal = db.newWal
		} else {
			if v, ok := db.olderWal[pos.Fid]; ok {
				wal = v
			} else {
				panic("存在错误！！")
			}
		}
		record, err := wal.readRecord(pos.Offset, pos.Length)
		if err != nil {
			return nil, err
		}
		return record.Value, nil
	}

	return nil, fmt.Errorf("key not found: %s", string(key))
}

// 进行flush数据刷新
func (db *Db) Flush() error {
	// Step 1: 刷新当前 WAL
	if err := db.freshWal(); err != nil {
		return fmt.Errorf("failed to refresh WAL: %w", err)
	}

	// Step 2: 提前存储 oldwal 的快照
	olderWalSnapshot := make(map[uint32]*WAL)
	for fid, wal := range db.olderWal {
		olderWalSnapshot[fid] = wal
	}

	// Step 3: 遍历 memtable 数据
	errOccurred := false
	db.memtable.Fold(func(key []byte, pos *Pos) bool {
		var wal *WAL
		var ok bool

		// 从快照中获取对应的 WAL
		if wal, ok = olderWalSnapshot[pos.Fid]; !ok {
			errOccurred = true
			// 错误日志记录
			fmt.Printf("error: missing WAL file in snapshot for Fid %d\n", pos.Fid)
			return false // 停止遍历
		}

		// 读取 WAL 的记录
		record, err := wal.readRecord(pos.Offset, pos.Length)
		if err != nil {
			errOccurred = true
			fmt.Printf("error reading record from WAL Fid %d: %v\n", pos.Fid, err)
			return false
		}

		// 写入记录到数据库
		if err := db.putRecord(record); err != nil {
			errOccurred = true
			fmt.Printf("error writing record: %v\n", err)
			return false
		}

		return true // 继续遍历
	})

	// Step 4: 检查遍历过程中是否发生错误
	if errOccurred {
		return fmt.Errorf("error occurred during memtable flush")
	}

	// Step 5: 清理旧 WAL 文件
	for fid, wal := range olderWalSnapshot {
		if err := wal.delete(); err != nil {
			fmt.Printf("error deleting old WAL Fid %d: %v\n", fid, err)
			return fmt.Errorf("failed to delete old WAL Fid %d: %w", fid, err)
		}
		delete(db.olderWal, fid) // 从 db.olderWal 中移除
	}

	return nil // Flush 成功
}
