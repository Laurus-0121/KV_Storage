package KV_Storage

import (
	"KV_Storage/index"
	"KV_Storage/storage"
	"KV_Storage/utils"
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

var (
	ErrEmptyKey         = errors.New("kvdb: the key is empty")
	ErrKeyTooLarge      = errors.New("kvdb: key exceeded the max length")
	ErrValueTooLarge    = errors.New("kbdb: value exceeded the max length")
	ErrKeyNotExist      = errors.New("kvdb: key not exist")
	ErrNilIndexer       = errors.New("kvdb: indexer is nil")
	ErrCfgNotExist      = errors.New("kvdb: the config file not exist")
	ErrReclaimUnreached = errors.New("mindb: unused space not reach the threshold")

	ErrExtraContainsSeparator = errors.New("mindb: extra contains separator \\0")

	ErrInvalidTTL = errors.New("mindb: invalid ttl")
	ErrKeyExpired = errors.New("kvdb: key is expired")
)

const (
	configSaveFile = string(os.PathSeparator) + "db.cfg"
	dbMetaSaveFile = string(os.PathSeparator) + "db.meta"
	reclaimPath    = string(os.PathSeparator) + "kvdb_reclaim"
	ExtraSeparator = "\\0"
	expireFile     = string(os.PathSeparator) + "db.expires"
)

type (
	KvDB struct {
		activeFile    ActiveFiles
		activeFileIds ActiveFileIds
		archFiles     ArchivedFiles
		strIndex      *StrIdx
		listIndex     *ListIdx
		hashIndex     *HashIdx
		setIndex      *SetIdx
		zsetIndex     *ZsetIdx
		config        Config
		mu            sync.RWMutex
		meta          *storage.DBMeta
		expires       storage.Expires
	}

	ActiveFiles   map[DataType]*storage.DBFile
	ActiveFileIds map[DataType]uint32 // 不同类型的当前活跃文件id

	// ArchivedFiles 不同类型的已封存的文件map索引，索引：key为id val 为 文件信息
	ArchivedFiles map[DataType]map[uint32]*storage.DBFile
)

// Open 打开一个数据库实例
func Open(config Config) (*KvDB, error) {

	//如果配置目录不存在则创建
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil { // 创建配置中的文件目录
			return nil, err
		}
	}

	//加载数据文件信息，用一个map记录
	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	// 加载活跃文件
	activeFiles := make(ActiveFiles)
	for dataType, fileId := range activeFileIds { // 遍历每一种类型的活跃文件
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles[dataType] = file // 将活跃文件信息进行缓存
	}

	// 加载过期字典
	expires := storage.LoadExpires(config.DirPath + expireFile)

	// 加载数据库额外信息（meta）
	meta, _ := storage.LoadMeta(config.DirPath + dbMetaSaveFile)

	// 更新当前活跃文件的写偏移
	for dataType, file := range activeFiles {
		file.Offset = meta.ActiveWriteOff[dataType]
	}

	db := &KvDB{
		activeFile:    activeFiles,
		activeFileIds: activeFileIds,
		archFiles:     archFiles,
		config:        config,
		meta:          meta,
		strIndex:      newStrIdx(),
		listIndex:     newListIdx(),
		hashIndex:     newHashIdx(),
		setIndex:      newSetIdx(),
		zsetIndex:     newZsetIdx(),
		expires:       expires,
	}

	// 从文件中加载索引信息
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Reopen 根据配置重新打开数据库
func Reopen(path string) (*KvDB, error) {
	if exist := utils.Exist(path + configSaveFile); !exist {
		return nil, ErrCfgNotExist
	}

	var config Config

	bytes, err := ioutil.ReadFile(path + configSaveFile)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(bytes, &config); err != nil {
		return nil, err
	}
	return Open(config)
}

// Close 关闭数据库，保存相关配置
func (db *KvDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.saveConfig(); err != nil {
		return err
	}

	if err := db.saveMeta(); err != nil {
		return err
	}

	if err := db.expires.SaveExpires(db.config.DirPath + expireFile); err != nil { // 保存过期信息
		return err
	}

	// close and sync the active file
	for _, file := range db.activeFile {
		if err := file.Close(true); err != nil {
			return err
		}
	}

	// close the archived files.
	for _, archFile := range db.archFiles {
		for _, file := range archFile {
			if err := file.Sync(); err != nil {
				return err
			}
		}
	}

	return nil
}

// 关闭数据库之前保存配置
func (db *KvDB) saveConfig() (err error) {
	//保存配置
	path := db.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)

	bytes, err := json.Marshal(db.config)
	_, err = file.Write(bytes)
	err = file.Close()

	return
}

// 持久化数据库信息
func (db *KvDB) saveMeta() error {
	metaPath := db.config.DirPath + dbMetaSaveFile
	return db.meta.Store(metaPath)
}

// 建立索引
func (db *KvDB) buildIndex(e *storage.Entry, idx *index.Indexer) error {

	if db.config.IdxMode == KeyValueRamMode { // 如果开启了key value都在内存中的模式就把value也放在索引中
		idx.Meta.Value = e.Meta.Value
		idx.Meta.ValueSize = uint32(len(e.Meta.Value))
	}
	switch e.Type {
	case storage.String: // 如果是string，就把当前索引加入到跳表中
		db.buildStringIndex(idx, e.Mark)
	case storage.List: // 如果是list，就建立list索引
		db.buildListIndex(idx, e.Mark)
	case storage.Hash:
		db.buildHashIndex(idx, e.Mark)
	case storage.Set:
		db.buildSetIndex(idx, e.Mark)
	case storage.ZSet:
		db.buildZsetIndex(idx, e.Mark)
	}

	return nil
}

// 写数据
func (db *KvDB) store(e *storage.Entry) error {

	//如果数据文件空间不够，则持久化该文件，并新打开一个文件
	config := db.config
	if db.activeFile[e.Type].Offset+int64(e.Size()) > config.BlockSize {
		if err := db.activeFile[e.Type].Sync(); err != nil {
			return err
		}

		//保存旧的文件
		activeFileId := db.activeFileIds[e.Type]
		db.archFiles[e.Type][activeFileId] = db.activeFile[e.Type]
		activeFileId = activeFileId + 1

		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize, e.Type)
		if err != nil {
			return err
		}
		db.activeFile[e.Type] = newDbFile
		db.activeFileIds[e.Type] = activeFileId
		db.meta.ActiveWriteOff[e.Type] = 0
	}
	//
	////如果key已经存在，则原来的值被舍弃，所以需要新增可回收的磁盘空间值
	//if e := db.idxList.Get(e.Meta.Key); e != nil {
	//	item := e.Value().(*index.Indexer)
	//	if item != nil {
	//		db.meta.UnusedSpace += uint64(item.EntrySize)
	//	}
	//}

	// 写入entry至文件中
	if err := db.activeFile[e.Type].Write(e); err != nil {
		return err
	}

	db.meta.ActiveWriteOff[e.Type] = db.activeFile[e.Type].Offset

	// 数据持久化
	if config.Sync {
		if err := db.activeFile[e.Type].Sync(); err != nil {
			return err
		}
	}

	return nil
}

// 检查key value是否符合规范
func (db *KvDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}

	return nil
}

func (db *KvDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {
	if e == nil {
		return false
	}
	mark := e.Mark
	switch e.Type {

	case String:
		if mark == StringSet { // 如果本条entry是set操作，将其的值与当前最新的值进行比较
			// 首先判断该entry中的key是否过期
			now := uint32(time.Now().Unix()) // 得到当前时间的纳秒数
			if deadline, exist := db.expires[string(e.Meta.Key)]; exist && deadline <= now {
				return false // 从过期字典中取出当前key的过期时间，如果有过期时间且已过期，则该记录无效
			}

			// check the data position.
			node := db.strIndex.idxList.Get(e.Meta.Key) // 从跳表索引中查询当前entry中的key
			if node == nil {                            // 如果该key在跳表中不存在，说明无效
				return false
			}
			indexer := node.Value().(*index.Indexer)
			if bytes.Compare(indexer.Meta.Key, e.Meta.Key) == 0 {
				if indexer != nil && indexer.FileId == fileId && indexer.Offset == offset {
					return true
				}
			}
			return false
		}
	case List:
		if mark == ListLPush || mark == ListRPush || mark == ListLInsert || mark == ListLSet {
			// TODO 由于List是链表结构，无法有效的进行检索，取出全部数据依次比较的开销太大
			if db.LValExists(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case Hash:
		if mark == HashHSet {
			if val := db.HGet(e.Meta.Key, e.Meta.Extra); string(val) == string(e.Meta.Value) {
				return true
			}
		}
	case Set:
		if mark == SetSMove { // 如果是移动member的操作
			if db.SIsMember(e.Meta.Extra, e.Meta.Value) {
				return true
			}
		}

		if mark == SetSAdd {
			if db.SIsMember(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case ZSet:
		if mark == ZSetZAdd {
			if val, err := utils.StrToFloat64(string(e.Meta.Extra)); err == nil {
				score := db.ZScore(e.Meta.Key, e.Meta.Value)
				if score == val {
					return true
				}
			}
		}
	}
	return false
}
