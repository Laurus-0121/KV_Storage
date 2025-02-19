package KV_Storage

import (
	"KV_Storage/ds/list"
	"KV_Storage/index"
	"KV_Storage/storage"
	"KV_Storage/utils"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DataType 数据类型定义
type DataType = uint16

// 数据类型定义
const (
	String DataType = iota // 0表示string
	List
	Hash
	Set
	ZSet
)

// 字符串相关操作标识
const (
	StringSet uint16 = iota
	StringRem
)

// 列表相关操作标识
const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListLRem
	ListLInsert
	ListLSet
	ListLTrim
)

// 哈希相关操作标识
const (
	HashHSet uint16 = iota
	HashHDel
)

// 集合相关操作标识
const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
)

// 有序集合相关操作标识
const (
	ZSetZAdd uint16 = iota
	ZSetZRem
)

// 建立字符串索引
func (db *KvDB) buildStringIndex(idx *index.Indexer, opt uint16) {
	if db.strIndex == nil || idx == nil {
		return
	}

	now := uint32(time.Now().Unix())
	if deadline, exist := db.expires[string(idx.Meta.Key)]; exist && deadline <= now {
		return
	}

	switch opt {
	case StringSet:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
	case StringRem:
		db.strIndex.idxList.Remove(idx.Meta.Key)
	}
}

// 建立列表索引
func (db *KvDB) buildListIndex(idx *index.Indexer, opt uint16) {
	if db.listIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt { // 根据操作类型对列表执行相应操作
	case ListLPush:
		db.listIndex.indexes.LPush(key, idx.Meta.Value)
	case ListLPop:
		db.listIndex.indexes.LPop(key)
	case ListRPush:
		db.listIndex.indexes.RPush(key, idx.Meta.Value)
	case ListRPop:
		db.listIndex.indexes.RPop(key)
	case ListLRem:
		if count, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil {
			db.listIndex.indexes.LRem(key, idx.Meta.Value, count)
		}
	case ListLInsert:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err == nil {
				db.listIndex.indexes.LInsert(string(idx.Meta.Key), list.InsertOption(opt), pivot, idx.Meta.Value)
			}
		}
	case ListLSet:
		if i, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil {
			db.listIndex.indexes.LSet(key, i, idx.Meta.Value)
		}
	case ListLTrim:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])

			db.listIndex.indexes.LTrim(string(idx.Meta.Key), start, end)
		}
	}
}

// 建立哈希索引
func (db *KvDB) buildHashIndex(idx *index.Indexer, opt uint16) {

	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case HashHSet:
		db.hashIndex.indexes.HSet(key, string(idx.Meta.Extra), idx.Meta.Value)
	case HashHDel:
		db.hashIndex.indexes.HDel(key, string(idx.Meta.Extra))
	}
}

// 建立集合索引
func (db *KvDB) buildSetIndex(idx *index.Indexer, opt uint16) {

	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case SetSAdd:
		db.setIndex.indexes.SAdd(key, idx.Meta.Value)
	case SetSRem:
		db.setIndex.indexes.SRem(key, idx.Meta.Value)
	case SetSMove:
		extra := idx.Meta.Extra
		db.setIndex.indexes.SMove(key, string(extra), idx.Meta.Value)
	}
}

// 建立有序集合索引
func (db *KvDB) buildZsetIndex(idx *index.Indexer, opt uint16) {

	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case ZSetZAdd:
		if score, err := utils.StrToFloat64(string(idx.Meta.Extra)); err == nil {
			db.zsetIndex.indexes.ZAdd(key, score, string(idx.Meta.Value))
		}
	case ZSetZRem:
		db.zsetIndex.indexes.ZRem(key, string(idx.Meta.Value))
	}
}

// 从文件中加载String、List、Hash、Set、ZSet索引
func (db *KvDB) loadIdxFromFiles() error {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(5)
	for dataType := 0; dataType < 5; dataType++ { // 遍历五种数据类型的文件
		go func(dType uint16) { // 分别开启一个goroutine去执行
			defer func() { // 每个goroutine最后要将wg减一
				wg.Done()
			}()

			// archived files
			var fileIds []int                          // 记录文件id
			dbFile := make(map[uint32]*storage.DBFile) // 记录文件id与数据文件信息的map
			for k, v := range db.archFiles[dType] {    // 遍历当前类型的所有文件
				dbFile[k] = v                     // 构造id与文件信息的map
				fileIds = append(fileIds, int(k)) // 记录文件id
			}

			// active file
			dbFile[db.activeFileIds[dType]] = db.activeFile[dType]
			fileIds = append(fileIds, int(db.activeFileIds[dType]))

			// load the db files in a specified order.
			sort.Ints(fileIds)
			for i := 0; i < len(fileIds); i++ {
				fid := uint32(fileIds[i])
				df := dbFile[fid]
				var offset int64 = 0

				for offset <= db.config.BlockSize {
					if e, err := df.Read(offset); err == nil {
						idx := &index.Indexer{
							Meta:      e.Meta,
							FileId:    fid,
							EntrySize: e.Size(),
							Offset:    offset,
						}
						offset += int64(e.Size())

						if len(e.Meta.Key) > 0 {
							if err := db.buildIndex(e, idx); err != nil {
								log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
							}
						}
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
					}
				}
			}
		}(uint16(dataType))
	}
	wg.Wait()
	return nil
}
