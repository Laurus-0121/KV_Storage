package KV_Storage

import (
	"KV_Storage/index"
	"KV_Storage/storage"
	"bytes"
	"log"
	"strings"
	"sync"
	"time"
)

// StrIdx string idx
type StrIdx struct {
	mu      sync.RWMutex
	idxList *index.SkipList
}

func newStrIdx() *StrIdx {
	return &StrIdx{idxList: index.NewSkipList()}
}

// Set 将字符串值 value 关联到 key
// 如果 key 已经持有其他值，SET 就覆写旧值
func (db *KvDB) Set(key, value []byte) error {

	//if err := db.checkKeyValue(key, value); err != nil {
	//	return err
	//}
	//
	//db.mu.Lock()
	//defer db.mu.Unlock()
	//
	//e := storage.NewEntryNoExtra(key, value, String, StringSet) // 先写文件
	//if err := db.store(e); err != nil {
	//	return err
	//}
	//
	////数据索引
	//idx := &index.Indexer{
	//	Meta: &storage.Meta{
	//		KeySize: uint32(len(e.Meta.Key)),
	//		Key:     e.Meta.Key,
	//	},
	//	FileId:    db.activeFileId,
	//	EntrySize: e.Size(),
	//	Offset:    db.activeFile.Offset - int64(e.Size()),
	//}
	//
	//if err := db.buildIndex(e, idx); err != nil { // 后写内存索引
	//	return err
	//}
	if err := db.doSet(key, value); err != nil {
		return err
	}
	//清除过期时间
	db.Persist(key)

	return nil
}

func (db *KvDB) SetNx(key, value []byte) error {
	if exist := db.StrExists(key); exist {
		return nil
	}
	return db.Set(key, value)
}

func (db *KvDB) Get(key []byte) ([]byte, error) {
	keySize := uint32(len(key))
	if keySize == 0 {
		return nil, ErrEmptyKey
	}

	node := db.strIndex.idxList.Get(key) // 从索引（跳表）中查找
	if node == nil {
		return nil, ErrKeyNotExist
	}

	idx := node.Value().(*index.Indexer) // 类型断言为indexer
	if idx == nil {
		return nil, ErrNilIndexer
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	//判断是否过期
	if db.expireIfNeeded(key) {
		return nil, ErrKeyExpired
	}

	//如果key和value均在内存中，则取内存中的value
	if db.config.IdxMode == KeyValueRamMode {
		return idx.Meta.Value, nil
	}

	//如果只有key在内存中，那么需要从db file中获取value
	if db.config.IdxMode == KeyOnlyRamMode {
		df := db.activeFile[String]
		if idx.FileId != db.activeFileIds[String] {
			df = db.archFiles[String][idx.FileId]
		}

		e, err := df.Read(idx.Offset)
		if err != nil {
			return nil, err
		}

		return e.Meta.Value, nil

	}

	return nil, ErrKeyNotExist
}

// GetSet 将键 key 的值设为 value ， 并返回键 key 在被设置之前的旧值。
func (db *KvDB) GetSet(key, val []byte) (res []byte, err error) {

	if res, err = db.Get(key); err != nil {
		return
	}

	if err = db.Set(key, val); err != nil {
		return
	}

	return
}

// Append 如果key存在，则将value追加至原来的value末尾
// 如果key不存在，则相当于Set方法
func (db *KvDB) Append(key, value []byte) error {

	if err := db.checkKeyValue(key, value); err != nil {
		return err
	}

	e, err := db.Get(key)
	if err != nil && err != ErrKeyNotExist {
		return err
	}

	if db.expireIfNeeded(key) {
		return ErrKeyExpired
	}
	appendExist := false

	if e != nil {
		appendExist = true
		e = append(e, value...)
	} else {
		e = value
	}

	if err := db.doSet(key, e); err != nil {
		return err
	}

	if !appendExist {
		db.Persist(key)
	}

	return nil
}

// StrLen 返回key存储的字符串值的长度
func (db *KvDB) StrLen(key []byte) int {

	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	e := db.strIndex.idxList.Get(key)
	if e != nil {
		if db.expireIfNeeded(key) {
			return 0
		}
		idx := e.Value().(*index.Indexer)
		return int(idx.Meta.ValueSize)
	}

	return 0
}

// StrExists 判断key是否存在
func (db *KvDB) StrExists(key []byte) bool {

	if err := db.checkKeyValue(key, nil); err != nil {
		return false
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	exist := db.strIndex.idxList.Exist(key)
	if exist && !db.expireIfNeeded(key) {
		return true
	}
	return false
}

// StrRem 删除key及其数据
func (db *KvDB) StrRem(key []byte) error {
	if err := db.checkKeyValue(key, nil); err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if ele := db.strIndex.idxList.Remove(key); ele != nil {
		delete(db.expires, string(key))
		e := storage.NewEntryNoExtra(key, nil, String, StringRem)
		if err := db.store(e); err != nil {
			return err
		}
	}

	return nil
}

// PrefixScan 根据前缀查找所有匹配的 key 对应的 value
// 参数 limit 和 offset 控制取数据的范围，类似关系型数据库中的分页操作
// 如果 limit 为负数，则返回所有满足条件的结果
func (db *KvDB) PrefixScan(prefix string, limit, offset int) (val [][]byte, err error) {

	if limit == 0 {
		return
	}

	if offset < 0 {
		offset = 0
	}

	// 检查key value是否符合规范
	if err = db.checkKeyValue([]byte(prefix), nil); err != nil {
		return
	}
	// 对索引加读锁
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	// 找到第一个和给定前缀匹配的节点
	e := db.strIndex.idxList.FindPrefix([]byte(prefix))

	if limit > 0 { // 往后偏移offset个满足前缀的key
		for i := 0; i < offset && e != nil && strings.HasPrefix(string(e.Key()), prefix); i++ {
			e = e.Next()
		}
	}

	for e != nil && strings.HasPrefix(string(e.Key()), prefix) && limit != 0 {
		item := e.Value().(*index.Indexer) //item为e相应的索引信息
		var value []byte

		if db.config.IdxMode == KeyOnlyRamMode { // 如果只有key存在内存
			value, err = db.Get(e.Key()) // 就去磁盘中相应位置拿到value值
			if err != nil {
				return
			}
		} else { // 如果键值都在内存，直接从索引信息中拿到value值
			if item != nil {
				value = item.Meta.Value
			}
		}

		expired := db.expireIfNeeded(e.Key()) // 检查key是否过期
		if !expired {                         // 如果没有过期就加入到结果集中
			val = append(val, value)
			e = e.Next()
		}
		if limit > 0 && !expired { // limit减一然后进入下一个循环
			limit--
		}
	}
	return
}

// RangeScan 范围扫描，查找 key 从 start 到 end 之间的数据
func (db *KvDB) RangeScan(start, end []byte) (val [][]byte, err error) {

	node := db.strIndex.idxList.Get(start) // 通过跳表的查找接口直接找到start对应的节点
	if node == nil {                       // 如果节点为空，则返回错误
		return nil, ErrKeyNotExist
	}

	db.strIndex.mu.RLock() // 加读锁对跳表进行操作
	defer db.strIndex.mu.RUnlock()

	for node != nil && bytes.Compare(node.Key(), end) <= 0 { // 从start节点开始往后遍历，直接和end节点比较
		if db.expireIfNeeded(node.Key()) { // 如果中间某个节点过期了，就跳过该节点
			node = node.Next()
			continue
		}
		var value []byte
		if db.config.IdxMode == KeyOnlyRamMode { // 仍然是要判断配置的是键值都在内存中还是另一种
			value, err = db.Get(node.Key())
			if err != nil {
				return nil, err
			}
		} else {
			value = node.Value().(*index.Indexer).Meta.Value
		}

		val = append(val, value) // 将查出来的value放入结果集中
		node = node.Next()
	}

	return
}

// Expire 设置key的过期时间
func (db *KvDB) Expire(key []byte, seconds uint32) (err error) {
	if exist := db.StrExists(key); !exist {
		return ErrKeyNotExist
	}
	if seconds <= 0 {
		return ErrInvalidTTL
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline := uint32(time.Now().Unix()) + seconds
	db.expires[string(key)] = deadline
	return
}

// Persist 清除key的过期时间
func (db *KvDB) Persist(key []byte) {

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	delete(db.expires, string(key))
}

// TTL 获取key的过期时间
func (db *KvDB) TTL(key []byte) (ttl uint32) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.expireIfNeeded(key) {
		return
	}
	deadline, exist := db.expires[string(key)]
	if !exist {
		return
	}

	now := uint32(time.Now().Unix())
	if deadline > now {
		ttl = deadline - now
	}
	return
}

// 检查key是否过期并删除相应的值
func (db *KvDB) expireIfNeeded(key []byte) (expired bool) {
	deadline := db.expires[string(key)]
	if deadline <= 0 {
		return
	}

	if time.Now().Unix() > int64(deadline) {
		expired = true
		//删除过期字典对应的key
		delete(db.expires, string(key))

		//删除索引及数据
		if ele := db.strIndex.idxList.Remove(key); ele != nil {
			e := storage.NewEntryNoExtra(key, nil, String, StringRem)
			if err := db.store(e); err != nil {
				log.Printf("remove expired key err [%+v] [%+v]\n", key, err)
			}
		}
	}
	return
}

func (db *KvDB) doSet(key, value []byte) (err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return err
	}

	// 如果新增的 value 和设置的 value 一样，则不做任何操作
	if db.config.IdxMode == KeyValueRamMode {
		if existVal, _ := db.Get(key); existVal != nil && bytes.Compare(existVal, value) == 0 {
			return
		}
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, value, String, StringSet)
	if err := db.store(e); err != nil {
		return err
	}
	//数据索引  store in skiplist.
	idx := &index.Indexer{
		Meta: &storage.Meta{
			KeySize: uint32(len(e.Meta.Key)),
			Key:     e.Meta.Key,
		},
		FileId:    db.activeFileIds[String],
		EntrySize: e.Size(),
		Offset:    db.activeFile[String].Offset - int64(e.Size()),
	}

	if err = db.buildIndex(e, idx); err != nil {
		return err
	}
	return
}
