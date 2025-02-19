package index

import (
	"KV_Storage/storage"
)

type Indexer struct {
	Meta      *storage.Meta //元数据信息
	FileId    uint32        //存储数据的文件id
	EntrySize uint32        //数据entry的大小
	Offset    int64         //Entry数据的查询起始位置
}
