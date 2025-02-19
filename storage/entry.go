package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	ErrInvalidEntry = errors.New("storage/entry: invalid entry")
	ErrInvalidCrc   = errors.New("storage/entry: invalid crc")
)

const (
	//keySize, valueSize, ExtraSize, crc32 均为 uint32 类型，各占 4 字节
	//Type 和 Mark 占 2 + 2
	//4 + 4 + 4 + 4 + 2 + 2 = 20
	entryHeaderSize = 20
)

// Value的数据结构类型
const (
	String uint16 = iota
	List
	Hash
	Set
	ZSet
)

type (
	Entry struct {
		Meta  *Meta
		Type  uint16
		Mark  uint16
		crc32 uint32
	}
	Meta struct {
		Key       []byte
		Value     []byte
		Extra     []byte
		KeySize   uint32
		ValueSize uint32
		ExtraSize uint32
	}
)

func NewEntry(key, value, extra []byte, t, mark uint16) *Entry {
	return &Entry{
		Meta: &Meta{
			Key:       key,
			Value:     value,
			Extra:     extra,
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
			ExtraSize: uint32(len(extra)),
		},
		Type: t,
		Mark: mark,
	}
}

func NewEntryNoExtra(key, value []byte, t, mark uint16) *Entry {
	return NewEntry(key, value, nil, t, mark)
}

func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.Meta.ExtraSize + e.Meta.ValueSize + e.Meta.KeySize
}

func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.Meta.KeySize == 0 {
		return nil, ErrEmptyEntry
	}
	ks, vs := e.Meta.KeySize, e.Meta.ValueSize
	es := e.Meta.ExtraSize
	buf := make([]byte, e.Size())

	binary.BigEndian.PutUint32(buf[4:8], ks)
	binary.BigEndian.PutUint32(buf[8:12], vs)
	binary.BigEndian.PutUint32(buf[12:16], es)
	binary.BigEndian.PutUint16(buf[16:18], e.Type)
	binary.BigEndian.PutUint16(buf[18:20], e.Mark)

	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Meta.Key)
	copy(buf[entryHeaderSize+ks:(entryHeaderSize+ks+vs)], e.Meta.Value)

	if es > 0 {
		copy(buf[(entryHeaderSize+ks+vs):(entryHeaderSize+ks+vs+es)], e.Meta.Extra)
	}
	crc := crc32.ChecksumIEEE(e.Meta.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])
	es := binary.BigEndian.Uint32(buf[12:16])
	t := binary.BigEndian.Uint16(buf[16:18])
	mark := binary.BigEndian.Uint16(buf[18:20])
	crc := binary.BigEndian.Uint32(buf[0:4])

	return &Entry{
		Meta: &Meta{
			KeySize:   ks,
			ValueSize: vs,
			ExtraSize: es,
		},
		Type:  t,
		Mark:  mark,
		crc32: crc,
	}, nil
}
