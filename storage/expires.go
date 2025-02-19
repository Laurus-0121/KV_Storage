package storage

import (
	"encoding/binary"
	"io"
	"log"
	"os"
)

const expireHeadSize = 12

type Expires map[string]uint32

type ExpireValue struct {
	Key      []byte
	KeySize  uint32
	Deadline uint64
}

func (e *Expires) SaveExpires(path string) (err error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	var offset int64 = 0
	for k, v := range *e {
		ev := &ExpireValue{
			Key:      []byte(k),
			KeySize:  uint32(len([]byte(k))),
			Deadline: uint64(v),
		}

		buf := make([]byte, ev.KeySize+expireHeadSize)
		binary.BigEndian.PutUint32(buf[0:4], ev.KeySize)
		binary.BigEndian.PutUint64(buf[4:12], ev.Deadline)
		copy(buf[expireHeadSize:], ev.Key)

		_, err = file.WriteAt(buf, offset)
		if err != nil {
			return err
		}
		offset += int64(expireHeadSize + ev.KeySize)
	}
	return
}

func LoadExpires(path string) (expires Expires) {
	expires = make(Expires)
	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return
	}
	defer file.Close()

	var offset int64 = 0
	for {
		ev, err := readExpire(file, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("load expire err: ", err)
			return
		}
		offset += int64(ev.KeySize + expireHeadSize)
		expires[string(ev.Key)] = uint32(ev.Deadline)
	}
	return
}

func readExpire(file *os.File, offset int64) (ev *ExpireValue, err error) {
	buf := make([]byte, expireHeadSize)
	_, err = file.ReadAt(buf, offset)
	if err != nil {
		return
	}
	ev = decodeExpire(buf)
	offset += expireHeadSize
	key := make([]byte, ev.KeySize)
	_, err = file.ReadAt(key, offset)
	if err != nil {
		return
	}
	ev.Key = key
	return

}

func decodeExpire(buf []byte) *ExpireValue {
	ev := &ExpireValue{}
	ev.KeySize = binary.BigEndian.Uint32(buf[0:4])
	ev.Deadline = binary.BigEndian.Uint64(buf[4:12])
	return ev
}
