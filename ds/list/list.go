package list

import (
	"container/list"
	"reflect"
)

type InsertOption uint8

const (
	Before InsertOption = iota
	After
)

var existFlag = struct {
}{}

type (
	List struct {
		record Record
		values map[string]map[string]struct{}
	}
	Record map[string]*list.List
)

func New() *List {
	return &List{
		make(Record),
		make(map[string]map[string]struct{}),
	}
}

func (l *List) LPush(key string, val ...[]byte) int {
	return l.push(true, key, val...)
}

// LPop 取出列表头部的元素
func (lis *List) LPop(key string) []byte {
	return lis.pop(true, key)
}

// RPush 在列表的尾部添加元素，返回添加后的列表长度
func (lis *List) RPush(key string, val ...[]byte) int {
	return lis.push(false, key, val...)
}

// RPop 取出列表尾部的元素
func (lis *List) RPop(key string) []byte {
	return lis.pop(false, key)
}

// LKeyExists check if the key of a List exists.
func (lis *List) LKeyExists(key string) (ok bool) {
	_, ok = lis.record[key]
	return
}
func (l *List) LValExists(key string, val []byte) (ok bool) {
	if l.values[key] != nil {
		_, ok = l.values[key][string(val)]
	}
	return
}

// LIndex 返回列表在index处的值，如果不存在则返回nil
func (lis *List) LIndex(key string, index int) []byte {
	ok, newIndex := lis.validIndex(key, index)
	if !ok {
		return nil
	}

	index = newIndex
	var val []byte
	e := lis.index(key, index)
	if e != nil {
		val = e.Value.([]byte) // 取出element对应的value并返回
	}

	return val
}

// LRem 根据参数 count 的值，移除列表中与参数 value 相等的元素
// count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count
// count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值
// count = 0 : 移除列表中所有与 value 相等的值
// 返回成功删除的元素个数
func (lis *List) LRem(key string, val []byte, count int) int {
	item := lis.record[key] // 拿到key对应的list
	if item == nil {
		return 0
	}

	var ele []*list.Element
	if count == 0 { // 删除所有与val相等的值
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) { // 将与val相等的值加入到ele切片中
				ele = append(ele, p)
			}
		}
	}

	if count > 0 { // 从前到后删除count个与val相等的值
		for p := item.Front(); p != nil && len(ele) < count; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}

	if count < 0 { // 从后到前删除count个与val相等的值
		for p := item.Back(); p != nil && len(ele) < -count; p = p.Prev() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}

	for _, e := range ele { // 遍历ele切片挨个删除
		item.Remove(e)
	}

	length := len(ele)
	ele = nil // ele切片置空 （目的是啥子？）

	if lis.values[key] != nil {
		delete(lis.values[key], string(val))
	}

	return length
}

// LInsert 将值 val 插入到列表 key 当中，位于值 pivot 之前或之后
// 如果命令执行成功，返回插入操作完成之后，列表的长度。 如果没有找到 pivot ，返回 -1
func (lis *List) LInsert(key string, option InsertOption, pivot, val []byte) int {
	e := lis.find(key, pivot) // 找到相应的element
	if e == nil {             // 如果不存在就返回
		return -1
	}

	item := lis.record[key]
	if option == Before {
		item.InsertBefore(val, e) // 调用list自带的库函数在element前插入val
	}
	if option == After {
		item.InsertAfter(val, e)
	}

	if lis.values[key] == nil {
		lis.values[key] = make(map[string]struct{})
	}
	lis.values[key][string(val)] = existFlag

	return item.Len()
}

func (l *List) LSet(key string, index int, val []byte) bool {
	e := l.index(key, index)
	if e == nil {
		return false
	}
	if l.values != nil {
		l.values[key] = make(map[string]struct{})
	}
	if e.Value != nil {
		delete(l.values[key], string(e.Value.([]byte)))
	}
	e.Value = val
	l.values[key][string(val)] = existFlag

	return true
}
func (l *List) LRange(key string, start, end int) [][]byte {
	var val [][]byte
	item := l.record[key]

	if item == nil || item.Len() <= 0 {
		return val
	}
	length := item.Len()
	start, end = l.handleIndex(length, start, end)
	if start > end || start >= length {
		return val
	}

	mid := length >> 1
	if end <= mid || end-mid < mid-start {
		flag := 0
		for p := item.Front(); p != nil && flag <= end; p, flag = p.Next(), flag+1 {
			if flag >= start {
				val = append(val, p.Value.([]byte))
			}
		}
	} else {
		flag := length - 1
		for p := item.Back(); p != nil && flag >= start; p, flag = p.Prev(), flag-1 {
			if flag <= end {
				val = append(val, p.Value.([]byte))
			}
		}
		if len(val) > 0 { // 从后往前遍历注意要将结果集反转下
			for i, j := 0, len(val)-1; i < j; i, j = i+1, j-1 {
				val[i], val[j] = val[j], val[i]
			}
		}
	}
	return val
}

func (l *List) LTrim(key string, start, end int) bool {
	item := l.record[key]
	l.values[key] = nil
	if item == nil || item.Len() <= 0 {
		return false
	}
	length := item.Len()
	start, end = l.handleIndex(length, start, end)

	if start <= 0 && end >= length {
		return false
	}
	if start > end || start >= length {
		l.record[key] = nil
		return true
	}
	startEle, endEle := l.index(key, start), l.index(key, end)
	if end-start+1 < (length >> 1) {
		newList := list.New()
		newValueMap := make(map[string]struct{})
		for p := startEle; p != endEle.Next(); p = p.Next() {
			newList.PushBack(p.Value)
			if p.Value != nil {
				newValueMap[string(p.Value.([]byte))] = existFlag
			}
		}
		item = nil
		l.record[key] = newList
		l.values[key] = newValueMap
	} else {
		var ele []*list.Element
		for p := item.Front(); p != startEle; p = p.Next() {
			ele = append(ele, p)
		}
		for p := item.Back(); p != endEle; p = p.Prev() {
			ele = append(ele, p)
		}
		for _, e := range ele {
			item.Remove(e)
			if l.values[key] != nil && e.Value != nil {
				delete(l.values[key], string(e.Value.([]byte)))
			}
		}
		ele = nil
	}
	return true
}

func (l *List) find(key string, val []byte) *list.Element {
	item := l.record[key]
	var e *list.Element

	if item != nil {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				e = p
				break
			}

		}
	}
	return e
}

func (l *List) index(key string, index int) *list.Element {
	ok, newIndex := l.validIndex(key, index)
	if !ok {
		return nil
	}
	index = newIndex
	item := l.record[key]
	var e *list.Element

	if item != nil && item.Len() > 0 {
		if index <= (item.Len() >> 1) {
			val := item.Front()
			for i := 0; i < index; i++ {
				val = val.Next()
			}
			e = val
		} else {
			val := item.Back()
			for i := item.Len() - 1; i > index; i-- {
				val = val.Prev()
			}
			e = val
		}
	}
	return e

}

func (l *List) push(front bool, key string, val ...[]byte) int {

	if l.record[key] == nil {
		l.record[key] = list.New()
	}
	if l.values[key] == nil {
		l.values[key] = make(map[string]struct{})
	}
	for _, n := range val {
		if front {
			l.record[key].PushFront(n)
		} else {
			l.record[key].PushBack(n)
		}
		l.values[key][string(n)] = existFlag
	}
	return l.record[key].Len()

}

// LLen 返回指定key的列表中的元素个数
func (lis *List) LLen(key string) int {
	length := 0
	if lis.record[key] != nil {
		length = lis.record[key].Len()
	}

	return length
}
func (l *List) pop(front bool, key string) []byte {
	item := l.record[key]
	var val []byte
	if item != nil && item.Len() > 0 {
		var e *list.Element
		if front {
			e = item.Front()
		} else {
			e = item.Back()
		}
		val = e.Value.([]byte)
		item.Remove(e)
		if l.values[key] != nil {
			delete(l.values[key], string(val))
		}
	}
	return val
}

func (l *List) validIndex(key string, index int) (bool, int) {
	item := l.record[key]
	if item == nil || item.Len() <= 0 {
		return false, index
	}
	length := item.Len()
	if length < 0 {
		index += length
	}
	return index >= 0 && index < length, index
}

func (l *List) handleIndex(length, start, end int) (int, int) {
	if start < 0 {
		start += length
	}
	if end < 0 {
		end += length
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	return start, end
}
