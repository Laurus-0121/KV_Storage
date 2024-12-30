package index

import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

const (
	maxLevel    int     = 18
	probability float64 = 1 / math.E
)

type handleEle func(e *Element) bool

type (
	Node struct {
		next []*Element
	}
	Element struct {
		Node
		key   []byte
		value interface{}
	}

	SkipList struct {
		Node
		maxLevel       int
		Len            int
		randsource     rand.Source
		probability    float64
		probTable      []float64
		prevNodesCache []*Node
	}
)

func (e *Element) Key() []byte {
	return e.key
}

func (e *Element) Value() interface{} {
	return e.value
}

func (e *Element) SetValue(val interface{}) {
	e.value = val
}

func (e *Element) Next() *Element {
	return e.next[0]
}

func (t *SkipList) Put(key []byte, value interface{}) *Element {
	var element *Element
	prev := t.backNodes(key)
	if element = prev[0].next[0]; element != nil && bytes.Compare(key, element.key) <= 0 {
		element.value = value
		return element
	}
	element = &Element{
		Node{next: make([]*Element, t.randomLevel())},
		key, value,
	}
	for i := range element.next {
		element.next[i] = prev[i].next[i]
		prev[i].next[i] = element
	}
	t.Len++
	return element
}

func (t *SkipList) Exist(key []byte) bool {
	return t.Get(key) != nil
}

func (t SkipList) Remove(key []byte) *Element {
	prev := t.backNodes(key)
	if element := prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		for k, v := range element.next {
			prev[k].next[k] = v
		}
		t.Len--
		return element
	}
	return nil
}
func (t *SkipList) Foreach(fun handleEle) {
	for p := t.Front(); p != nil; p = p.Next() {
		if ok := fun(p); !ok {
			break
		}
	}
}

// 找到key对应的前一个节点索引的信息，即key节点在每一层索引的前一个节点
func (t *SkipList) backNodes(key []byte) []*Node {
	var prev = &t.Node
	var next *Element
	prevs := t.prevNodesCache
	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
		prevs[i] = prev
	}
	return prevs
}

func (t *SkipList) FindPrefix(prefix []byte) *Element {
	var prev = &t.Node
	var next *Element
	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(next.key, prefix) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}
	if next == nil {
		next = t.Front()
	}
	return next
}

func NewSkipList() *SkipList {
	return &SkipList{
		Node:           Node{next: make([]*Element, maxLevel)},
		prevNodesCache: make([]*Node, maxLevel),
		maxLevel:       maxLevel,
		randsource:     rand.New(rand.NewSource(time.Now().UnixNano())),
		probability:    probability,
		probTable:      probabilityTable(probability, maxLevel),
	}
}
func (t *SkipList) randomLevel() (level int) {
	r := float64(t.randsource.Int63()) / (1 << 63)
	level = 1
	for level < t.maxLevel && r < t.probTable[level] {
		level++
	}
	return
}

func (t *SkipList) Front() *Element {
	return t.next[0]
}

func (t *SkipList) Get(key []byte) *Element {
	var prev = &t.Node
	var next *Element
	for i := maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next != nil && bytes.Compare(next.key, key) <= 0 {
		return next
	}
	return nil

}
func probabilityTable(p float64, level int) (table []float64) {
	for i := 1; i <= maxLevel; i++ {
		prob := math.Pow(probability, float64(i-1))
		table = append(table, prob)
	}
	return table
}
