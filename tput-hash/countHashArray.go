package tput_hash

import "github.com/cevian/disttopk"

import (
	"fmt"
	"io"
	"sort"
)

var _ = fmt.Println

type CountHashArray struct {
	*disttopk.CountMinHash
	Data *disttopk.CountArray
}

func NewCountHashArray(size uint) *CountHashArray {
	return &CountHashArray{disttopk.NewCountMinHash(1, int(size)), disttopk.NewCountArray(int(size))}
}

func (t *CountHashArray) Len() int {
	return t.Data.Len()
}

func (t *CountHashArray) Add(key []byte, count uint) (int) {
	index := int(t.GetIndexNoOffset(key, uint32(0)))
	current := t.Data.Get(index)

	if count > current {
		//fmt.Println("Setting", index, )
		t.Data.Set(index, count)
	}
	return index
}

func (t *CountHashArray) GetIndex(key []byte) uint {
	index := int(t.GetIndexNoOffset(key, uint32(0)))
	return uint(index)
}

func (t *CountHashArray) Query(key []byte) uint {
	index := int(t.GetIndexNoOffset(key, uint32(0)))
	return t.Data.Get(index)
}

func (t *CountHashArray) DebugSum() uint {
	sum := uint(0)
	for i := 0; i < t.Data.Len(); i++ {
		current := t.Data.Get(i)
		sum += current
	}
	return sum
}

func (t *CountHashArray) Merge(cha *CountHashArray) {
	if t.Data.Len() != cha.Data.Len() {
		panic("Has to be the same size")
	}

	for i := 0; i < t.Data.Len(); i++ {
		current := t.Data.Get(i)
		n := cha.Data.Get(i)
		t.Data.Set(i, current+n)
	}
}

func (t *CountHashArray) AddResponses(m map[int]int) {
	for i := uint(0); i < uint(t.Data.Len()); i++ {
		current := t.Data.Get(int(i))
		if current != 0 {
			m[int(i)] += 1
		}
	}

}

func (t *CountHashArray) GetKthCount(k int) uint {
	list := make([]int, t.Data.Len())
	for i := uint(0); i < uint(t.Data.Len()); i++ {
		current := t.Data.Get(int(i))
		list[i] = int(current)
	}

	sort.Ints(list)

	if len(list) <= k {
		panic("Error")
	}
	index := len(list) - k
	return uint(list[index])
}

func (t *CountHashArray) GetBloomFilter(thresh uint, responses map[int]int, oldthresh uint, nnodes uint) *disttopk.Bloom {
	b := disttopk.NewBloom(1, t.Data.Len())
	for i := uint(0); i < uint(t.Data.Len()); i++ {
		current := t.Data.Get(int(i))
		if nnodes < uint(responses[int(i)]) {
			panic(fmt.Sprint("Cannot have more responses than nodes ", nnodes, responses[int(i)]))
		}
		missing := nnodes - uint(responses[int(i)])
		if current != 0 && missing == nnodes {
			panic("Should not happen")
		}
		upperBound := (missing * oldthresh) + current
		//fmt.Println("HV", i, "Missing ", missing, "nnodes", nnodes, "oldthresh", oldthresh, "current", current, "upperbound", upperBound)

		if upperBound >= thresh {
			if missing == nnodes {
				panic("shopuld not happen")
			}
			b.Data.Set(i)
		}
	}
	return b
}

func (t *CountHashArray) Serialize(w io.Writer) error {
	//fmt.Println("In count hash array serializing count array length :", t.Data.Len())
	return t.Data.Serialize(w)
}

func (t *CountHashArray) Deserialize(r io.Reader) error {
	t.Data = &disttopk.CountArray{}
	if err := t.Data.Deserialize(r); err != nil {
		return err
	}
	t.CountMinHash = disttopk.NewCountMinHash(1, t.Data.Len())
	return nil
}
