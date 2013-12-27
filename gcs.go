package disttopk

import (
	"encoding/binary"
	//"fmt"
	"io"
	"math"

//	"sort"
)

type Gcs struct {
	*CountMinHash
	Data *HashValueSlice
}

/*
type HashValueSlice struct {
	hvs []uint32
}

func NewHashValueSlice() *HashValueSlice {
	return &HashValueSlice{make([]uint32, 0)}
}

func (p HashValueSlice) Len() int            { return len(p.hvs) }
func (p HashValueSlice) Less(i, j int) bool  { return p.hvs[i] < p.hvs[j] }
func (p *HashValueSlice) Swap(i, j int)      { p.hvs[i], p.hvs[j] = p.hvs[j], p.hvs[i] }
func (p *HashValueSlice) GetSlice() []uint32 { return p.hvs }
func (t *HashValueSlice) Insert(v uint32) {
	if !t.Contains(v) {
		t.hvs = append(t.hvs, v)
		sort.Sort(t)
	}
}
func (t *HashValueSlice) Merge(n *HashValueSlice) {
	for _, v := range n.hvs {
		t.Insert(v)
	}
}

func (t *HashValueSlice) Contains(value uint32) bool {
	ret := sort.Search(len(t.hvs), func(i int) bool { return t.hvs[i] >= value })
	if ret < len(t.hvs) && t.hvs[ret] == value {
		return true
	}
	return false
}*/

type HashValueSlice struct {
	hvs map[uint32]bool
}

func NewHashValueSlice() *HashValueSlice {
	return &HashValueSlice{make(map[uint32]bool)}
}

func NewHashValueSliceLen(n int) *HashValueSlice {
	return &HashValueSlice{make(map[uint32]bool)}
}

func (p *HashValueSlice) Len() int { return len(p.hvs) }

func (p *HashValueSlice) GetSlice() []uint32 {
	slice := make([]uint32, 0, p.Len())
	for k, _ := range p.hvs {
		slice = append(slice, k)
	}
	return slice
}
func (t *HashValueSlice) Insert(v uint32) {
	t.hvs[v] = true
}

func (t *HashValueSlice) Merge(n *HashValueSlice) {
	for k, _ := range n.hvs {
		t.Insert(k)
	}
}

func (t *HashValueSlice) Contains(value uint32) bool {
	return t.hvs[value]
}

func EstimateEpsGcs(N_est int, n_est int, penalty int) float64 {
	//TODO change!
	eps := (2.0 * 1.44) / (float64(penalty) * math.Log(2) * (float64(N_est/n_est) - 1.0))
	return eps
}

func EstimateMGcs(n int, eps float64) int {
	// eps=(1-e^(kn/m))^k
	//For k=1:
	//eps=(1-e^(n/m))
	//e^(-n/m) = 1-eps
	//-n/m = ln (1-eps)
	//m = -n/ln(1-eps)
	//by the taylor series for eps in our range ln(1-eps) ~ -eps. so
	//m  ~ n/eps
	return int(float64(n) / eps)
}

func NewGcs(m int) *Gcs {
	s := Gcs{
		NewCountMinHash(1, m),
		NewHashValueSlice(),
	}
	return &s
}

func (b *Gcs) CreateNew() *Gcs {
	return NewGcs(b.Columns)
}

func (b *Gcs) ByteSize() int {
	return (b.Data.Len() * 4)
}

func (s *Gcs) AddString(key string) {
	s.Add([]byte(key))
}

func (s *Gcs) AddInt(key int) {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	s.Add(tmp)
}

func (b *Gcs) Add(id []byte) {
	index := b.GetIndexNoOffset(id, 0)
	b.Data.Insert(index)
}

func (b *Gcs) HashValues() *HashValueSlice {
	return b.Data
}

func (b *Gcs) GetM() uint {
	return uint(b.Columns)
}

func (s *Gcs) NumberHashes() int {
	return s.Hashes
}

func (s *Gcs) QueryHashValues(hvs []uint32) bool {
	if len(hvs) < s.NumberHashes() {
		panic("wrong num idx")
	}
	cols := s.Columns
	index := hvs[0] % uint32(cols)
	return s.Data.Contains(index)
}

func (s *Gcs) Query(key []byte) bool {
	index := s.GetIndexNoOffset(key, 0)
	return s.Data.Contains(index)
}

func (p *Gcs) Serialize(w io.Writer) error {
	m := p.CountMinHash.Columns

	if err := SerializeIntAsU32(w, &m); err != nil {
		return err
	}

	array := make([]int, p.Data.Len())
	for i, v := range p.Data.GetSlice() {
		array[i] = int(v)
	}

	return GolumbEncodeWriter(w, array)
}

func (p *Gcs) Deserialize(r io.Reader) error {

	m := int(0)
	if err := DeserializeIntAsU32(r, &m); err != nil {
		return err
	}

	p.CountMinHash = NewCountMinHash(1, m)

	array, err := GolumbDecodeReader(r)
	if err != nil {
		return err
	}

	p.Data = NewHashValueSliceLen(len(array))
	for i, _ := range array {
		p.Data.Insert(uint32(array[i]))
	}
	return nil

}
