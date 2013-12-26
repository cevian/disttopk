package disttopk

import (
	"encoding/binary"
	//"fmt"
	"io"
	"math"
	"sort"
)

type Gcs struct {
	*CountMinHash
	Data []uint32
}

type DataSlice []uint32

func (p DataSlice) Len() int           { return len(p) }
func (p DataSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p DataSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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
		make([]uint32, 0),
	}
	return &s
}

func (b *Gcs) CreateNew() *Gcs {
	return NewGcs(b.Columns)
}

func (b *Gcs) ByteSize() int {
	return (len(b.Data) * 4)
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
	if !b.contains(index) {
		b.Data = append(b.Data, index)
		sort.Sort(DataSlice(b.Data))
	}
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
	return s.contains(index)
}

func (s *Gcs) contains(value uint32) bool {
	ret := sort.Search(len(s.Data), func(i int) bool { return s.Data[i] >= value })
	if ret < len(s.Data) && s.Data[ret] == value {
		return true
	}
	return false
}

func (s *Gcs) Query(key []byte) bool {
	index := s.GetIndexNoOffset(key, 0)
	return s.contains(index)
}

func (p *Gcs) Serialize(w io.Writer) error {
	m := p.CountMinHash.Columns

	if err := SerializeIntAsU32(w, &m); err != nil {
		return err
	}

	array := make([]int, len(p.Data))
	for i, _ := range p.Data {
		array[i] = int(p.Data[i])
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

	p.Data = make([]uint32, len(array))
	for i, _ := range array {
		p.Data[i] = uint32(array[i])
	}
	return nil

}
