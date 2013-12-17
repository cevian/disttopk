package disttopk

import (
	"encoding/binary"
	"fmt"
	"math"
)

type Bloom struct {
	*CountMinHash
	Data []bool
	k    int
}

func EstimateM(N_est int, n_est int, penalty int) int {
	eps := EstimateEps(N_est, n_est, penalty)
	m := EstimateMSimple(n_est, eps)
	fmt.Println("eps = ", eps, " m = ", int(m), " bytes = ", int(m)/8)
	return int(m)
}

func EstimateEps(N_est int, n_est int, penalty int) float64 {
	eps := (2.0 * 1.44) / (float64(penalty) * math.Log(2) * (float64(N_est/n_est) - 1.0))
	return eps
}

func EstimateMSimple(n int, eps float64) int {
	return int(math.Ceil(1.44*math.Log2(1.0/eps))) * n
}

func NewOptBloom(n int, N int, penalty int) *Bloom {
	m := math.Ceil(1.44 * float64(n) * math.Log2(float64(penalty)*math.Log(2)*(float64(N/n)-1.0)/1.44))
	//k := math.Ceil(m / float64(n) * math.Log(2))
	return NewBloomSimpleEst(int(m), n)
}

func NewBloomSimpleEst(m int, n_est int) *Bloom {
	k := math.Ceil((float64(m) / float64(n_est)) * math.Log(2))
	return NewBloom(int(k), int(m))
}

/* k is number hashes, m is size of data */
func NewBloom(k int, m int) *Bloom {
	s := Bloom{
		NewCountMinHash(k, m),
		make([]bool, m),
		k,
	}
	return &s
}

func (b *Bloom) CreateNew() *Bloom {
	return NewBloom(b.k, len(b.Data))
}

func (b *Bloom) ByteSize() int {
	return (len(b.Data) / 8) + 4
}

func (s *Bloom) AddString(key string) {
	s.Add([]byte(key))
}

func (s *Bloom) AddInt(key int) {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	s.Add(tmp)
}

func (b *Bloom) Add(id []byte) {
	for hash := 0; hash < b.k; hash++ {
		index := b.GetIndexNoOffset(id, uint32(hash))
		b.Data[index] = true
	}
}

func (s *Bloom) GetIndexes(key []byte) []uint32 {
	idx := make([]uint32, s.k)
	for hash := 0; hash < s.k; hash++ {
		index := s.GetIndexNoOffset(key, uint32(hash))
		idx[hash] = index
	}
	return idx
}

func (s *Bloom) NumberHashes() int {
	return s.k
}

func (s *Bloom) QueryHashValues(hvs []uint32) bool {
	if len(hvs) < s.k {
		panic("wrong num idx")
	}
	cols := s.Columns
	for _, hv := range hvs[:s.k] {
		if false == s.Data[hv%uint32(cols)] {
			return false
		}
	}
	return true
}

func (s *Bloom) QueryIndexes(idx []uint32) bool {
	if len(idx) != s.k {
		panic("wrong num idx")
	}
	for _, id := range idx {
		if false == s.Data[id] {
			return false
		}
	}
	return true
}

func (s *Bloom) Query(key []byte) bool {
	for hash := 0; hash < s.k; hash++ {
		index := s.GetIndexNoOffset(key, uint32(hash))
		if false == s.Data[index] {
			return false
		}
	}
	return true
}

func (s *Bloom) QueryString(key string) bool {
	return s.Query([]byte(key))
}

func (s *Bloom) QueryInt(key int) bool {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	return s.Query(tmp)
}

func (s *Bloom) Merge(tom *Bloom) {
	if len(s.Data) != len(tom.Data) {
		panic("Data has to be the same length")
	}
	if s.k != tom.k {
		panic("k has to be the same")
	}

	for k, v := range s.Data {
		s.Data[k] = v || tom.Data[k]
	}
}
