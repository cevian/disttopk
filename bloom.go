package disttopk

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

var _ = fmt.Println

type BloomFilter interface {
	Serializer
	//CreateNew() BloomFilter
	ByteSize() int
	Add(id []byte)
	//AddInt(key int)
	QueryHashValues(hvs []uint32) bool
	Query(key []byte) bool
	NumberHashes() int
	GetHashValues(key []byte) []uint32
}

type Bloom struct {
	*CountMinHash
	Data *BitArray
}

/*
func EstimateM(N_est int, n_est int, penalty int) int {
	eps := EstimateEps(N_est, n_est, penalty)
	m := EstimateMSimple(n_est, eps)
	//fmt.Println("eps = ", eps, " m = ", int(m), " bytes = ", int(m)/8)
	return int(m)
}*/

func EstimateEps(N_est int, n_est int, penalty_bits int, NumBloomTransfers int) float64 {
	//for m = size of bloom, p = size of each record sent as false pos, s = # times filter sent across the wire
	//total (t) = s  * m + (N-n) * eps * p
	// m = n *  1.44 * log_2(1/eps) = n * 1.44 * 1/ln(2) * ln (1/eps)
	// dt/deps  = s * n * 1.44 * 1/ln(2) * 1/(1/eps) * (-1) 1/eps^2 + (N-n) * p
	// 0 =   -1 * s *  n * 1.44 / ln (2) * 1 / eps + (N-n) * p
	// (s * n * (1.44 / ln (2))) / ((N -n) * p) = eps
	// eps = s * 1.44 / (N/n -1) * p * ln (2)
	eps := (float64(NumBloomTransfers) * 1.44) / (float64(penalty_bits) * math.Log(2) * (float64(N_est/n_est) - 1.0))
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

func NewBloomSimpleEst(m int, n int) *Bloom {
	k := math.Ceil((float64(m) / float64(n)) * math.Log(2))
	return NewBloom(int(k), int(m))
}

/* k is number hashes, m is size of data */
func NewBloom(k int, m int) *Bloom {
	s := Bloom{
		NewCountMinHash(k, m),
		NewBitArray(uint(m)),
	}
	return &s
}

func (b *Bloom) CreateNew() *Bloom {
	return NewBloom(b.Hashes, int(b.Data.NumBits()))
}

func (b *Bloom) Len() int {
	return int(b.Data.NumBits())
}

func (b *Bloom) ByteSize() int {
	return b.Data.ByteSize() + 8
}

func (b *Bloom) CountSetBit() uint {
	return b.Data.CountSetBit()
}

func (b *Bloom) VisitSetHashValues(visitor func(int)) {
	b.Data.VisitSetBit(visitor)
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
	for hash := 0; hash < b.NumberHashes(); hash++ {
		index := b.GetIndexNoOffset(id, uint32(hash))
		b.Data.Set(uint(index))
	}
}

func (s *Bloom) GetIndexes(key []byte) []uint32 {
	idx := make([]uint32, s.NumberHashes())
	for hash := 0; hash < s.NumberHashes(); hash++ {
		index := s.GetIndexNoOffset(key, uint32(hash))
		idx[hash] = index
	}
	return idx
}

func (s *Bloom) NumberHashes() int {
	return s.Hashes
}

func (s *Bloom) QueryHashValues(hvs []uint32) bool {
	if len(hvs) < s.NumberHashes() {
		panic("wrong num idx")
	}
	cols := s.Columns
	for _, hv := range hvs[:s.NumberHashes()] {
		index := uint(hv) % uint(cols)

		if false == s.Data.Check(index) {
			return false
		}
	}
	return true
}

func (s *Bloom) QueryIndexes(idx []uint32) bool {
	if len(idx) != s.NumberHashes() {
		panic("wrong num idx")
	}
	for _, id := range idx {
		if false == s.Data.Check(uint(id)) {
			return false
		}
	}
	return true
}

func (s *Bloom) Query(key []byte) bool {
	for hash := 0; hash < s.NumberHashes(); hash++ {
		index := s.GetIndexNoOffset(key, uint32(hash))
		if false == s.Data.Check(uint(index)) {
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

	if s.NumberHashes() != tom.NumberHashes() {
		panic("k has to be the same")
	}

	s.Data.MergeOr(tom.Data)
}

func (p *Bloom) Serialize(w io.Writer) error {
	if err := p.CountMinHash.Serialize(w); err != nil {
		return err
	}
	return p.Data.Serialize(w)
}

func (p *Bloom) Deserialize(r io.Reader) error {
	p.CountMinHash = &CountMinHash{}
	if err := p.CountMinHash.Deserialize(r); err != nil {
		return err
	}
	p.Data = &BitArray{}
	return p.Data.Deserialize(r)

}

func (p *Bloom) Equal(obj *Bloom) bool {
	return p.CountMinHash.Equal(obj.CountMinHash) && p.Data.Equal(obj.Data)
}

func (p *Bloom) GetInfo() string {
	return fmt.Sprintln("Bloom Filter: columns ", p.Columns, " Hashes ", p.Hashes)
}

/*
type bloomserialize struct {
	CountMinHash **CountMinHash
	Data         **BitArray
}

func (b *Bloom) export() *bloomserialize {
	return &bloomserialize{CountMinHash: &b.CountMinHash, Data: &b.Data}
}

func (p *Bloom) GobEncode() ([]byte, error) {
	prv := p.export()
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)
	gob.Register(PlainFilterAdaptor{})
	gob.Register(GcsFilterAdaptor{})
	if err := e.Encode(prv); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *Bloom) GobDecode(b []byte) error {
	prv := p.export()
	buf := bytes.NewReader(b)
	e := gob.NewDecoder(buf)
	return e.Decode(&prv)
}*/
