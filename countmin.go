/*
Count-Min Sketch, an approximate counting data structure for summarizing data streams

for more information see http://github.com/jehiah/countmin
*/
package disttopk

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
)

type Sketch interface {
	//Add([]byte, uint32)
	Query([]byte) uint32
	//AddString(string, uint32)
	//QueryString(string) uint32
	Merge(Sketch)
}

type CountMinHash struct {
	Hashes  int
	Columns int
	hasha   []uint32
	hashb   []uint32
}

/* error margin within err (over) with probability prob*/

func NewCountMinHash(hashes int, columns int) *CountMinHash {
	r := rand.New(rand.NewSource(99))

	hasha := make([]uint32, hashes)
	hashb := make([]uint32, hashes)

	for k, _ := range hasha {
		hasha[k] = r.Uint32()
		hashb[k] = r.Uint32()
	}

	s := CountMinHash{
		Hashes:  hashes,
		Columns: columns,
		hasha:   hasha,
		hashb:   hashb,
	}

	return &s
}

func (s *CountMinHash) GetIndexNoOffset(key []byte, hashNo uint32) uint32 {
	a := s.hasha[hashNo]
	b := s.hashb[hashNo]

	h := fnv.New64a()
	h.Write(key)
	x := h.Sum64()

	result := (uint64(a) * x) + uint64(b)
	result = ((result >> 31) + result) & ((1 << 31) - 1)

	columns := uint32(s.Columns)
	index := uint32(result) % columns

	return index
}

func (s *CountMinHash) GetIndex(key []byte, hashNo uint32) uint32 {

	columns := uint32(s.Columns)

	return hashNo*columns + s.GetIndexNoOffset(key, hashNo)
}

type CountMinSketch struct {
	*CountMinHash
	Data []uint32
}

func (c *CountMinSketch) ByteSize() int {
	return len(c.Data) * 4
}

func (c *CountMinSketch) GetInfo() string {
	cmItems := c.Hashes * c.Columns

	ret := fmt.Sprintln("Count min: hashes ", c.Hashes, "Columns", c.Columns, "Items", cmItems)

	return ret

}

func (c *CountMinSketch) CreateFromList(list ItemList) {
	for _, v := range list {
		c.AddInt(v.Id, uint32(v.Score))
	}

}

func NewCountMinSketchPb(err float64, prob float64) *CountMinSketch {
	hashes := math.Ceil(math.Log(1.0 / prob))
	columns := math.Ceil(math.E / err)
	return NewCountMinSketch(int(hashes), int(columns))
}

// Create a new Sketch. Settings for hashes and columns affect performance
// of Adding and Querying items, but also accuracy.
func NewCountMinSketch(hashes int, columns int) *CountMinSketch {
	s := CountMinSketch{
		NewCountMinHash(hashes, columns),
		make([]uint32, hashes*columns),
	}
	return &s
}

func (s *CountMinSketch) AddString(key string, count uint32) {
	s.Add([]byte(key), count)
}

func (s *CountMinSketch) AddInt(key int, count uint32) {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	s.Add(tmp, count)
}

func (s *CountMinSketch) QueryString(key string) uint32 {
	return s.Query([]byte(key))
}

func (s *CountMinSketch) QueryInt(key int) uint32 {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	return s.Query(tmp)
}

func (s *CountMinSketch) Add(key []byte, count uint32) {
	for hash := 0; hash < s.Hashes; hash++ {
		index := s.GetIndex(key, uint32(hash))
		s.Data[index] += count
		/*if count > s.Data[index] {
			s.Data[index] = count
		}*/
	}

	/*// TODO: this is a bad implementation because we hash all twice in worst case.
	newValue := s.Query(key) + count
	h := fnv.New64a()
	h.Write(key)
	columns := uint32(s.Columns)
	var b []byte
	for base := uint32(0); base < uint32(s.Hashes)*columns; base += columns {
		binary.Write(h, binary.LittleEndian, uint32(base))
		index := crc32.ChecksumIEEE(h.Sum(b)) % columns
		if s.Data[base+index] <= newValue {
			s.Data[base+index] = newValue
		}
	}
	return newValue*/
}

func (s *CountMinSketch) Query(key []byte) uint32 {
	var min uint32

	for hash := 0; hash < s.Hashes; hash++ {
		index := s.GetIndex(key, uint32(hash))
		v := s.Data[index]
		if hash == 0 || v < min {
			min = v
		}
	}
	return min
	/*	h := fnv.New64a()
		h.Write(key)
		var min uint32
		var b []byte
		columns := uint32(s.Columns)
		for base := uint32(0); base < uint32(s.Hashes)*columns; base += columns {
			binary.Write(h, binary.LittleEndian, uint32(base))
			index := crc32.ChecksumIEEE(h.Sum(b)) % columns
			v := s.Data[base+index]
			if base == 0 || v < min {
				min = v
			}
		}
		return min*/
}

func (s *CountMinSketch) Merge(toadd Sketch) {
	cm := toadd.(*CountMinSketch)

	if len(s.Data) != len(cm.Data) {
		panic("Data has to be the same length")
	}

	for k, _ := range s.Data {
		s.Data[k] += cm.Data[k]
	}
}
