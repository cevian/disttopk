package disttopk

import (
	"encoding/binary"
	"math"
)

type CountMinFilter struct {
	*CountMinHash
	Data []bool
}

func NewCountMinFilter(hashes int, columns int) *CountMinFilter {
	s := CountMinFilter{
		NewCountMinHash(hashes, columns),
		make([]bool, hashes*columns),
	}
	return &s
}

func NewCountMinFilterFromSketch(cms *CountMinSketch, thresh uint32) *CountMinFilter {
	cmf := NewCountMinFilter(cms.Hashes, cms.Columns)

	for k, v := range cms.Data {
		cmf.Data[k] = (v >= thresh)
	}

	return cmf
}

func (c *CountMinFilter) ByteSize() int {
	return int(math.Ceil((float64(len(c.Data)) / 8.0)))
}

func (s *CountMinFilter) QueryInt(key int) bool {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	return s.Query(tmp)
}

func (s *CountMinFilter) Query(key []byte) bool {
	for hash := 0; hash < s.Hashes; hash++ {
		index := s.GetIndex(key, uint32(hash))
		v := s.Data[index]
		if v == false {
			return false
		}
	}
	return true
}
