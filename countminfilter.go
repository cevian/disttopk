package disttopk

import (
	"encoding/binary"
	"io"
	"math"
)

type CountMinFilter struct {
	*CountMinHash
	Data []*BitArray
}

func NewCountMinFilter(hashes int, columns int) *CountMinFilter {
	data := make([]*BitArray, hashes)
	for k, _ := range data {
		data[k] = NewBitArray(uint(columns))
	}
	s := CountMinFilter{
		NewCountMinHash(hashes, columns),
		data,
	}
	return &s
}

func NewCountMinFilterFromSketch(cms *CountMinSketch, thresh uint32) *CountMinFilter {
	cmf := NewCountMinFilter(cms.Hashes, cms.Columns)

	for k, hasharray := range cms.Data {
		for i := int(0); i < hasharray.Len(); i++ {
			cmf.Data[k].SetValue(uint(i), (hasharray.Get(int(i)) >= uint(thresh)))
		}
	}

	return cmf
}

func (c *CountMinFilter) ByteSize() int {
	return int(math.Ceil((float64(len(c.Data)) / 8.0)))
}

func (s *CountMinFilter) PassesInt(key int) bool {
	return s.QueryInt(key)
}

func (s *CountMinFilter) GetInfo() string {
	return "CM Filter"
}

func (s *CountMinFilter) QueryInt(key int) bool {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	return s.Query(tmp)
}

func (s *CountMinFilter) Query(key []byte) bool {
	for hash := 0; hash < s.Hashes; hash++ {
		index := s.GetIndexNoOffset(key, uint32(hash))
		v := s.Data[hash].Check(uint(index))
		if v == false {
			return false
		}
	}
	return true
}

func (p *CountMinFilter) Serialize(w io.Writer) error {
	length := uint32(len(p.Data))

	if err := binary.Write(w, binary.BigEndian, &length); err != nil {
		return err
	}

	for _, v := range p.Data {
		if err := v.Serialize(w); err != nil {
			return err
		}
	}
	return nil
}

func (p *CountMinFilter) Deserialize(r io.Reader) error {
	length := uint32(0)

	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return err
	}

	cas := make([]*BitArray, length)
	for k, _ := range cas {
		ca := &BitArray{}
		if err := ca.Deserialize(r); err != nil {
			return err
		}
		cas[k] = ca
	}
	p.Data = cas
	p.CountMinHash = NewCountMinHash(int(length), int(p.Data[0].NumBits()))

	return nil
}
