/*
Count-Min Sketch, an approximate counting data structure for summarizing data streams

for more information see http://github.com/jehiah/countmin
*/
package disttopk

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"math/rand"
)

type Sketch interface {
	//Add([]byte, uint32)
	Query([]byte) uint32
	//AddString(string, uint32)
	//QueryString(string) uint32
	//	Merge(Sketch)
}

type CountMinHash struct {
	Hashes  int
	Columns int
	hasha   []uint32
	hashb   []uint32
}

/* error margin within err (over) with probability prob*/

func NewCountMinHash(hashes int, columns int) *CountMinHash {

	s := CountMinHash{
		Hashes:  hashes,
		Columns: columns,
	}

	s.generateArrays()

	return &s
}

type countMinHashSerialized struct {
	Hashes  *int
	Columns *int
}

func (s *CountMinHash) generateArrays() {
	r := rand.New(rand.NewSource(99))

	hasha := make([]uint32, s.Hashes)
	hashb := make([]uint32, s.Hashes)

	for k, _ := range hasha {
		hasha[k] = r.Uint32()
		hashb[k] = r.Uint32()
	}

	s.hasha = hasha
	s.hashb = hashb
}

func (s *CountMinHash) GetHashValues(key []byte) []uint32 {
	hvs := make([]uint32, s.Hashes)
	for hash := 0; hash < s.Hashes; hash++ {
		index := s.GetIndexNoOffsetNoMod(key, uint32(hash))
		hvs[hash] = index
	}
	return hvs

}

func (s *CountMinHash) GetIndexNoOffsetNoMod(key []byte, hashNo uint32) uint32 {
	a := s.hasha[hashNo]
	b := s.hashb[hashNo]

	h := fnv.New64a()
	h.Write(key)
	x := h.Sum64()

	result := (uint64(a) * x) + uint64(b)
	result = ((result >> 31) + result) & ((1 << 31) - 1)

	index := uint32(result)
	return index
}

func (s *CountMinHash) GetIndexNoOffset(key []byte, hashNo uint32) uint32 {
	columns := uint32(s.Columns)
	result := s.GetIndexNoOffsetNoMod(key, hashNo)
	index := result % columns
	return index
}

func (s *CountMinHash) GetIndex(key []byte, hashNo uint32) uint32 {

	columns := uint32(s.Columns)

	return hashNo*columns + s.GetIndexNoOffset(key, hashNo)
}

func (s *CountMinHash) ByteSize() int {
	return 8
}

type CountMinSketch struct {
	*CountMinHash
	Data []*CountArray
}

func (c *CountMinSketch) ByteSize() int {
	return len(c.Data) * 4 * c.Data[0].Len()
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
	data := make([]*CountArray, hashes)
	for index, _ := range data {
		data[index] = NewCountArray(columns)
	}
	s := CountMinSketch{
		NewCountMinHash(hashes, columns),
		data,
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
		index := int(s.GetIndexNoOffset(key, uint32(hash)))
		prev := s.Data[hash].Get(index)
		s.Data[hash].Set(index, prev+uint(count))
		//s.Data[index] += count
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
	var min uint

	for hash := 0; hash < s.Hashes; hash++ {
		index := int(s.GetIndexNoOffset(key, uint32(hash)))
		v := s.Data[hash].Get(index)
		if hash == 0 || v < min {
			min = v
		}
	}
	return uint32(min)
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

	for k, hashArray := range s.Data {
		for hash_idx := 0; hash_idx < hashArray.Len(); hash_idx++ {
			prev := hashArray.Get(hash_idx)
			newv := cm.Data[k].Get(hash_idx)
			s.Data[k].Set(hash_idx, prev+newv)
		}
		//s.Data[k] += cm.Data[k]
	}
}

func (p *CountMinSketch) Serialize(w io.Writer) error {
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

func (p *CountMinSketch) Deserialize(r io.Reader) error {
	length := uint32(0)

	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return err
	}

	cas := make([]*CountArray, length)
	for k, _ := range cas {
		ca := &CountArray{}
		if err := ca.Deserialize(r); err != nil {
			return err
		}
		cas[k] = ca
	}
	p.Data = cas
	p.CountMinHash = NewCountMinHash(int(length), p.Data[0].Len())

	return nil
}

func (p *CountMinHash) Serialize(w io.Writer) error {
	hashes := uint32(p.Hashes)
	columns := uint32(p.Columns)

	if err := binary.Write(w, binary.BigEndian, &hashes); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &columns); err != nil {
		return err
	}
	return nil
}

func (p *CountMinHash) Deserialize(r io.Reader) error {
	hashes := uint32(0)
	columns := uint32(0)

	if err := binary.Read(r, binary.BigEndian, &hashes); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &columns); err != nil {
		return err
	}
	p.Hashes = int(hashes)
	p.Columns = int(columns)
	p.generateArrays()
	return nil
}

func (p *CountMinHash) Equal(obj *CountMinHash) bool {
	return p.Hashes == obj.Hashes && p.Columns == obj.Columns
}

/*
func (b *CountMinHash) export() *countMinHashSerialized {
	return &countMinHashSerialized{Hashes: &b.Hashes, Columns: &b.Columns}
}

func (p *CountMinHash) GobEncode() ([]byte, error) {
	prv := p.export()
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)
	if err := e.Encode(prv); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *CountMinHash) GobDecode(b []byte) error {
	prv := p.export()
	buf := bytes.NewReader(b)
	e := gob.NewDecoder(buf)
	err := e.Decode(prv)
	if err != nil {
		return err
	}
	p.generateArrays()
	return nil
}*/
