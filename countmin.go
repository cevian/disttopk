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

const USE_NORMALIZATION = true //this saves a lot of bandwidth
const NORM_SCALE = 10.0
const SERIALIZE_BAG = false //this is innefctive if using standard compression as well
const SERIALIZE_GCS = true
const ADD_VALUES_ON_ADD = false

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

func EstimateEpsCmNew(N_est int, n_sent int, n_filter int, penalty_bits int) float64 {
	//for b = size of bloom in bits, p = size of each record sent as false pos in bits,
	//n_filter = number of items sent from central in filter (# bits set in filter)
	//n_sent = number of items in cm sent from peers to controller
	//total (t) = b + (N) * eps_filter * p
	// b =  n_sent *  1.44 * 0.7 * log_2(1/eps)
	// eps_filter = n_filter/m
	// m = n_sent / eps
	// t =  n_sent *  1.44 * 0.7 * log_2(1/eps) + N * n_filter/n_sent * eps * p
	// dt/deps = n_sent * 1.44 * 0.7 * 1/ln(2) * 1/(1/eps) * (-1) 1/eps^2  + N * p * n_filter/n_sent
	// 0 = n_sent * (-1.44 * 0.7 / ln(2) ) * 1/eps + N * p * n_filter/n_sent
	// eps = n_sent * (1.44 * 0.7 / ln(2) ) /  ( N * p * n_filter/n_sent )
	// eps = (1.44 * 0.7 / ln(2) ) /  ( N * p * n_filter/n_sent^2 )

	// the 2.0 is a correction (may be because sketch sent twice more than actual records (to and from coord))
	eps := (2.0 * (1.0 / math.Log(2)) * 1.44 * 0.7) / ((float64(N_est) * float64(penalty_bits) * float64(n_filter)) / (float64(n_sent) * float64(n_sent)))

	size_bits := float64(n_sent) * (1.44 * 0.7) * math.Log2(1/eps)
	eps_filter := (float64(n_filter) / float64(n_sent)) * eps
	fp_bits := float64(N_est) * eps_filter * float64(penalty_bits)
	fp_items := float64(N_est) * eps_filter
	cols := float64(n_sent) / eps
	info := fmt.Sprintln("size_bits", size_bits, "eps_filter", eps_filter, "fp_bits", fp_bits, "tot bits", size_bits+fp_bits, "fp_items", fp_items, "cols", cols, "n_sent", n_sent, "n_filter", n_filter)
	_ = info
	//fmt.Println(info)

	/*eps_2 := eps * 2.0
	size_bits_2 := float64(n_sent) * (1.44 * 0.7) * math.Log2(1/eps_2)
	eps_filter_2 := (float64(n_filter) / float64(n_sent)) * eps_2
	fp_bits_2 := float64(N_est) * eps_filter_2 * float64(penalty_bits)
	fmt.Println("size_bits 2", size_bits_2, "eps_filter 2", eps_filter_2, "fp_bits 2", fp_bits_2, "tot bits", size_bits_2+fp_bits_2) */
	return eps

}

/*func EstimateEpsCm(N_est int, n_est int, penalty_bits int, NumTransfers int) float64 {
	//TODO change! -- this is base on the bloom filter approximation with k != 1
	//for compressed filters, needs to change.

	//for m = size of bloom, p = size of each record sent as false pos, s = # times filter sent across the wire
	//total (t) = s  * m + (N-n) * eps * p
	// m = n *  1.44 * 0.7 * log_2(1/eps) = n * 1.44 * 1/ln(2) * ln (1/eps)
	// dt/deps  = s * n * 1.44 * 0.7 * 1/ln(2) * 1/(1/eps) * (-1) 1/eps^2 + (N-n) * p
	// 0 =   -1 * s *  n * 1.44 * 0.7  / ln (2) * 1 / eps + (N-n) * p
	// (s * n * (1.44 * 0.7 / ln (2))) / ((N -n) * p) = eps
	// eps = s * 1.44 * 0.7 / (N/n -1) * p * ln (2)

	//fmt.Printf("N %v n %v penalty %v, NumTransfers %v\n", N_est, n_est, penalty_bits, NumTransfers)

	//for b = size of bloom in bits, p = size of each record sent as false pos in bits,
	//n_filter = number of items sent from central in filter (# bits set in filter)
	//n_sent = number of items in cm sent from peers to controller
	//total (t) = b + (N) * eps_filter * p
	// b =  n_sent *  1.44 * 0.7 * log_2(1/eps)
	// eps_filter = n_filter/m
	// m = n_sent / eps
	// t =  n_sent *  1.44 * 0.7 * log_2(1/eps) + N * n_filter/n_sent * eps * p
	// dt/deps = n_sent * 1.44 * 0.7 * 1/ln(2) * 1/(1/eps) * (-1) 1/eps^2  + N * p * n_filter/n_sent
	// 0 = n_sent * (-1.44 * 0.7 / ln(2) ) * 1/eps + N * p * n_filter/n_sent
	// eps = n_sent * (-1.44 * 0.7 / ln(2) ) /  ( N * p * n_filter/n_sent )
	// eps = (-1.44 * 0.7 / ln(2) ) /  ( N * p * n_filter/n_sent^2 )

	//
	//total (t) = s  * m + (N) * eps * p
	// m = n *  1.44 * 0.7 * log_2(1/eps) = n * 1.44 * 1/ln(2) * ln (1/eps)
	// dt/deps  = s * n * 1.44 * 0.7 * 1/ln(2) * 1/(1/eps) * (-1) 1/eps^2 + (N) * p
	// 0 =   -1 * s *  n * 1.44 * 0.7  / ln (2) * 1 / eps + (N) * p
	// (s * n * (1.44 * 0.7 / ln (2))) / (N * p) = eps
	// eps = s * 1.44 * 0.7 / (N/n) * p * ln (2)

	if true || float64(N_est/n_est) < 2.0 {
		eps := (float64(NumTransfers) * 1.44 * 0.7) / (float64(penalty_bits) * math.Log(2) * (float64(N_est / n_est)))

		//this eps (eps_est) is for n_est but we will have n_actual if n_actual << n_est then
		// m_est := n_est/eps_est
		// for n_actual the eps will be : eps_actual = n_actual/m_est = n_actual / (n_est/eps_est) = n_actual * eps_est / n_est= n_actual/n_est * eps

		size_bits := float64(n_est) * (1.44 * 0.7) * math.Log2(1/eps)
		fp_bits := float64(N_est) * eps * float64(penalty_bits)
		fp_items := float64(N_est) * eps
		cols := float64(n_est) / eps
		fmt.Print("size_bits", size_bits, "fp_bits", fp_bits, "fp_items", fp_items, "cols", cols)
		//fmt.Println("Eps alt", eps, N_est, n_est, (float64(NumTransfers)*1.44*0.7)*math.Log2(1/eps), , float64(N_est)*eps,
		//float64(n_est)*1.44*0.7*math.Log2(1/eps), float64(n_est)/eps, 398.0/float64(n_est)*eps, 398.0/float64(n_est)*eps*float64(N_est))
		return eps
		//N_est = n_est * 2
	}

	eps := (float64(NumTransfers) * 1.44 * 0.7) / (float64(penalty_bits) * math.Log(2) * (float64(N_est/n_est) - 1.0))
	fmt.Println("Eps", eps, N_est, n_est)
	return eps
}*/

type CountMinSketch struct {
	*CountMinHash
	Data   []*CountArray
	Cutoff uint
}

func (c *CountMinSketch) ByteSize() int {
	return len(c.Data) * 4 * c.Data[0].Len()
}

func (t *CountMinSketch) GetValueBits(hashNo int) uint8 {
	return t.Data[hashNo].GetValueBits()
}

func (c *CountMinSketch) GetInfo() string {
	cmItems := c.Hashes * c.Columns

	ret := fmt.Sprintln("Count min: hashes ", c.Hashes, "Columns", c.Columns, "Cells", cmItems, "Cutoff", c.Cutoff, "Items[0]", c.Data[0].CountNonZero())

	return ret

}

func (c *CountMinSketch) CreateFromList(list ItemList) {
	for _, v := range list {
		c.AddInt(v.Id, uint32(v.Score))
	}

}

func CountMinColumnsEst(eps float64) int {
	columns := math.Ceil(math.E / eps)
	return int(columns)
}

func CountMinColumnsEstPow2(eps float64) int {
	columns := math.Ceil(math.E / eps)
	bits := math.Log2(columns)
	rounded, f := math.Modf(bits)
	if f > 0.5 {
		rounded += 1
	}
	return int(1 << uint(rounded))
}

func CountMinColumnsEstBloomPow2(n int, eps float64) int {
	columns := math.Ceil(float64(n) / eps)
	bits := math.Log2(columns)
	rounded, f := math.Modf(bits)
	if f > 0.5 {
		rounded += 1
	}
	return int(1 << uint(rounded))
}

func CountMinHashesEst(prob float64) int {
	hashes := math.Ceil(math.Log(1.0 / prob))
	return int(hashes)
}

func NewCountMinSketchPb(err float64, prob float64) *CountMinSketch {
	return NewCountMinSketch(CountMinHashesEst(prob), CountMinColumnsEst(err))
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
		0,
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

func (s *CountMinSketch) AddWithCutoff(key []byte, count uint, cutoff uint) {
	if count <= cutoff {
		return
	}
	value := count - cutoff
	if s.Cutoff == 0 {
		s.Cutoff = cutoff
	} else if s.Cutoff != cutoff {
		panic("SNH")
	}

	for hash := 0; hash < s.Hashes; hash++ {
		index := int(s.GetIndexNoOffset(key, uint32(hash)))
		prev := s.Data[hash].Get(index)

		if ADD_VALUES_ON_ADD {
			s.Data[hash].Set(index, prev+value)
		} else {
			if value > prev { //set it to the max
				s.Data[hash].Set(index, value)
			}

		}
	}
}

func (s *CountMinSketch) Add(key []byte, count uint32) {
	s.AddWithCutoff(key, uint(count), 0)
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
	return uint32(min + s.Cutoff)
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
		/*if hashArray.Len() != cm.Data[k].Len() {
			panic("Has to be the same length")
		}*/
		if hashArray.Len() <= cm.Data[k].Len() {
			if cm.Data[k].Len()%hashArray.Len() != 0 {
				panic("Has to be divisible")
			}
			for hash_idx := 0; hash_idx < cm.Data[k].Len(); hash_idx++ {
				my_idx := hash_idx % hashArray.Len()
				prev := hashArray.Get(my_idx)
				newv := cm.Data[k].Get(hash_idx)
				s.Data[k].Set(my_idx, prev+newv)
			}

		} else {
			if hashArray.Len()%cm.Data[k].Len() != 0 {
				panic("Has to be divisible")
			}
			for hash_idx := 0; hash_idx < hashArray.Len(); hash_idx++ {
				toadd_idx := hash_idx % cm.Data[k].Len()
				prev := hashArray.Get(hash_idx)
				newv := cm.Data[k].Get(toadd_idx)
				s.Data[k].Set(hash_idx, prev+newv)
			}
		}
		/*
			for hash_idx := 0; hash_idx < hashArray.Len(); hash_idx++ {
				prev := hashArray.Get(hash_idx)
				newv := cm.Data[k].Get(hash_idx)
				s.Data[k].Set(hash_idx, prev+newv)
			}*/
		//s.Data[k] += cm.Data[k]
	}
	s.Cutoff += cm.Cutoff
}

func (p *CountMinSketch) Serialize(w io.Writer) error {
	length := uint32(len(p.Data))

	if err := binary.Write(w, binary.BigEndian, &length); err != nil {
		return err
	}

	cutoff := uint32(p.Cutoff)
	if err := binary.Write(w, binary.BigEndian, &cutoff); err != nil {
		return err
	}

	for _, v := range p.Data {
		//fmt.Println("In count min serializing count array length :", v.Len())
		if USE_NORMALIZATION {
			v.LogNormalize(NORM_SCALE)
		}
		serf := v.Serialize
		if SERIALIZE_BAG {
			serf = v.SerializeWithBag
		}
		if SERIALIZE_GCS {
			serf = v.SerializeGcs
		}
		if err := serf(w); err != nil {
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

	cutoff := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &cutoff); err != nil {
		return err
	}
	p.Cutoff = uint(cutoff)

	cas := make([]*CountArray, length)
	for k, _ := range cas {
		ca := &CountArray{}

		deserf := ca.Deserialize
		if SERIALIZE_BAG {
			deserf = ca.DeserializeWithBag
		}
		if SERIALIZE_GCS {
			deserf = ca.DeserializeGcs
		}
		if err := deserf(r); err != nil {
			return err
		}
		if USE_NORMALIZATION {
			ca.LogDenormalize(NORM_SCALE)
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
