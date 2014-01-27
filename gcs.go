package disttopk

import (
	"encoding/binary"
	"fmt"
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

func (p *HashValueSlice) Eval(f func(uint32)) {
	for k, _ := range p.hvs {
		f(k)
	}
}

func (t *HashValueSlice) Insert(v uint32) {
	t.hvs[v] = true
}

func (t *HashValueSlice) InsertAll(n *HashValueSlice) {
	for k, _ := range n.hvs {
		t.Insert(k)
	}
}

func (t *HashValueSlice) Contains(value uint32) bool {
	return t.hvs[value]
}

func EstimateEpsGcs(N_est int, n_est int, penalty_bits int, NumTransfers int) float64 {
	//TODO change! -- this is base on the bloom filter approximation with k != 1
	//for compressed filters, needs to change.

	//for m = size of bloom, p = size of each record sent as false pos, s = # times filter sent across the wire
	//total (t) = s  * m + (N-n) * eps * p
	// m = n *  1.44 * log_2(1/eps) = n * 1.44 * 1/ln(2) * ln (1/eps)
	// dt/deps  = s * n * 1.44 * 1/ln(2) * 1/(1/eps) * (-1) 1/eps^2 + (N-n) * p
	// 0 =   -1 * s *  n * 1.44 / ln (2) * 1 / eps + (N-n) * p
	// (s * n * (1.44 / ln (2))) / ((N -n) * p) = eps
	// eps = s * 1.44 / (N/n -1) * p * ln (2)

	//fmt.Printf("N %v n %v penalty %v, NumTransfers %v\n", N_est, n_est, penalty_bits, NumTransfers)

	/* this is a hack to prevent eps going to inf */
	/*if n_est*2 > N_est {
		n_est = N_est / 2
	}*/

	eps := (2.0 * 1.44) / (float64(penalty_bits) * math.Log(2) * (float64(N_est/n_est) - 1.0))
	//fmt.Println("Eps", eps, "N_est", N_est, "n_est", n_est)
	/*if eps > 1 {
		eps = 1
	}*/
	return eps
}

func EstimateEpsGcsAdjuster(N_est int, n_est int, penalty_bits int, NumTransfers int, adjuster float64) float64 {
	//TODO change! -- this is base on the bloom filter approximation with k != 1
	//for compressed filters, needs to change.

	//for m = size of bloom, p = size of each record sent as false pos, s = # times filter sent across the wire, A = probability filter will be used
	//total (t) = s  * m + (N-n) * eps * p * A
	// m = n *  1.44 * log_2(1/eps) = n * 1.44 * 1/ln(2) * ln (1/eps)
	// dt/deps  = s * n * 1.44 * 1/ln(2) * 1/(1/eps) * (-1) 1/eps^2 + (N-n) * p * A
	// 0 =   -1 * s *  n * 1.44 / ln (2) * 1 / eps + (N-n) * p * A
	// (s * n * (1.44 / ln (2))) / ((N -n) * p * A) = eps
	// eps = s * 1.44 / (N/n -1) * p * ln (2) * A

	//fmt.Printf("N %v n %v penalty %v, NumTransfers %v\n", N_est, n_est, penalty_bits, NumTransfers)

	/* this is a hack to prevent eps going to inf */
	/*if n_est*2 > N_est {
		n_est = N_est / 2
	}*/

	eps := (2.0 * 1.44) / (float64(penalty_bits) * adjuster * math.Log(2) * (float64(N_est/n_est) - 1.0))

	m := float64(n_est) * 1.44 * (1.0 / math.Log(2)) * math.Log(1.0/eps)
	sketch := 2.0 * m

	penalty := (float64(N_est) - float64(n_est)) * float64(penalty_bits) * adjuster * eps
	fmt.Println("Eps debug: eps", eps, "sketch", sketch, "penalty", penalty)
	//fmt.Println("Eps", eps, "N_est", N_est, "n_est", n_est)
	/*if eps > 1 {
		eps = 1
	}*/
	return eps
}

func EstimateEpsGcsAlt(n_est int, penalty_bits int, numNodes int, itemsPerNode int, numTransfers int) float64 {
	//TODO change! -- this is base on the bloom filter approximation with k != 1
	//for compressed filters, needs to change.

	//for m = size of bloom, p = size of each record sent as false pos, s = # times filter sent across the wire, A = probability filter will be used
	// x = num nodes, l = items per node
	//total (t) = s*x*m + (l-n) * eps * p * x
	// m = n *  1.44 * log_2(1/eps) = n * 1.44 * 1/ln(2) * ln (1/eps)
	// dt/deps  =  s*x*n * 1.44 * 1/ln(2) * 1/(1/eps) * (-1) 1/eps^2 + (l-n) * p * x
	// 0 =   -1  * s * x * n * 1.44 / ln (2) * 1 / eps + (l-n) * p * x
	// ( s* x *n * (1.44 / ln (2))) / ((l -n) * p * x) = eps
	// eps = s * 1.44 / (l/n -1) * p * ln (2)

	//fmt.Printf("N %v n %v penalty %v, NumTransfers %v\n", N_est, n_est, penalty_bits, NumTransfers)

	/* this is a hack to prevent eps going to inf */
	/*if n_est*2 > N_est {
		n_est = N_est / 2
	}*/

	fmt.Println("n_est", n_est, "penalty_bits", penalty_bits, "numNodes", numNodes, "itemspn", itemsPerNode)
	size_adj := 0.8
	eps := (float64(numTransfers) * size_adj * 1.44) / (float64(penalty_bits) * math.Log(2) * (float64(itemsPerNode/n_est) - 1.0))

	effective_m := float64(n_est) * size_adj * 1.44 * (1.0 / math.Log(2)) * math.Log(1.0/eps)
	sketch := (effective_m + (9.0 * 8.0)) * float64(numNodes) //9 bytes is the overhead
	penalty := (float64(itemsPerNode) - float64(n_est)) * float64(penalty_bits) * float64(numNodes) * eps
	fmt.Println("Eps debug: eps", eps, "sketch per transfer ", sketch, "bits ", sketch/8, "bytes\n penalty", penalty, "bits", penalty/8, "bytes sum", sketch+penalty, "bits", (sketch+penalty)/8, "bytes")

	actual_m := MakePowerOf2(EstimateMGcs(n_est, eps))
	eps_test := float64(n_est) / float64(actual_m)
	effective_m = float64(n_est) * size_adj * 1.44 * (1.0 / math.Log(2)) * math.Log(1.0/eps_test)
	sketch = (effective_m + (9.0 * 8.0)) * float64(numNodes) //9 bytes is the overhead
	penalty = (float64(itemsPerNode) - float64(n_est)) * float64(penalty_bits) * float64(numNodes) * eps_test
	fmt.Println("ipn", itemsPerNode, "n_est", n_est, "eps_test", eps_test)
	penalty_items := (float64(itemsPerNode) - float64(n_est)) * eps_test
	fmt.Println("Eps debug: m ", actual_m, effective_m, "eps", eps, "sketch per transfer", sketch, "bits ", sketch/8, "bytes\npenalty", penalty, "bits", penalty/8, " bytes. Items sent as fp ", penalty_items, "/node. Sum", sketch+penalty, "bits", (sketch+penalty)/8, "bytes")

	//fmt.Println("Eps", eps, "N_est", N_est, "n_est", n_est)
	/*if eps > 1 {
		eps = 1
	}*/
	return eps
}

func EstimateMGcs(n int, eps float64) int {
	if eps >= 1 {
		return 0
	}
	// eps=(1-e^(kn/m))^k
	//For k=1:
	//eps=(1-e^(n/m))
	//e^(-n/m) = 1-eps
	//-n/m = ln (1-eps)
	//m = -n/ln(1-eps)
	//by the taylor series for eps in our range ln(1-eps) ~ -eps. so
	//m  ~ n/eps
	m := int(float64(n) / eps)
	return m
}

func NewGcs(m int) *Gcs {
	//fmt.Println("Making GCS of size M=", m)
	/*
		m == 0 should be valid
		if m < 1 {
			panic(fmt.Sprintf("Wrong size %v", m))
		}*/

	if m > math.MaxUint32 {
		panic("m bigger than uint32")
	}

	s := Gcs{
		NewCountMinHash(1, m),
		NewHashValueSlice(),
	}
	return &s
}

func (b *Gcs) CreateNew() *Gcs {
	return NewGcs(b.Columns)
}

func (b *Gcs) GetInfo() string {
	return fmt.Sprintf("Gcs modulus %v, hash values set %v", b.Columns, b.Data.Len())
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
	if b.Columns == 0 {
		return
	}
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
	if cols == 0 {
		return true
	}
	index := hvs[0] % uint32(cols)
	return s.Data.Contains(index)
}

func (s *Gcs) Query(key []byte) bool {
	if s.Columns == 0 {
		return true
	}
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

/*
func orderGcsMerge(left, right *Gcs) *Gcs {
	//fmt.Println("Merging, ", left.Columns, right.Columns)
	// this merge merges larger into smaller, you can also imagine merging smaller into larger
	if left.Columns == right.Columns {
		left.Data.InsertAll(right.Data)
		return left
	} else {
		if right.Columns < left.Columns || right.Columns%left.Columns != 0 {
			panic("Gcs not mergeable")
		}

		right.Data.Eval(func(v uint32) { left.Data.Insert(v % uint32(right.Columns)) })
		return left
	}
}

func GcsMerge(left, right *Gcs) *Gcs {
	lc := left.Columns
	rc := right.Columns

	if lc > rc {
		return orderGcsMerge(right, left)
	}
	return orderGcsMerge(left, right)
}

// this can change either gcs or both
func (t *Gcs) Merge(tomerge *Gcs) *Gcs {
	return GcsMerge(t, tomerge)
}
*/
