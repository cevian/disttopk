package disttopk

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
)

type HashValueFilter struct {
	filters map[uint8]*HashValueSlice
}

func NewHashValueFilter() *HashValueFilter {
	return &HashValueFilter{make(map[uint8]*HashValueSlice)}
}

func (t *HashValueFilter) GetFilters() map[uint8]*HashValueSlice {
	return t.filters
}

func (t *HashValueFilter) NumHashValues() int {
	l := 0
	for _, hvs := range t.filters {
		l += hvs.Len()
	}
	return l
}

func (t *HashValueFilter) GetModulusBits(modulus uint) uint8 {
	l := math.Log2(float64(modulus))
	i, f := math.Modf(l)
	if f != 0.0 {
		panic("Modulus has to be a power of 2")
	}
	return uint8(i)
}

func (t *HashValueFilter) Insert(modulus_bits uint8, hv uint32) {
	hvs := t.filters[modulus_bits]
	if hvs == nil {
		t.filters[modulus_bits] = NewHashValueSlice()
		hvs = t.filters[modulus_bits]
	}
	hvs.Insert(hv)
}

func (t *HashValueFilter) InsertHashValueSlice(modulus_bits uint8, nhvs *HashValueSlice) {
	hvs := t.filters[modulus_bits]
	if hvs == nil {
		t.filters[modulus_bits] = NewHashValueSlice()
		hvs = t.filters[modulus_bits]
	}
	hvs.InsertAll(nhvs)
}

type BloomHistogramScore uint32

type BloomHistogramEntry struct {
	filter BloomFilter
	max    BloomHistogramScore
	min    BloomHistogramScore
	eps    float64 //debug
}

func NewBloomHistogramEntry(filter BloomFilter, eps float64) *BloomHistogramEntry {
	return &BloomHistogramEntry{filter, 0, 0, eps}
}

func (t *BloomHistogramEntry) Add(id uint, score BloomHistogramScore) {
	if score > t.max {
		t.max = (score)
	}
	if score < t.min || t.min == 0 {
		t.min = score
	}
	t.filter.Add(IntKeyToByteKey(int(id)))
}

func (c *BloomHistogramEntry) GetFilter() BloomFilter {
	return c.filter
}

func (c *BloomHistogramEntry) GetMax() BloomHistogramScore {
	return c.max
}
func (c *BloomHistogramEntry) GetMin() BloomHistogramScore {
	return c.min
}

func (c *BloomHistogramEntry) GetInfo() string {
	if SAVE_DEBUG {
		return fmt.Sprintln("BloomEntry: max ", c.max, "k ", c.filter.NumberHashes(), "size", c.filter.ByteSize(), "eps", c.eps)
	} else {
		return fmt.Sprintln("BloomEntry: max ", c.max, "k ", c.filter.NumberHashes(), "size", c.filter.ByteSize())
	}
}

func (c *BloomHistogramEntry) AddToHashValueFilter(hvf *HashValueFilter) {
	gcs := c.filter.(*Gcs)
	m_bits := hvf.GetModulusBits(gcs.GetM())
	hvs := gcs.HashValues()
	/*h := hvf.filters[m_bits]
	old_len := 0
	if h != nil {
		old_len = h.Len()
	}*/
	hvf.InsertHashValueSlice(m_bits, hvs)

}

type FilterAdaptor interface {
	CreateBloomEntryFilter(N_est int, n int, numpeers int, entry_max uint, scorek uint, listlen int) (BloomFilter, float64)
	CreateBloomFilterToDeserialize() BloomFilter
}

type PlainFilterAdaptor struct{}

func (p PlainFilterAdaptor) CreateBloomEntryFilter(N_est int, n int, numpeers int, entry_max uint, scorek uint, listlen int) (BloomFilter, float64) {
	//m := EstimateM(N_est, n, RECORD_SIZE)     // * (totalblooms - (k - 1))
	eps := EstimateEps(N_est, n, RECORD_SIZE*8, numpeers+1) // * (totalblooms - (k - 1))
	m := EstimateMSimple(n, eps)
	entry := NewBloomSimpleEst(m, n)
	return entry, eps
}

func (p PlainFilterAdaptor) CreateBloomFilterToDeserialize() BloomFilter {
	return &Bloom{}
}

type EstimateParameter struct{
	NestimateParameter float64
	Adjuster float64
	DisableProbabilityAdjuster bool
}

type GcsFilterAdaptor struct{
	Est EstimateParameter
}

func (p GcsFilterAdaptor) CreateBloomEntryFilter(N_est int, n int, numpeers int, entry_max uint, scorek uint, listlen int) (BloomFilter, float64) {
	adjuster := 1.0
	if !p.Est.DisableProbabilityAdjuster && entry_max < scorek {
		//score_k = x * entry_max
		// x = score k / entry_max
		x := float64(scorek) / float64(entry_max)
		adjuster = 1.0 / x
	}

	estimateN := N_est
	if p.Est.NestimateParameter >= 0 {
		estimateN = listlen + int(p.Est.NestimateParameter*float64(listlen)*float64(numpeers-1))
	}
	if p.Est.NestimateParameter == -2.0 {
		panic("wtf")
	}


	//eps := EstimateEpsGcsAdjuster(N_est, n, RECORD_SIZE*8, numpeers+1, adjuster)
	eps := EstimateEpsGcsAlt(n, RECORD_SIZE*8, numpeers, estimateN, 2, adjuster, listlen)
	//eps := 0.01
	m_est := EstimateMGcs(n, eps)
	//fmt.Println("Eps ", eps, "n", n, "m_est", m_est)
	m_log := GetRoundedBits(m_est)
	if p.Est.Adjuster != 1.0 {
		m_log = int(float64(m_log)* p.Est.Adjuster)
	}
	m := GetValueFromBits(m_log)
	if m == 0 {
		return nil, eps
	}
	//fmt.Println("Estimate N", estimateN, eps, m_est, m_log, m)
	//fmt.Printf("GCS info: N_est %v, n %v, eps %v m_est %v, m_log %v (rounded %v) m %v\n", N_est, n, eps, m_est, m_log, m_log_rounded, m)
	entry := NewGcs(m)
	return entry, eps
}

func (p GcsFilterAdaptor) CreateBloomFilterToDeserialize() BloomFilter {
	return &Gcs{}
}

type BloomHistogram struct {
	FilterAdaptor
	Data   []*BloomHistogramEntry
	cutoff BloomHistogramScore
}

func NewBloomHistogram(f FilterAdaptor) *BloomHistogram {
	return &BloomHistogram{f, nil, 0}
}

type BloomHistogramFilter struct {
	*BloomHistogram
	Thresh uint32
}

func NewBloomHistogramFilter(bh *BloomHistogram) *BloomHistogramFilter {
	return &BloomHistogramFilter{bh, 0}
}

func (b *BloomHistogram) ByteSize() int {
	sz := 0
	for _, v := range b.Data {
		sz += v.filter.ByteSize() + 4
	}
	return sz + 4
}

func (c *BloomHistogram) AddToHashValueFilter(hvf *HashValueFilter) {
	for _, v := range c.Data {
		v.AddToHashValueFilter(hvf)
	}
}

func (c *BloomHistogram) GetInfo() string {
	//ret := fmt.Sprintf("BloomSketch: buckets", len(c.Data))

	ret := ""
	if SAVE_DEBUG {
		ret = fmt.Sprintln("bloom sketch: numblooms", len(c.Data), "cutoff", c.cutoff)
	} else {
		ret = fmt.Sprintln("bloom sketch: numblooms", len(c.Data), "cutoff", c.cutoff)
	}
	/*
		for _, v := range c.Data {
			ret += v.GetInfo()
		}*/
	return ret

}

/*
func (b *BloomHistogram) CreateFromList(list ItemList) {
	//topk := 10
	//n := 33
	scorek := list[b.topk-1].Score
	minscore := uint32(scorek) / uint32(b.numpeers)

	first_index_past_minscore := len(list)
	for i, item := range list {
		if uint32(item.Score) < minscore {
			first_index_past_minscore = i
			break
		}
	}

	if PRINT_BUCKETS {
		fmt.Println("first_idx_past_min ", first_index_past_minscore, "minscore", minscore, "score-k", scorek)
	}
	current_index := 0
	bucket_items := b.topk
	b.Data = make([]*BloomHistogramEntry, 0)
	for current_index < first_index_past_minscore && len(b.Data) < 10 {
		entry_start_index := current_index
		items_in_entry := bucket_items
		if items_in_entry > first_index_past_minscore-entry_start_index || len(b.Data) == 9 {
			items_in_entry = first_index_past_minscore - entry_start_index
		}

		filter, eps := b.CreateBloomEntryFilter(b.N_est, items_in_entry, b.numpeers)

		//m := EstimateM(2700000, corrected_items, RECORD_SIZE)     // * (totalblooms - (k - 1))
		//eps := EstimateEps(2700000, corrected_items, RECORD_SIZE) // * (totalblooms - (k - 1))
		entry := &BloomHistogramEntry{filter, 0, 0, eps}

		endindex := current_index + items_in_entry
		for current_index < len(list) && (current_index < endindex) {
			item := list[current_index]
			entry.Add(uint(item.Id), uint(item.Score))
			current_index += 1
		}
		entry.n_max = current_index - entry_start_index
		b.Data = append(b.Data, entry)
		if PRINT_BUCKETS {
			fmt.Println("Interval", len(b.Data), "max", entry.max, "min", list[current_index-1].Score, "#", current_index-entry_start_index, "k", entry.filter.NumberHashes())
		}
		bucket_items = b.topk
	}
	if current_index < len(list) {
		b.cutoff = uint32(list[current_index].Score)
		//fmt.Println("Cutoff", b.cutoff, list[current_index-1].Score, current_index, first_index_past_minscore, len(list))
	}

}
*/
func (s *BloomHistogramFilter) PassesInt(key int) bool {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	return s.Passes(tmp)
}

func (s *BloomHistogramFilter) Passes(key []byte) bool {
	if s.Thresh == 0 {
		panic("Thresh not sent")
	}
	pass := s.Query(key) >= s.Thresh
	/*if pass {
		s.Deb(key)
		fmt.Println("Pass", s.Query(key), s.Thresh)
	}*/
	return pass

}

func (s *BloomHistogram) Deb(key []byte) {
	total := BloomHistogramScore(0)
	for k, entry := range s.Data {
		if entry.filter.Query(key) {
			fmt.Println("k", k, "max, ", entry.max)
			total += entry.max
		}
	}
	fmt.Println("total, ", total)

}

func (s *BloomHistogram) LastEntry() *BloomHistogramEntry {
	return s.Data[len(s.Data)-1]
}

func (s *BloomHistogram) LowestMax() BloomHistogramScore {
	return s.Data[len(s.Data)-1].max
}

func (s *BloomHistogram) ByteSizeLastFilter() int {
	return s.Data[len(s.Data)-1].filter.ByteSize() + 4
}

func (s *BloomHistogram) CutoffChangePop() BloomHistogramScore {
	if len(s.Data) > 0 {
		return s.LowestMax() - s.Cutoff()
	}
	return 0
}

func (s *BloomHistogram) Cutoff() BloomHistogramScore {
	return s.cutoff
}

func (s *BloomHistogram) SetCutoff(cutoff BloomHistogramScore) {
	if cutoff > math.MaxUint32 {
		panic("Overflow")
	}
	s.cutoff = cutoff
}

func (s *BloomHistogram) Pop() BloomHistogramScore {
	max := s.Data[len(s.Data)-1].max
	s.Data = s.Data[:len(s.Data)-1]
	old_cutoff := s.cutoff
	s.cutoff = max
	return max - old_cutoff
}

/*
func (s *BloomSketch) GetIndexes(key []byte) []uint32 {
	if len(s.Data) > 0 {
		return s.Data[0].filter.GetIndexes(key)
	}
	return nil
}
*/
func (s *BloomHistogram) NumberHashes() int {
	max := 0
	for _, entry := range s.Data {
		if entry.filter.NumberHashes() > max {
			max = entry.filter.NumberHashes()
		}
	}
	return max
}
func (s *BloomHistogram) GetHashValues(key []byte) []uint32 {
	if len(s.Data) == 0 {
		return nil
	}

	max := 0
	index := 0
	for i, entry := range s.Data {
		if entry.filter.NumberHashes() > max {
			max = entry.filter.NumberHashes()
			index = i
		}
	}

	return s.Data[index].filter.GetHashValues(key)

}

func (s *BloomHistogram) QueryHashValues(hvs []uint32) uint32 {
	for _, entry := range s.Data {
		if entry.filter.QueryHashValues(hvs) {
			return uint32(entry.max)
		}
	}
	return uint32(s.cutoff)
}

/*func (s *BloomSketch) QueryIndexes(idx []uint32) uint32 {
	for _, entry := range s.Data {
		if entry.filter.QueryIndexes(idx) {
			return entry.max
		}
	}
	return s.cutoff
}*/

func (s *BloomHistogram) Query(key []byte) uint32 {
	//total := uint32(0)
	for _, entry := range s.Data {
		if entry.filter.Query(key) {
			//total += entry.max
			return uint32(entry.max)
		}
	}
	//return total + s.cutoff
	return uint32(s.cutoff)
}

/*
func (s *BloomSketch) Merge(toadd Sketch) {
	bs := toadd.(*BloomSketch)

	if len(s.Data) != len(bs.Data) {
		panic("Data has to be the same length")
	}

	for k, _ := range s.Data {
		s.Data[k].filter.Merge(bs.Data[k].filter)
		s.Data[k].max += bs.Data[k].max
		s.Data[k].n_max += bs.Data[k].n_max
	}

	s.cutoff += bs.cutoff
}*/

type BloomHistogramCollection struct {
	sketches      []*BloomHistogram
	Thresh        BloomHistogramScore
	stats_queried int //debug
	stats_passed  int //debug
}

// Len is part of sort.Interface.
func (s *BloomHistogramCollection) Len() int {
	return len(s.sketches)
}

// Swap is part of sort.Interface.
func (s *BloomHistogramCollection) Swap(i, j int) {
	s.sketches[i], s.sketches[j] = s.sketches[j], s.sketches[i]
}

func (s *BloomHistogramCollection) SketchScore(i int) float64 {
	return float64(s.sketches[i].ByteSizeLastFilter()) / float64(s.sketches[i].CutoffChangePop())
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *BloomHistogramCollection) Less(i, j int) bool {
	return s.SketchScore(i) > s.SketchScore(j)
	//return (float64(s.sketches[i].ByteSize()) / s.sketches[i].CutoffChangePop()) > (uint32(s.sketches[j].ByteSize()) / s.sketches[j].CutoffChangePop())
}

func (s *BloomHistogramCollection) Sort() {
	sort.Sort(s)
}

func NewBloomSketchCollection() *BloomHistogramCollection {
	return &BloomHistogramCollection{make([]*BloomHistogram, 0), 0, 0, 0}
}

func (bc *BloomHistogramCollection) ByteSize() int {
	t := 0
	for _, sk := range bc.sketches {
		t += sk.ByteSize()
	}
	return t
}

func (c *BloomHistogramCollection) AddToHashValueFilter(hvf *HashValueFilter) {
	for _, v := range c.sketches {
		v.AddToHashValueFilter(hvf)
	}
}

func (bc *BloomHistogramCollection) PopLast(t BloomHistogramScore) {

	cutoff := BloomHistogramScore(0)
	for _, sk := range bc.sketches {
		cutoff += sk.Cutoff()
	}

	/* cutoff the last, most expensive entry from each sketch */
	count := 0
	bc.Sort()
	for count < len(bc.sketches) && cutoff+bc.sketches[count].CutoffChangePop() < t {
		cutoff += bc.sketches[count].Pop()
		count++
	}
	//fmt.Println("Final cutoff", cutoff)
}

func (bc *BloomHistogramCollection) SetThresh(t BloomHistogramScore) {
	bc.Thresh = t
	bc.PopLast(t)
}

func (bc *BloomHistogramCollection) PopMax(t BloomHistogramScore) {
	cutoff := BloomHistogramScore(0)
	for _, sk := range bc.sketches {
		cutoff += sk.Cutoff()
	}

	bc.Sort()
	for /* bc.SketchScore(0) > 0.1 &&*/ cutoff+bc.sketches[0].CutoffChangePop() < (t) {
		cutoff += bc.sketches[0].Pop()
		bc.Sort()
	}
}

func (bc *BloomHistogramCollection) Merge(toadd Sketch) {
	bs := toadd.(*BloomHistogram)
	bc.sketches = append(bc.sketches, bs)
}

func (bc *BloomHistogramCollection) Query(key []byte) uint32 {
	//idx := bc.sketches[0].GetIndexes(key)
	max := 0
	index := 0
	for i, sk := range bc.sketches {
		if sk.NumberHashes() > max {
			max = sk.NumberHashes()
			index = i
		}
	}

	hvs := bc.sketches[index].GetHashValues(key)

	t := uint32(0)
	for _, sk := range bc.sketches {
		//idx := sk.GetIndexes(key)

		//t += sk.QueryIndexes(idx)
		t += sk.QueryHashValues(hvs)
	}
	return t
}

func (s *BloomHistogramCollection) PassesInt(key int) bool {
	return s.Passes(IntKeyToByteKey(key))
}

func (s *BloomHistogramCollection) Passes(key []byte) bool {
	s.stats_queried += 1
	if s.Thresh == 0 {
		panic("Thresh not sent")
	}
	pass := BloomHistogramScore(s.Query(key)) >= s.Thresh
	/*if pass {
		s.Deb(key)
		fmt.Println("Pass", s.Query(key), s.Thresh)
	}*/
	if pass {
		s.stats_passed += 1
	}
	return pass
}

/*func (bc *BloomSketchCollection) GetInfo() string {
	tot_nmax := 0
	estimatedfp := 0.0
	s := ""
	for _, sk := range bc.sketches {
		max := 0
		for _, entry := range sk.Data {
			max += entry.n_max
			estimatedfp += float64(bc.stats_queried) * entry.eps
		}
		tot_nmax += max

		//s += fmt.Sprintln("k", k, "filters = ", len(sk.Data), "cutoff", sk.cutoff, "n_max (total) ", max)
		//tot += sk.cutoff
		//s += "\n" + sk.GetInfo()
	}
	s += fmt.Sprintln("Bloom collection sketch sketches: ", len(bc.sketches), "queried", bc.stats_queried, "passed", bc.stats_passed, "fp", bc.stats_passed-tot_nmax, "estimated fp", estimatedfp)
	return s
}
*/

//This is estimated off of all items, so no need to multiply by num peers
func (bc *BloomHistogramCollection) EstimatedFp() float64 {
	if SAVE_DEBUG {
		allItems := 24000000
		estimatedFp := 0.0
		for _, sk := range bc.sketches {
			for _, entry := range sk.Data {
				estimatedFp += (float64(allItems)) * entry.eps
			}
		}
		return estimatedFp
	}
	return 0
}

func (bc *BloomHistogramCollection) TotalCutoff() int {
	cutoff := 0
	for _, sk := range bc.sketches {
		cutoff += int(sk.Cutoff())
	}
	return cutoff
}

func (bc *BloomHistogramCollection) TotalFilters() int {
	filters := 0
	for _, sk := range bc.sketches {
		filters += len(sk.Data)

	}
	return filters
}

func (bc *BloomHistogramCollection) GetInfo() string {
	if SAVE_DEBUG {
		return fmt.Sprintln("Bloom sketch collection, # sketches: ", len(bc.sketches), "total cutoff", bc.TotalCutoff(), "num filters", bc.TotalFilters(), " estimated fp", bc.EstimatedFp())
	}
	return fmt.Sprintln("Bloom sketch collection, # sketches: ", len(bc.sketches), "total cutoff", bc.TotalCutoff(), "num filters", bc.TotalFilters())
}

/*
func (bc *BloomSketchCollection) GetInfo() string {
	s := ""
	tot := uint32(0)
	tot_nmax := 0
	items := 24000000
	estimatedfp := 0.0
	for k, sk := range bc.sketches {
		max := 0
		for _, entry := range sk.Data {
			max += entry.n_max
			estimatedfp += (float64(items)) * entry.eps
		}
		tot_nmax += max

		s += fmt.Sprintln("k", k, "filters = ", len(sk.Data), "cutoff", sk.cutoff, "n_max (total) ", max)
		tot += sk.cutoff
		//s += "\n" + sk.GetInfo()
	}
	s += fmt.Sprintln("Bloom collection sketch sketches: ", len(bc.sketches), "total cutoff", tot, "total nmax (per sketch)", tot_nmax, "nmax sent by all", tot_nmax*33, " estimated fp", estimatedfp)
	return s
}*/

/////////////////////////serialization stuff//////////////////////

func (p *BloomHistogramEntry) Serialize(w io.Writer) error {
	if err := p.filter.Serialize(w); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, &p.max); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &p.min); err != nil {
		return err
	}
	if SAVE_DEBUG {
		if err := binary.Write(w, binary.BigEndian, &p.eps); err != nil {
			return err
		}
	}
	return nil
}

func (p *BloomHistogramEntry) Deserialize(r io.Reader) error {
	if p.filter == nil {
		panic("Have to initialize filter beforehand")
	}
	if err := p.filter.Deserialize(r); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &p.max); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &p.min); err != nil {
		return err
	}

	if SAVE_DEBUG {
		if err := binary.Read(r, binary.BigEndian, &p.eps); err != nil {
			return err
		}
	}

	return nil
}

func getFilterAdaptorId(f FilterAdaptor) uint8 {
	switch f.(type) {
	case PlainFilterAdaptor:
		return 1
	case GcsFilterAdaptor:
		return 2
	default:
		panic(fmt.Sprintf("Unknown filter type %T", f))
	}
}

func getFilterAdaptorById(id uint8) FilterAdaptor {
	switch id {
	case 1:
		return PlainFilterAdaptor{}
	case 2:
		return GcsFilterAdaptor{EstimateParameter{NestimateParameter: -2.0, Adjuster: -1.0}}
	default:
		panic("Unknown filter type")
	}
}

func (p *BloomHistogramFilter) Serialize(w io.Writer) error {
	panic("Not Implemented")
}
func (p *BloomHistogram) Serialize(w io.Writer) error {
	filterid := getFilterAdaptorId(p.FilterAdaptor)
	if err := binary.Write(w, binary.BigEndian, &filterid); err != nil {
		return err
	}

	datal := uint32(len(p.Data))
	if err := binary.Write(w, binary.BigEndian, &datal); err != nil {
		return err
	}

	for _, v := range p.Data {
		if err := v.Serialize(w); err != nil {
			return err
		}
	}

	if err := binary.Write(w, binary.BigEndian, &p.cutoff); err != nil {
		return err
	}
	return nil
}

func (p *BloomHistogram) Deserialize(r io.Reader) error {
	filterid := uint8(0)
	if err := binary.Read(r, binary.BigEndian, &filterid); err != nil {
		return err
	}
	filter := getFilterAdaptorById(filterid)
	p.FilterAdaptor = filter

	datal := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &datal); err != nil {
		return err
	}
	p.Data = make([]*BloomHistogramEntry, datal)
	for i := uint32(0); i < datal; i++ {
		entry := &BloomHistogramEntry{filter: p.CreateBloomFilterToDeserialize()}
		entry.Deserialize(r)
		p.Data[i] = entry
	}

	if err := binary.Read(r, binary.BigEndian, &p.cutoff); err != nil {
		return err
	}

	return nil
}

func (p *BloomHistogramCollection) Serialize(w io.Writer) error {
	sketchesl := uint32(len(p.sketches))
	if err := binary.Write(w, binary.BigEndian, &sketchesl); err != nil {
		return err
	}

	for _, v := range p.sketches {
		if err := v.Serialize(w); err != nil {
			return err
		}
	}

	if err := binary.Write(w, binary.BigEndian, &p.Thresh); err != nil {
		return err
	}

	if err := SerializeIntAsU32(w, &p.stats_queried); err != nil {
		return err
	}
	if err := SerializeIntAsU32(w, &p.stats_passed); err != nil {
		return err
	}
	return nil
}

func (p *BloomHistogramCollection) Deserialize(r io.Reader) error {
	sketchesl := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &sketchesl); err != nil {
		return err
	}

	p.sketches = make([]*BloomHistogram, sketchesl)
	for i := uint32(0); i < sketchesl; i++ {
		sketch := &BloomHistogram{}
		sketch.Deserialize(r)
		p.sketches[i] = sketch
	}

	if err := binary.Read(r, binary.BigEndian, &p.Thresh); err != nil {
		return err
	}

	if err := DeserializeIntAsU32(r, &p.stats_queried); err != nil {
		return err
	}
	if err := DeserializeIntAsU32(r, &p.stats_passed); err != nil {
		return err
	}
	return nil
}

/*
func (p *Bloom) Deserialize(r io.Reader) error {
	p.CountMinHash = &CountMinHash{}
	if err := p.CountMinHash.Deserialize(r); err != nil {
		return err
	}
	p.Data = &BitArray{}
	return p.Data.Deserialize(r)

}*/

/*type bloomsketchserialize struct {
	Adaptor  *FilterAdaptor
	Data     *[]*BloomEntry
	Topk     *int
	NumPeers *int
	N_est    *int
	Cutoff   *uint32
	Thresh   *uint32
	M        *int
	Nn_est   *int
}

func (b *BloomSketch) export() *bloomsketchserialize {
	return &bloomsketchserialize{Adaptor: &b.FilterAdaptor, Data: &b.Data, Topk: &b.topk, NumPeers: &b.numpeers, N_est: &b.N_est, Cutoff: &b.cutoff, Thresh: &b.Thresh, M: &b.m, Nn_est: &b.n_est}
}

func (p *BloomSketch) GobEncode() ([]byte, error) {
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

func (p *BloomSketch) GobDecode(b []byte) error {
	prv := p.export()
	buf := bytes.NewReader(b)
	e := gob.NewDecoder(buf)
	return e.Decode(&prv)
}

type bloomentryserialize struct {
	Filter *BloomFilter
	Max    *uint32
	N_max  *int
	Eps    *float64
}

func (b *BloomEntry) export() *bloomentryserialize {
	return &bloomentryserialize{Filter: &b.filter, Max: &b.max, N_max: &b.n_max, Eps: &b.eps}
}

func (p *BloomEntry) GobEncode() ([]byte, error) {
	prv := p.export()
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)
	gob.Register(&Bloom{})
	gob.Register(&Gcs{})
	if err := e.Encode(prv); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *BloomEntry) GobDecode(b []byte) error {
	prv := p.export()
	buf := bytes.NewReader(b)
	e := gob.NewDecoder(buf)
	//gob.Register(Bloom{})
	//gob.Register(Gcs{})
	err := e.Decode(&prv)
	return err
}

type bloomsketchcollectionserialize struct {
	Sketches      *[]*BloomSketch
	Thresh        *uint32
	Stats_queried *int
	Stats_passed  *int
}

func (b *BloomSketchCollection) export() *bloomsketchcollectionserialize {
	return &bloomsketchcollectionserialize{Sketches: &b.sketches, Thresh: &b.Thresh, Stats_queried: &b.stats_queried, Stats_passed: &b.stats_passed}
}

func (p *BloomSketchCollection) GobEncode() ([]byte, error) {
	prv := p.export()
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)
	if err := e.Encode(prv); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *BloomSketchCollection) GobDecode(b []byte) error {
	prv := p.export()
	buf := bytes.NewReader(b)
	e := gob.NewDecoder(buf)
	return e.Decode(prv)
}*/
