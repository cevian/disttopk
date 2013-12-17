package disttopk

import (
	"encoding/binary"
	"fmt"
	"sort"
)

type BloomEntry struct {
	filter *Bloom
	max    uint32
	n_max  int
	eps    float64
}

type BloomSketch struct {
	Data   []*BloomEntry
	m      int
	n_est  int
	cutoff uint32
	Thresh uint32
}

func NewBloomSketch(numbloom int, m int, n_est int) *BloomSketch {
	return &BloomSketch{make([]*BloomEntry, numbloom), m, n_est, 0, 0}
}

func (b *BloomSketch) ByteSize() int {
	sz := 0
	for _, v := range b.Data {
		sz += v.filter.ByteSize() + 4
	}
	return sz + 4 + 4
}

func (c *BloomSketch) GetInfo() string {
	ret := fmt.Sprintln("bloom sketch: numblooms", len(c.Data), " m ", c.m, " n_est", c.n_est, " k ", c.Data[0].filter.Hashes, "cutoff", c.cutoff)
	for _, v := range c.Data {
		ret += fmt.Sprintln("\n", "max ", v.max, " with cutoff ", v.max+c.cutoff, "size", v.filter.ByteSize(), "n_max", v.n_max)
	}
	return ret

}

func (b *BloomSketch) CreateFromList(list ItemList) {
	/*max := uint32(list[0].Score)
	min := uint32(list[len(list)-1].Score)

	r := max - (min - 1)

	interval := r / uint32(len(b.Data))

	listindex := 0
	for k, _ := range b.Data {
		maxinterval := max - (interval * uint32(k))
		mininterval := max - (interval * (uint32(k) + 1)) + 1

		b.Data[k] = &BloomEntry{NewBloomSimpleEst(b.m, b.n_est), maxinterval}

		orig := listindex
		for listindex < len(list) && uint32(list[listindex].Score) >= mininterval {
			b.Data[k].filter.AddInt(list[listindex].Id)
			listindex += 1
		}

		fmt.Println("Interval", k, "max", maxinterval, "min", mininterval, "#", listindex-orig)
	}*/

	/*count := len(list)
	num := count / len(b.Data)

	listindex := 0
	for k, _ := range b.Data {

		b.Data[k] = &BloomEntry{NewBloomSimpleEst(b.m, b.n_est), 0}

		first := true
		orig := listindex
		for listindex < len(list) && listindex <= num*(k+1) {
			b.Data[k].filter.AddInt(list[listindex].Id)
			if first {
				b.Data[k].max = uint32(list[listindex].Score)
				first = false
			}
			listindex += 1
		}
		fmt.Println("Interval", k, "max", b.Data[k].max, "min", list[listindex-1].Score, "#", listindex-orig)
	}*/

	/*topk := 10

	listindex := 0
	for k, _ := range b.Data {

		b.Data[k] = &BloomEntry{NewBloomSimpleEst(b.m, b.n_est), 0}

		first := true
		orig := listindex
		for listindex < len(list) && (k == len(b.Data)-1 || listindex <= (topk*5)*(k+1)) {
			b.Data[k].filter.AddInt(list[listindex].Id)
			if first {
				b.Data[k].max = uint32(list[listindex].Score)
				first = false
			}
			listindex += 1
		}
		fmt.Println("Interval", k, "max", b.Data[k].max, "min", list[listindex-1].Score, "#", listindex-orig)
	}*/

	topk := 10
	n := 33
	scorek := list[topk-1].Score
	minscore := uint32(scorek) / uint32(n)

	lastindex := len(list) - 1
	for i, item := range list {
		if uint32(item.Score) < minscore {
			lastindex = i
			break
		}
	}

	//count := lastindex + 1

	perList := ((lastindex + 1) / len(b.Data)) + 1
	//perBloom := perList

	//m := EstimateMSimple(perBloom, 0.0001)
	//m := EstimateM(2700000, perBloom, RECORD_SIZE)

	fmt.Println("perList = ", perList, " lastindex ", lastindex, "minscore", minscore, "score-k", scorek)

	listindex := 0
	items := topk
	b.Data = make([]*BloomEntry, 0)
	for listindex <= lastindex {
		orig := listindex
		corrected_items := items
		if items > lastindex-listindex+1 {
			corrected_items = lastindex - listindex + 1
		}
		m := EstimateM(2700000, corrected_items, RECORD_SIZE)     // * (totalblooms - (k - 1))
		eps := EstimateEps(2700000, corrected_items, RECORD_SIZE) // * (totalblooms - (k - 1))
		entry := &BloomEntry{NewBloomSimpleEst(m, corrected_items), 0, 0, eps}

		endindex := listindex + corrected_items
		first := true
		for listindex < len(list) && (listindex < endindex) {
			entry.filter.AddInt(list[listindex].Id)
			if first {
				entry.max = uint32(list[listindex].Score)
				first = false
			}
			listindex += 1
		}
		entry.n_max = listindex - orig
		b.Data = append(b.Data, entry)
		fmt.Println("Interval", len(b.Data), "max", entry.max, "min", list[listindex-1].Score, "#", listindex-orig, "m", m, "k", entry.filter.Hashes)
		items = (items << 2)
	}

	/*
		for k, _ := range b.Data {

			first := true
			orig := listindex

			totalblooms := len(b.Data)


			//items := 0
			//div := 3
			//perlevel := (totalblooms - 1)/(div-1)
			//if (k==0){
			//	items = int(float64(count) * (1.0 / float64((totalblooms-1)*)))
			//}




			//items := perList
			items := 0
			if k == 0 {
				items = int(float64(count) * (1.0 / float64((totalblooms-1)*2)))
			} else {
				items = int(float64(count) * (1.0 / float64((totalblooms-k)*2)))
			}

			m := EstimateM(2700000, items, RECORD_SIZE) // * (totalblooms - (k - 1))
			b.Data[k] = &BloomEntry{NewBloomSimpleEst(m, items), 0, 0}

			endindex := listindex + items
			if k == len(b.Data)-1 && endindex < lastindex {
				endindex = lastindex + 1
			}

			for listindex < len(list) && (listindex < endindex) {
				b.Data[k].filter.AddInt(list[listindex].Id)
				if first {
					b.Data[k].max = uint32(list[listindex].Score)
					first = false
				}
				listindex += 1
			}
			b.Data[k].n_max = listindex - orig
			fmt.Println("Interval", k, "max", b.Data[k].max, "min", list[listindex-1].Score, "#", listindex-orig, "m", m, "items", items)
		}*/
	if listindex < len(list) {
		b.cutoff = uint32(list[listindex].Score)
	}

}

func (s *BloomSketch) PassesInt(key int) bool {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	return s.Passes(tmp)
}

func (s *BloomSketch) Passes(key []byte) bool {
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

func (s *BloomSketch) Deb(key []byte) {
	total := uint32(0)
	for k, entry := range s.Data {
		if entry.filter.Query(key) {
			fmt.Println("k", k, "max, ", entry.max, "n_max", entry.n_max)
			total += entry.max
		}
	}
	fmt.Println("total, ", total)

}

func (s *BloomSketch) LowestMax() uint32 {
	return s.Data[len(s.Data)-1].max
}

func (s *BloomSketch) CutoffChangePop() uint32 {
	if len(s.Data) > 0 {
		return s.LowestMax() - s.Cutoff()
	}
	return 0
}

func (s *BloomSketch) Cutoff() uint32 {
	return s.cutoff
}

func (s *BloomSketch) Pop() uint32 {
	max := s.Data[len(s.Data)-1].max
	s.Data = s.Data[:len(s.Data)-1]
	old_cutoff := s.cutoff
	s.cutoff = max
	return max - old_cutoff
}

func (s *BloomSketch) GetIndexes(key []byte) []uint32 {
	if len(s.Data) > 0 {
		return s.Data[0].filter.GetIndexes(key)
	}
	return nil
}

func (s *BloomSketch) NumberHashes() int {
	max := 0
	for _, entry := range s.Data {
		if entry.filter.NumberHashes() > max {
			max = entry.filter.NumberHashes()
		}
	}
	return max
}
func (s *BloomSketch) GetHashValues(key []byte) []uint32 {
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

func (s *BloomSketch) QueryHashValues(hvs []uint32) uint32 {
	for _, entry := range s.Data {
		if entry.filter.QueryHashValues(hvs) {
			return entry.max
		}
	}
	return s.cutoff
}

func (s *BloomSketch) QueryIndexes(idx []uint32) uint32 {
	for _, entry := range s.Data {
		if entry.filter.QueryIndexes(idx) {
			return entry.max
		}
	}
	return s.cutoff
}

func (s *BloomSketch) Query(key []byte) uint32 {
	//total := uint32(0)
	for _, entry := range s.Data {
		if entry.filter.Query(key) {
			//total += entry.max
			return entry.max
		}
	}
	//return total + s.cutoff
	return s.cutoff
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

type BloomSketchCollection struct {
	sketches      []*BloomSketch
	Thresh        uint32
	stats_queried int
	stats_passed  int
}

// Len is part of sort.Interface.
func (s *BloomSketchCollection) Len() int {
	return len(s.sketches)
}

// Swap is part of sort.Interface.
func (s *BloomSketchCollection) Swap(i, j int) {
	s.sketches[i], s.sketches[j] = s.sketches[j], s.sketches[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *BloomSketchCollection) Less(i, j int) bool {
	return s.sketches[i].CutoffChangePop() < s.sketches[j].CutoffChangePop()
}

func (s *BloomSketchCollection) Sort() {
	sort.Sort(s)
}

func NewBloomSketchCollection() *BloomSketchCollection {
	return &BloomSketchCollection{make([]*BloomSketch, 0), 0, 0, 0}
}

func (bc *BloomSketchCollection) ByteSize() int {
	t := 0
	for _, sk := range bc.sketches {
		t += sk.ByteSize()
	}
	return t
}

func (bc *BloomSketchCollection) SetThresh(t uint32) {
	bc.Thresh = t

	cutoff := uint32(0)
	for _, sk := range bc.sketches {
		cutoff += sk.Cutoff()
	}

	none := false
	for !none && cutoff < t {
		none = true
		bc.Sort()
		for _, sk := range bc.sketches {
			change := sk.CutoffChangePop()
			if change > 0 && cutoff+change < t {
				cutoff += sk.Pop()
				none = false
			}
		}
	}
}

func (bc *BloomSketchCollection) Merge(toadd Sketch) {
	bs := toadd.(*BloomSketch)
	bc.sketches = append(bc.sketches, bs)
}

func (bc *BloomSketchCollection) Query(key []byte) uint32 {
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

func (s *BloomSketchCollection) PassesInt(key int) bool {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	return s.Passes(tmp)
}

func (s *BloomSketchCollection) Passes(key []byte) bool {
	s.stats_queried += 1
	if s.Thresh == 0 {
		panic("Thresh not sent")
	}
	pass := s.Query(key) >= s.Thresh
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
}
