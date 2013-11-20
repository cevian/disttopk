package disttopk

import (
	"encoding/binary"
	"fmt"
)

type BloomEntry struct {
	filter *Bloom
	max    uint32
	n_max  int
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
	//scorek := list[topk-1].Score
	//minscore := uint32(scorek) / n

	perList := topk * 4
	perBloom := perList * n

	m := EstimateMSimple(perBloom, 0.01)

	listindex := 0
	for k, _ := range b.Data {

		b.Data[k] = &BloomEntry{NewBloomSimpleEst(m, perBloom), 0, 0}

		first := true
		orig := listindex
		for listindex < len(list) && (listindex <= (perList)*(k+1)) {
			b.Data[k].filter.AddInt(list[listindex].Id)
			if first {
				b.Data[k].max = uint32(list[listindex].Score)
				first = false
			}
			listindex += 1
		}
		b.Data[k].n_max = listindex - orig
		fmt.Println("Interval", k, "max", b.Data[k].max, "min", list[listindex-1].Score, "#", listindex-orig)
	}
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
}
