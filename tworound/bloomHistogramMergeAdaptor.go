package tworound

import "github.com/cevian/disttopk"

import (
	"fmt"
	"math"
)

var _ = fmt.Println

const MERGE_TOPK_AT_COORD = true

type BloomHistogramMergeSketchAdaptor struct{}

type MaxHashMapUnionSketch struct {
	*disttopk.MaxHashMap
}

func (t *MaxHashMapUnionSketch) Merge(sketch disttopk.Sketch, il disttopk.ItemList) {
	b := sketch.(*disttopk.BloomHistogram)
	//fmt.Println("Cutoff before", b.Cutoff())
	//b.Pop() //todo: change
	count := 0
	test := make(map[uint32]bool)
	for _, entry := range b.Data {
		g := entry.GetFilter().(*disttopk.Gcs)
		//fmt.Println("On Merge, merging len filter", g.Data.Len(), k, g.Columns)
		m := g.Columns
		if m == 0 {
			panic("Should never get 0 length filters")
		}
		m_bits := uint32(math.Log2(float64(m)))
		max := entry.GetMax()
		min := entry.GetMin()
		//fmt.Println("m = ", m)

		g.Data.Eval(func(hv uint32) {
			count += 1
			//fmt.Println("Hv ", hv, count)
			test[hv] = true
			t.Add(uint(hv), uint(m_bits), uint(max), uint(min), uint(b.Cutoff()))
		})
	}
	if MERGE_TOPK_AT_COORD {
		m_bits := t.GetModulusBits()
		m := (1 << m_bits)
		hash := disttopk.NewCountMinHash(1, m)
		for _, item := range il {
			hv := hash.GetIndexNoOffset(disttopk.IntKeyToByteKey(item.Id), 0)
			t.Add(uint(hv), uint(m_bits), uint(item.Score), uint(item.Score), uint(b.Cutoff()))

		}
	}
	//fmt.Println("Cutoff after", b.Cutoff(), count, len(test))
	t.AddCutoff(uint(b.Cutoff()))
}

/*type BloomHistogramSketchWraper struct {
	*disttopk.BloomHistogram
}

func (bhsw *BloomHistogramSketchWraper) Merge(t disttopk.Sketch) {
	b := t.(*disttopk.BloomHistogram)
	for k, entry := range bhsw.Data {
		g := entry.GetFilter().(*disttopk.Gcs)
		fmt.Println("Debug", k, g.Data.Len())
	}
	fmt.Println()
	bhsw.BloomHistogram.Merge(b)
	for k, entry := range bhsw.Data {
		g := entry.GetFilter().(*disttopk.Gcs)
		fmt.Println("Debug After", k, g.Data.Len())
	}
}*/

func NewBloomHistogramMergeSketchAdaptor() UnionSketchAdaptor {
	return &BloomHistogramMergeSketchAdaptor{}
}

func (t *BloomHistogramMergeSketchAdaptor) getUnionSketch(frs FirstRoundSketch, il disttopk.ItemList) UnionSketch {
	bs := frs.(*disttopk.BloomHistogram)
	mhm := &MaxHashMapUnionSketch{disttopk.NewMaxHashMap()}
	mhm.Merge(bs, il)
	return mhm
}

func (t *BloomHistogramMergeSketchAdaptor) mergeIntoUnionSketch(us UnionSketch, frs FirstRoundSketch, il disttopk.ItemList) {
	mhm := us.(*MaxHashMapUnionSketch)
	bs := frs.(*disttopk.BloomHistogram)
	mhm.Merge(bs, il)
}

func (t *BloomHistogramMergeSketchAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList, listlensum int) (UnionFilter, uint) {
	bs := us.(*MaxHashMapUnionSketch)
	//fmt.Println("Uf info before set thresh: ", bs.GetInfo())
	flt, v := bs.GetFilter(uint(thresh)), uint(thresh)
	if flt != nil {
		return flt, v
	}
	return nil, v
}

func (t *BloomHistogramMergeSketchAdaptor) copyUnionFilter(uf UnionFilter) UnionFilter {
	bs := uf.(*disttopk.Gcs)

	copy_uf := *bs
	return &copy_uf
}

func (t *BloomHistogramMergeSketchAdaptor) serialize(uf UnionFilter) Serialized {
	obj, ok := uf.(*disttopk.Gcs)

	if !ok {
		panic("Unexpected")
	}
	b, err := disttopk.SerializeObject(obj)
	if err != nil {
		panic(err)
	}
	return b
}

func (*BloomHistogramMergeSketchAdaptor) deserialize(s Serialized) UnionFilter {
	bs := s
	obj := &disttopk.Gcs{}
	err := disttopk.DeserializeObject(obj, []byte(bs))
	if err != nil {
		panic(err)
	}
	return obj
}

func (t *BloomHistogramMergeSketchAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int, sent_item_filter map[int]bool) ([]disttopk.Item, *disttopk.AlgoStats) {
	if uf == nil {
		remaining_list := list[cutoff_sent:]
		exactlist := make([]disttopk.Item, 0, len(remaining_list))
		for _, item := range remaining_list {
			if !sent_item_filter[item.Id] {
				exactlist = append(exactlist, item)
			}
		}

		//copy(exactlist, remaining_list)
		return exactlist, &disttopk.AlgoStats{Serial_items: len(remaining_list)}
	}

	gcs := uf.(*disttopk.Gcs)
	filter := disttopk.NewGcsMergeIndexableFilter(gcs)
	return disttopk.GetListIndexedHashTable(filter, list, sent_item_filter)
}

type BloomHistogramMergePeerSketchAdaptor struct {
	*BloomHistogramPeerSketchAdaptor
}

func NewBloomHistogramMergePeerSketchAdaptor(topk int, numpeer int, N_est int) PeerSketchAdaptor {
	return &BloomHistogramMergePeerSketchAdaptor{&BloomHistogramPeerSketchAdaptor{topk, numpeer, N_est}}
}

func (t *BloomHistogramMergePeerSketchAdaptor) createSketch(list disttopk.ItemList, localtop disttopk.ItemList) (FirstRoundSketch, int) {
	s := disttopk.NewBloomSketchGcs(t.topk, t.numpeer, t.N_est)
	if MERGE_TOPK_AT_COORD {
		return s, s.CreateFromListWithScoreK(list[len(localtop):], list[t.topk-1].Score)
	} else {
		return s, s.CreateFromList(list) - len(localtop)
	}
}

type BloomHistogramMergeGcsApproxUnionSketchAdaptor struct {
	*BloomHistogramMergeSketchAdaptor
	topk                int
	gamma               float64
	numUnionFilterCalls int
}

func NewBloomHistogramMergeGcsApproxUnionSketchAdaptor(topk int) UnionSketchAdaptor {
	bhm := NewBloomHistogramMergeSketchAdaptor()
	return &BloomHistogramMergeGcsApproxUnionSketchAdaptor{bhm.(*BloomHistogramMergeSketchAdaptor), topk, 0.5, 0}
}

func (t *BloomHistogramMergeGcsApproxUnionSketchAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList, listlensum int) (UnionFilter, uint) {
	if t.numUnionFilterCalls == 0 {
		bs := us.(*MaxHashMapUnionSketch)

		approxthresh := bs.GetThreshApprox(t.topk, t.gamma)
		cutoff := bs.Cutoff()
		fmt.Println("Approximating thresh at: ", approxthresh, " Original: ", thresh, "Gamma:", t.gamma)
		if cutoff >= approxthresh {
			old := approxthresh
			approxthresh = cutoff + 1
			fmt.Println("The Approximation threshold is too high for cutoff, resetting to", approxthresh, " Was ", old, ", cutoff ", cutoff)
		}
		filter := bs.GetFilter(approxthresh)
		if filter == nil {
			panic("Should never get nil filter here")
		}
		t.numUnionFilterCalls = 1
		return filter, approxthresh
	} else {
		return t.BloomHistogramMergeSketchAdaptor.getUnionFilter(us, thresh, il, listlensum)
	}
	//filter, approxthresh := bs.GetFilterApprox(uint(thresh), t.topk+1) //+1 to get the max below the k'th elem
	//fmt.Println("Approximating thresh at: ", approxthresh, " Original: ", thresh)
}
