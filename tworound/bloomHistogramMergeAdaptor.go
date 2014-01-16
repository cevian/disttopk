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
		m := g.Columns
		m_bits := uint32(math.Log2(float64(m)))
		max := entry.GetMax()
		//fmt.Println("m = ", m)

		g.Data.Eval(func(hv uint32) {
			count += 1
			//fmt.Println("Hv ", hv, count)
			test[hv] = true
			t.Add(uint(hv), uint(m_bits), uint(max), uint(b.Cutoff()))
		})
	}
	if MERGE_TOPK_AT_COORD {
		m_bits := t.GetModulusBits()
		m := (1 << m_bits)
		hash := disttopk.NewCountMinHash(1, m)
		for _, item := range il {
			hv := hash.GetIndexNoOffset(disttopk.IntKeyToByteKey(item.Id), 0)
			t.Add(uint(hv), uint(m_bits), uint(item.Score), uint(b.Cutoff()))

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

func (t *BloomHistogramMergeSketchAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList) UnionFilter {
	bs := us.(*MaxHashMapUnionSketch)
	//fmt.Println("Uf info before set thresh: ", bs.GetInfo())
	return bs.GetFilter(uint(thresh))
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

func (t *BloomHistogramMergeSketchAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int) ([]disttopk.Item, *disttopk.AlgoStats) {
	gcs := uf.(*disttopk.Gcs)
	//fmt.Println("entering get round two list")
	//list_items := list.Len()

	/*exactlist := make([]disttopk.Item, 0)
	for index, v := range list {
		if index >= cutoff_sent && gcs.Query(disttopk.IntKeyToByteKey(v.Id)) == true {
			exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
		}
	}
	return exactlist, &disttopk.AlgoStats{Serial_items: len(list), Random_access: 0, Random_items: 0}
	*/
	hvf := disttopk.NewHashValueFilter()
	m_bits := hvf.GetModulusBits(gcs.GetM())
	hvs := gcs.HashValues()
	hvf.InsertHashValueSlice(m_bits, hvs)

	//create hash table
	ht_bits := uint8(math.Ceil(math.Log2(float64(list.Len()))))
	ht := disttopk.NewHashTable(ht_bits)
	for _, v := range list {
		ht.Insert(v.Id, v.Score)
	}

	hvs_sent := disttopk.NewHashValueSlice() //store hashes tested and sent here

	ids_sent := make(map[uint]bool)
	for i := 0; i < cutoff_sent; i++ {
		ids_sent[uint(list[i].Id)] = true
	}

	exactlist := make([]disttopk.Item, 0)
	items_tested := 0
	random_access := 0

	for mod_bits, hf_hvslice := range hvf.GetFilters() {
		//println("Mod 2", mod_bits, hvslice.Len())
		for _, hf_hv := range hf_hvslice.GetSlice() {
			table_hvs := ht.GetTableHashValues(uint(hf_hv), mod_bits)
			for _, table_hv := range table_hvs {
				if !hvs_sent.Contains(uint32(table_hv)) { //if we haven't processed this hv before
					hvs_sent.Insert(uint32(table_hv))
					random_access += 1

					visitor := func(id uint, score uint) {
						id_check := ids_sent[id]
						if id_check == false { //not sent in previous round
							items_tested += 1
							if gcs.Query(disttopk.IntKeyToByteKey(int(id))) {

								exactlist = append(exactlist, disttopk.Item{int(id), float64(score)})
							}
						}
					}

					ht.VisitHashValue(table_hv, visitor)
				}
			}
		}
	}
	//fmt.Println("Round two list items tested", items_tested, "random access", random_access, "total items", len(list))
	return exactlist, &disttopk.AlgoStats{Serial_items: 0, Random_access: random_access, Random_items: items_tested}

}

type BloomHistogramMergePeerSketchAdaptor struct {
	*BloomHistogramPeerSketchAdaptor
}

func NewBloomHistogramMergePeerSketchAdaptor(topk int, numpeer int, N_est int) PeerSketchAdaptor {
	return &BloomHistogramMergePeerSketchAdaptor{&BloomHistogramPeerSketchAdaptor{topk, numpeer, N_est}}
}

func (t *BloomHistogramMergePeerSketchAdaptor) createSketch(list disttopk.ItemList) FirstRoundSketch {
	s := disttopk.NewBloomSketchGcs(t.topk, t.numpeer, t.N_est)
	if MERGE_TOPK_AT_COORD {
		s.CreateFromListWithScoreK(list[t.topk:], list[t.topk-1].Score)
	} else {
		s.CreateFromList(list)
	}
	return s
}

type BloomHistogramMergeGcsApproxUnionSketchAdaptor struct {
	*BloomHistogramMergeSketchAdaptor
	topk int
}

func NewBloomHistogramMergeGcsApproxUnionSketchAdaptor(topk int) UnionSketchAdaptor {
	bhm := NewBloomHistogramMergeSketchAdaptor()
	return &BloomHistogramMergeGcsApproxUnionSketchAdaptor{bhm.(*BloomHistogramMergeSketchAdaptor), topk}
}

func (t *BloomHistogramMergeGcsApproxUnionSketchAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList) UnionFilter {
	bs := us.(*MaxHashMapUnionSketch)
	filter, approxthresh := bs.GetFilterApprox(uint(thresh), t.topk+1) //+1 to get the max below the k'th elem
	fmt.Println("Approximating thresh at: ", approxthresh, " Original: ", thresh)
	return filter
}
