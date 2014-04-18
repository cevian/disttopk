package tworound

import "github.com/cevian/disttopk"
import "fmt"

var _ = fmt.Println

type ApproximateBloomGcsFilterAdaptor struct {
	topk        int
	numpeer     int
	N_est       int
	Beta        float64
	Gamma       float64
	FilterItems disttopk.ItemList
}

func NewApproximateBloomGcsFilterAdaptor(topk int, numpeer int, N_est int) UnionSketchAdaptor {
	return &ApproximateBloomGcsFilterAdaptor{topk, numpeer, N_est, 0.0, 1.0, disttopk.NewItemList()}
}

func (t *ApproximateBloomGcsFilterAdaptor) getUnionSketch(frs FirstRoundSketch, il disttopk.ItemList, peerId int) UnionSketch {
	return nil
}
func (t *ApproximateBloomGcsFilterAdaptor) mergeIntoUnionSketch(us UnionSketch, frs FirstRoundSketch, il disttopk.ItemList, peerId int) {
}

func (t *ApproximateBloomGcsFilterAdaptor) getFilteredItems() disttopk.ItemList {
	return t.FilterItems
}

func (t *ApproximateBloomGcsFilterAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList, listlensum int) (UnionFilter, uint) {
	maxCount := int(float64(t.topk) * t.Gamma)
	if maxCount != 0 && maxCount < len(il) {
		il.Sort()
		il = il[:maxCount]
	}
	//fmt.Println("guf:", t.Gamma, maxCount, len(il), orig_len)

	//eps := disttopk.EstimateEpsGcsAdjuster(t.N_est, maxCount, disttopk.RECORD_SIZE*8, 2, 1.0)
	eps := disttopk.EstimateEpsGcsAlt(maxCount, disttopk.RECORD_SIZE*8, t.numpeer, listlensum, 1, 1.0, listlensum/t.numpeer)

	m_est := disttopk.EstimateMGcs(maxCount, eps)

	//reason to make power of 2 here is so that it will be compatible with hashtable at the peers
	m := disttopk.MakePowerOf2(m_est)

	if m == 0 {
		panic(fmt.Sprint("Should never get 0 here", t.N_est, maxCount, eps))
	}
	//fmt.Printf("GCS info: N_est %v, n %v, eps %v m_est %v, m_log %v (rounded %v) m %v\n", N_est, n, eps, m_est, m_log, m_log_rounded, m)
	gcs := disttopk.NewGcs(m)

	for _, v := range il {
		gcs.Add(disttopk.IntKeyToByteKey(v.Id))
		t.FilterItems = append(t.FilterItems, v)
	}
	return gcs, uint(il[len(il)-1].Score)

}

func (t *ApproximateBloomGcsFilterAdaptor) copyUnionFilter(uf UnionFilter) UnionFilter {
	bs := uf.(*disttopk.Gcs)
	copy_uf := *bs
	return &copy_uf
}

func (t *ApproximateBloomGcsFilterAdaptor) serialize(uf UnionFilter) Serialized {
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

func (*ApproximateBloomGcsFilterAdaptor) deserialize(s Serialized) UnionFilter {
	bs := s
	obj := &disttopk.Gcs{}
	err := disttopk.DeserializeObject(obj, []byte(bs))
	if err != nil {
		panic(err)
	}
	return obj
}

func (t *ApproximateBloomGcsFilterAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, ht *disttopk.HashTable, cutoff_sent int, sent_item_filter map[int]bool) ([]disttopk.Item, *disttopk.AlgoStatsRound) {
	gcs := uf.(*disttopk.Gcs)
	filter := disttopk.NewGcsMergeIndexableFilter(gcs)
	return disttopk.GetListIndexedHashTable(filter, list, ht, sent_item_filter)
}
