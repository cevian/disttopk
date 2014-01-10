package tworound

import "github.com/cevian/disttopk"

type ApproximateBloomFilterAdaptor struct {
	topk    int
	numpeer int
	N_est   int
	Beta    float64
}

func NewApproximateBloomFilterAdaptor(topk int, numpeer int, N_est int) *ApproximateBloomFilterAdaptor {
	return &ApproximateBloomFilterAdaptor{topk, numpeer, N_est, 2.0}
}

func (t *ApproximateBloomFilterAdaptor) getUnionSketch(frs FirstRoundSketch) UnionSketch {
	return nil
}

func (t *ApproximateBloomFilterAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList) UnionFilter {
	eps := 0.0000001
	n := len(il)
	m := disttopk.EstimateMSimple(n, eps)
	bloom := disttopk.NewBloomSimpleEst(m, n)
	for _, v := range il {
		bloom.Add(disttopk.IntKeyToByteKey(v.Id))
	}
	return bloom

}

func (t *ApproximateBloomFilterAdaptor) copyUnionFilter(uf UnionFilter) UnionFilter {
	bs := uf.(*disttopk.Bloom)
	copy_uf := *bs
	return &copy_uf
}

func (t *ApproximateBloomFilterAdaptor) serialize(uf UnionFilter) Serialized {
	obj, ok := uf.(*disttopk.Bloom)
	if !ok {
		panic("Unexpected")
	}
	b, err := disttopk.SerializeObject(obj)
	if err != nil {
		panic(err)
	}
	return b
}

func (*ApproximateBloomFilterAdaptor) deserialize(s Serialized) UnionFilter {
	bs := s
	obj := &disttopk.Bloom{}
	err := disttopk.DeserializeObject(obj, []byte(bs))
	if err != nil {
		panic(err)
	}
	return obj
}

func (t *ApproximateBloomFilterAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int) ([]disttopk.Item, *disttopk.AlgoStats) {
	bloom := uf.(*disttopk.Bloom)
	exactlist := disttopk.NewItemList()
	for index, v := range list {
		if index >= cutoff_sent && bloom.Query(disttopk.IntKeyToByteKey(v.Id)) == true {
			exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
		}
	}

	maxCount := int(float64(t.topk) * t.Beta)
	exactlist.Sort()
	if maxCount < len(exactlist)-1 {
		exactlist = exactlist[:maxCount]
	}

	return exactlist, &disttopk.AlgoStats{Serial_items: len(list)}
}
