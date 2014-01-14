package tworound

import "github.com/cevian/disttopk"

import (
	"fmt"
	"math"
)

type UnionSketchAdaptor interface {
	getUnionSketch(FirstRoundSketch) UnionSketch
	getUnionFilter(us UnionSketch, threshhold uint32, il disttopk.ItemList) UnionFilter //disttopk.NewCountMinFilterFromSketch(ucm, uint32(localthresh)
	copyUnionFilter(UnionFilter) UnionFilter                                            //disttopk.NewCountMinFilterFromSketch(ucm, uint32(localthresh)
	serialize(UnionFilter) Serialized                                                   //disttopk.NewCountMinFilterFromSketch(ucm, uint32(localthresh)
	deserialize(Serialized) UnionFilter
	getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int) ([]disttopk.Item, *disttopk.AlgoStats)
}

type BloomHistogramUnionSketchAdaptor struct{}

func NewBloomHistogramUnionSketchAdaptor() UnionSketchAdaptor {
	return &BloomHistogramUnionSketchAdaptor{}
}

func (t *BloomHistogramUnionSketchAdaptor) getUnionSketch(frs FirstRoundSketch) UnionSketch {
	bs := frs.(*disttopk.BloomHistogram)
	bsc := disttopk.NewBloomSketchCollection()
	bsc.Merge(bs)
	return bsc
}

func (t *BloomHistogramUnionSketchAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList) UnionFilter {
	bs := us.(*disttopk.BloomHistogramCollection)
	fmt.Println("Uf info before set thresh: ", bs.GetInfo())
	bs.SetThresh(thresh)
	return bs
}

func (t *BloomHistogramUnionSketchAdaptor) copyUnionFilter(uf UnionFilter) UnionFilter {
	bs := uf.(*disttopk.BloomHistogramCollection)

	copy_uf := *bs
	return &copy_uf
}

func (t *BloomHistogramUnionSketchAdaptor) serialize(uf UnionFilter) Serialized {
	obj, ok := uf.(*disttopk.BloomHistogramCollection)
	if !ok {
		panic("Unexpected")
	}
	b, err := disttopk.SerializeObject(obj)
	if err != nil {
		panic(err)
	}
	return b
}

func (*BloomHistogramUnionSketchAdaptor) deserialize(s Serialized) UnionFilter {
	bs := s
	obj := &disttopk.BloomHistogramCollection{}
	err := disttopk.DeserializeObject(obj, []byte(bs))
	if err != nil {
		panic(err)
	}
	return obj
}

func (t *BloomHistogramUnionSketchAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int) ([]disttopk.Item, *disttopk.AlgoStats) {
	bhc := uf.(*disttopk.BloomHistogramCollection)
	exactlist := make([]disttopk.Item, 0)
	for index, v := range list {
		if index >= cutoff_sent && bhc.PassesInt(v.Id) == true {
			exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
		}
	}
	return exactlist, &disttopk.AlgoStats{Serial_items: len(list) /*, Length: len(list)*/}
}

type BloomHistogramGcsUnionSketchAdaptor struct {
	*BloomHistogramUnionSketchAdaptor
}

func NewBloomHistogramGcsUnionSketchAdaptor() UnionSketchAdaptor {
	return &BloomHistogramGcsUnionSketchAdaptor{&BloomHistogramUnionSketchAdaptor{}}
}

func (t *BloomHistogramGcsUnionSketchAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int) ([]disttopk.Item, *disttopk.AlgoStats) {
	bhc := uf.(*disttopk.BloomHistogramCollection)

	hvf := disttopk.NewHashValueFilter()
	bhc.AddToHashValueFilter(hvf)

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
							if bhc.PassesInt(int(id)) {

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

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
type CountMinUnionSketchAdaptor struct{}

func NewCountMinUnionSketchAdaptor() UnionSketchAdaptor {
	return &CountMinUnionSketchAdaptor{}
}

func (t *CountMinUnionSketchAdaptor) getUnionSketch(frs FirstRoundSketch) UnionSketch {
	cm := frs.(*disttopk.CountMinSketch)
	ucm := disttopk.NewCountMinSketch(cm.Hashes, cm.Columns)
	ucm.Merge(cm)
	return ucm
}

func (t *CountMinUnionSketchAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList) UnionFilter {
	ucm := us.(*disttopk.CountMinSketch)
	return disttopk.NewCountMinFilterFromSketch(ucm, uint32(thresh))

}

func (t *CountMinUnionSketchAdaptor) copyUnionFilter(uf UnionFilter) UnionFilter {
	bs := uf.(*disttopk.CountMinFilter)
	copy_uf := *bs
	return &copy_uf
}

func (t *CountMinUnionSketchAdaptor) serialize(uf UnionFilter) Serialized {
	obj, ok := uf.(*disttopk.CountMinFilter)
	if !ok {
		panic("Unexpected")
	}
	b, err := disttopk.SerializeObject(obj)
	if err != nil {
		panic(err)
	}
	return b
}

func (*CountMinUnionSketchAdaptor) deserialize(s Serialized) UnionFilter {
	bs := s
	obj := &disttopk.CountMinFilter{}
	err := disttopk.DeserializeObject(obj, []byte(bs))
	if err != nil {
		panic(err)
	}
	return obj
}

func (t *CountMinUnionSketchAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int) ([]disttopk.Item, *disttopk.AlgoStats) {
	cmf := uf.(*disttopk.CountMinFilter)
	exactlist := make([]disttopk.Item, 0)
	for index, v := range list {
		if index >= cutoff_sent && cmf.PassesInt(v.Id) == true {
			exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
		}
	}
	return exactlist, &disttopk.AlgoStats{Serial_items: len(list) /*, Length: len(list)*/}
}
