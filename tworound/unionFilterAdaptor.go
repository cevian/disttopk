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
	//fmt.Println("entering get round two list")
	list_items := list.Len()

	hvf := disttopk.NewHashValueFilter()
	bhc.AddToHashValueFilter(hvf)

	if hvf.NumHashValues() < list_items {

		ht_bits := uint8(math.Ceil(math.Log2(float64(list_items))))
		//ht_bits = 26 //CHANGE ME
		ht := disttopk.NewHashTable(ht_bits)

		for _, v := range list {
			ht.Insert(v.Id, v.Score)
		}
		hvs_sent := disttopk.NewHashValueSlice() //hack wont store hash values
		for i := 0; i < cutoff_sent; i++ {
			hvs_sent.Insert(uint32(list[i].Id))
		}

		//fmt.Println("entering for loops get round two list")

		exactlist := make([]disttopk.Item, 0)
		items_tested := 0
		random_access := 0
		for mod_bits, hvslice := range hvf.GetFilters() {
			//println("Mod 2", mod_bits, hvslice.Len())
			for _, hv := range hvslice.GetSlice() {
				items_map, ra := ht.GetByHashValue(uint(hv), mod_bits)
				random_access += ra
				items_tested += len(items_map)
				for id, score := range items_map {
					if !hvs_sent.Contains(uint32(id)) && bhc.PassesInt(id) == true {
						exactlist = append(exactlist, disttopk.Item{id, score})
						hvs_sent.Insert(uint32(id))
					}
				}
			}
		}

		//fmt.Println("Round two list items tested", items_tested, "random access", random_access, "total items", len(list))
		return exactlist, &disttopk.AlgoStats{Serial_items: 0, Random_access: random_access, Random_items: items_tested /*, Length: len(list)*/}
	} else {
		exactlist := make([]disttopk.Item, 0)
		for index, v := range list {
			if index >= cutoff_sent && bhc.PassesInt(v.Id) == true {
				exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
			}
		}
		//fmt.Println("Round two list items used serial test, total items (all sequential tested)", len(list))
		return exactlist, &disttopk.AlgoStats{Serial_items: len(list), Random_access: 0, Random_items: 0 /*, Length: len(list)*/}
	}
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
