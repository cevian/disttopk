package tworound

import "github.com/cevian/disttopk"

import (
	"fmt"
	//"math"
)

type UnionSketchAdaptor interface {
	getUnionSketch(FirstRoundSketch, disttopk.ItemList) UnionSketch
	mergeIntoUnionSketch(UnionSketch, FirstRoundSketch, disttopk.ItemList)
	getUnionFilter(us UnionSketch, threshhold uint32, il disttopk.ItemList) (uf UnionFilter, threshold uint)
	copyUnionFilter(UnionFilter) UnionFilter //disttopk.NewCountMinFilterFromSketch(ucm, uint32(localthresh)
	serialize(UnionFilter) Serialized        //disttopk.NewCountMinFilterFromSketch(ucm, uint32(localthresh)
	deserialize(Serialized) UnionFilter
	getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int, sent_item_filter map[int]bool) ([]disttopk.Item, *disttopk.AlgoStats)
}

type UnionSketchFilterItemsReporter interface {
	getFilteredItems() disttopk.ItemList
}

type BloomHistogramUnionSketchAdaptor struct{}

func NewBloomHistogramUnionSketchAdaptor() UnionSketchAdaptor {
	return &BloomHistogramUnionSketchAdaptor{}
}

func (t *BloomHistogramUnionSketchAdaptor) getUnionSketch(frs FirstRoundSketch, il disttopk.ItemList) UnionSketch {
	bs := frs.(*disttopk.BloomHistogram)
	bsc := disttopk.NewBloomSketchCollection()
	bsc.Merge(bs)
	return bsc
}

func (t *BloomHistogramUnionSketchAdaptor) mergeIntoUnionSketch(us UnionSketch, frs FirstRoundSketch, il disttopk.ItemList) {
	bsc := us.(*disttopk.BloomHistogramCollection)
	bs := frs.(*disttopk.BloomHistogram)
	bsc.Merge(bs)
}

func (t *BloomHistogramUnionSketchAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList) (UnionFilter, uint) {
	bs := us.(*disttopk.BloomHistogramCollection)
	fmt.Println("Uf info before set thresh: ", bs.GetInfo())
	bs.SetThresh(thresh)
	return bs, uint(thresh)
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

func (t *BloomHistogramUnionSketchAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int, sent_item_filter map[int]bool) ([]disttopk.Item, *disttopk.AlgoStats) {
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

func (t *BloomHistogramGcsUnionSketchAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int, sent_item_filter map[int]bool) ([]disttopk.Item, *disttopk.AlgoStats) {
	bhc := uf.(*disttopk.BloomHistogramCollection)

	filter := NewBloomHistogramCollectionIndexableFilter(bhc)
	return GetListIndexedHashTable(filter, list, sent_item_filter)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
type CountMinUnionSketchAdaptor struct{}

func NewCountMinUnionSketchAdaptor() UnionSketchAdaptor {
	return &CountMinUnionSketchAdaptor{}
}

func (t *CountMinUnionSketchAdaptor) getUnionSketch(frs FirstRoundSketch, il disttopk.ItemList) UnionSketch {
	cm := frs.(*disttopk.CountMinSketch)
	ucm := disttopk.NewCountMinSketch(cm.Hashes, cm.Columns)
	ucm.Merge(cm)
	return ucm
}

func (t *CountMinUnionSketchAdaptor) mergeIntoUnionSketch(us UnionSketch, frs FirstRoundSketch, il disttopk.ItemList) {
	ucm := us.(*disttopk.CountMinSketch)
	cm := frs.(*disttopk.CountMinSketch)
	ucm.Merge(cm)
}

func (t *CountMinUnionSketchAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList) (UnionFilter, uint) {
	ucm := us.(*disttopk.CountMinSketch)
	return disttopk.NewCountMinFilterFromSketch(ucm, uint32(thresh)), uint(thresh)

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

func (t *CountMinUnionSketchAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int, sent_item_filter map[int]bool) ([]disttopk.Item, *disttopk.AlgoStats) {
	cmf := uf.(*disttopk.CountMinFilter)
	exactlist := make([]disttopk.Item, 0)
	for index, v := range list {
		if index >= cutoff_sent && cmf.PassesInt(v.Id) == true {
			exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
		}
	}
	return exactlist, &disttopk.AlgoStats{Serial_items: len(list) /*, Length: len(list)*/}
}
