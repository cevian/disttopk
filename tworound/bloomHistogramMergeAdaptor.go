package tworound

import "github.com/cevian/disttopk"

import (
	"fmt"
	"math"
)

var _ = fmt.Println

type BloomHistogramMergeSketchAdaptor struct{}

type MaxHashMapUnionSketch struct {
	*disttopk.MaxHashMap
}

func (t *MaxHashMapUnionSketch) Merge(sketch disttopk.Sketch) {
	b := sketch.(*disttopk.BloomHistogram)
	//fmt.Println("Cutoff before", b.Cutoff())
	b.Pop() //todo: change
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

func (t *BloomHistogramMergeSketchAdaptor) getUnionSketch(frs FirstRoundSketch) UnionSketch {
	bs := frs.(*disttopk.BloomHistogram)
	mhm := &MaxHashMapUnionSketch{disttopk.NewMaxHashMap()}
	mhm.Merge(bs)
	return mhm
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
	bhc := uf.(*disttopk.Gcs)
	//fmt.Println("entering get round two list")
	//list_items := list.Len()

	exactlist := make([]disttopk.Item, 0)
	for index, v := range list {
		if index >= cutoff_sent && bhc.Query(disttopk.IntKeyToByteKey(v.Id)) == true {
			exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
		}
	}
	return exactlist, &disttopk.AlgoStats{Serial_items: len(list), Random_access: 0, Random_items: 0 /*, Length: len(list)*/}
	/*
		//bsc := uf.(*disttopk.BloomHistogramCollection)
		hvf := disttopk.NewHashValueFilter()
		bhc.AddToHashValueFilter(hvf)

		if false {

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
			return exactlist, &disttopk.AlgoStats{Serial_items: 0, Random_access: random_access, Random_items: items_tested }
		} else {
			//fmt.Println("Here", bhc.GetInfo(), cutoff_sent)
			for k, entry := range bhc.Data {
				g := entry.GetFilter().(*disttopk.Gcs)
				fmt.Println("Debug checking ", k, g.Data.Len(), bhc.Thresh, entry.GetMax())
			}
			exactlist := make([]disttopk.Item, 0)
			for index, v := range list {
				if index >= cutoff_sent && bhc.PassesInt(v.Id) == true {
					exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
				}
			}
			//fmt.Println("Round two list items used serial test, total items (all sequential tested)", len(list))
			return exactlist, &disttopk.AlgoStats{Serial_items: len(list), Random_access: 0, Random_items: 0 }
		}*/
}
