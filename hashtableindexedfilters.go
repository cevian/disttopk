package disttopk

import (
	"math"
)

type IndexableFilter interface {
	HashValueFilter() *HashValueFilter
	Query([]byte) bool
}

func GetListIndexedHashTable(filter IndexableFilter, list ItemList, sent_item_filter map[int]bool) ([]Item, *AlgoStats) {
	hvf := filter.HashValueFilter()

	//create hash table
	ht_bits := uint8(math.Ceil(math.Log2(float64(list.Len()))))
	ht := NewHashTable(ht_bits)
	for _, v := range list {
		ht.Insert(v.Id, v.Score)
	}

	hvs_sent := NewHashValueSlice() //store hashes tested and sent here

	exactlist := make([]Item, 0)
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
						id_check := sent_item_filter[int(id)]
						if id_check == false { //not sent in previous round
							items_tested += 1
							if filter.Query(IntKeyToByteKey(int(id))) {

								exactlist = append(exactlist, Item{int(id), float64(score)})
							}
						}
					}

					ht.VisitHashValue(table_hv, visitor)
				}
			}
		}
	}
	//fmt.Println("Round two list items tested", items_tested, "random access", random_access, "total items", len(list))
	return exactlist, &AlgoStats{Serial_items: 0, Random_access: random_access, Random_items: items_tested}

}

type GcsMergeIndexableFilter struct {
	gcs *Gcs
}

func NewGcsMergeIndexableFilter(gcs *Gcs) *GcsMergeIndexableFilter {
	return &GcsMergeIndexableFilter{gcs}
}

func (t *GcsMergeIndexableFilter) HashValueFilter() *HashValueFilter {
	hvf := NewHashValueFilter()
	m_bits := hvf.GetModulusBits(t.gcs.GetM())
	hvs := t.gcs.HashValues()
	hvf.InsertHashValueSlice(m_bits, hvs)
	return hvf
}

func (t *GcsMergeIndexableFilter) Query(in []byte) bool {
	return t.gcs.Query(in)
}

type BloomHistogramCollectionIndexableFilter struct {
	bhc *BloomHistogramCollection
}

func NewBloomHistogramCollectionIndexableFilter(bhc *BloomHistogramCollection) *BloomHistogramCollectionIndexableFilter {
	return &BloomHistogramCollectionIndexableFilter{bhc}
}

func (t *BloomHistogramCollectionIndexableFilter) HashValueFilter() *HashValueFilter {
	hvf := NewHashValueFilter()
	t.bhc.AddToHashValueFilter(hvf)
	return hvf
}

func (t *BloomHistogramCollectionIndexableFilter) Query(in []byte) bool {
	return t.bhc.Passes(in)
}

type BloomIndexableFilter struct {
	bloom *Bloom
}

func NewBloomIndexableFilter(bloom *Bloom) *BloomIndexableFilter {
	return &BloomIndexableFilter{bloom}
}

func (t *BloomIndexableFilter) HashValueFilter() *HashValueFilter {
	hvf := NewHashValueFilter()
	m_bits := hvf.GetModulusBits(uint(t.bloom.Len()))
	t.bloom.VisitSetHashValues(func(hv int) { hvf.Insert(m_bits, uint32(hv)) })
	return hvf
}

func (t *BloomIndexableFilter) Query(in []byte) bool {
	return t.bloom.Query(in)
}
