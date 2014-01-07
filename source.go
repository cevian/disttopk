package disttopk

import (
	//	"fmt"
	"math"
	"math/rand"
	"sort"
)

type ZipfSource struct {
	MaxItems uint32
	zipParam float64
	zipNorm  float64
}

func NewZipfSource(max uint32, param float64) ZipfSource {
	var norm float64
	norm = 0
	i := uint32(1)
	for i < max {
		norm += math.Pow(float64(i), -param)
		i++
	}
	return ZipfSource{max, param, norm}

}

type Item struct {
	Id    int
	Score float64
}

func NewItemList() ItemList {
	return make([]Item, 0)
}

type ItemList []Item

func (il ItemList) Swap(i, j int) { il[i], il[j] = il[j], il[i] }
func (il ItemList) Len() int      { return len(il) }
func (il ItemList) Less(i, j int) bool {
	if il[i].Score == il[j].Score {
		return il[i].Id < il[j].Id
	}
	return il[i].Score < il[j].Score
}

func (il ItemList) Append(i Item) ItemList { return append(il, i) }

func (il ItemList) Sort() { sort.Sort(sort.Reverse(il)) }

func (il ItemList) AddToMap(m map[int]float64) map[int]float64 {
	if m == nil {
		m = make(map[int]float64)
	}
	for _, item := range il {
		score := m[item.Id]
		m[item.Id] = score + item.Score
	}
	return m
}

func (il ItemList) AddToReverseIndexMap(m map[int]int) map[int]int {
	if m == nil {
		m = make(map[int]int)
	}
	for index, item := range il {
		m[item.Id] = index
	}
	return m
}

func (il ItemList) AddToCountMap(m map[int]int) map[int]int {
	if m == nil {
		m = make(map[int]int)
	}
	for _, item := range il {
		score := m[item.Id]
		m[item.Id] = score + 1
	}
	return m
}

func MakeItemList(m map[int]float64) ItemList {
	il := make([]Item, len(m))
	i := 0
	for k, v := range m {
		il[i] = Item{k, v}
		i++
	}
	return ItemList(il)

}

func (src *ZipfSource) GenerateItem(rank int, offset int) Item {
	id := rand.Int()
	//	id = rank
	//score := (float64(rank) + (float64(offset) * 0.1)) * 100
	score := float64(int((math.Pow(float64(rank), -(src.zipParam))/src.zipNorm)*10000000 + float64(offset)))
	//score := math.Pow(float64(rank), -(src.zipParam)) / src.zipNorm
	return Item{id, score}
}

func (src *ZipfSource) GetList(offset int) ItemList {
	l := make([]Item, 0, src.MaxItems)
	i := uint32(1)
	sum := 0.0
	for i < src.MaxItems {
		l = append(l, src.GenerateItem(int(i), offset))
		sum += l[len(l)-1].Score
		i++
	}
	//fmt.Println("sum = ", sum)
	return ItemList(l)
}

func GetListSet(nlists int, nitems uint32, param float64, overlap float64) []ItemList {
	src := NewZipfSource(nitems, param)
	lists := make([]ItemList, nlists)
	for k, _ := range lists {
		lists[k] = src.GetList(k)
	}

	nOver := int(overlap * float64(nitems))
	//nOver := 10
	for i := 0; i <= nOver; i++ {
		first := lists[0]
		index := rand.Int() % len(first)
		id := first[index].Id
		for _, l := range lists[1:] {
			index := rand.Int() % len(l)
			//println("over", id, index, k)
			l[index].Id = id
		}
	}

	for k, l := range lists {
		m := ItemList(l).AddToMap(nil)
		//v, ok := m[2553153660041385501]
		//println(v, ok)
		l := MakeItemList(m)
		l.Sort()
		lists[k] = l
	}

	return lists
}
