package disttopk

import (
	"fmt"
	"math"
	"math/rand"
)

var _ = fmt.Println

type SimpleZipfSource struct {
	MaxItems uint32
	zipParam float64
	zipNorm  float64
	scale    float64
}

func NewSimpleZipfSource(maxItems uint32, param float64, nlists int) SimpleZipfSource {
	var norm float64
	norm = 0
	i := uint32(1)
	for i < (maxItems + 1) {
		norm += math.Pow(float64(i), -param)
		i++
	}

	minItem := math.Pow(float64(maxItems), -param) / norm
	//we want minitem score to be 1 so:
	// minZipfValue*scale = 1
	// scale = 1/MinZipfValue
	scaleBy := 1.0 / minItem

	maxItem := math.Pow(float64(1), -param) / norm
	if maxItem*float64(nlists)*scaleBy > math.MaxUint32 {
		fmt.Println("Have to rescale to fit in uint32")
		// has to be maxItem*nlists*scaleby = (math.MaxUint32-1)
		//scaleby = (math.MaxUint32-1)/(maxItem*nlists)
		scaleBy = (math.MaxUint32 - 1) / (maxItem * float64(nlists))
	}

	return SimpleZipfSource{maxItems, param, norm, scaleBy}

}

func (src *SimpleZipfSource) GenerateItem(rank int) Item {
	id := rand.Int()

	zipfValue := math.Pow(float64(rank), -src.zipParam) / src.zipNorm
	//score := (zipfValue * src.scale) + float64(id%10)
	score := zipfValue * src.scale

	//fmt.Println("gen", zipfValue, score, (zipfValue * src.scale), src.scale, id, id%100)

	act_score := float64(score)
	if act_score < 1.0 {
		act_score = 1.0
	}
	return Item{id, float64(uint(act_score))}
}

func (src *SimpleZipfSource) GetList() ItemList {
	l := make([]Item, 0, src.MaxItems)
	i := uint32(1)
	sum := 0.0
	for i < (src.MaxItems + 1) {
		l = append(l, src.GenerateItem(int(i)))
		sum += l[len(l)-1].Score
		i++
	}
	//fmt.Println("sum = ", sum)
	return ItemList(l)
}

func GetDisjointSimpleList(nlists int, nitemsPerList uint32, param float64) []ItemList {
	src := NewSimpleZipfSource(nitemsPerList, param, nlists)
	lists := make([]ItemList, nlists)
	for k, _ := range lists {
		l := src.GetList()
		l = MakeSureItemsUnique(l)
		lists[k] = l
	}
	return lists
}

func GetFullOverlapSimpleList(nlists int, nitemsPerList uint32, param float64) []ItemList {
	src := NewSimpleZipfSource(nitemsPerList, param, nlists)
	reference_list := src.GetList()
	reference_list = MakeSureItemsUnique(reference_list)

	lists := make([]ItemList, nlists)
	for k, _ := range lists {
		copy_list := make([]Item, len(reference_list))
		copy(copy_list, reference_list)
		lists[k] = copy_list
	}
	return lists
}

func GetFullOverlapOrderPermutedSimpleList(nlists int, nitemsPerList uint32, param float64, reorder int) []ItemList {
	return GetFullOverlapOrderPermutedSimpleListSeed(nlists, nitemsPerList, param, reorder, 99)
}

func GetFullOverlapOrderPermutedSimpleListSeed(nlists int, nitemsPerList uint32, param float64, reorder int, seed int64) []ItemList {
	return GetFullOverlapOrderPermutedSimpleListSeedOverlap(nlists, nitemsPerList, param, reorder, seed, 1.0)
}

func GetFullOverlapOrderPermutedSimpleListSeedOverlap(nlists int, nitemsPerList uint32, param float64, reorder int, seed int64, overlap float64) []ItemList {
	rand.Seed(seed)
	lists := GetFullOverlapSimpleList(nlists, nitemsPerList, param)

	//reference_list := ItemList(make([]Item, len(lists[0])))
	//copy(reference_list, lists[0])

	m := make(map[int]float64)
	for k, list := range lists {
		/*if k == 0 { this is wrong makes the overlap with one list greater than with the rest
			continue
		}*/
		newItems := int(float64(len(list)) * (1.0 - overlap))
		list.AddToMap(m)
		replaced := make(map[int]bool, len(list))
		for i := 0; i < newItems; {
			pos := rand.Intn(len(list))
			if !replaced[pos] {
				i++
				replaced[pos] = true
				//new_id := rand.Int()
				ok := true
				new_id := 0
				for ok {
					new_id = rand.Int()
					_, ok = m[new_id]
				}
				m[new_id] = list[pos].Score
				list[pos].Id = new_id
			}
		}
		lists[k] = list
	}

	if reorder > 0 {
		for k, list := range lists {
			for pos, _ := range list {
				to_reorder := reorder
				if pos+to_reorder > len(list)-1 {
					to_reorder = (len(list) - 1) - pos
				}
				if to_reorder == 0 {
					continue
				}
				pos_to_reorder := rand.Intn(to_reorder)
				new_pos := pos + pos_to_reorder
				list[pos].Id, list[new_pos].Id = list[new_pos].Id, list[pos].Id
				//fmt.Println("reordering after", list[pos], list[new_pos])
			}
			lists[k] = list
		}
	}

	for k, list := range lists {
		list = MakeSureItemsUnique(list)
		list.Sort()
		lists[k] = list
		//fmt.Println(list[:10])
	}

	/*
		true_list := GetTrueList(lists)
		true_list.Sort()
		m = reference_list.AddToMap(nil)
		count := 0
		for _, item := range true_list[0:10] {
				_, ok := m[item.Id]
				if ok {
					count++
					//fmt.Println("Overlapping is", item.Id)
				}
		}
		fmt.Println("Overlap of top-k with reference,", float64(count)/10.0)
	*/

	/*first := lists[1]
	m := first.AddToMap(nil)
	for _, list := range lists {
		count := 0
		for _, item := range list {
			_, ok := m[item.Id]
			if ok {
				count++
			}
		}
		fmt.Println("Overlap,", float64(count)/float64(len(list)))
	} */

	return lists
}

func MakeSureItemsUnique(list ItemList) ItemList {
	m := list.AddToMap(nil)
	l := MakeItemList(m)
	l.Sort()
	return l
}

/*
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
}*/
