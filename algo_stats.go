package disttopk

import "fmt"

type AlgoStats struct {
	Bytes_transferred uint64
	Serial_items      int
	Random_access     int
	Random_items      int
	//	Length            int
	Recall        float64
	Abs_err       float64
	Rel_err       float64
	Edit_distance float64
	Rounds        int
}

func (t *AlgoStats) Merge(other AlgoStats) {
	t.Bytes_transferred += other.Bytes_transferred
	t.Serial_items += other.Serial_items
	t.Random_access += other.Random_access
	t.Random_items += other.Random_items
	//t.Length += other.Length
}

func (t *AlgoStats) CalculatePerformance(exact ItemList, approx ItemList, k int) {
	t.Recall = getRecall(exact, approx, k)
	t.Abs_err = getScoreError(exact, approx, k)
	t.Rel_err = getScoreErrorRel(exact, approx, k)
	t.Edit_distance = JWDistance(exact, approx, k)

}

func getRecall(exact ItemList, approx ItemList, k int) float64 {
	em := exact[:k].AddToMap(nil)
	found := 0
	for i := 0; i < k; i++ {
		item := approx[i]
		_, ok := em[item.Id]
		if ok {
			found += 1
		}
	}
	return float64(found) / float64(k)
}

func getMatches(exact ItemList, approx ItemList, k int) (ItemList, ItemList) {
	em := exact[:k].AddToMap(nil)

	exactMatch := NewItemList()
	approxMatch := NewItemList()

	for i := 0; i < k; i++ {
		item := approx[i]
		exactScore, ok := em[item.Id]
		exactItem := Item{item.Id, exactScore}
		if ok {
			exactMatch = exactMatch.Append(exactItem)
			approxMatch = approxMatch.Append(item)
		}
	}

	if len(exactMatch) > k || len(exactMatch) != len(approxMatch) {
		panic(fmt.Sprintln("snh", len(exactMatch), len(approxMatch), k))
	}
	return exactMatch, approxMatch
}

func getScoreError(exact ItemList, approx ItemList, k int) float64 {
	exactMatch, approxMatch := getMatches(exact, approx, k)
	err := 0.0
	for k, eitem := range exactMatch {
		aitem := approxMatch[k]
		e := 0.0
		if aitem.Score > eitem.Score {
			e = aitem.Score - eitem.Score
		} else {
			e = eitem.Score - aitem.Score
		}
		err += e
	}
	return err / float64(k)
}

func getScoreErrorRel(exact ItemList, approx ItemList, k int) float64 {
	exactMatch, approxMatch := getMatches(exact, approx, k)
	err := 0.0
	for k, eitem := range exactMatch {
		aitem := approxMatch[k]
		e := 0.0
		if aitem.Score > eitem.Score {
			e = aitem.Score - eitem.Score
		} else {
			e = eitem.Score - aitem.Score
		}
		err += ( e / eitem.Score)
	}
	return err / float64(k)


}


/* this is closest to the klee paper but is a bad metric 
func getScoreError(exact ItemList, approx ItemList, k int) float64 {
	err := 0.0
	for i := 0; i < k; i++ {
		aitem := approx[i]
		eitem := exact[i]
		e := 0.0
		if aitem.Score > eitem.Score {
			e = aitem.Score - eitem.Score
		} else {
			e = eitem.Score - aitem.Score
		}
		err += e
	}
	return err / float64(k)
}

func getScoreErrorRel(exact ItemList, approx ItemList, k int) float64 {
	err := 0.0
	for i := 0; i < k; i++ {
		aitem := approx[i]
		eitem := exact[i]
		e := 0.0
		if aitem.Score > eitem.Score {
			e = aitem.Score - eitem.Score
		} else {
			e = eitem.Score - aitem.Score
		}
		err += (e / eitem.Score)
	}
	return err / float64(k)
}
*/
func itemList2item(ilist ItemList) []int {
	keys := make([]int, len(ilist))
	for i, item := range ilist {
		keys[i] = item.Id
	}
	return keys
}

func IMax(i, j int) int {
	if i > j {
		return i
	} else {
		return j
	}
}

func IMin(i, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}
func JWDistance(exact_list ItemList, approx_list ItemList, k int) float64 {
	// approximately the Jaro edit distance: 0 is no match, 1 is perfect match
	//Inspiration from https://code.google.com/p/duke/source/browse/src/main/java/no/priv/garshol/duke/JaroWinkler.java
	matches := 0.0
	transpositions := 0.0

	exact_keys := itemList2item(exact_list)[0:k]
	approx_keys := itemList2item(approx_list)

	if len(approx_keys) < k {
		panic("XXX, this case not yet implemented in JWDistance")

		//perhaps should just append with nils?
	}

	search_window_width := len(approx_keys) / 2
	last_match_in_approx := -1
	for i := 0; i < k; i++ {
		to_match := exact_keys[i]
		search_start := IMax(0, i-search_window_width)
		search_end := IMin(i+search_window_width+1, len(approx_keys))
		for j := search_start; j < search_end; j++ {
			if to_match == approx_keys[j] {
				matches++
				if last_match_in_approx != -1 && j < last_match_in_approx {
					transpositions++ // moved back before earlier
				}
				last_match_in_approx = j
				break
			}
		}
	}

	fmt.Println("Edit distance debug: ", matches, "matches", transpositions, "transpositions, k= ", k, "algo output length", len(approx_keys))

	if matches == 0 {
		return 0
	} else {
		k_f := float64(k)
		return (matches/k_f + matches/k_f + (matches-transpositions)/matches) / 3.0

	}
}
