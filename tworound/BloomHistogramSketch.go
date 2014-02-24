package tworound

import "github.com/cevian/disttopk"
import (
	"fmt"
	"io"
)

const MIN_ITEMS_IN_BUCKET = false

type BloomHistogramSketch struct {
	*disttopk.BloomHistogram
	topk     int
	numpeers int
	N_est    int
}

func NewBloomHistogramSketch(bh *disttopk.BloomHistogram, topk int, numpeers int, N_est int) *BloomHistogramSketch {
	return &BloomHistogramSketch{bh, topk, numpeers, N_est}
}

func NewBloomHistogramSketchGcs(topk int, numpeers int, N_est int) *BloomHistogramSketch {
	bh := disttopk.NewBloomHistogram(disttopk.GcsFilterAdaptor{})
	return NewBloomHistogramSketch(bh, topk, numpeers, N_est)
}

func NewBloomHistogramSketchPlain(topk int, numpeers int, N_est int) *BloomHistogramSketch {
	bh := disttopk.NewBloomHistogram(disttopk.PlainFilterAdaptor{})
	return NewBloomHistogramSketch(bh, topk, numpeers, N_est)
}

func (b *BloomHistogramSketch) CreateFromList(list disttopk.ItemList) (serialAccess int) {
	scorek := list[b.topk-1].Score
	return b.CreateFromListWithScoreK(list, scorek)
}

/*
func itemsInEntry(list disttopk.ItemList, entry_start_index int, range_in_entry int, bucket_items int) (numItems int) {
	score_after_entry := int(list[entry_start_index].Score) - range_in_entry
	index_after_entry := len(list)
	for i, item := range list[entry_start_index:] {
		if int(item.Score) <= score_after_entry {
			index_after_entry = i + entry_start_index
			break
		}
	}

	items_in_entry := index_after_entry - entry_start_index

	if MIN_ITEMS_IN_BUCKET {
		if items_in_entry < bucket_items {
			items_in_entry = bucket_items
		}
	}

	return items_in_entry
}*/

func GetMinScoreEqExact(scorek disttopk.BloomHistogramScore, numpeer int) (mineqscore disttopk.BloomHistogramScore) {
	cutoffScore := disttopk.BloomHistogramScore(int(scorek) / numpeer) //floor to assue minscore*numpeer <= scorek
	if int(cutoffScore)*numpeer == int(scorek) {
		return cutoffScore //the actual cutoffscore score needs to be a smidg smaller
	}
	return cutoffScore + 1 //the mineq has to be cutoff+1
}

func GetIndexAfter(list disttopk.ItemList, start_index int, mineqscore disttopk.BloomHistogramScore) int {
	if disttopk.BloomHistogramScore(list[start_index].Score) < mineqscore {
		panic("snh")
	}

	for index, item := range list[start_index:] {
		if disttopk.BloomHistogramScore(item.Score) < mineqscore {
			return index + start_index
		}
	}
	return len(list)
}

func GetNumItemsMinScore(list disttopk.ItemList, start_index int, mineqscore disttopk.BloomHistogramScore) int {
	after := GetIndexAfter(list, start_index, mineqscore)
	return after - start_index
}

func CreateFromListMinscore(b *disttopk.BloomHistogram, list disttopk.ItemList, scorek disttopk.BloomHistogramScore, start_index int, total_entries int, mineqscore disttopk.BloomHistogramScore, N_est int, numpeers int, topk int) (serialAccess int) {
	if disttopk.PRINT_BUCKETS {
		fmt.Println("mineqscore ", mineqscore, "score-k", scorek, " entries ", total_entries)
	}
	current_score := disttopk.BloomHistogramScore(list[start_index].Score)
	current_index := start_index
	b.Data = make([]*disttopk.BloomHistogramEntry, 0)

	for current_score >= mineqscore {
		range_left := (current_score - mineqscore)
		entries_left := total_entries - len(b.Data)
		range_per_entry := disttopk.BloomHistogramScore(int(range_left) / entries_left)

		//range can be 0, which means mineqentryscore == current_score, the entry will cover current_score and thats it

		mineqentryscore := current_score - range_per_entry
		if entries_left == 1 && mineqentryscore != mineqscore {
			panic("snh")
		}

		entry, next_index := CreateEntryMinscore(b, list, int(scorek), current_index, mineqentryscore, N_est, numpeers, topk)
		//fmt.Println("Made entry ", len(b.Data), " from ", current_index, "to", next_index, "mineqentryscore", mineqentryscore, "mineq", mineqscore, "range", range_per_entry, "entries", total_entries, "left", entries_left, "next index", next_index, "max score", list[current_index].Score, "next_score", list[next_index].Score)
		if current_index == next_index {
			if entry != nil {
				panic("snh")
			}
			break
		}
		if entry == nil {
			panic("snh")
		}
		b.Data = append(b.Data, entry)
		if next_index < len(list) {
			current_score = disttopk.BloomHistogramScore(list[next_index].Score)
		} else {
			current_score = 0
		}
		current_index = next_index
		if current_score >= mineqentryscore {
			break
		}
	}
	if current_score > 0 {
		b.SetCutoff(current_score)
	}
	return current_index - start_index
}

func CreateEntryMinscore(b *disttopk.BloomHistogram, list disttopk.ItemList, scorek int, entry_start_index int, mineqscore disttopk.BloomHistogramScore, N_est int, numpeers int, topk int) (*disttopk.BloomHistogramEntry, int) {

	start_score := disttopk.BloomHistogramScore(list[entry_start_index].Score)

	//fmt.Println("Start_score", start_score, "mineq score", mineqscore, "index", entry_start_index)
	items_in_entry := GetNumItemsMinScore(list, entry_start_index, mineqscore)
	filter, eps := b.CreateBloomEntryFilter(N_est, items_in_entry, numpeers, uint(list[entry_start_index].Score), uint(scorek), len(list))

	//fmt.Println("Range mineq", mineqscore, filter == nil, start_score)
	if filter == nil { // the algorithm can decide that it is not worth sending a filter, will be more expensive than its worth
		range_entry := start_score - mineqscore
		for range_entry > 0 && filter == nil {
			range_entry = range_entry / 2 //note range_entry can become 0 here
			mineqscore = start_score - range_entry
			items_in_entry := GetNumItemsMinScore(list, entry_start_index, mineqscore)
			filter, eps = b.CreateBloomEntryFilter(N_est, items_in_entry, numpeers, uint(list[entry_start_index].Score), uint(scorek), len(list))
			//fmt.Println("Range", range_entry, filter == nil)
		}

		if filter == nil {
			return nil, entry_start_index
		}
	}

	entry := disttopk.NewBloomHistogramEntry(filter, eps)
	end_index := GetIndexAfter(list, entry_start_index, mineqscore)

	current_index := entry_start_index
	for current_index < len(list) && (current_index < end_index) {
		item := list[current_index]
		entry.Add(uint(item.Id), disttopk.BloomHistogramScore(item.Score))
		current_index += 1
	}

	//b.Data = append(b.Data, entry)
	if disttopk.PRINT_BUCKETS {
		max := entry.GetMax()
		min := disttopk.BloomHistogramScore(list[current_index-1].Score)
		fmt.Println("Interval", len(b.Data), "max", max, "min (tight)", min, "range", max-min, "#", current_index-entry_start_index, "k", entry.GetFilter().NumberHashes() /*range_left, entries_left, range_per_entry, score_after_entry, index_after_entry, list[index_after_entry].Score, entry_start_index*/)
	}
	return entry, current_index
}

/*
func (b *BloomHistogramSketch) MinScoreIndex(list disttopk.ItemList, scorek float64) (minscore int, first_index_past_minscore int) {
	minscore = int(scorek) / int(b.numpeers)

	first_index_past_minscore = len(list)
	for i, item := range list {
		if int(item.Score) < minscore {
			first_index_past_minscore = i
			break
		}
	}
	fmt.Println("Minscore = ", minscore, "fip", first_index_past_minscore, len(list))
	return minscore, first_index_past_minscore
}
*/
func (b *BloomHistogramSketch) CreateFromListWithScoreK(list disttopk.ItemList, scorek float64) (serialAccess int) {
	//scorek := disttopk.BloomHistogramScore(list[topk].Score)
	minscore := GetMinScoreEqExact(disttopk.BloomHistogramScore(scorek), b.numpeers)
	numentries := 10

	//fmt.Println("start_score", list[0].Score, "minscore", minscore, "numentries", numentries, "len", len(list))
	return CreateFromListMinscore(b.BloomHistogram, list, disttopk.BloomHistogramScore(scorek), 0, numentries, minscore, b.N_est, b.numpeers, b.topk)
}

/*
func (b *BloomHistogramSketch) CreateFirstRoundFromList(list disttopk.ItemList, start_index int, topk int) (serialAccess int) {
	scorek := disttopk.BloomHistogramScore(list[topk].Score)
	//minscore, first_index_past_minscore := b.MinScoreIndex(list, scorek)

	minscore := GetMinScoreEqExact(scorek, b.numpeers)
	exact_index := GetIndexAfter(list, 0, minscore)

	numentries := 10
	multiplier := 1 //TODO: change to 5
	if exact_index > topk*multiplier {
		index := topk * multiplier
		score := disttopk.BloomHistogramScore(list[index].Score)
		score_max := disttopk.BloomHistogramScore(list[0].Score)
		rangeadj := float64(score_max-score) / float64(score_max-minscore)
		numentries = int(rangeadj * float64(numentries))
		if numentries < 1 {
			numentries = 1
		}
		minscore = score
	}
	//fmt.Println("start_score", list[start_index].Score, "minscore", minscore, "numentries", numentries, "exact_index", exact_index, "len", len(list))
	return CreateFromListMinscore(b.BloomHistogram, list, scorek, start_index, numentries, minscore, b.N_est, b.numpeers, b.topk)
}
*/
func (p *BloomHistogramSketch) Serialize(w io.Writer) error {
	panic("Not Implemented")
}

type BloomHistogramSketchSplit struct {
	first        *disttopk.BloomHistogram
	second       *disttopk.BloomHistogram
	topk         int
	numpeers     int
	N_est        int
	totalEntries int
	nextIndex    int
	Multiplier   int
}

func NewBloomHistogramSketchSplit(first *disttopk.BloomHistogram, second *disttopk.BloomHistogram, topk int, numpeers int, N_est int, multiplier int) *BloomHistogramSketchSplit {
	return &BloomHistogramSketchSplit{first, second, topk, numpeers, N_est, 10, 0, multiplier}
}

func (bhss *BloomHistogramSketchSplit) CreateFirstRoundFromList(list disttopk.ItemList, start_index int) (serialAccess int) {
	scorek := disttopk.BloomHistogramScore(list[bhss.topk].Score)
	//minscore, first_index_past_minscore := b.MinScoreIndex(list, scorek)

	minscore := GetMinScoreEqExact(scorek, bhss.numpeers)
	exact_index := GetIndexAfter(list, 0, minscore)

	//fmt.Println("Scorek", scorek, "minscore", minscore, "exact_index", exact_index)

	numentries := bhss.totalEntries
	//bhss.Multiplier := 5 //TODO: change to 5
	if exact_index > bhss.topk*bhss.Multiplier && bhss.Multiplier != 0 {
		//fmt.Println("Here!")
		index := bhss.topk * bhss.Multiplier
		score := disttopk.BloomHistogramScore(list[index].Score)
		score_max := disttopk.BloomHistogramScore(list[0].Score)
		rangeadj := float64(score_max-score) / float64(score_max-minscore)
		numentries = int(rangeadj * float64(numentries))
		if numentries < 1 {
			numentries = 1
		}
		minscore = score
	}
	//fmt.Println("start_score", list[start_index].Score, "minscore", minscore, "numentries", numentries, "exact_index", exact_index, "len", len(list))
	items := CreateFromListMinscore(bhss.first, list, scorek, start_index, numentries, minscore, bhss.N_est, bhss.numpeers, bhss.topk)
	bhss.nextIndex = items + start_index
	return items
}

func (b *BloomHistogramSketchSplit) CreateSecondRoundFromList(list disttopk.ItemList, rangeNeededInt int) (serialAccess int) {
	scorek := disttopk.BloomHistogramScore(list[b.topk].Score)
	current_score := disttopk.BloomHistogramScore(list[b.nextIndex].Score)
	rangeNeeded := disttopk.BloomHistogramScore(rangeNeededInt)
	minscore := disttopk.BloomHistogramScore(0)
	if rangeNeeded < current_score {
		minscore = (current_score - rangeNeeded) + 1
	}
	numentries := b.totalEntries - len(b.first.Data)
	if numentries < 1 {
		numentries = 1
	}
	items := CreateFromListMinscore(b.second, list, scorek, b.nextIndex, numentries, minscore, b.N_est, b.numpeers, b.topk)
	return items
}

func NewBloomHistogramSketchSplitGcs(topk int, numpeers int, N_est int, Multiplier int) *BloomHistogramSketchSplit {
	first := disttopk.NewBloomHistogram(disttopk.GcsFilterAdaptor{})
	second := disttopk.NewBloomHistogram(disttopk.GcsFilterAdaptor{})
	return NewBloomHistogramSketchSplit(first, second, topk, numpeers, N_est, Multiplier)
}

/*
func (b *BloomHistogramSketch) CreateFirstRoundFromList(list disttopk.ItemList, scorek float64, topk int) (serialAccess int) {
	minscore, first_index_past_minscore := b.MinScoreIndex(list, scorek)

	numentries := 10
	multiplier := 1 //TODO: change to 5
	if first_index_past_minscore > topk*multiplier {
		first_index_past_minscore = topk * multiplier
		score_max := int(list[0].Score)
		score_min := int(list[first_index_past_minscore-1].Score)
		rangeadj := float64(score_max-score_min) / float64(score_max-minscore)
		numentries := int(rangeadj * float64(numentries))
		if numentries < 1 {
			numentries = 1
		}
	}
	fmt.Println("Fip", first_index_past_minscore, list[0].Score, list[first_index_past_minscore].Score)
	return b.CreateFromListDetailed(list, scorek, numentries, first_index_past_minscore)
}

func (b *BloomHistogramSketch) CreateFromListDetailed(list disttopk.ItemList, scorek float64, total_entries int, maxIndex int) (serialAccess int) {
	if disttopk.PRINT_BUCKETS {
		fmt.Println("max Index ", maxIndex, "maxIndex", maxIndex, "score-k", scorek, " entries ", total_entries)
	}
	current_index := 0
	b.Data = make([]*disttopk.BloomHistogramEntry, 0)
	for current_index < maxIndex && len(b.Data) < total_entries {
		entry_start_index := current_index
		min_score_to_cover := uint32(list[maxIndex-1].Score)
		min_score_after_cover := uint32(list[maxIndex].Score)
		entry_start_score := uint32(list[current_index].Score)
		range_left := (entry_start_score - min_score_to_cover) + 1
		entries_left := total_entries - len(b.Data)
		range_per_entry := int(range_left) / entries_left
		if range_per_entry < 1 {
			range_per_entry = 1
		}

		items_in_entry := itemsInEntry(list, entry_start_index, range_per_entry, b.topk)

		if items_in_entry > maxIndex-entry_start_index || (len(b.Data) == total_entries-1 && entry_start_index+items_in_entry < maxIndex) {
			panic(fmt.Sprint("Snh", entry_start_index+items_in_entry, maxIndex, items_in_entry, maxIndex-entry_start_index, range_per_entry, min_score_to_cover, entry_start_score, min_score_after_cover))
		}

		entry, items := CreateEntry(b.BloomHistogram, list, int(scorek), entry_start_index, range_per_entry, b.N_est, b.numpeers, b.topk)
		if items == 0 {
			break
		}
		b.Data = append(b.Data, entry)
		if items > items_in_entry {
			break
		}
		current_index += items
	}

	if current_index < len(list) {
		//fmt.Println("Minscore =", minscore, "fipm", first_index_past_minscore, "len", len(list), current_index)
		b.SetCutoff(disttopk.BloomHistogramScore(list[current_index].Score))
		//fmt.Println("Cutoff", b.cutoff)
		return current_index + 1
		//fmt.Println("Cutoff", b.cutoff, list[current_index-1].Score, current_index, first_index_past_minscore, len(list))
	}
	return current_index

}

func CreateEntry(b *disttopk.BloomHistogram, list disttopk.ItemList, scorek int, entry_start_index int, range_per_entry int, N_est int, numpeers int, topk int) (*disttopk.BloomHistogramEntry, int) {

	items_in_entry := itemsInEntry(list, entry_start_index, range_per_entry, topk)
	filter, eps := b.CreateBloomEntryFilter(N_est, items_in_entry, numpeers, uint(list[entry_start_index].Score), uint(scorek), len(list))

	if filter == nil { // the algorithm can decide that it is not worth sending a filter, will be more expensive than its worth
		for range_per_entry > 1 && filter == nil {
			range_per_entry = range_per_entry / 2
			items_in_entry = itemsInEntry(list, entry_start_index, range_per_entry, topk)
			//fmt.Println("range", range_per_entry, "num", items_in_entry, "range_left", range_left, "entries_left", entries_left)
			filter, eps = b.CreateBloomEntryFilter(N_est, items_in_entry, numpeers, uint(list[entry_start_index].Score), uint(scorek), len(list))
		}

		if filter == nil {
			return nil, 0
		}
	}

	entry := disttopk.NewBloomHistogramEntry(filter, eps)

	endindex := entry_start_index + items_in_entry
	current_index := entry_start_index
	for current_index < len(list) && (current_index < endindex) {
		item := list[current_index]
		entry.Add(uint(item.Id), disttopk.BloomHistogramScore(item.Score))
		current_index += 1
	}

	//b.Data = append(b.Data, entry)
	if disttopk.PRINT_BUCKETS {
		max := entry.GetMax()
		min := disttopk.BloomHistogramScore(list[current_index-1].Score)
		fmt.Println("Interval", len(b.Data), "max", max, "min (tight)", min, "range", max-min, "#", current_index-entry_start_index, "k", entry.GetFilter().NumberHashes() /*range_left, entries_left, range_per_entry, score_after_entry, index_after_entry, list[index_after_entry].Score, entry_start_index)
	}
	return entry, current_index - entry_start_index
}
*/
