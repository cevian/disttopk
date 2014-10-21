package tworound

import "github.com/cevian/disttopk"

import (
	"fmt"
	"io"
	"math"
)

var _ = fmt.Println

type BhErUnionSketch struct {
	bhs map[int]*disttopk.BloomHistogram //map peer id => bh
	ils map[int]disttopk.ItemList        //map peer id => item list
}

func (t *BhErUnionSketch) GetInfo() string {
	return "BhErUnionSketch"
}

func (t *BhErUnionSketch) MergeFirstRound(bh *disttopk.BloomHistogram, il disttopk.ItemList, peerId int) {
	t.bhs[peerId] = bh
	t.ils[peerId] = il
}

func (t *BhErUnionSketch) MergeSecondRound(bh *disttopk.BloomHistogram, il disttopk.ItemList, peerId int) {
	oldbh := t.bhs[peerId]
	for _, entry := range bh.Data {
		oldbh.Data = append(oldbh.Data, entry)
	}
	oldbh.SetCutoff(bh.Cutoff())
	t.bhs[peerId] = oldbh
}

func (t *BhErUnionSketch) GetMinModulusBits() int {
	min_modulus := 0
	max_modulus := 0
	sum := 0
	count := 0
	for _, bh := range t.bhs {
		for _, entry := range bh.Data {
			g := entry.GetFilter().(*disttopk.Gcs)
			m := g.Columns
			sum += m
			count += 1
			if min_modulus == 0 || m < min_modulus {
				min_modulus = m
			}
			if m > max_modulus {
				max_modulus = m
			}
		}
	}
	m_bits := int(math.Log2(float64(min_modulus)))
	/*
			fmt.Println("DBG: min", min_modulus, "avg", sum/count,"first", t.bhs[0].Data[0].GetFilter().(*disttopk.Gcs).Columns, "bits", m_bits, "max", t.bhs[0].Data[0].GetMax(), t.bhs[0].Data[1].GetMax())
		        for _, entry := range t.bhs[0].Data{
				fmt.Println("entry", entry.GetFilter().(*disttopk.Gcs).Columns, entry.GetMax())
			}*/
	return m_bits
}

func (t *BhErUnionSketch) GetMaxHashMap(modulus_bits int) *MaxHashMapUnionSketch {

	length_hint := 0
	for _, bh := range t.bhs {
		length_hint += bh.SumLen(modulus_bits)
	}

	mhm := NewMaxHashMapUnionSketch(length_hint)
	mhm.SetModulusBits(modulus_bits)
	for peer_id, bh := range t.bhs {
		mhm.Merge(bh, t.ils[peer_id])
	}
	return mhm
}

func (t *BhErUnionSketch) GetFilter(thresh int64) (*disttopk.Gcs, int64) {
	mhm := t.GetMaxHashMap(t.GetMinModulusBits())
	fmt.Println("Mhm info on get filter", mhm.GetInfo())
	return mhm.GetFilter(thresh)
}

type BhErGcsFilter struct {
	*disttopk.Gcs
	ExtraRange int
}

func (t *BhErGcsFilter) isEmpty() bool {
	return t.Data.Len() == 0
}

func (t *BhErGcsFilter) Serialize(w io.Writer) error {
	if err := t.Gcs.Serialize(w); err != nil {
		return err
	}
	if err := disttopk.SerializeIntAsU32(w, &t.ExtraRange); err != nil {
		return err
	}
	return nil
}

func (t *BhErGcsFilter) Deserialize(r io.Reader) error {
	t.Gcs = &disttopk.Gcs{}
	if err := t.Gcs.Deserialize(r); err != nil {
		return err
	}

	if err := disttopk.DeserializeIntAsU32(r, &t.ExtraRange); err != nil {
		return err
	}
	return nil
}

func NewBhErUnionSketch() *BhErUnionSketch {
	return &BhErUnionSketch{make(map[int]*disttopk.BloomHistogram), make(map[int]disttopk.ItemList)}
}

type BhErUnionSketchAdaptor struct {
	topk                int
	numpeer             int
	gamma               float64
	useCutoffHeuristic  bool
	numUnionFilterCalls int
	firstRoundFilter    *disttopk.Gcs
}

func NewBhErUnionSketchAdaptor(topk int, numpeer int, gamma float64, ch bool) UnionSketchAdaptor {
	return &BhErUnionSketchAdaptor{topk, numpeer, gamma, ch, 0, nil}
}

func (t *BhErUnionSketchAdaptor) getUnionSketch(frs FirstRoundSketch, il disttopk.ItemList, peerId int) UnionSketch {
	bs := frs.(*disttopk.BloomHistogram)
	bhers := NewBhErUnionSketch()
	bhers.MergeFirstRound(bs, il, peerId)
	return bhers
}

func (t *BhErUnionSketchAdaptor) mergeIntoUnionSketch(us UnionSketch, frs FirstRoundSketch, il disttopk.ItemList, peerId int) {
	bhers := us.(*BhErUnionSketch)
	bs := frs.(*disttopk.BloomHistogram)
	bhers.MergeFirstRound(bs, il, peerId)
}

func (t *BhErUnionSketchAdaptor) mergeAdditionalSketchIntoUnionSketch(us UnionSketch, frs FirstRoundSketch, il disttopk.ItemList, peerId int) {
	bhers := us.(*BhErUnionSketch)
	bs := frs.(*disttopk.BloomHistogram)
	bhers.MergeSecondRound(bs, il, peerId)
}

func (t *BhErUnionSketchAdaptor) GetCutoffHeuristic(bs *BhErUnionSketch, topkapprox int64, threshforfilter int64) int64 {
	mhm := bs.GetMaxHashMap(bs.GetMinModulusBits())
	cutoff := int64(mhm.Cutoff())

	bestcutoff := cutoff
	if cutoff > 0 {
		testcut := cutoff
		lowestnh, _ := mhm.GetCountHashesWithCutoff(topkapprox, testcut, threshforfilter)
		for testcut > 0 {
			nh, nextcut := mhm.GetCountHashesWithCutoff(topkapprox, testcut, threshforfilter)
			//fmt.Println("Cutoff:", testcut, "count", nh)
			if nh < lowestnh {
				lowestnh = nh
				bestcutoff = testcut
			}
			if nh < t.topk {
				break
			}
			if nextcut >= testcut {
				panic("snh")
			}
			testcut = nextcut
		}

		/*referencecut := cutoff+1 // mhm.GetMaxCutoff(topkapprox)
		if referencecut <= cutoff {
			panic(fmt.Sprintln("Error", referencecut, cutoff))
		}
		reference := mhm.GetCountHashesWithCutoff(topkapprox, referencecut, threshforfilter)
		testcut := cutoff
		best := float64(0)
		for testcut > 0 {
			c := mhm.GetCountHashesWithCutoff(topkapprox, testcut, threshforfilter)
			ratio := float64(reference-c) / float64(referencecut-testcut)
			if ratio > best {
				best = ratio
				bestcutoff = testcut
			}
			fmt.Println("Heuristic Cutoff is ", testcut, "num hash", c, "ratio", ratio, reference, referencecut, reference-c, referencecut-testcut)
			testcut--
		}*/
	}
	return bestcutoff
}

func (t *BhErUnionSketchAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList, listlensum int) (UnionFilter, uint) {
	if t.numUnionFilterCalls == 0 {
		bs := us.(*BhErUnionSketch)
		mhm := bs.GetMaxHashMap(bs.GetMinModulusBits())

		underApprox := mhm.UnderApprox(t.topk)
		overApprox := mhm.OverApprox(t.topk)
		if overApprox < underApprox {
			panic(fmt.Sprintln("should not happen", overApprox, underApprox))
		}

		approxthresh := underApprox + int64(float64(overApprox-underApprox)*t.gamma)

		cutoff := int64(mhm.Cutoff())

		underLow := int64(thresh)
		if underLow == 0 {
			//Note we are using the underapprox here not the threshold
			underLow = underApprox - 1
		}

		mincutoff := cutoff
		needed_cutoff_per_node := 0
		if t.useCutoffHeuristic {
			heur := t.GetCutoffHeuristic(bs, underLow, approxthresh)
			fmt.Println("Using cutoff heuristic. Got: ", heur, "current cutoff:", cutoff)
			if heur < cutoff {
				mincutoff = heur
				needed_cutoff_per_node = int(math.Ceil(float64(cutoff-mincutoff) / float64(t.numpeer)))
			}

		}

		if underLow < mincutoff {
			mincutoff = underLow
			needed_cutoff_per_node = int(math.Ceil(float64(cutoff-mincutoff) / float64(t.numpeer)))
		}
		/*
			if int64(topkapprox) <= cutoff {
				mincutoff = int64(topkapprox)
				//needed_cutoff_per_node = int(math.Ceil(float64(needed_cutoff) / float64(t.numpeer)))
				//needed_cutoff_per_node = 100
			}
			//fmt.Println("First mincutoff is ", mincutoff, cutoff, topkapprox)
			if mincutoff > 0 {
				testcut := mincutoff - 1
				first := mhm.GetCountHashesWithCutoff(topkapprox, cutoff)
				best := float64(0)
				for testcut > 0 {
					c := mhm.GetCountHashesWithCutoff(topkapprox, testcut)
					ratio := float64(first-c) / float64(cutoff-testcut)
					if ratio > best {
						best = ratio
						mincutoff = testcut
					}
					//fmt.Println("Cutoff is ", testcut, "num", c, ratio)
					testcut--
				}
			}
			//fmt.Println("Second mincutoff is ", mincutoff, cutoff)

			needed_cutoff_per_node := 0
			if mincutoff < cutoff {
				needed_cutoff_per_node = int(math.Ceil(float64(cutoff-mincutoff) / float64(t.numpeer)))
			}
		*/
		fmt.Println("Approximating thresh at: ", approxthresh, " Original: ", thresh, "Gamma:", t.gamma, "Under:", underApprox, "Over:", overApprox, "Cutoff:", cutoff, "Needed cutoff per node", needed_cutoff_per_node, "mincutoff", mincutoff)

		if cutoff >= approxthresh {
			old := approxthresh
			approxthresh = cutoff + 1
			fmt.Println("The Approximation threshold is too high for cutoff, resetting to", approxthresh, " Was ", old, ", cutoff ", cutoff)
		}

		/* experimental optimization: we dont know if its useful, turn off for now */
		/*
			if cutoff > underApprox {
				//this optimization tries to avoid the case that the cutoff is above the threshold in the 3rd round and thus everything needs to be sent.
				//by sending more data now, we avoid the possibility of having to send everything in the last round because our k_score is below cutoff.
				old := approxthresh
				approxthresh = cutoff + 1
				fmt.Println("The cutoff is above the underapprox. Setting approxthresh just above cutoff to be safe:", approxthresh, ". It was:", old)
			}
		*/

		filter, approxthresh := bs.GetFilter(approxthresh)
		if filter == nil {
			panic("Should never get nil filter here")
		}
		t.firstRoundFilter = filter
		t.numUnionFilterCalls = 1

		return &BhErGcsFilter{filter, int(mincutoff) / t.numpeer}, uint(approxthresh)
	} else {
		bs := us.(*BhErUnionSketch)
		old_filter := t.firstRoundFilter

		//fmt.Println("Uf info before set thresh: ", bs.GetInfo())
		fmt.Println("Getting round 3 filter for: thresh=", thresh)

		m_bits := int(math.Log2(float64(old_filter.GetM())))
		mhm := bs.GetMaxHashMap(m_bits)
		gcs, thresh := mhm.GetFilter(int64(thresh))
		//gcs, thresh := bs.GetFilter(int64(thresh))
		if gcs != nil {
			gcs.SubtractGcs(old_filter)
			return &BhErGcsFilter{gcs, 0}, uint(thresh)
		}
		return nil, uint(thresh)
	}
	//filter, approxthresh := bs.GetFilterApprox(uint(thresh), t.topk+1) //+1 to get the max below the k'th elem
	//fmt.Println("Approximating thresh at: ", approxthresh, " Original: ", thresh)
}

func (t *BhErUnionSketchAdaptor) copyUnionFilter(uf UnionFilter) UnionFilter {
	bs := uf.(*BhErGcsFilter)

	copy_uf := *bs
	return &copy_uf
}

func (t *BhErUnionSketchAdaptor) serialize(uf UnionFilter) Serialized {
	obj, ok := uf.(*BhErGcsFilter)

	if !ok {
		panic("Unexpected")
	}
	b, err := disttopk.SerializeObject(obj)
	if err != nil {
		panic(err)
	}
	return b
}

func (*BhErUnionSketchAdaptor) deserialize(s Serialized) UnionFilter {
	bs := s
	obj := &BhErGcsFilter{}
	err := disttopk.DeserializeObject(obj, []byte(bs))
	if err != nil {
		panic(err)
	}
	return obj
}

func (t *BhErUnionSketchAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, ht *disttopk.HashTable, cutoff_sent int, sent_item_filter map[int]bool) ([]disttopk.Item, *disttopk.AlgoStatsRound) {
	if uf == nil {
		remaining_list := list[cutoff_sent:]
		exactlist := make([]disttopk.Item, 0, len(remaining_list))
		for _, item := range remaining_list {
			if !sent_item_filter[item.Id] {
				exactlist = append(exactlist, item)
			}
		}

		//copy(exactlist, remaining_list)
		return exactlist, &disttopk.AlgoStatsRound{Serial_items: len(remaining_list)}
	}

	bhgcs := uf.(*BhErGcsFilter)
	filter := disttopk.NewGcsMergeIndexableFilter(bhgcs.Gcs)
	return disttopk.GetListIndexedHashTable(filter, list, ht, sent_item_filter)
}

type BhErPeerSketchAdaptor struct {
	*BloomHistogramPeerSketchAdaptor
	Multiplier   int
	totalEntries int
}

func NewBhErPeerSketchAdaptor(topk int, numpeer int, N_est int, multiplier int, EstimateParameter disttopk.EstimateParameter) PeerSketchAdaptor {
	return &BhErPeerSketchAdaptor{&BloomHistogramPeerSketchAdaptor{topk, numpeer, N_est, EstimateParameter}, multiplier, 10}
}

func (t *BhErPeerSketchAdaptor) createSketch(list disttopk.ItemList, localtop disttopk.ItemList) (FirstRoundSketch, int) {
	s := NewBloomHistogramSketchSplitGcs(t.topk, t.numpeer, t.N_est, t.Multiplier, t.EstimateParameter)
	s.totalEntries = t.totalEntries
	if MERGE_TOPK_AT_COORD {
		items := s.CreateFirstRoundFromList(list, len(localtop))
		return s, items
	} else {
		return s, s.CreateFirstRoundFromList(list, 0)
		//return s, s.CreateFromList(list) - len(localtop)
	}
}

func (t *BhErPeerSketchAdaptor) getAdditionalSketch(uf UnionFilter, list disttopk.ItemList, prevSketch FirstRoundSketch) (sketch FirstRoundSketch, SerialAccess int) {
	bhgcs := uf.(*BhErGcsFilter)

	/* interpret extra range as new min*/
	s := prevSketch.(*BloomHistogramSketchSplit)

	old_cutoff := s.FirstRoundCutoff(list)
	if int(old_cutoff) <= bhgcs.ExtraRange || bhgcs.ExtraRange == 0 {
		return nil, 0
	}

	items := s.CreateSecondRoundFromList(list, bhgcs.ExtraRange)
	//fmt.Println("Get Additional Sketch", items, bhgcs.ExtraRange)
	return s, items
}

func (*BhErPeerSketchAdaptor) serializeAdditionalSketch(c FirstRoundSketch) Serialized {
	obj, ok := c.(*BloomHistogramSketchSplit)
	if !ok {
		panic("Unexpected")
	}
	b, err := disttopk.SerializeObject(obj.second)
	if err != nil {
		panic(err)
	}
	return b
	//return c
}

func (t *BloomHistogramPeerSketchAdaptor) deserializeAdditionalSketch(frs Serialized) FirstRoundSketch {
	bs := frs
	obj := &disttopk.BloomHistogram{}
	err := disttopk.DeserializeObject(obj, []byte(bs))
	if err != nil {
		panic(err)
	}
	return obj
	//return frs.(FirstRoundSketch)
}

func (*BhErPeerSketchAdaptor) serialize(c FirstRoundSketch) Serialized {
	obj, ok := c.(*BloomHistogramSketchSplit)
	if !ok {
		panic("Unexpected")
	}
	b, err := disttopk.SerializeObject(obj.first)
	if err != nil {
		panic(err)
	}
	return b
	//return c
}
