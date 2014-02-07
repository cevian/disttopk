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

func (t *BhErUnionSketch) GetMaxHashMap() *MaxHashMapUnionSketch {
	mhm := NewMaxHashMapUnionSketch()
	for peer_id, bh := range t.bhs {
		mhm.Merge(bh, t.ils[peer_id])
	}
	return mhm
}

func (t *BhErUnionSketch) GetFilter(thresh int64) *disttopk.Gcs {
	mhm := t.GetMaxHashMap()
	return mhm.GetFilter(thresh)
}

type BhErGcsFilter struct {
	*disttopk.Gcs
	ExtraRange int
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
	numUnionFilterCalls int
}

func NewBhErUnionSketchAdaptor(topk int, numpeer int) UnionSketchAdaptor {
	return &BhErUnionSketchAdaptor{topk, numpeer, 0.9, 0}
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

func (t *BhErUnionSketchAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList, listlensum int) (UnionFilter, uint) {
	if t.numUnionFilterCalls == 0 {
		bs := us.(*BhErUnionSketch)
		mhm := bs.GetMaxHashMap()

		underApprox := mhm.UnderApprox(t.topk)
		overApprox := mhm.OverApprox(t.topk)

		approxthresh := underApprox + int64(float64(overApprox-underApprox)*t.gamma)

		cutoff := int64(mhm.Cutoff())

		topkapprox := int64(thresh)
		mincutoff := cutoff
		if topkapprox == 0 {
			//Note we are using the underapprox here not the threshold
			topkapprox = underApprox
		}
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

		fmt.Println("Approximating thresh at: ", approxthresh, " Original: ", thresh, "Gamma:", t.gamma, "Under:", underApprox, "Cutoff:", cutoff, "Needed cutoff per node", needed_cutoff_per_node)

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

		filter := bs.GetFilter(approxthresh)
		if filter == nil {
			panic("Should never get nil filter here")
		}
		t.numUnionFilterCalls = 1

		return &BhErGcsFilter{filter, needed_cutoff_per_node}, uint(approxthresh)
	} else {
		bs := us.(*BhErUnionSketch)
		//fmt.Println("Uf info before set thresh: ", bs.GetInfo())
		gcs := bs.GetFilter(int64(thresh))
		if gcs != nil {
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

func (t *BhErUnionSketchAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int, sent_item_filter map[int]bool) ([]disttopk.Item, *disttopk.AlgoStats) {
	if uf == nil {
		remaining_list := list[cutoff_sent:]
		exactlist := make([]disttopk.Item, 0, len(remaining_list))
		for _, item := range remaining_list {
			if !sent_item_filter[item.Id] {
				exactlist = append(exactlist, item)
			}
		}

		//copy(exactlist, remaining_list)
		return exactlist, &disttopk.AlgoStats{Serial_items: len(remaining_list)}
	}

	bhgcs := uf.(*BhErGcsFilter)
	filter := disttopk.NewGcsMergeIndexableFilter(bhgcs.Gcs)
	return disttopk.GetListIndexedHashTable(filter, list, sent_item_filter)
}

type BhErPeerSketchAdaptor struct {
	*BloomHistogramPeerSketchAdaptor
	Multiplier int
}

func NewBhErPeerSketchAdaptor(topk int, numpeer int, N_est int, multiplier int) PeerSketchAdaptor {
	return &BhErPeerSketchAdaptor{&BloomHistogramPeerSketchAdaptor{topk, numpeer, N_est}, multiplier}
}

func (t *BhErPeerSketchAdaptor) createSketch(list disttopk.ItemList, localtop disttopk.ItemList) (FirstRoundSketch, int) {
	s := NewBloomHistogramSketchSplitGcs(t.topk, t.numpeer, t.N_est, t.Multiplier)
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
	if bhgcs.ExtraRange == 0 {
		return nil, 0
	}
	s := prevSketch.(*BloomHistogramSketchSplit)
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
