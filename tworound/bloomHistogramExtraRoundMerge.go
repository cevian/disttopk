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
	return &BhErUnionSketchAdaptor{topk, numpeer, 0.5, 0}
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

func (t *BhErUnionSketchAdaptor) getUnionFilter(us UnionSketch, thresh uint32, il disttopk.ItemList, listlensum int) (UnionFilter, uint) {
	if t.numUnionFilterCalls == 0 {
		bs := us.(*BhErUnionSketch)
		mhm := bs.GetMaxHashMap()

		underApprox := mhm.UnderApprox(t.topk)
		overApprox := mhm.OverApprox(t.topk)

		approxthresh := underApprox + int64(float64(overApprox-underApprox)*t.gamma)

		cutoff := int64(mhm.Cutoff())
		fmt.Println("Approximating thresh at: ", approxthresh, " Original: ", thresh, "Gamma:", t.gamma, "Under:", underApprox, "Cutoff:", cutoff)
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

		needed_cutoff_per_node := 0
		if int64(thresh) <= cutoff {
			needed_cutoff := cutoff - int64(thresh)
			needed_cutoff_per_node = int(math.Ceil(float64(needed_cutoff) / float64(t.numpeer)))
		}

		return &BhErGcsFilter{filter, needed_cutoff_per_node}, uint(approxthresh)
	} else {
		bs := us.(*BhErUnionSketch)
		//fmt.Println("Uf info before set thresh: ", bs.GetInfo())
		gcs := bs.GetFilter(int64(thresh))
		if gcs != nil {
			return &BhErGcsFilter{gcs, int(thresh)}, uint(thresh)
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
}

func NewBhErPeerSketchAdaptor(topk int, numpeer int, N_est int) PeerSketchAdaptor {
	return &BhErPeerSketchAdaptor{&BloomHistogramPeerSketchAdaptor{topk, numpeer, N_est}}
}

func (t *BhErPeerSketchAdaptor) createSketch(list disttopk.ItemList, localtop disttopk.ItemList) (FirstRoundSketch, int) {
	s := disttopk.NewBloomSketchGcs(t.topk, t.numpeer, t.N_est)
	if MERGE_TOPK_AT_COORD {
		return s, s.CreateFirstRoundFromList(list[len(localtop):], list[t.topk-1].Score, t.topk)
	} else {
		return s, s.CreateFirstRoundFromList(list, list[t.topk-1].Score, t.topk)
		//return s, s.CreateFromList(list) - len(localtop)
	}
}
