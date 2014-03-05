package klee

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	"fmt"
)

const SERIALIZE_CLF = true

func NewPeer(list disttopk.ItemList, k int, clrRound bool) *Peer {
	return &Peer{stream.NewHardStopChannelCloser(), nil, nil, list, k, 0, clrRound, true, 0.10}
}

type Peer struct {
	*stream.HardStopChannelCloser
	forward              chan<- disttopk.DemuxObject
	back                 <-chan stream.Object
	list                 disttopk.ItemList
	k                    int
	id                   int
	clrRound             bool
	clrRoundTopkEstimate bool
	c                    float64
}

type FirstRound struct {
	list  disttopk.ItemList
	bh    []byte
	stats *disttopk.AlgoStatsRound
}

type ClrRoundSpec struct {
	thresh   uint32
	cl_size  uint32
	topk_ids []int
}

type ClrRoundReply struct {
	payload interface{}
	list    disttopk.ItemList
	stats   *disttopk.AlgoStatsRound
}

type FinalListReply struct {
	payload disttopk.ItemList
	stats   *disttopk.AlgoStatsRound
}

func getThreshIndex(list disttopk.ItemList, thresh uint32) int {
	index := 0
	for k, v := range list {
		index = k
		if v.Score < float64(thresh) {
			break
		}
	}
	return index
}

func (src *Peer) Run() error {
	//defer close(src.forward)
	//src.list.Sort()
	//fmt.Println("Sort", src.list[:10])

	if src.k > len(src.list) {
		fmt.Println("warning tput: list shorter than k")
		src.k = len(src.list)
	}

	localtop := src.list[:src.k]
	bh := disttopk.NewBloomHistogramKlee()
	bh.CreateFromList(src.list, src.c)
	bhs, err := disttopk.SerializeObject(bh)
	if err != nil {
		panic(err)
	}

	compressed := disttopk.CompressBytes(bhs)
	first_round_access := &disttopk.AlgoStatsRound{Bytes_sketch: uint64(len(compressed)), Serial_items: src.k, Transferred_items: src.k, Random_access: 0, Random_items: 0}

	select {
	case src.forward <- disttopk.DemuxObject{src.id, &FirstRound{localtop, compressed , first_round_access}}:
	case <-src.StopNotifier:
		return nil
	}

	if src.clrRound {
		thresh := uint32(0)
		cl_size := uint32(0)
		var topk_ids []int
		select {
		case obj := <-src.back:
			spec := obj.(ClrRoundSpec)
			thresh = spec.thresh
			cl_size = spec.cl_size
			if src.clrRoundTopkEstimate {
				topk_ids = spec.topk_ids
			}
		case <-src.StopNotifier:
			return nil
		}

		thresh_index := getThreshIndex(src.list, thresh)
		candidate_list := disttopk.NewItemList()
		if src.k < len(src.list)-1 && (thresh_index+1) > src.k {
			candidate_list = src.list[src.k : thresh_index+1]
		}
		candidate_list_map := candidate_list.AddToMap(nil)

		var estimatelist disttopk.ItemList
		estimatelist_map := make(map[int]bool)
		if src.clrRoundTopkEstimate {
			estimatelist = disttopk.NewItemList()
			if len(topk_ids) != src.k {
				panic("snh")
			}
			for _, id := range topk_ids {
				score, ok := candidate_list_map[id]
				if ok {
					item := disttopk.Item{id, score}
					estimatelist_map[id] = true
					estimatelist = append(estimatelist, item)
				}
			}
		}
		//fmt.Println("Estimate list debug", len(topk_ids), len(estimatelist), len(candidate_list_map), len(candidate_list), topk_ids, candidate_list_map)

		clfRow := NewClfRow(int(cl_size))
		for _, item := range candidate_list {
			if !estimatelist_map[item.Id] {
				histo_cell := bh.HistoCellIndex(uint32(item.Score))
				//fmt.Println("Insert into row", histo_cell, item.Score)
				clfRow.Add(disttopk.IntKeyToByteKey(item.Id), uint32(histo_cell))
			}
		}

		/*
			list_in_row := disttopk.NewItemList()
			//fmt.Println("before row ass", src.k, len(src.list)-1)
			if src.k < len(src.list)-1 && (thresh_index+1) > src.k {
				list_in_row = src.list[src.k : thresh_index+1]
				//fmt.Println("List in row sz", len(list_in_row), thresh, src.list[thresh_index+1].Score, thresh_index)
				for _, item := range list_in_row {
					histo_cell := bh.HistoCellIndex(uint32(item.Score))
					//fmt.Println("Insert into row", histo_cell, item.Score)
					clfRow.Add(disttopk.IntKeyToByteKey(item.Id), uint32(histo_cell))
				}
			} */

			clf_round_access := &disttopk.AlgoStatsRound{Serial_items: len(candidate_list), Transferred_items: len(estimatelist), Random_access: 0, Random_items: 0}

		if SERIALIZE_CLF {
			payload, err := disttopk.SerializeObject(clfRow)
			if err != nil {
				panic(err)
			}
			compressed := disttopk.CompressBytes(payload)
			clf_round_access.Bytes_sketch = uint64(len(compressed))
			select {
			case src.forward <- disttopk.DemuxObject{src.id, &ClrRoundReply{compressed, estimatelist, clf_round_access}}:
			case <-src.StopNotifier:
				return nil
			}

		} else {
			panic("acces bytes sketch not implemented yet")
			select {
			case src.forward <- disttopk.DemuxObject{src.id, &ClrRoundReply{clfRow, estimatelist, clf_round_access}}:
			case <-src.StopNotifier:
				return nil
			}
		}

		var bitArray *disttopk.BitArray
		select {
		case obj := <-src.back:
			bitArray = &disttopk.BitArray{}
			ser := disttopk.DecompressBytes(obj.([]byte))
			if err := disttopk.DeserializeObject(bitArray, ser); err != nil {
				panic(err)
			}
		case <-src.StopNotifier:
			return nil
		}

		secondlist := disttopk.NewItemList()

		for _, item := range candidate_list {
			if !estimatelist_map[item.Id] {
				idx := clfRow.GetIndex(disttopk.IntKeyToByteKey(item.Id))
				if bitArray.Check(uint(idx)) {
					secondlist = secondlist.Append(item)
				}
			}
		}

		final_list_round_access := &disttopk.AlgoStatsRound{Serial_items: len(candidate_list), Transferred_items: len(secondlist)}
		//fmt.Println("Secondlist ", len(secondlist))

		select {
		case src.forward <- disttopk.DemuxObject{src.id, &FinalListReply{secondlist, final_list_round_access}}:
		case <-src.StopNotifier:
			return nil
		}

	} else {
		thresh := float64(0)
		select {
		case obj := <-src.back:
			thresh = obj.(float64)
		case <-src.StopNotifier:
			return nil
		}

		thresh_index := getThreshIndex(src.list, uint32(thresh))
		//fmt.Println("Peer ", src.id, " got ", thresh, " index ", thresh_index)
		//v.Score >= thresh included

		var secondlist disttopk.ItemList
		if thresh_index > src.k {
			secondlist = src.list[src.k : thresh_index+1]
		}
		final_list_round_access := &disttopk.AlgoStatsRound{Serial_items: len(secondlist), Transferred_items: len(secondlist)}
		select {
		case src.forward <- disttopk.DemuxObject{src.id, &FinalListReply{secondlist, final_list_round_access}}:
		case <-src.StopNotifier:
			return nil
		}
	}
	return nil
}

func NewCoord(k int, clrRound bool) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, k, clrRound, true, disttopk.AlgoStats{}}
}

type Coord struct {
	*stream.HardStopChannelCloser
	input        chan disttopk.DemuxObject
	backPointers []chan<- stream.Object
	//lists        [][]disttopk.Item
	FinalList            []disttopk.Item
	k                    int
	clrRound             bool
	clrRoundTopkEstimate bool
	Stats                disttopk.AlgoStats
}

func (src *Coord) Add(p *Peer) {
	id := len(src.backPointers)
	back := make(chan stream.Object, 3)
	src.backPointers = append(src.backPointers, back)
	p.id = id
	p.back = back
	p.forward = src.input
}

func (src *Coord) Run() error {
	defer func() {
		for _, ch := range src.backPointers {
			close(ch)
		}
	}()

	m := make(map[int]float64)
	mresp := make(map[int]int)
	src_resp_map := make(map[int]map[int]bool) //src_id to map of item.Id =>bool
	bh_map := make(map[int]*disttopk.BloomHistogramKlee)

	nnodes := len(src.backPointers)
	thresh := 0.0
	items := 0
	bh_bytes := 0

	access_stats := &disttopk.AlgoStats{}

	round_1_stats := disttopk.NewAlgoStatsRoundUnion()
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			fr := dobj.Obj.(*FirstRound)
			round_1_stats.AddPeerStats(*fr.stats)
			id := dobj.Id
			il := fr.list
			items += len(il)
			m = il.AddToMap(m)
			mresp = il.AddToCountMap(mresp)
			resp_map := make(map[int]bool)
			for _, v := range il {
				resp_map[v.Id] = true
			}
			src_resp_map[id] = resp_map

			bh_bytes += len(fr.bh)
			bhs := disttopk.DecompressBytes(fr.bh)
			bh := &disttopk.BloomHistogramKlee{}
			if err := disttopk.DeserializeObject(bh, bhs); err != nil {
				panic(err)
			}
			bh_map[id] = bh

		case <-src.StopNotifier:
			return nil

		}
	}
	access_stats.AddRound(*round_1_stats)

	il_est := disttopk.MakeItemList(m)
	//this becomes an estimate
	for k, v := range il_est {
		for src_id, resp_map := range src_resp_map {
			_, ok := resp_map[v.Id]
			if !ok {
				missing_value := bh_map[src_id].Query(disttopk.IntKeyToByteKey(v.Id))
				v.Score += float64(missing_value)
				il_est[k] = v
			}
		}
	}

	il_est.Sort()
	if len(il_est) < src.k {
		fmt.Println("ERROR k less than list")
	}
	thresh = il_est[src.k-1].Score
	localthresh := thresh / float64(nnodes)
	bytesRound := (items * disttopk.RECORD_SIZE) + bh_bytes
	fmt.Println("Round 1 klee: got ", items, " items, thresh ", thresh, ", local thresh will be ", localthresh, " bytes used", bytesRound)
	bytes := bytesRound

	bytesRound = 0
	round_2_stats := disttopk.NewAlgoStatsRoundUnion()
	if src.clrRound {
		src.Stats.Rounds = 3
		bytes_clround := 0
		max_size_candidate_list := uint32(0)
		for _, bh := range bh_map {
			bh_mscl := bh.MaxSizeCandidateList(uint32(localthresh))
			if bh_mscl > max_size_candidate_list {
				max_size_candidate_list = bh_mscl
			}
		}

		eps := 0.06
		load_factor := 1.0 / eps
		size := uint32(load_factor * float64(max_size_candidate_list))
		//fmt.Println("Coord: Thresh size", localthresh, size, load_factor, max_size_candidate_list)

		var topk_ids []int
		topk_ids_bytes := 0
		if src.clrRoundTopkEstimate {
			topk_ids = make([]int, src.k)
			for k, item := range il_est[:src.k] {
				topk_ids[k] = item.Id
			}
			topk_ids_bytes = disttopk.RECORD_INDEX_SIZE * src.k
		}

		round_clf_stats := disttopk.NewAlgoStatsRoundUnion()
		for _, ch := range src.backPointers {
			round_clf_stats.AddPeerStats(disttopk.AlgoStatsRound{Bytes_sketch: uint64(8+topk_ids_bytes)})
			select {
			case ch <- ClrRoundSpec{uint32(localthresh), size, topk_ids}:
			case <-src.StopNotifier:
				return nil
			}
		}
		bytes_clround += nnodes*(8 + topk_ids_bytes)

		clf_map := make(map[int]*ClfRow)
		estimate_items := 0
		for cnt := 0; cnt < nnodes; cnt++ {
			select {
			case dobj := <-src.input:
				var clr *ClfRow
				clr_reply := dobj.Obj.(*ClrRoundReply)
				round_clf_stats.AddPeerStats(*clr_reply.stats)
				if SERIALIZE_CLF {
					compressed_payload := clr_reply.payload.([]byte)
					bytes_clround += len(compressed_payload)
					payload := disttopk.DecompressBytes(compressed_payload)
					clr = &ClfRow{}
					if err := disttopk.DeserializeObject(clr, payload); err != nil {
						panic(err)
					}
				} else {
					clr = clr_reply.payload.(*ClfRow)
				}
				id := dobj.Id
				clf_map[id] = clr
				if src.clrRoundTopkEstimate {
					il := clr_reply.list
					m = il.AddToMap(m)
					bytes_clround += len(il) * disttopk.RECORD_SIZE
					estimate_items += len(il)
				}
			case <-src.StopNotifier:
				return nil

			}
		}

		bitArray := disttopk.NewBitArray(uint(size))

		/* this is not in paper but makes no sense without it
		we need to add data from first list to the filter*/
		recv_by_histo_index := make(map[int]uint32)
		clfRowForHashing := NewClfRow(int(size))
		for id, score := range m {
			hash_index := clfRowForHashing.GetIndex(disttopk.IntKeyToByteKey(id))
			recv_by_histo_index[int(hash_index)] += uint32(score)
		}
		count_idx := 0
		for clf_idx := 0; clf_idx < int(size); clf_idx++ {
			ub_sum := recv_by_histo_index[clf_idx]
			for peer_id, row := range clf_map {
				if row.HasHistoCellIndex(clf_idx) {
					histo_idx := int(row.QueryHistoCellIndex(clf_idx))
					//fmt.Println("histo_idx", histo_idx, bh_map[peer_id].Len())
					ub := bh_map[peer_id].GetUpperBoundByIndex(histo_idx)
					ub_sum += ub
				}
			}
			//fmt.Println("ubsum", ub_sum)
			if ub_sum > uint32(thresh) {
				bitArray.Set(uint(clf_idx))
				count_idx++
			}
		}
		//fmt.Println("count idx", count_idx, size, len(clf_map))

		bitArraySer, err := disttopk.SerializeObject(bitArray)
		if err != nil {
			panic(err)
		}
		compressedBitArray := disttopk.CompressBytes(bitArraySer)
		for _, ch := range src.backPointers {
			round_clf_stats.AddPeerStats(disttopk.AlgoStatsRound{Bytes_sketch:uint64(len(compressedBitArray))})
			select {
			case ch <- compressedBitArray:
			case <-src.StopNotifier:
				return nil
			}
		}
		access_stats.AddRound(*round_clf_stats)
		bytes_clround += len(compressedBitArray) * nnodes
		fmt.Println("Round CLF klee: got ", estimate_items, " estimate items. size filter", size, " round:", bytes_clround)
		bytes += bytes_clround
		//bytes_round += bytes_clround
	} else {
		src.Stats.Rounds = 2
		for _, ch := range src.backPointers {
			round_2_stats.AddPeerStats(disttopk.AlgoStatsRound{Bytes_sketch:uint64(4)})
			select {
			case ch <- localthresh:
			case <-src.StopNotifier:
				return nil
			}
		}
		bytesRound += (4 * nnodes)
	}
	il := disttopk.MakeItemList(m)

	round2items := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			flr := dobj.Obj.(*FinalListReply)
			round_2_stats.AddPeerStats(*flr.stats)
			il := flr.payload
			round2items += len(il)
			m = il.AddToMap(m)
			mresp = il.AddToCountMap(mresp)
		case <-src.StopNotifier:
			return nil
		}
	}
	access_stats.AddRound(*round_2_stats)

	bytesRound += round2items * disttopk.RECORD_SIZE

	il = disttopk.MakeItemList(m)
	il.Sort()
	/*if len(il) < src.k {
		fmt.Println("ERROR k less than list")
	}
	secondthresh := il[src.k-1].Score*/

	fmt.Println("Round 2 klee: got ", round2items, " items. bytes in round: ", bytesRound)
	bytes += bytesRound
	src.Stats = *access_stats
	src.Stats.Bytes_transferred = uint64(bytes)

	il = disttopk.MakeItemList(m)
	il.Sort()
	//fmt.Println("Sorted Global List: ", il[:src.k])
	if disttopk.OUTPUT_RESP {
		for _, it := range il[:src.k] {
			fmt.Println("Resp: ", it.Id, it.Score, mresp[it.Id])
		}
	}
	src.FinalList = il
	return nil
}
