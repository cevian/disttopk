package klee

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	"fmt"
)

const SERIALIZE_CLF = true

func NewPeer(list disttopk.ItemList, k int, clrRound bool) *Peer {
	return &Peer{stream.NewHardStopChannelCloser(), nil, nil, list, k, 0, clrRound}
}

type Peer struct {
	*stream.HardStopChannelCloser
	forward  chan<- disttopk.DemuxObject
	back     <-chan stream.Object
	list     disttopk.ItemList
	k        int
	id       int
	clrRound bool
}

type FirstRound struct {
	list disttopk.ItemList
	bh   []byte
}

type ClrRoundSpec struct {
	thresh  uint32
	cl_size uint32
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
	src.list.Sort()
	//fmt.Println("Sort", src.list[:10])

	if src.k > len(src.list) {
		fmt.Println("warning tput: list shorter than k")
		src.k = len(src.list)
	}

	localtop := src.list[:src.k]
	bh := disttopk.NewBloomHistogramKlee()
	bh.CreateFromList(src.list, 10)
	bhs, err := disttopk.SerializeObject(bh)
	if err != nil {
		panic(err)
	}
	select {
	case src.forward <- disttopk.DemuxObject{src.id, &FirstRound{localtop, bhs}}:
	case <-src.StopNotifier:
		return nil
	}

	if src.clrRound {
		thresh := uint32(0)
		cl_size := uint32(0)
		select {
		case obj := <-src.back:
			spec := obj.(ClrRoundSpec)
			thresh = spec.thresh
			cl_size = spec.cl_size
		case <-src.StopNotifier:
			return nil
		}

		clfRow := NewClfRow(int(cl_size))
		thresh_index := getThreshIndex(src.list, thresh)
		list_in_row := src.list[src.k : thresh_index+1]
		for _, item := range list_in_row {
			histo_cell := bh.HistoCellIndex(uint32(item.Score))
			//fmt.Println("Insert into row", histo_cell, item.Score)
			clfRow.Add(disttopk.IntKeyToByteKey(item.Id), uint32(histo_cell))
		}

		if SERIALIZE_CLF {
			payload, err := disttopk.SerializeObject(clfRow)
			if err != nil {
				panic(err)
			}
			select {
			case src.forward <- disttopk.DemuxObject{src.id, payload}:
			case <-src.StopNotifier:
				return nil
			}

		} else {
			select {
			case src.forward <- disttopk.DemuxObject{src.id, clfRow}:
			case <-src.StopNotifier:
				return nil
			}
		}

		var bitArray *disttopk.BitArray
		select {
		case obj := <-src.back:
			bitArray = obj.(*disttopk.BitArray)
		case <-src.StopNotifier:
			return nil
		}

		secondlist := disttopk.NewItemList()

		for _, item := range list_in_row {
			idx := clfRow.GetIndex(disttopk.IntKeyToByteKey(item.Id))
			if bitArray.Check(uint(idx)) {
				secondlist = secondlist.Append(item)
			}
		}

		//fmt.Println("Secondlist ", len(secondlist))

		select {
		case src.forward <- disttopk.DemuxObject{src.id, secondlist}:
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
		//fmt.Println("Peer ", src.id, " got ", thresh, " index ", index)
		//v.Score >= thresh included

		var secondlist disttopk.ItemList
		if thresh_index > src.k {
			secondlist = src.list[src.k : thresh_index+1]
		}
		select {
		case src.forward <- disttopk.DemuxObject{src.id, secondlist}:
		case <-src.StopNotifier:
			return nil
		}
	}
	return nil
}

func NewCoord(k int, clrRound bool) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, k, clrRound, disttopk.AlgoStats{}}
}

type Coord struct {
	*stream.HardStopChannelCloser
	input        chan disttopk.DemuxObject
	backPointers []chan<- stream.Object
	//lists        [][]disttopk.Item
	FinalList []disttopk.Item
	k         int
	clrRound  bool
	Stats     disttopk.AlgoStats
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
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			fr := dobj.Obj.(*FirstRound)
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

			bhs := fr.bh
			bh_bytes += len(bhs)
			bh := &disttopk.BloomHistogramKlee{}
			if err := disttopk.DeserializeObject(bh, bhs); err != nil {
				panic(err)
			}
			bh_map[id] = bh

		case <-src.StopNotifier:
			return nil

		}
	}

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
	if src.clrRound {
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
		fmt.Println("Coord: Thresh size", localthresh, size, load_factor, max_size_candidate_list)

		for _, ch := range src.backPointers {
			select {
			case ch <- ClrRoundSpec{uint32(localthresh), size}:
			case <-src.StopNotifier:
				return nil
			}
		}
		bytes_clround += nnodes * 8

		clf_map := make(map[int]*ClfRow)
		for cnt := 0; cnt < nnodes; cnt++ {
			select {
			case dobj := <-src.input:
				var clr *ClfRow

				if SERIALIZE_CLF {
					payload := dobj.Obj.([]byte)
					clr = &ClfRow{}
					if err := disttopk.DeserializeObject(clr, payload); err != nil {
						panic(err)
					}
					bytes_clround += len(payload)
				} else {
					clr = dobj.Obj.(*ClfRow)
				}
				id := dobj.Id
				clf_map[id] = clr
			case <-src.StopNotifier:
				return nil

			}
		}

		bitArray := disttopk.NewBitArray(uint(size))
		for clf_idx := 0; clf_idx < int(size); clf_idx++ {
			ub_sum := uint32(0)
			for peer_id, row := range clf_map {
				if row.HasHistoCellIndex(clf_idx) {
					histo_idx := int(row.QueryHistoCellIndex(clf_idx))
					//fmt.Println("histo_idx", histo_idx, bh_map[peer_id].Len())
					ub := bh_map[peer_id].GetUpperBoundByIndex(histo_idx)
					ub_sum += ub
				}
			}
			//fmt.Println("Ubsum", ub_sum, thresh)
			if ub_sum > uint32(thresh) {
				bitArray.Set(uint(clf_idx))
			}
		}

		for _, ch := range src.backPointers {
			select {
			case ch <- bitArray:
			case <-src.StopNotifier:
				return nil
			}
		}
		bytes_clround += (int(bitArray.NumBits()/8) + 1) * nnodes
		fmt.Println("Round CLF klee: got bytes in round: ", bytes_clround)
		bytes += bytes_clround
		//bytes_round += bytes_clround
	} else {
		for _, ch := range src.backPointers {
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
			il := dobj.Obj.(disttopk.ItemList)
			round2items += len(il)
			m = il.AddToMap(m)
			mresp = il.AddToCountMap(mresp)
		case <-src.StopNotifier:
			return nil
		}
	}

	bytesRound += round2items * disttopk.RECORD_SIZE

	il = disttopk.MakeItemList(m)
	il.Sort()
	/*if len(il) < src.k {
		fmt.Println("ERROR k less than list")
	}
	secondthresh := il[src.k-1].Score*/

	fmt.Println("Round 2 klee: got ", round2items, " items. bytes in round: ", bytesRound)
	bytes += bytesRound
	src.Stats.BytesTransferred = uint64(bytes)

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
