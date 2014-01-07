package klee

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	"fmt"
)

func NewPeer(list disttopk.ItemList, k int) *Peer {
	return &Peer{stream.NewHardStopChannelCloser(), nil, nil, list, k, 0}
}

type Peer struct {
	*stream.HardStopChannelCloser
	forward chan<- disttopk.DemuxObject
	back    <-chan stream.Object
	list    disttopk.ItemList
	k       int
	id      int
}

type FirstRound struct {
	list disttopk.ItemList
	bh   []byte
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

	thresh := float64(0)
	select {
	case obj := <-src.back:
		thresh = obj.(float64)
	case <-src.StopNotifier:
		return nil
	}

	index := 0
	for k, v := range src.list {
		index = k
		if v.Score < thresh {
			break
		}
	}
	//fmt.Println("Peer ", src.id, " got ", thresh, " index ", index)
	//v.Score >= thresh included

	var secondlist disttopk.ItemList
	if index > src.k {
		secondlist = src.list[src.k:index]
	}
	select {
	case src.forward <- disttopk.DemuxObject{src.id, secondlist}:
	case <-src.StopNotifier:
		return nil
	}

	return nil
}

func NewCoord(k int) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, k, disttopk.AlgoStats{}}
}

type Coord struct {
	*stream.HardStopChannelCloser
	input        chan disttopk.DemuxObject
	backPointers []chan<- stream.Object
	//lists        [][]disttopk.Item
	FinalList []disttopk.Item
	k         int
	Stats   disttopk.AlgoStats
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

	for _, ch := range src.backPointers {
		select {
		case ch <- localthresh:
		case <-src.StopNotifier:
			return nil
		}
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

	il = disttopk.MakeItemList(m)
	il.Sort()
	/*if len(il) < src.k {
		fmt.Println("ERROR k less than list")
	}
	secondthresh := il[src.k-1].Score*/

	bytesRound = round2items*disttopk.RECORD_SIZE + (nnodes * 4)
	fmt.Println("Round 2 klee: got ", round2items, " items. bytes in round", bytesRound)
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

