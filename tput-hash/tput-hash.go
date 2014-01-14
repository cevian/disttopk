package tput_hash

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
	list  disttopk.ItemList
	count uint32
}

type FirstRoundResponse struct {
	thresh    uint32
	arraySize uint32
}

type SecondRound struct {
	cha             *CountHashArray
	items_looked_at uint
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
	select {
	case src.forward <- disttopk.DemuxObject{src.id, FirstRound{localtop, uint32(len(src.list))}}:
	case <-src.StopNotifier:
		return nil
	}

	thresh := uint32(0) // in paper: T
	arraySize := uint(0)
	select {
	case obj := <-src.back:
		frr := obj.(FirstRoundResponse)
		thresh = frr.thresh
		arraySize = uint(frr.arraySize)
	case <-src.StopNotifier:
		return nil
	}

	last_index_to_send := 0
	for k, v := range src.list {
		if uint32(v.Score) < thresh {
			break
		}
		last_index_to_send = k
	}

	cha := NewCountHashArray(arraySize)

	//fmt.Println("Peer ", src.id, " got ", thresh, " index ", index, "k", src.k, "list[index+1].score", src.list[index+1].Score)
	//v.Score >= thresh included

	items_looked_at := uint(0)
	if last_index_to_send >= src.k {
		for _, list_item := range src.list[src.k : last_index_to_send+1] {
			items_looked_at += 1
			cha.Add(disttopk.IntKeyToByteKey(list_item.Id), uint(list_item.Score))
		}
	}
	select {
	case src.forward <- disttopk.DemuxObject{src.id, SecondRound{cha, items_looked_at}}:
	case <-src.StopNotifier:
		return nil
	}

	var bloom *disttopk.Bloom
	select {
	case obj := <-src.back:
		bloom = obj.(*disttopk.Bloom)
	case <-src.StopNotifier:
		return nil
	}

	exactlist := make([]disttopk.Item, 0)
	for _, li := range src.list[src.k:] {
		if bloom.Query(disttopk.IntKeyToByteKey(li.Id)) {
			exactlist = append(exactlist, disttopk.Item{li.Id, li.Score})
		}
	}

	select {
	case src.forward <- disttopk.DemuxObject{src.id, disttopk.ItemList(exactlist)}:
	case <-src.StopNotifier:
		return nil
	}
	return nil
}

func NewCoord(k int) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, nil, k, disttopk.AlgoStats{}, 0.5}
}

type Coord struct {
	*stream.HardStopChannelCloser
	input        chan disttopk.DemuxObject
	backPointers []chan<- stream.Object
	lists        [][]disttopk.Item
	FinalList    []disttopk.Item
	k            int
	Stats        disttopk.AlgoStats
	alpha        float64
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

	access_stats := &disttopk.AlgoStats{}
	nnodes := len(src.backPointers)
	thresh := 0.0
	items := 0
	items_at_peers := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			fr := dobj.Obj.(FirstRound)
			il := fr.list
			items_at_peers += int(fr.count)
			access_stats.Serial_items += len(il)
			items += len(il)
			m = il.AddToMap(m)
			mresp = il.AddToCountMap(mresp)
		case <-src.StopNotifier:
			return nil

		}
	}

	il := disttopk.MakeItemList(m)
	il.Sort()
	if len(il) < src.k {
		fmt.Println("ERROR k less than list")
	}
	thresh = il[src.k-1].Score
	localthresh := uint32((thresh / float64(nnodes)) * src.alpha)
	bytesRound := items*disttopk.RECORD_SIZE + 4
	fmt.Println("Round 1 tput: got ", items, " items, thresh ", thresh, ", local thresh will be ", localthresh, " bytes used", bytesRound)
	bytes := bytesRound

	for _, ch := range src.backPointers {
		select {
		case ch <- FirstRoundResponse{uint32(localthresh), uint32(items_at_peers)}:
		case <-src.StopNotifier:
			return nil
		}
	}

	cha := NewCountHashArray(uint(items_at_peers))
	hash_responses := make(map[int]int)
	for _, list_item := range il {
		cha.Add(disttopk.IntKeyToByteKey(list_item.Id), uint(list_item.Score))
		index := int(cha.GetIndex(disttopk.IntKeyToByteKey(list_item.Id)))
		responses := mresp[list_item.Id]
		if responses > hash_responses[index] {
			hash_responses[index] = responses
		}
	}

	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			sr := dobj.Obj.(SecondRound)
			cha_got := sr.cha
			access_stats.Serial_items += int(sr.items_looked_at)
			cha.Merge(cha_got)
			cha_got.AddResponses(hash_responses)
		case <-src.StopNotifier:
			return nil
		}
	}

	secondthresh := cha.GetKthCount(src.k)

	if secondthresh < uint(thresh) {
		panic(fmt.Sprintln("Something went wrong", thresh, secondthresh))
	}

	bloom := cha.GetBloomFilter(secondthresh, hash_responses, uint(localthresh), uint(nnodes))

	//bytesRound = round2items*disttopk.RECORD_SIZE + (nnodes * 4)
	//fmt.Println("Round 2 tput: got ", round2items, " items, thresh ", secondthresh, ", unique items fetching ", len(ids), " bytes ", bytesRound)
	bytes += bytesRound

	for _, ch := range src.backPointers {
		select {
		case ch <- bloom:
		case <-src.StopNotifier:
			return nil
		}
	}

	round3items := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			il := dobj.Obj.(disttopk.ItemList)
			access_stats.Random_items += len(il)
			access_stats.Random_access += len(il)
			m = il.AddToMap(m)
			round3items += len(il)
			mresp = il.AddToCountMap(mresp)
		case <-src.StopNotifier:
			return nil
		}
	}

	//bytesRound = round3items*disttopk.RECORD_SIZE + (nnodes * len(ids) * 4)
	fmt.Println("Round 3 tput: got ", round3items, " items, bytes ", bytesRound)
	bytes += bytesRound
	src.Stats.Bytes_transferred = uint64(bytes)
	src.Stats.Merge(*access_stats)

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
