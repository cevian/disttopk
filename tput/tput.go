package tput

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
	case src.forward <- disttopk.DemuxObject{src.id, localtop}:
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

	//fmt.Println("Peer ", src.id, " got ", thresh, " index ", index, "k", src.k, "list[index+1].score", src.list[index+1].Score)
	//v.Score >= thresh included

	var secondlist disttopk.ItemList
	if index >= src.k {
		secondlist = src.list[src.k : index+1]
	}
	select {
	case src.forward <- disttopk.DemuxObject{src.id, secondlist}:
	case <-src.StopNotifier:
		return nil
	}

	var ids []int
	select {
	case obj := <-src.back:
		ids = obj.([]int)
	case <-src.StopNotifier:
		return nil
	}

	m := src.list.AddToMap(nil)
	ri := src.list.AddToReverseIndexMap(nil)

	exactlist := make([]disttopk.Item, 0)
	for _, id := range ids {
		index, ok := ri[id]
		score, ok := m[id]
		if ok && score <= thresh && index >= src.k { //haven't sent before
			exactlist = append(exactlist, disttopk.Item{id, m[id]})
		}
	}
	//fmt.Println("Peer ", src.id, " got ", ids, thresh)

	select {
	case src.forward <- disttopk.DemuxObject{src.id, disttopk.ItemList(exactlist)}:
	case <-src.StopNotifier:
		return nil
	}
	return nil
}

func NewCoord(k int) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, nil, k, disttopk.AlgoStats{}}
}

type Coord struct {
	*stream.HardStopChannelCloser
	input        chan disttopk.DemuxObject
	backPointers []chan<- stream.Object
	lists        [][]disttopk.Item
	FinalList    []disttopk.Item
	k            int
	Stats        disttopk.AlgoStats
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

	nnodes := len(src.backPointers)
	thresh := 0.0
	items := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			il := dobj.Obj.(disttopk.ItemList)
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
	localthresh := thresh / float64(nnodes)
	bytesRound := items * disttopk.RECORD_SIZE
	fmt.Println("Round 1 tput: got ", items, " items, thresh ", thresh, ", local thresh will be ", localthresh, " bytes used", bytesRound)
	bytes := bytesRound

	for _, ch := range src.backPointers {
		select {
		case ch <- localthresh:
		case <-src.StopNotifier:
			return nil
		}
	}

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
	if len(il) < src.k {
		fmt.Println("ERROR k less than list")
	}
	secondthresh := il[src.k-1].Score

	ids := make([]int, 0)
	for id, score := range m {
		resp := mresp[id]
		missing := nnodes - resp
		upperBound := (float64(missing) * thresh) + score
		if upperBound >= secondthresh {
			ids = append(ids, id)
		}
	}

	bytesRound = round2items*disttopk.RECORD_SIZE + (nnodes * 4)
	fmt.Println("Round 2 tput: got ", round2items, " items, thresh ", secondthresh, ", unique items fetching ", len(ids), " bytes ", bytesRound)
	bytes += bytesRound

	for _, ch := range src.backPointers {
		select {
		case ch <- ids:
		case <-src.StopNotifier:
			return nil
		}
	}

	round3items := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			il := dobj.Obj.(disttopk.ItemList)
			m = il.AddToMap(m)
			round3items += len(il)
			mresp = il.AddToCountMap(mresp)
		case <-src.StopNotifier:
			return nil
		}
	}

	bytesRound = round3items*disttopk.RECORD_SIZE + (nnodes * len(ids) * 4)
	fmt.Println("Round 3 tput: got ", round3items, " items, bytes ", bytesRound)
	bytes += bytesRound
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
