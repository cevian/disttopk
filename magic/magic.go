package magic

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	//"errors"
	"fmt"
)

func NewPeer(list []disttopk.Item, k int, groundtruth []disttopk.Item) *Peer {
	return &Peer{stream.NewHardStopChannelCloser(), nil, nil, list, 0, k, groundtruth}
}

type Peer struct {
	*stream.HardStopChannelCloser
	forward     chan<- disttopk.DemuxObject
	back        <-chan stream.Object
	list        disttopk.ItemList
	id          int
	k           int
	groundTruth disttopk.ItemList
}

func (src *Peer) Run() error {
	//defer close(src.forward)
	//	var rcv <-chan stream.Object
	//	rcv = nil
	//	sent := false

	sent := make(map[int]bool)
	//src.list.Sort()
	list := src.list
	if src.k > 0 && src.k < len(src.list) {
		list = src.list[:src.k]
	}
	for _, item := range list {
		sent[item.Id] = true
	}

	select {
	case src.forward <- disttopk.DemuxObject{src.id, list}:
	case <-src.StopNotifier:
		return nil
	}

	src.groundTruth.Sort()
	list = disttopk.NewItemList()
	for i := 0; i < src.k && i < len(src.groundTruth); i++ {
		item := src.groundTruth[i]
		if !sent[item.Id] {
			list.Append(item)
		}
	}

	select {
	case src.forward <- disttopk.DemuxObject{src.id, list}:
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

	items := 0
	nnodes := len(src.backPointers)

	m := make(map[int]float64)
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			list := dobj.Obj.(disttopk.ItemList)
			list.AddToMap(m)
			src.Stats.Serial_items += len(list)
			items += len(list)
		case <-src.StopNotifier:
			return nil
		}
	}

	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			list := dobj.Obj.(disttopk.ItemList)
			list.AddToMap(m)
			src.Stats.Random_items += len(list)
			items += len(list)

		case <-src.StopNotifier:
			return nil
		}
	}

	src.Stats.Bytes_transferred = uint64(items * disttopk.RECORD_SIZE)

	il := disttopk.MakeItemList(m)
	il.Sort()

	src.Stats.Rounds = 2
	if disttopk.OUTPUT_RESP {
		for _, it := range il[:10] {
			fmt.Println("Resp: ", it.Id, it.Score) //, mresp[it.Id])
		}
	}
	src.FinalList = il
	return nil
}
