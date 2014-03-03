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
	//fmt.Println("list", list)
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

	select {
	case <-src.back:
	case <-src.StopNotifier:
		return nil
	}

	//you cannot modify groundtruth at all. these run in parallel
	//src.groundTruth.Sort() <- race condition
	m := src.list.AddToMap(nil)
	list = disttopk.NewItemList()
	sent_first_round := 0
	for i := 0; i < src.k && i < len(src.groundTruth); i++ {
		id := src.groundTruth[i].Id
		score, ok := m[id]
		if !sent[id] {
			if ok {
				item := disttopk.Item{id, score}
				list = list.Append(item)
				if len(list) == 0 {
					panic("snh")
				}
			}
		} else {
			sent_first_round++
		}
	}

	//fmt.Println("Sent first round", sent_first_round, "Sending second round", len(list), float64((len(list)+sent_first_round))/float64(src.k))

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

	fmt.Println("Got round 1 items:", items)

	for _, ch := range src.backPointers {
		select {
		case ch <- true:
		case <-src.StopNotifier:
			return nil
		}
	}

	items2 := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			list := dobj.Obj.(disttopk.ItemList)
			list.AddToMap(m)
			src.Stats.Random_items += len(list)
			items += len(list)
			items2 += len(list)

		case <-src.StopNotifier:
			return nil
		}
	}
	fmt.Println("Got round 1 items:", items2)

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
