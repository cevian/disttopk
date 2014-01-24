package naive

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	"errors"
	"fmt"
)

func NewNaivePeer(list []disttopk.Item, cutoff int) *NaivePeer {
	return &NaivePeer{stream.NewHardStopChannelCloser(), nil, nil, list, 0, cutoff}
}

type NaivePeer struct {
	*stream.HardStopChannelCloser
	forward chan<- disttopk.DemuxObject
	back    <-chan stream.Object
	list    disttopk.ItemList
	id      int
	cutoff  int
}

func (src *NaivePeer) Run() error {
	//defer close(src.forward)
	var rcv <-chan stream.Object
	rcv = nil
	sent := false

	src.list.Sort()
	list := src.list
	if src.cutoff > 0 && src.cutoff < len(src.list) {
		list = src.list[:src.cutoff]
	}

	for {
		if sent == false {
			rcv = nil
		} else {
			rcv = src.back
		}
		select {
		case src.forward <- disttopk.DemuxObject{src.id, list}:
			return nil
		case <-rcv:
			return errors.New("No second round in naive implementation")
		case <-src.StopNotifier:
			return nil
		}
	}
}

func NewNaiveCoord(cutoff int) *NaiveCoord {
	return &NaiveCoord{stream.NewHardStopChannelCloser(), make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, nil, cutoff, disttopk.AlgoStats{}}
}

type NaiveCoord struct {
	*stream.HardStopChannelCloser
	input        chan disttopk.DemuxObject
	backPointers []chan<- stream.Object
	lists        []disttopk.ItemList
	FinalList    []disttopk.Item
	cutoff       int
	Stats        disttopk.AlgoStats
}

func (src *NaiveCoord) Add(p *NaivePeer) {
	id := len(src.backPointers)
	back := make(chan stream.Object, 3)
	src.backPointers = append(src.backPointers, back)
	p.id = id
	p.back = back
	p.forward = src.input
}

func (src *NaiveCoord) Run() error {
	defer func() {
		for _, ch := range src.backPointers {
			close(ch)
		}
	}()

	src.lists = make([]disttopk.ItemList, len(src.backPointers))
	cnt := 0
	items := 0
	for {
		select {
		case dobj := <-src.input:
			cnt++
			list := dobj.Obj.(disttopk.ItemList)
			src.lists[dobj.Id] = list
			src.Stats.Serial_items += len(list)
			items += len(list)
			if cnt == len(src.backPointers) {
				m := make(map[int]float64)
				for _, l := range src.lists {
					il := disttopk.ItemList(l)
					il.AddToMap(m)
				}

				il := disttopk.MakeItemList(m)

				il.Sort()

				//				fmt.Printf("Total bytes naive (cutoff=%d): %E\n", src.cutoff, float64(items*disttopk.RECORD_SIZE))
				src.Stats.Bytes_transferred = uint64(items * disttopk.RECORD_SIZE)
				src.Stats.Rounds = 1
				if disttopk.OUTPUT_RESP {
					for _, it := range il[:10] {
						fmt.Println("Resp: ", it.Id, it.Score) //, mresp[it.Id])
					}
				}
				src.FinalList = il
				return nil
			}
		case <-src.StopNotifier:
			return nil
		}
	}
}
