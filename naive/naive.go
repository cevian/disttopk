package naive

import (
	"time"

	"github.com/cevian/go-stream/stream"
)
import "github.com/cevian/disttopk"

import (
	"encoding/gob"
	"errors"
	"fmt"
)

func NewNaivePeer(list []disttopk.Item, cutoff int) *NaivePeer {
	return &NaivePeer{stream.NewHardStopChannelCloser(), nil, nil, list, 0, cutoff}
}

type InitRound struct {
	Id int
}

type NaivePeer struct {
	*stream.HardStopChannelCloser
	forward chan<- stream.Object
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

	init := <-src.back
	src.id = init.(InitRound).Id

	//src.list.Sort()
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
	return &NaiveCoord{stream.NewHardStopChannelCloser(), make(chan stream.Object, 3), make([]chan<- stream.Object, 0), nil, nil, cutoff, disttopk.AlgoStats{}}
}

type NaiveCoord struct {
	*stream.HardStopChannelCloser
	input        chan stream.Object
	backPointers []chan<- stream.Object
	lists        []disttopk.ItemList
	FinalList    []disttopk.Item
	cutoff       int
	Stats        disttopk.AlgoStats
}

func (src *NaiveCoord) Add(peer stream.Operator) {
	p := peer.(*NaivePeer)
	back := make(chan stream.Object, 3)
	src.backPointers = append(src.backPointers, back)
	p.back = back
	p.forward = src.input
}

func (src *NaiveCoord) Run() error {
	defer func() {
		for _, ch := range src.backPointers {
			close(ch)
		}
	}()

	start := time.Now()
	for i, ch := range src.backPointers {
		select {
		case ch <- InitRound{i}:
		case <-src.StopNotifier:
			panic("wtf!")
		}
	}

	src.lists = make([]disttopk.ItemList, len(src.backPointers))
	cnt := 0
	items := 0
	round_1_stats := disttopk.NewAlgoStatsRoundUnion()

	for {
		select {
		case obj := <-src.input:
			dobj := obj.(disttopk.DemuxObject)
			cnt++
			list := dobj.Obj.(disttopk.ItemList)
			src.lists[dobj.Id] = list
			round_stat_peer := disttopk.AlgoStatsRound{Transferred_items: len(list), Serial_items: len(list)}
			round_1_stats.AddPeerStats(round_stat_peer)
			items += len(list)
			if cnt == len(src.backPointers) {
				src.Stats.AddRound(*round_1_stats)
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
				src.Stats.Took = time.Since(start)
				return nil
			}
		case <-src.StopNotifier:
			return nil
		}
	}
}

func (t *NaivePeer) SetNetwork(readCh chan stream.Object, writeCh chan stream.Object) {
	t.back = readCh
	t.forward = writeCh
}

func (src *NaiveCoord) AddNetwork(channel chan stream.Object) {
	src.backPointers = append(src.backPointers, channel)
}

func (src *NaiveCoord) GetFinalList() disttopk.ItemList {
	return src.FinalList
}
func (src *NaiveCoord) GetStats() disttopk.AlgoStats {
	return src.Stats
}
func (t *NaiveCoord) InputChannel() chan stream.Object {
	return t.input
}

func RegisterGob() {
	gob.Register(InitRound{})
	gob.Register(disttopk.DemuxObject{})
	gob.Register(disttopk.ItemList{})
}
