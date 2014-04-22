package tput

import (
	"encoding/gob"
	"time"

	"github.com/cevian/go-stream/stream"
)
import "github.com/cevian/disttopk"

import (
	"fmt"
)

type InitRound struct {
	Id int
}

func NewPeer(list disttopk.ItemList, k int) *Peer {
	return &Peer{stream.NewHardStopChannelCloser(), nil, nil, list, k, 0}
}

type Peer struct {
	*stream.HardStopChannelCloser
	forward chan<- stream.Object
	back    <-chan stream.Object
	list    disttopk.ItemList
	k       int
	id      int
}

func (src *Peer) Run() error {
	//defer close(src.forward)
	//src.list.Sort()
	//fmt.Println("Sort", src.list[:10])

	init := <-src.back
	src.id = init.(InitRound).Id

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

	last_index_to_send := 0
	for k, v := range src.list {
		if v.Score < thresh {
			break
		}
		last_index_to_send = k
	}

	//fmt.Println("Peer ", src.id, " got ", thresh, " index ", index, "k", src.k, "list[index+1].score", src.list[index+1].Score)
	//v.Score >= thresh included

	var secondlist disttopk.ItemList
	if last_index_to_send >= src.k {
		secondlist = src.list[src.k : last_index_to_send+1]
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
		if ok && score < thresh && index >= src.k { //haven't sent before
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
	return &Coord{stream.NewHardStopChannelCloser(), make(chan stream.Object, 3), make([]chan<- stream.Object, 0), nil, nil, k, disttopk.AlgoStats{}, 0.5}
}

type Coord struct {
	*stream.HardStopChannelCloser
	input        chan stream.Object
	backPointers []chan<- stream.Object
	lists        [][]disttopk.Item
	FinalList    []disttopk.Item
	k            int
	Stats        disttopk.AlgoStats
	alpha        float64
}

func (src *Coord) Add(peer stream.Operator) {
	p := peer.(*Peer)
	back := make(chan stream.Object, 3)
	src.backPointers = append(src.backPointers, back)
	p.back = back
	p.forward = src.input
}

func (src *Coord) Run() error {
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

	m := make(map[int]float64)
	mresp := make(map[int]int)

	access_stats := disttopk.NewAlgoStats()
	round_1_stats := disttopk.NewAlgoStatsRoundUnion()
	nnodes := len(src.backPointers)
	thresh := 0.0
	items := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case obj := <-src.input:
			dobj := obj.(disttopk.DemuxObject)
			il := dobj.Obj.(disttopk.ItemList)
			round_stat_peer := disttopk.AlgoStatsRound{Serial_items: len(il), Transferred_items: len(il)}
			round_1_stats.AddPeerStats(round_stat_peer)
			items += len(il)
			m = il.AddToMap(m)
			mresp = il.AddToCountMap(mresp)
		case <-src.StopNotifier:
			return nil

		}
	}
	access_stats.AddRound(*round_1_stats)

	il := disttopk.MakeItemList(m)
	il.Sort()
	if len(il) < src.k {
		fmt.Println("ERROR k less than list")
	}
	thresh = il[src.k-1].Score
	localthresh := (thresh / float64(nnodes)) * src.alpha
	bytesRound := items * disttopk.RECORD_SIZE
	fmt.Println("Round 1 tput: got ", items, " items, thresh ", thresh, ", local thresh will be ", localthresh, " bytes used", bytesRound)
	bytes := bytesRound

	round_2_stats := disttopk.NewAlgoStatsRoundUnion()
	for _, ch := range src.backPointers {
		round_2_stats.AddPeerStats(disttopk.AlgoStatsRound{Bytes_sketch: 4})
		select {
		case ch <- localthresh:
		case <-src.StopNotifier:
			return nil
		}
	}

	round2items := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case obj := <-src.input:
			dobj := obj.(disttopk.DemuxObject)
			il := dobj.Obj.(disttopk.ItemList)
			round_stat_peer := disttopk.AlgoStatsRound{Serial_items: len(il), Transferred_items: len(il)}
			round_2_stats.AddPeerStats(round_stat_peer)
			round2items += len(il)
			m = il.AddToMap(m)
			mresp = il.AddToCountMap(mresp)
		case <-src.StopNotifier:
			return nil
		}
	}
	access_stats.AddRound(*round_2_stats)

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

	round_3_stats := disttopk.NewAlgoStatsRoundUnion()
	for _, ch := range src.backPointers {
		round_3_stats.AddPeerStats(disttopk.AlgoStatsRound{Bytes_sketch: uint64(len(ids) * disttopk.RECORD_ID_SIZE)})
		select {
		case ch <- ids:
		case <-src.StopNotifier:
			return nil
		}
	}

	round3items := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case obj := <-src.input:
			dobj := obj.(disttopk.DemuxObject)
			il := dobj.Obj.(disttopk.ItemList)
			round_stat_peer := disttopk.AlgoStatsRound{Random_items: len(il), Random_access: len(il), Transferred_items: len(il)}
			round_3_stats.AddPeerStats(round_stat_peer)
			m = il.AddToMap(m)
			round3items += len(il)
			mresp = il.AddToCountMap(mresp)
		case <-src.StopNotifier:
			return nil
		}
	}
	access_stats.AddRound(*round_3_stats)

	bytesRound = round3items*disttopk.RECORD_SIZE + (nnodes * len(ids) * 4)
	fmt.Println("Round 3 tput: got ", round3items, " items, bytes ", bytesRound)
	bytes += bytesRound
	src.Stats = *access_stats
	src.Stats.Bytes_transferred = uint64(bytes)
	src.Stats.Rounds = 3

	il = disttopk.MakeItemList(m)
	il.Sort()
	//fmt.Println("Sorted Global List: ", il[:src.k])
	if disttopk.OUTPUT_RESP {
		for _, it := range il[:src.k] {
			fmt.Println("Resp: ", it.Id, it.Score, mresp[it.Id])
		}
	}
	src.FinalList = il
	src.Stats.Took = time.Since(start)
	return nil
}

func (t *Peer) SetNetwork(readCh chan stream.Object, writeCh chan stream.Object) {
	t.back = readCh
	t.forward = writeCh
}

func (src *Coord) AddNetwork(channel chan stream.Object) {
	src.backPointers = append(src.backPointers, channel)
}

func (src *Coord) GetFinalList() disttopk.ItemList {
	return src.FinalList
}
func (src *Coord) GetStats() disttopk.AlgoStats {
	return src.Stats
}
func (t *Coord) InputChannel() chan stream.Object {
	return t.input
}

func RegisterGob() {
	gob.Register(InitRound{})
	gob.Register(disttopk.DemuxObject{})
	gob.Register(disttopk.ItemList{})
}
