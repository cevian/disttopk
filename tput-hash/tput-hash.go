package tput_hash

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	"fmt"
)

const APPROXIMATE_T2 = false

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
	cha             []byte
	items_looked_at uint //only for serial access accounting
}

type ThirdRound struct {
	list            disttopk.ItemList
	items_looked_at uint //only for serial access accounting
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

	cha_ser, err := disttopk.SerializeObject(cha)
	if err != nil {
		panic(err)
	}

	cha_comp := disttopk.CompressBytes(cha_ser)
	//fmt.Println("Compressed size", len(cha_comp), items_looked_at)

	select {
	case src.forward <- disttopk.DemuxObject{src.id, SecondRound{cha_comp, items_looked_at}}:
	case <-src.StopNotifier:
		return nil
	}

	var bloom *disttopk.Bloom
	select {
	case obj := <-src.back:
		bloom_ser := disttopk.DecompressBytes(obj.([]byte))
		bloom = &disttopk.Bloom{}
		if err := disttopk.DeserializeObject(bloom, bloom_ser); err != nil {
			panic(err)
		}
	case <-src.StopNotifier:
		return nil
	}

	exactlist := make([]disttopk.Item, 0)
	items_looked_at = 0
	for _, li := range src.list[src.k:] {
		items_looked_at += 1
		if bloom.Query(disttopk.IntKeyToByteKey(li.Id)) {
			exactlist = append(exactlist, disttopk.Item{li.Id, li.Score})
		}
	}

	select {
	case src.forward <- disttopk.DemuxObject{src.id, ThirdRound{disttopk.ItemList(exactlist), items_looked_at}}:
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
	fmt.Println("Round 1 tput-hash: got ", items, " items, thresh ", thresh, ", local thresh will be ", localthresh, " cha size", items_at_peers, " bytes used", bytesRound)
	bytes := bytesRound

	bytesRound = 8 * nnodes
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

	bytes_cha := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			sr := dobj.Obj.(SecondRound)
			bytes_cha += len(sr.cha)
			cha_got_ser := disttopk.DecompressBytes(sr.cha)
			//bytes_cha += len(cha_got_ser)
			cha_got := &CountHashArray{}
			if err := disttopk.DeserializeObject(cha_got, cha_got_ser); err != nil {
				panic(err)
			}
			access_stats.Serial_items += int(sr.items_looked_at)

			cha.Merge(cha_got)
			cha_got.AddResponses(hash_responses)
		case <-src.StopNotifier:
			return nil
		}
	}
	bytesRound += bytes_cha

	secondthresh := uint(thresh)
	if APPROXIMATE_T2 {
		secondthresh = cha.GetKthCount(src.k)
	}

	if secondthresh < uint(thresh) {
		panic(fmt.Sprintln("Something went wrong", thresh, secondthresh))
	}

	bloom := cha.GetBloomFilter(secondthresh, hash_responses, uint(localthresh), uint(nnodes))

	fmt.Println("Round 2 tput-hash: thresh ", secondthresh, ", cha bytes", bytes_cha, "(", cha.Len(), " size). bloom sets", bloom.CountSetBit(), "(out of ", bloom.Len(), ") bytes ", bytesRound)
	bytes += bytesRound

	bloom_ser, err := disttopk.SerializeObject(bloom)
	if err != nil {
		panic(err)
	}

	bytes_compressed_sample := disttopk.CompressBytes(bloom_ser)
	bytesRound = len(bytes_compressed_sample) * nnodes
	for _, ch := range src.backPointers {
		select {
		case ch <- disttopk.CompressBytes(bloom_ser):
		case <-src.StopNotifier:
			return nil
		}
	}

	round3items := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			tr := dobj.Obj.(ThirdRound)
			il := tr.list
			access_stats.Serial_items += int(tr.items_looked_at)
			m = il.AddToMap(m)
			round3items += len(il)
			mresp = il.AddToCountMap(mresp)
		case <-src.StopNotifier:
			return nil
		}
	}

	bytesRound += round3items * disttopk.RECORD_SIZE
	fmt.Println("Round 3 tput-hash: got ", round3items, " items,  bytes ", bytesRound)
	bytes += bytesRound
	src.Stats.Bytes_transferred = uint64(bytes)
	src.Stats.Merge(*access_stats)

	il = disttopk.MakeItemList(m)
	il.Sort()

	score_k := uint(il[src.k-1].Score)
	if score_k < secondthresh {
		panic(fmt.Sprintln("4th round needed but not implemented yet", score_k, secondthresh))
	}

	//fmt.Println("Sorted Global List: ", il[:src.k])
	if disttopk.OUTPUT_RESP {
		for _, it := range il[:src.k] {
			fmt.Println("Resp: ", it.Id, it.Score, mresp[it.Id])
		}
	}
	src.FinalList = il
	return nil
}
