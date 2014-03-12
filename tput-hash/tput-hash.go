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
	cha             []byte
	items_looked_at uint //only for serial access accounting
}

type ThirdRound struct {
	list disttopk.ItemList
	//items_looked_at uint //only for serial access accounting
	stats *disttopk.AlgoStatsRound
}

func (src *Peer) Run() error {
	//defer close(src.forward)
	//src.list.Sort()
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

	sent_items := make(map[int]bool)
	for _, li := range src.list[:src.k] {
		sent_items[int(li.Id)] = true
	}

	for {
		var bloom *disttopk.Bloom
		select {
		case obj, ok := <-src.back:
			if !ok {
				return nil
			}
			bloom_ser := disttopk.DecompressBytes(obj.([]byte))
			bloom = &disttopk.Bloom{}
			if err := disttopk.DeserializeObject(bloom, bloom_ser); err != nil {
				panic(err)
			}
		case <-src.StopNotifier:
			return nil
		}

		bif := disttopk.NewBloomIndexableFilter(bloom)
		exactlist, stats := disttopk.GetListIndexedHashTable(bif, src.list, sent_items)
		stats.Transferred_items = len(exactlist)

		for _, item := range exactlist {
			sent_items[item.Id] = true
		}

		select {
		case src.forward <- disttopk.DemuxObject{src.id, ThirdRound{disttopk.ItemList(exactlist), stats}}:
		case <-src.StopNotifier:
			return nil
		}
	}
	return nil
}

func NewCoord(k int, approximate_t2 bool) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, nil, k, disttopk.AlgoStats{}, 0.5, approximate_t2}
}

type Coord struct {
	*stream.HardStopChannelCloser
	input          chan disttopk.DemuxObject
	backPointers   []chan<- stream.Object
	lists          [][]disttopk.Item
	FinalList      []disttopk.Item
	k              int
	Stats          disttopk.AlgoStats
	alpha          float64
	approximate_t2 bool
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
	peerm := make(map[int]map[int]float64)

	access_stats := &disttopk.AlgoStats{}
	nnodes := len(src.backPointers)
	thresh := 0.0
	items := 0
	items_at_peers := 0
	round_1_stats := disttopk.NewAlgoStatsRoundUnion()
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			fr := dobj.Obj.(FirstRound)
			il := fr.list
			items_at_peers += int(fr.count)
			round_stat_peer := disttopk.AlgoStatsRound{Bytes_sketch:4, Serial_items: len(il), Transferred_items: len(il)}
			round_1_stats.AddPeerStats(round_stat_peer)
			items += len(il)
			m = il.AddToMap(m)
			mresp = il.AddToCountMap(mresp)
			peerm[dobj.Id] = il.AddToMap(nil)
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
	localthresh := uint32((thresh / float64(nnodes)) * src.alpha)
	bytesRound := items*disttopk.RECORD_SIZE + (4*nnodes)
	fmt.Println("Round 1 tput-hash: got ", items, " items, thresh ", thresh, ", local thresh will be ", localthresh, " cha size", items_at_peers, " bytes used", bytesRound)
	bytes := bytesRound

	//rounding items at peers so that cha and bloom will have size power of 2
	//this is needed so that the hashtable at the peers can use indexing to reduce accesses
	cha_size := uint(disttopk.MakePowerOf2(int(items_at_peers)))

	bytesRound = 8 * nnodes
	round_2_stats := disttopk.NewAlgoStatsRoundUnion()
	for _, ch := range src.backPointers {
		round_2_stats.AddPeerStats(disttopk.AlgoStatsRound{Bytes_sketch: 8})
		select {
		case ch <- FirstRoundResponse{uint32(localthresh), uint32(cha_size)}:
		case <-src.StopNotifier:
			return nil
		}
	}

	cha := NewCountHashArray(uint(cha_size))
	hash_responses := make(map[int]int)

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
		
			for peerlocaltopid, peerlocaltopscore := range peerm[dobj.Id] {
				cha_got.Add(disttopk.IntKeyToByteKey(peerlocaltopid), uint(peerlocaltopscore))
			}
			
			round_stat_peer := disttopk.AlgoStatsRound{Serial_items:int(sr.items_looked_at), Bytes_sketch: uint64(len(sr.cha))}
			round_2_stats.AddPeerStats(round_stat_peer)

			cha.Merge(cha_got)
			cha_got.AddResponses(hash_responses)
		case <-src.StopNotifier:
			return nil
		}
	}
	access_stats.AddRound(*round_2_stats)
	bytesRound += bytes_cha

	secondthresh := uint(thresh)
	if src.approximate_t2 {
		secondthresh = cha.GetKthCount(src.k)
	}

	if secondthresh < uint(thresh) {
		collision_detector := make(map[uint]bool, src.k)
		collision := false
		for _, item := range il[:src.k]{
			index := cha.GetIndex(disttopk.IntKeyToByteKey(item.Id))
			if collision_detector[index]{
				collision = true 
				break
			}
			collision_detector[index]= true
			//fmt.Println("Item", item.Id, "score", item.Score, "cha score", cha.Query(disttopk.IntKeyToByteKey(item.Id)), "Index", cha.GetIndex(disttopk.IntKeyToByteKey(item.Id)), "collision", collision, collision_detector)
		}
		if collision {
			secondthresh = uint(thresh)
		} else {
			panic(fmt.Sprintln("Something went wrong", thresh, secondthresh))
		}
	}

	bloom := cha.GetBloomFilter(secondthresh, hash_responses, uint(localthresh), uint(nnodes))

	fmt.Println("Round 2 tput-hash: thresh ", secondthresh, ", cha bytes", bytes_cha, "(", cha.Len(), " size). bloom sets", bloom.CountSetBit(), "(out of ", bloom.Len(), ") bytes ", bytesRound)
	bytes += bytesRound

	round3items := 0
	bytesRound, round3items, m, mresp = src.SendBloom(bloom, nnodes, access_stats, m, mresp)
	il = disttopk.MakeItemList(m)
	il.Sort()

	score_k := uint(il[src.k-1].Score)

	fmt.Println("Round 3 tput-hash: got ", round3items, " items, score_k", score_k, "  bytes ", bytesRound)
	bytes += bytesRound

	access_stats.Rounds = 3
	if score_k < secondthresh {
		access_stats.Rounds = 4
		thirdthresh := score_k

		//no need to update hash_responses as stuff sent before won't be sent again anyway
		bloom := cha.GetBloomFilter(thirdthresh, hash_responses, uint(localthresh), uint(nnodes))
		fmt.Println("Round 3 tput-hash extra-round: thresh ", thirdthresh, " bloom sets", bloom.CountSetBit(), "(out of ", bloom.Len(), ")")

		round4items := 0
		bytesRound, round4items, m, mresp = src.SendBloom(bloom, nnodes, access_stats, m, mresp)
		il = disttopk.MakeItemList(m)
		il.Sort()

		fmt.Println("Round 4 tput-hash: got ", round4items, " items,  bytes ", bytesRound)
		bytes += bytesRound
	}

	src.Stats = *access_stats
	src.Stats.Bytes_transferred = uint64(bytes)

	//fmt.Println("Sorted Global List: ", il[:src.k])
	if disttopk.OUTPUT_RESP {
		for _, it := range il[:src.k] {
			fmt.Println("Resp: ", it.Id, it.Score, mresp[it.Id])
		}
	}
	src.FinalList = il
	return nil
}

func (src *Coord) SendBloom(bloom *disttopk.Bloom, nnodes int, access_stats *disttopk.AlgoStats, m map[int]float64, mresp map[int]int) (int, int, map[int]float64, map[int]int) {
	bloom_ser, err := disttopk.SerializeObject(bloom)
	if err != nil {
		panic(err)
	}

	compressed := disttopk.CompressBytes(bloom_ser)
	bytesRound := len(compressed) * nnodes
	round_3_stats := disttopk.NewAlgoStatsRoundUnion()
	for _, ch := range src.backPointers {
		round_3_stats.AddPeerStats(disttopk.AlgoStatsRound{Bytes_sketch: uint64(len(compressed))})
		select {
		case ch <- compressed:
		case <-src.StopNotifier:
			panic("should not happen")
		}
	}

	round3items := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			tr := dobj.Obj.(ThirdRound)
			il := tr.list
			round_3_stats.AddPeerStats(*tr.stats)
			m = il.AddToMap(m)
			round3items += len(il)
			mresp = il.AddToCountMap(mresp)
		case <-src.StopNotifier:
			panic("should not happen")
		}
	}
	access_stats.AddRound(*round_3_stats)

	bytesRound += round3items * disttopk.RECORD_SIZE

	return bytesRound, round3items, m, mresp
}
