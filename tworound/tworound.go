package tworound

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	"fmt"
	"math"
	"runtime"
)

var _ = math.Log2

func NewBloomPeer(list disttopk.ItemList, topk int, numpeer int, N_est int) *Peer {
	return NewPeer(list, NewBloomHistogramPeerSketchAdaptor(topk, numpeer, N_est), NewBloomHistogramUnionSketchAdaptor(), topk)
}

func NewBloomGcsPeer(list disttopk.ItemList, topk int, numpeer int, N_est int) *Peer {
	return NewPeer(list, NewBloomHistogramGcsPeerSketchAdaptor(topk, numpeer, N_est), NewBloomHistogramGcsUnionSketchAdaptor(), topk)
}
func NewBloomGcsMergePeer(list disttopk.ItemList, topk int, numpeer int, N_est int) *Peer {
	return NewPeer(list, NewBloomHistogramMergePeerSketchAdaptor(topk, numpeer, N_est), NewBloomHistogramMergeSketchAdaptor(), topk)
}

func NewCountMinPeer(list disttopk.ItemList, topk int, numpeer int, N_est int) *Peer {
	return NewPeer(list, NewCountMinPeerSketchAdaptor(topk, numpeer, N_est), NewCountMinUnionSketchAdaptor(), topk)
}

func NewApproximateBloomFilterPeer(list disttopk.ItemList, topk int, numpeer int, N_est int) *Peer {
	peer := NewPeer(list, NewNonePeerSketchAdaptor(), NewApproximateBloomFilterAdaptor(topk, numpeer, N_est), topk)
	return peer
}

func NewApproximateBloomGcsMergePeer(list disttopk.ItemList, topk int, numpeer int, N_est int) *Peer {
	peer := NewPeer(list, NewBloomHistogramMergePeerSketchAdaptor(topk, numpeer, N_est), NewBloomHistogramMergeGcsApproxUnionSketchAdaptor(topk), topk)
	peer.Alpha = 0 // Send 0 first top-k elements from each peer. rely on sketch to give you estimate for t1
	return peer
}

func NewPeer(list disttopk.ItemList, psa PeerSketchAdaptor, usa UnionSketchAdaptor, k int) *Peer {
	return &Peer{stream.NewHardStopChannelCloser(), psa, usa, nil, nil, list, k, 0, 1}
}

func compress(in []byte) []byte {
	return disttopk.CompressBytes(in)
}

func decompress(in []byte) []byte {
	return disttopk.DecompressBytes(in)
}

type Peer struct {
	*stream.HardStopChannelCloser
	PeerSketchAdaptor
	UnionSketchAdaptor
	forward chan<- disttopk.DemuxObject
	back    <-chan stream.Object
	list    disttopk.ItemList
	k       int
	id      int
	Alpha   float64
}

type FirstRoundSketch interface {
	ByteSize() int
}

type Serialized []byte

func (s *Serialized) ByteSize() int { return len(*s) }

type FirstRound struct {
	list   disttopk.ItemList
	sketch Serialized
	stats  *disttopk.AlgoStats
}

type SecondRound struct {
	ufser Serialized
}

type SecondRoundPeerReply struct {
	list  disttopk.ItemList
	stats *disttopk.AlgoStats
}

func (src *Peer) Run() error {
	//defer close(src.forward)
	src.list.Sort()
	//fmt.Println("Sort", src.list[:10])

	if src.k > len(src.list) {
		fmt.Println("warning cmfilter: list shorter than k")
		src.k = len(src.list)
	}

	localtop_index := int(float64(src.k) * src.Alpha)
	localtop := src.list[:localtop_index]

	sketch, serialAccessOverLocaltop := src.createSketch(src.list, localtop)
	ser := src.PeerSketchAdaptor.serialize(sketch)

	first_round_access := &disttopk.AlgoStats{Serial_items: localtop_index + serialAccessOverLocaltop, Random_access: 0, Random_items: 0}
	select {
	case src.forward <- disttopk.DemuxObject{src.id, FirstRound{localtop, compress(ser), first_round_access}}:
	case <-src.StopNotifier:
		return nil
	}

	var uf UnionFilter
	select {
	case obj := <-src.back:
		ufser := obj.(SecondRound).ufser
		if ufser != nil {
			uf = src.UnionSketchAdaptor.deserialize(decompress(ufser))
		}
	case <-src.StopNotifier:
		return nil
	}

	exactlist, round2Access := src.getRoundTwoList(uf, src.list, localtop_index)
	runtime.GC()
	/*exactlist := make([]disttopk.Item, 0)
	for index, v := range src.list {
		if index >= src.k && uf.PassesInt(v.Id) == true {
			exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
		}
	}*/

	//fmt.Println("SR", sr.cmf.GetInfo())

	select {
	case src.forward <- disttopk.DemuxObject{src.id, SecondRoundPeerReply{disttopk.ItemList(exactlist), round2Access}}:
	case <-src.StopNotifier:
		return nil
	}

	return nil
}

func NewBloomCoord(k int) *Coord {
	return NewCoord(k, NewBloomHistogramPeerSketchAdaptor(k, 0, 0), NewBloomHistogramUnionSketchAdaptor())
}

func NewBloomGcsCoord(k int) *Coord {
	return NewCoord(k, NewBloomHistogramGcsPeerSketchAdaptor(k, 0, 0), NewBloomHistogramGcsUnionSketchAdaptor())
}

func NewBloomGcsMergeCoord(k int) *Coord {
	return NewCoord(k, NewBloomHistogramMergePeerSketchAdaptor(k, 0, 0), NewBloomHistogramMergeSketchAdaptor())
}

func NewCountMinCoord(k int) *Coord {
	return NewCoord(k, NewCountMinPeerSketchAdaptor(k, 0, 0), NewCountMinUnionSketchAdaptor())
}

func NewApproximateBloomFilterCoord(k int) *Coord {
	coord := NewCoord(k, NewNonePeerSketchAdaptor(), NewApproximateBloomFilterAdaptor(k, 0, 0))
	coord.Approximate = true
	return coord
}

func NewApproximateBloomGcsMergeCoord(k int) *Coord {
	return NewCoord(k, NewBloomHistogramMergePeerSketchAdaptor(k, 0, 0), NewBloomHistogramMergeGcsApproxUnionSketchAdaptor(k))
}

func NewCoord(k int, psa PeerSketchAdaptor, usa UnionSketchAdaptor) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), psa, usa, make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, nil, k, disttopk.AlgoStats{}, false}
}

type UnionSketch interface {
	//	Merge(disttopk.Sketch)
	GetInfo() string
}

type UnionFilter interface {
	//	PassesInt(int) bool
	ByteSize() int
	GetInfo() string
}

type Coord struct {
	*stream.HardStopChannelCloser
	PeerSketchAdaptor
	UnionSketchAdaptor
	input        chan disttopk.DemuxObject
	backPointers []chan<- stream.Object
	lists        [][]disttopk.Item
	FinalList    []disttopk.Item
	k            int
	Stats        disttopk.AlgoStats
	Approximate  bool
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
	sketchsize := 0
	var ucm UnionSketch

	round1Access := &disttopk.AlgoStats{}
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			fr := dobj.Obj.(FirstRound)
			il := fr.list
			items += len(il)
			m = il.AddToMap(m)
			mresp = il.AddToCountMap(mresp)

			compressedsize := fr.sketch.ByteSize()
			decompressed := decompress(fr.sketch)
			if len(decompressed) > 0 {
				sketchsize += compressedsize
			}
			sketch := src.PeerSketchAdaptor.deserialize(decompress(fr.sketch))

			//cm := fr.cm.(*disttopk.CountMinSketch)
			//sketchsize += cm.ByteSize()
			round1Access.Merge(*fr.stats)

			if ucm == nil {
				ucm = src.getUnionSketch(sketch, il)
			} else {
				src.mergeIntoUnionSketch(ucm, sketch, il)
				//ucm.Merge(sketch.(disttopk.Sketch))
			}
		case <-src.StopNotifier:
			return nil
		}
	}
	fmt.Printf("Round 1 tr: access %+v\n", round1Access)

	il := disttopk.MakeItemList(m)
	il.Sort()

	if len(il) < src.k {
		fmt.Println("WARNING k less than list, cannot get an exact threshold. Using thresh=0")
	} else {
		thresh = il[src.k-1].Score
	}
	localthresh := thresh

	bytesRound := items*disttopk.RECORD_SIZE + sketchsize
	if ucm != nil {
		fmt.Println(ucm.GetInfo())
	}
	fmt.Println("Round 1 tr: got ", items, " items, thresh ", thresh, "sketches bytes", sketchsize, sketchsize/nnodes, "/node total bytes", bytesRound)
	bytes := bytesRound

	total_back_bytes := 0
	uf, ufThresh := src.getUnionFilter(ucm, uint32(localthresh), il)
	if uf != nil {
		fmt.Println("Uf info: ", uf.GetInfo())
	} else {
		fmt.Println("Uf is Nil. ALL remaining items will be sent in second round")
	}

	for _, ch := range src.backPointers {
		//uf := src.getUnionFilter(ucm, uint32(localthresh))
		var ser Serialized
		if uf != nil {
			cuf := src.copyUnionFilter(uf)
			ser = Serialized(compress(src.UnionSketchAdaptor.serialize(cuf)))
			total_back_bytes += ser.ByteSize()
		}
		select {
		case ch <- SecondRound{ser}:
		case <-src.StopNotifier:
			return nil
		}
	}

	round2items := 0
	round2Access := &disttopk.AlgoStats{}
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			srpr := dobj.Obj.(SecondRoundPeerReply)
			il := srpr.list
			m = il.AddToMap(m)
			round2items += len(il)
			mresp = il.AddToCountMap(mresp)
			round2Access.Merge(*srpr.stats)
		case <-src.StopNotifier:
			return nil
		}
	}

	bytesRound = round2items*disttopk.RECORD_SIZE + total_back_bytes
	bytes += bytesRound
	fmt.Print("Round 2 tr: got ", round2items, " items (", round2items/nnodes, "/node), bytes for records: ", round2items*disttopk.RECORD_SIZE, "bytes filter: ", total_back_bytes, ". BW Round: ", bytesRound, "BW total: ", bytes, "\n")
	fmt.Printf("Round 2 tr: access %+v\n", round2Access)
	src.Stats.Bytes_transferred = uint64(bytes)
	src.Stats.Merge(*round1Access)
	src.Stats.Merge(*round2Access)

	il = disttopk.MakeItemList(m)
	il.Sort()
	//fmt.Println("Sorted Global List: ", il[:src.k])
	if disttopk.OUTPUT_RESP {
		for _, it := range il[:src.k] {
			fmt.Println("Resp: ", it.Id, it.Score, mresp[it.Id])
		}
	}

	if uint(il[src.k-1].Score) < ufThresh {
		if src.Approximate {
			fmt.Println("WARNING, result may be inexact")
		} else {
			panic(fmt.Sprintf("topk-score < approx thresh. Need to implement third round. score %v, approxThresh %v", uint(il[src.k-1].Score), ufThresh))
		}
	}

	src.FinalList = il
	return nil
}
