package tworound

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	"fmt"
	"math"
	"runtime"
)

var _ = math.Log2

/*
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

func NewApproximateBloomGcsFilterPeer(list disttopk.ItemList, topk int, numpeer int, N_est int) *Peer {
	//use gcs here to allow for indexing to reduce serial accesses
	peer := NewPeer(list, NewNonePeerSketchAdaptor(), NewApproximateBloomGcsFilterAdaptor(topk, numpeer, N_est), topk)
	return peer
}

func NewExtraRoundBloomGcsMergePeer(list disttopk.ItemList, topk int, numpeer int, N_est int) *Peer {
	peer := NewPeer(list, NewBloomHistogramMergePeerSketchAdaptor(topk, numpeer, N_est), NewBloomHistogramMergeGcsApproxUnionSketchAdaptor(topk), topk)
	peer.Alpha = 0 // Send 0 first top-k elements from each peer. rely on sketch to give you estimate for t1
	return peer
}*/

func NewBloomPR(topk int, numpeer int, N_est int) *ProtocolRunner {
	return NewProtocolRunner(NewBloomHistogramPeerSketchAdaptor(topk, numpeer, N_est), NewBloomHistogramUnionSketchAdaptor(), topk, numpeer, N_est)
}

func NewBloomGcsPR(topk int, numpeer int, N_est int) *ProtocolRunner {
	return NewProtocolRunner(NewBloomHistogramGcsPeerSketchAdaptor(topk, numpeer, N_est), NewBloomHistogramGcsUnionSketchAdaptor(), topk, numpeer, N_est)
}
func NewBloomGcsMergePR(topk int, numpeer int, N_est int) *ProtocolRunner {
	return NewProtocolRunner(NewBloomHistogramMergePeerSketchAdaptor(topk, numpeer, N_est), NewBloomHistogramMergeSketchAdaptor(), topk, numpeer, N_est)
}

func NewCountMinPR(topk int, numpeer int, N_est int) *ProtocolRunner {
	return NewProtocolRunner(NewCountMinPeerSketchAdaptor(topk, numpeer, N_est), NewCountMinUnionSketchAdaptor(), topk, numpeer, N_est)
}

func NewApproximateBloomFilterPR(topk int, numpeer int, N_est int) *ProtocolRunner {
	peer := NewProtocolRunner(NewNonePeerSketchAdaptor(), NewApproximateBloomFilterAdaptor(topk, numpeer, N_est), topk, numpeer, N_est)
	peer.Approximate = true
	return peer
}

func NewApproximateBloomGcsFilterPR(topk int, numpeer int, N_est int) *ProtocolRunner {
	//use gcs here to allow for indexing to reduce serial accesses
	peer := NewProtocolRunner(NewNonePeerSketchAdaptor(), NewApproximateBloomGcsFilterAdaptor(topk, numpeer, N_est), topk, numpeer, N_est)
	peer.Approximate = true
	peer.SendLength = true
	return peer
}

func NewExtraRoundBloomGcsMergePR(topk int, numpeer int, N_est int) *ProtocolRunner {
	peer := NewProtocolRunner(NewBloomHistogramMergePeerSketchAdaptor(topk, numpeer, N_est), NewBloomHistogramMergeGcsApproxUnionSketchAdaptor(topk), topk, numpeer, N_est)
	peer.Alpha = 0 // Send 0 first top-k elements from each peer. rely on sketch to give you estimate for t1
	return peer
}

type ProtocolRunner struct {
	PeerSketchAdaptor
	UnionSketchAdaptor
	k                int
	numpeer          int
	N_est            int
	Alpha            float64
	SendLength       bool
	CompressSketches bool
	Approximate      bool
	GroundTruth      disttopk.ItemList
}

func NewProtocolRunner(psa PeerSketchAdaptor, usa UnionSketchAdaptor, k int, numpeer int, N_est int) *ProtocolRunner {
	return &ProtocolRunner{psa, usa, k, numpeer, N_est, 1, false, false, false, nil}
}

func (t *ProtocolRunner) NewPeer(list disttopk.ItemList) *Peer {
	return NewPeer(list, t)
}

func (t *ProtocolRunner) NewCoord() *Coord {
	return NewCoord(t)
}

func compress(in []byte) []byte {
	return disttopk.CompressBytes(in)
}

func decompress(in []byte) []byte {
	return disttopk.DecompressBytes(in)
}

type Peer struct {
	*stream.HardStopChannelCloser
	*ProtocolRunner
	forward chan<- disttopk.DemuxObject
	back    <-chan stream.Object
	list    disttopk.ItemList
	id      int
}

func NewPeer(list disttopk.ItemList, pr *ProtocolRunner) *Peer {
	return &Peer{stream.NewHardStopChannelCloser(), pr, nil, nil, list, 0}
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
	length uint32
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

	sent_items := make(map[int]bool)
	localtop_index := int(float64(src.k) * src.Alpha)
	localtop := src.list[:localtop_index]
	for _, item := range localtop {
		sent_items[item.Id] = true
	}

	sketch, serialAccessOverLocaltop := src.createSketch(src.list, localtop)
	ser := src.PeerSketchAdaptor.serialize(sketch)

	listlen := 0
	if src.SendLength {
		listlen = len(src.list)
	}

	first_round_access := &disttopk.AlgoStats{Serial_items: localtop_index + serialAccessOverLocaltop, Random_access: 0, Random_items: 0}
	select {
	case src.forward <- disttopk.DemuxObject{src.id, FirstRound{localtop, compress(ser), first_round_access, uint32(listlen)}}:
	case <-src.StopNotifier:
		return nil
	}

	for {
		var uf UnionFilter
		select {
		case obj, ok := <-src.back:
			if !ok {
				return nil
			}
			ufser := obj.(SecondRound).ufser
			if ufser != nil {
				uf = src.UnionSketchAdaptor.deserialize(decompress(ufser))
			}
		case <-src.StopNotifier:
			return nil
		}

		exactlist, round2Access := src.getRoundTwoList(uf, src.list, localtop_index, sent_items)
		for _, item := range exactlist {
			sent_items[item.Id] = true
		}

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
	}

	return nil
}

/*
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

func NewApproximateBloomGcsFilterCoord(k int, N_est int) *Coord {
	coord := NewCoord(k, NewNonePeerSketchAdaptor(), NewApproximateBloomGcsFilterAdaptor(k, 0, N_est))
	coord.Approximate = true
	return coord
}

func NewExtraRoundBloomGcsMergeCoord(k int) *Coord {
	return NewCoord(k, NewBloomHistogramMergePeerSketchAdaptor(k, 0, 0), NewBloomHistogramMergeGcsApproxUnionSketchAdaptor(k))
}
*/
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
	*ProtocolRunner
	input        chan disttopk.DemuxObject
	backPointers []chan<- stream.Object
	lists        [][]disttopk.Item
	FinalList    []disttopk.Item
	Stats        disttopk.AlgoStats
}

func NewCoord(pr *ProtocolRunner) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), pr, make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, nil, disttopk.AlgoStats{}}
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

	nnodes := len(src.backPointers)
	thresh := 0.0
	items := 0
	sketchsize := 0
	var ucm UnionSketch

	round1Access := &disttopk.AlgoStats{}
	listlensum := 0
	listlenbytes := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			fr := dobj.Obj.(FirstRound)
			il := fr.list
			items += len(il)
			m = il.AddToMap(m)

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

			if src.SendLength {
				listlensum += int(fr.length)
				listlenbytes += 4
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

	bytesRound := items*disttopk.RECORD_SIZE + sketchsize + listlenbytes
	if ucm != nil {
		fmt.Println(ucm.GetInfo())
	}
	fmt.Println("Round 1 tr: got ", items, " items, thresh ", thresh, "sketches bytes", sketchsize, sketchsize/nnodes, "/node total bytes", bytesRound)
	bytes := bytesRound

	err, round2Access, round2items, m, ufThresh, total_back_bytes := src.RunSendFilterThreshold(ucm, uint32(localthresh), il, m, listlensum)
	if err != nil {
		return err
	}

	bytesRound = round2items*disttopk.RECORD_SIZE + total_back_bytes
	bytes += bytesRound
	fmt.Print("Round 2 tr: got ", round2items, " items (", round2items/nnodes, "/node), bytes for records: ", round2items*disttopk.RECORD_SIZE, "bytes filter: ", total_back_bytes, ". BW Round: ", bytesRound, "BW total: ", bytes, "\n")
	fmt.Printf("Round 2 tr: access %+v\n", round2Access)
	src.Stats.Bytes_transferred = uint64(bytes)
	src.Stats.Merge(*round1Access)
	src.Stats.Merge(*round2Access)
	src.Stats.Rounds = 2

	il = disttopk.MakeItemList(m)
	il.Sort()
	//fmt.Println("Sorted Global List: ", il[:src.k])
	if uint(il[src.k-1].Score) < ufThresh {
		if src.Approximate {
			fmt.Println("WARNING, result may be inexact")
		} else {
			src.Stats.Rounds = 3
			round3Thresh := il[src.k-1].Score
			err, round3Access, round3items, m, ufThresh, round3_back_bytes := src.RunSendFilterThreshold(ucm, uint32(round3Thresh), il, m, listlensum)
			if err != nil {
				return err
			}
			if ufThresh < uint(round3Thresh) {
				panic("Should never happen")
			}

			bytesRound = round3items*disttopk.RECORD_SIZE + round3_back_bytes
			bytes += bytesRound
			fmt.Print("Round 3 tr: got ", round3items, " items (", round3items/nnodes, "/node), bytes for records: ", round3items*disttopk.RECORD_SIZE, "bytes filter: ", round3_back_bytes, ". BW Round: ", bytesRound, "BW total: ", bytes, "\n")
			fmt.Printf("Round 3 tr: access %+v\n", round3Access)
			src.Stats.Bytes_transferred = uint64(bytes)
			src.Stats.Merge(*round3Access)
			il = disttopk.MakeItemList(m)
			il.Sort()
			//		panic(fmt.Sprintf("topk-score < approx thresh. Need to implement third round. score %v, approxThresh %v", uint(il[src.k-1].Score), ufThresh))
		}
	}

	if disttopk.OUTPUT_RESP {
		for _, it := range il[:src.k] {
			fmt.Println("Resp: ", it.Id, it.Score)
		}
	}

	src.FinalList = il
	return nil
}

func (src *Coord) RunSendFilterThreshold(ucm UnionSketch, thresh uint32, il disttopk.ItemList, m map[int]float64, listlensum int) (err error, access *disttopk.AlgoStats, items int, ret_m map[int]float64, usedThresh uint, bytes_back int) {

	total_back_bytes := 0
	uf, ufThresh := src.getUnionFilter(ucm, thresh, il, listlensum)
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
			panic("wtf!")
		}
	}

	round2items := 0
	round2Access := &disttopk.AlgoStats{}
	groundtruthfp := -1
	filterfp := -1
	for cnt := 0; cnt < len(src.backPointers); cnt++ {
		select {
		case dobj := <-src.input:
			srpr := dobj.Obj.(SecondRoundPeerReply)
			il := srpr.list
			m = il.AddToMap(m)
			round2items += len(il)
			round2Access.Merge(*srpr.stats)

			fir, ok := src.UnionSketchAdaptor.(UnionSketchFilterItemsReporter)
			if ok {
				if filterfp == -1 {
					filterfp = 0
				}
				filtered_items := fir.getFilteredItems()
				if src.GroundTruth != nil {
					if groundtruthfp == -1 {
						groundtruthfp = 0
					}

					fp := src.CountFalsePositives(src.GroundTruth[:src.k], filtered_items)

					//fmt.Println("There were", fp, "items added to the filter that are not part of groundtruth")
					groundtruthfp += fp
				}

				fp_sketch := src.CountFalsePositives(filtered_items, il)
				filterfp += fp_sketch
				//fmt.Println("There were", fp_sketch, "items sent as false positive through the sketch")

			}
		case <-src.StopNotifier:
			panic("wtf")
		}
	}
	if groundtruthfp != -1 {
		fmt.Println("There was a total of ", groundtruthfp, "items added to the filter that are not part of groundtruth across all nodes")
	}
	if filterfp != -1 {
		fmt.Printf("There was a total of %v (%v/node) items sent as false positive through the sketch\n", filterfp, filterfp/len(src.backPointers))
	}

	return nil, round2Access, round2items, m, ufThresh, total_back_bytes
}

func (src *Coord) CountFalsePositives(reference disttopk.ItemList, test disttopk.ItemList) int {
	m := make(map[int]bool)
	for _, true_item := range reference {
		m[true_item.Id] = true
	}
	fp := 0
	for _, item := range test {
		if !m[item.Id] {
			fp++
		}
	}
	return fp
}
