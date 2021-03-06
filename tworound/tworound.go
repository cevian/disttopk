package tworound

import (
	"encoding/gob"
	"time"

	"github.com/cevian/go-stream/stream"
)
import "github.com/cevian/disttopk"

import (
	"fmt"
	"math"
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

func NewBloomPR(topk int, numpeer int, N_est int, EstimateParameter disttopk.EstimateParameter) *ProtocolRunner {
	return NewProtocolRunner(NewBloomHistogramPeerSketchAdaptor(topk, numpeer, N_est, EstimateParameter), NewBloomHistogramUnionSketchAdaptor(), topk, numpeer, N_est)
}

func NewBloomGcsPR(topk int, numpeer int, N_est int, EstimateParameter disttopk.EstimateParameter) *ProtocolRunner {
	return NewProtocolRunner(NewBloomHistogramGcsPeerSketchAdaptor(topk, numpeer, N_est, EstimateParameter), NewBloomHistogramGcsUnionSketchAdaptor(), topk, numpeer, N_est)
}
func NewBloomGcsMergePR(topk int, numpeer int, N_est int, EstimateParameter disttopk.EstimateParameter) *ProtocolRunner {
	return NewProtocolRunner(NewBloomHistogramMergePeerSketchAdaptor(topk, numpeer, N_est, EstimateParameter), NewBloomHistogramMergeSketchAdaptor(), topk, numpeer, N_est)
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
	peer.CompressSketches = false
	return peer
}

func NewExtraRoundBloomGcsMergePR(topk int, numpeer int, N_est int, EstimateParameter disttopk.EstimateParameter) *ProtocolRunner {
	peer := NewProtocolRunner(NewBhErPeerSketchAdaptor(topk, numpeer, N_est, 0, EstimateParameter), NewBhErUnionSketchAdaptor(topk, numpeer, 0.5, false), topk, numpeer, N_est)
	peer.Alpha = 0 // Send 0 first top-k elements from each peer. rely on sketch to give you estimate for t1
	return peer
}

func NewExtraRoundBloomGcsMergeSplitMoreEntriesPR(topk int, numpeer int, N_est int, EstimateParameter disttopk.EstimateParameter) *ProtocolRunner {
	psa := NewBhErPeerSketchAdaptor(topk, numpeer, N_est, 5, EstimateParameter)
	psa.(*BhErPeerSketchAdaptor).totalEntries = 100
	peer := NewProtocolRunner(psa, NewBhErUnionSketchAdaptor(topk, numpeer, 0.9, true), topk, numpeer, N_est)
	peer.Alpha = 0 // Send 0 first top-k elements from each peer. rely on sketch to give you estimate for t1
	return peer
}

func NewExtraRoundBloomGcsMergeSplitPR(topk int, numpeer int, N_est int, EstimateParameter disttopk.EstimateParameter) *ProtocolRunner {
	peer := NewProtocolRunner(NewBhErPeerSketchAdaptor(topk, numpeer, N_est, 5, EstimateParameter), NewBhErUnionSketchAdaptor(topk, numpeer, 0.9, true), topk, numpeer, N_est)
	peer.Alpha = 0 // Send 0 first top-k elements from each peer. rely on sketch to give you estimate for t1
	return peer
}

func NewExtraRoundBloomGcsMergeSplitNoChPR(topk int, numpeer int, N_est int, EstimateParameter disttopk.EstimateParameter) *ProtocolRunner {
	peer := NewProtocolRunner(NewBhErPeerSketchAdaptor(topk, numpeer, N_est, 5, EstimateParameter), NewBhErUnionSketchAdaptor(topk, numpeer, 0.9, false), topk, numpeer, N_est)
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
	return &ProtocolRunner{psa, usa, k, numpeer, N_est, 1, false, true, false, nil}
}

func (t *ProtocolRunner) NewPeer(list disttopk.ItemList, ht *disttopk.HashTable) *Peer {
	return NewPeer(list, ht, t)
}

func (t *ProtocolRunner) NewCoord() *Coord {
	return NewCoord(t)
}

func RegisterGob() {
	gob.Register(InitRound{})
	gob.Register(FirstRound{})
	gob.Register(SecondRound{})
	gob.Register(SecondRoundPeerReply{})
	gob.Register(disttopk.DemuxObject{})
}

func (t *ProtocolRunner) compress(in []byte) []byte {
	if t.CompressSketches {
		return disttopk.CompressBytes(in)
	} else {
		return in
	}
}

func (t *ProtocolRunner) decompress(in []byte) []byte {
	if t.CompressSketches {
		return disttopk.DecompressBytes(in)
	} else {
		return in
	}
}

type Peer struct {
	*stream.HardStopChannelCloser
	*ProtocolRunner
	forward chan<- stream.Object
	back    <-chan stream.Object
	list    disttopk.ItemList
	ht      *disttopk.HashTable
	id      int
}

func NewPeer(list disttopk.ItemList, ht *disttopk.HashTable, pr *ProtocolRunner) *Peer {
	return &Peer{stream.NewHardStopChannelCloser(), pr, nil, nil, list, ht, 0}
}

type FirstRoundSketch interface {
	//	ByteSize() int
}

type Serialized []byte

func (s *Serialized) ByteSize() int { return len(*s) }

type InitRound struct {
	Id int
}

type FirstRound struct {
	List   disttopk.ItemList
	Sketch Serialized
	Stats  *disttopk.AlgoStatsRound
	Length uint32
}

type SecondRound struct {
	Ufser Serialized
}

type SecondRoundPeerReply struct {
	List             disttopk.ItemList
	Stats            *disttopk.AlgoStatsRound
	AdditionalSketch Serialized
}

func (t *Peer) SetNetwork(readCh chan stream.Object, writeCh chan stream.Object) {
	t.back = readCh
	t.forward = writeCh
}

func (src *Peer) Run() error {
	//defer close(src.forward)
	//src.list.Sort()
	//fmt.Println("Sort", src.list[:10])

	if src.k > len(src.list) {
		fmt.Println("warning cmfilter: list shorter than k")
		src.k = len(src.list)
	}

	init := <-src.back
	src.id = init.(InitRound).Id

	sent_items := make(map[int]bool)
	localtop_index := int(float64(src.k) * src.Alpha)
	localtop := src.list[:localtop_index]
	for _, item := range localtop {
		sent_items[item.Id] = true
	}

	sketch, serialAccessOverLocaltop := src.createSketch(src.list, localtop)
	ser := src.PeerSketchAdaptor.serialize(sketch)
	compressed := src.compress(ser)
	listlen := 0
	if src.SendLength {
		listlen = len(src.list)
	}

	first_round_access := &disttopk.AlgoStatsRound{Serial_items: localtop_index + serialAccessOverLocaltop, Transferred_items: len(localtop)}
	if len(ser) > 0 {
		first_round_access.Bytes_sketch = uint64(len(compressed))
	}
	select {
	case src.forward <- disttopk.DemuxObject{src.id, FirstRound{localtop, compressed, first_round_access, uint32(listlen)}}:
	case <-src.StopNotifier:
		return nil
	}

	for {
		sketch_size := 0
		var uf UnionFilter
		select {
		case obj, ok := <-src.back:
			if !ok {
				return nil
			}
			ufser := obj.(SecondRound).Ufser
			if ufser != nil {
				decomp := src.decompress(ufser)
				if len(decomp) > 0 {
					sketch_size += len(ufser)
				}
				uf = src.UnionSketchAdaptor.deserialize(decomp)
			}
		case <-src.StopNotifier:
			return nil
		}

		exactlist, round2Access := src.getRoundTwoList(uf, src.list, src.ht, localtop_index, sent_items)
		round2Access.Bytes_sketch += uint64(sketch_size)

		for _, item := range exactlist {
			sent_items[item.Id] = true
		}

		var additionalSketch Serialized
		additonalPeer, ok := src.PeerSketchAdaptor.(PeerAdditionalSketchAdaptor)
		if ok && uf != nil {
			addsketch, serialaccess := additonalPeer.getAdditionalSketch(uf, src.list, sketch)
			round2Access.Serial_items += serialaccess
			if addsketch != nil {
				additionalSketch = additonalPeer.serializeAdditionalSketch(addsketch)
				round2Access.Bytes_sketch += uint64(len(additionalSketch))
			}
		}

		//runtime.GC()
		/*exactlist := make([]disttopk.Item, 0)
		for index, v := range src.list {
			if index >= src.k && uf.PassesInt(v.Id) == true {
				exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
			}
		}*/

		//fmt.Println("SR", sr.cmf.GetInfo())
		round2Access.Transferred_items = len(exactlist)
		select {
		case src.forward <- disttopk.DemuxObject{src.id, SecondRoundPeerReply{disttopk.ItemList(exactlist), round2Access, additionalSketch}}:
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

type UnionFilterEmpty interface {
	isEmpty() bool
}

type Coord struct {
	*stream.HardStopChannelCloser
	*ProtocolRunner
	input        chan stream.Object
	backPointers []chan<- stream.Object
	lists        [][]disttopk.Item
	FinalList    []disttopk.Item
	Stats        disttopk.AlgoStats
}

func (t *Coord) InputChannel() chan stream.Object {
	return t.input
}

func NewCoord(pr *ProtocolRunner) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), pr, make(chan stream.Object, 3), make([]chan<- stream.Object, 0), nil, nil, disttopk.AlgoStats{}}
}

func (src *Coord) Add(p *Peer) {
	//id := len(src.backPointers)
	back := make(chan stream.Object, 3)
	src.backPointers = append(src.backPointers, back)
	//p.id = id
	p.back = back
	p.forward = src.input
}

func (src *Coord) AddNetwork(channel chan stream.Object) {
	//id := len(src.backPointers)
	src.backPointers = append(src.backPointers, channel)
	//return id
	//p.forward = src.input
}

func (src *Coord) Run() error {
	defer func() {
		for _, ch := range src.backPointers {
			close(ch)
		}
	}()

	start := time.Now()
	m := make(map[int]float64)

	nnodes := len(src.backPointers)
	thresh := 0.0
	items := 0
	sketchsize := 0
	var ucm UnionSketch

	for i, ch := range src.backPointers {
		select {
		case ch <- InitRound{i}:
		case <-src.StopNotifier:
			panic("wtf!")
		}
	}

	round1Access := disttopk.NewAlgoStatsRoundUnion()
	listlensum := 0
	listlenbytes := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case obj := <-src.input:
			dobj := obj.(disttopk.DemuxObject)
			fr := dobj.Obj.(FirstRound)
			il := fr.List
			items += len(il)
			m = il.AddToMap(m)

			compressedsize := fr.Sketch.ByteSize()
			decompressed := src.decompress(fr.Sketch)
			if len(decompressed) > 0 {
				sketchsize += compressedsize
			}
			sketch := src.PeerSketchAdaptor.deserialize(decompressed)

			//cm := fr.cm.(*disttopk.CountMinSketch)
			//sketchsize += cm.ByteSize()
			round1Access.AddPeerStats(*fr.Stats)

			if ucm == nil {
				ucm = src.getUnionSketch(sketch, il, dobj.Id)
			} else {
				src.mergeIntoUnionSketch(ucm, sketch, il, dobj.Id)
				//ucm.Merge(sketch.(disttopk.Sketch))
			}

			if src.SendLength {
				listlensum += int(fr.Length)
				listlenbytes += 4
				round1Access.AddPeerStats(disttopk.AlgoStatsRound{Bytes_sketch: 4})
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

	err, round2Access, round2items, m, ufThresh, total_back_bytes, add_sketch_bytes := src.RunSendFilterThreshold(ucm, uint32(localthresh), il, m, listlensum)
	if err != nil {
		return err
	}

	bytesRound = round2items*disttopk.RECORD_SIZE + total_back_bytes + add_sketch_bytes
	bytes += bytesRound
	fmt.Print("Round 2 tr: got ", round2items, " items (", round2items/nnodes, "/node), bytes for records: ", round2items*disttopk.RECORD_SIZE, " bytes filter: ", total_back_bytes, ". bytes additional sketch: ", add_sketch_bytes, " BW Round: ", bytesRound, " BW total: ", bytes, "\n")
	fmt.Printf("Round 2 tr: access %+v\n", round2Access)
	src.Stats.Bytes_transferred = uint64(bytes)
	src.Stats.AddRound(*round1Access)
	src.Stats.AddRound(*round2Access)
	src.Stats.Rounds = 2

	il = disttopk.MakeItemList(m)
	il.Sort()
	fmt.Println("Sorted Global List:  top-k score", il[src.k-1].Score, "Thresh of previous filter", ufThresh)
	if uint(il[src.k-1].Score) < ufThresh {
		if src.Approximate {
			fmt.Println("WARNING, result may be inexact")
		} else {
			src.Stats.Rounds = 3
			round3Thresh := il[src.k-1].Score
			round2UfThresh := ufThresh
			err, round3Access, round3items, m, ufThresh, round3_back_bytes, add_sketch_bytes := src.RunSendFilterThreshold(ucm, uint32(round3Thresh), il, m, listlensum)

			if round3Access == nil {
				//round 3 aborted
				src.Stats.Rounds = 2
				goto end
			}

			if add_sketch_bytes > 0 {
				panic("snh")
			}
			if err != nil {
				return err
			}
			if round2UfThresh <= uint(round3Thresh) {
				panic(fmt.Sprintln("Should never happen", round2UfThresh, round3Thresh))
			}
			if ufThresh > uint(round3Thresh) {
				panic(fmt.Sprintln("Should never happen", ufThresh, round3Thresh))
			}

			bytesRound = round3items*disttopk.RECORD_SIZE + round3_back_bytes
			bytes += bytesRound
			fmt.Print("Round 3 tr: got ", round3items, " items (", round3items/nnodes, "/node), bytes for records: ", round3items*disttopk.RECORD_SIZE, "bytes filter: ", round3_back_bytes, ". BW Round: ", bytesRound, "BW total: ", bytes, "\n")
			fmt.Printf("Round 3 tr: access %+v\n", round3Access)
			src.Stats.Bytes_transferred = uint64(bytes)
			src.Stats.AddRound(*round3Access)
			il = disttopk.MakeItemList(m)
			il.Sort()
			//		panic(fmt.Sprintf("topk-score < approx thresh. Need to implement third round. score %v, approxThresh %v", uint(il[src.k-1].Score), ufThresh))
		}
	}

end:
	if disttopk.OUTPUT_RESP {
		for _, it := range il[:src.k] {
			fmt.Println("Resp: ", it.Id, it.Score)
		}
	}

	src.Stats.Took = time.Since(start)
	src.FinalList = il
	return nil
}

func (src *Coord) RunSendFilterThreshold(ucm UnionSketch, thresh uint32, il disttopk.ItemList, m map[int]float64, listlensum int) (err error, access *disttopk.AlgoStatsRoundUnion, items int, ret_m map[int]float64, usedThresh uint, bytes_back int, bytes_additional int) {

	total_back_bytes := 0
	uf, ufThresh := src.getUnionFilter(ucm, thresh, il, listlensum)
	if uf != nil {
		fmt.Println("Uf info: ", uf.GetInfo())
	} else {
		fmt.Println("Uf is Nil. ALL remaining items will be sent in second round")
	}
	ufe, ok := uf.(UnionFilterEmpty)
	if ok {
		if ufe.isEmpty() {
			fmt.Println("Uf is empty, exiting round 3")
			return nil, nil, 0, nil, 0, 0, 0
		}
	}

	round2Access := disttopk.NewAlgoStatsRoundUnion()
	for _, ch := range src.backPointers {
		//uf: := src.getUnionFilter(ucm, uint32(localthresh))
		var ser Serialized
		if uf != nil {
			cuf := src.copyUnionFilter(uf)
			ser = Serialized(src.compress(src.UnionSketchAdaptor.serialize(cuf)))
			total_back_bytes += ser.ByteSize()
		}
		select {
		case ch <- SecondRound{ser}:
		case <-src.StopNotifier:
			panic("wtf!")
		}
	}

	round2items := 0
	groundtruthfp := -1
	filterfp := -1
	additionalSketchBytes := 0
	for cnt := 0; cnt < len(src.backPointers); cnt++ {
		select {
		case obj := <-src.input:
			dobj := obj.(disttopk.DemuxObject)
			srpr := dobj.Obj.(SecondRoundPeerReply)
			il := srpr.List
			m = il.AddToMap(m)
			round2items += len(il)
			round2Access.AddPeerStats(*srpr.Stats)
			if srpr.AdditionalSketch != nil {
				additionalSketchBytes += len(srpr.AdditionalSketch)
				padd := src.PeerSketchAdaptor.(PeerAdditionalSketchAdaptor)
				addsketch := padd.deserializeAdditionalSketch(srpr.AdditionalSketch)
				uadd := src.UnionSketchAdaptor.(UnionAdditonalSketchAdaptor)
				uadd.mergeAdditionalSketchIntoUnionSketch(ucm, addsketch, il, dobj.Id)
			}

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

	return nil, round2Access, round2items, m, ufThresh, total_back_bytes, additionalSketchBytes
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
