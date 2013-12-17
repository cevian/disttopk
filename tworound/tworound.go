package tworound

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	"fmt"
	"math"
)

func NewBloomPeer(list disttopk.ItemList, k int, m int, n_est int, numbloom int) *Peer {
	createSketch := func() FirstRoundSketch {
		return disttopk.NewBloomSketch(numbloom, m, n_est)
	}

	serialize := func(c FirstRoundSketch) FirstRoundSerialized {
		return c
	}

	return NewPeer(list, k, createSketch, serialize)
}

func NewPeer(list disttopk.ItemList, k int, createSketch func() FirstRoundSketch, serialize func(FirstRoundSketch) FirstRoundSerialized) *Peer {
	return &Peer{stream.NewHardStopChannelCloser(), nil, nil, list, k, 0, createSketch, serialize}
}

type Peer struct {
	*stream.HardStopChannelCloser
	forward      chan<- disttopk.DemuxObject
	back         <-chan stream.Object
	list         disttopk.ItemList
	k            int
	id           int
	createSketch func() FirstRoundSketch
	serialize    func(FirstRoundSketch) FirstRoundSerialized
}

type FirstRoundSketch interface {
	CreateFromList(list disttopk.ItemList)
	ByteSize() int
}

type FirstRoundSerialized interface {
	ByteSize() int
}

type FirstRound struct {
	list disttopk.ItemList
	cm   FirstRoundSerialized
}

type SecondRound struct {
	cmf UnionFilter
}

func (src *Peer) Run() error {
	//defer close(src.forward)
	src.list.Sort()
	//fmt.Println("Sort", src.list[:10])

	if src.k > len(src.list) {
		fmt.Println("warning cmfilter: list shorter than k")
		src.k = len(src.list)
	}

	localtop := src.list[:src.k]

	sketch := src.createSketch()
	sketch.CreateFromList(src.list)
	ser := src.serialize(sketch)

	select {
	case src.forward <- disttopk.DemuxObject{src.id, FirstRound{localtop, ser}}:
	case <-src.StopNotifier:
		return nil
	}

	var sr SecondRound
	select {
	case obj := <-src.back:
		sr = obj.(SecondRound)
	case <-src.StopNotifier:
		return nil
	}

	exactlist := make([]disttopk.Item, 0)
	for index, v := range src.list {
		if index >= src.k && sr.cmf.PassesInt(v.Id) == true {
			exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
		}
	}

	//fmt.Println("SR", sr.cmf.GetInfo())

	select {
	case src.forward <- disttopk.DemuxObject{src.id, disttopk.ItemList(exactlist)}:
	case <-src.StopNotifier:
		return nil
	}

	return nil
}

func NewBloomCoord(k int) *Coord {
	deserialize := func(frs FirstRoundSerialized) FirstRoundSketch {
		return frs.(FirstRoundSketch)
	}

	guf := func(us UnionSketch, thresh uint32) UnionFilter {
		//bs := us.(*disttopk.BloomSketch)
		bs := us.(*disttopk.BloomSketchCollection)
		bs.SetThresh(thresh)

		return bs
	}

	cuf := func(uf UnionFilter) UnionFilter {
		bs := uf.(*disttopk.BloomSketchCollection)

		copy_uf := *bs
		return &copy_uf
	}

	gus := func(frs FirstRoundSketch) UnionSketch {
		bs := frs.(*disttopk.BloomSketch)
		bsc := disttopk.NewBloomSketchCollection()
		bsc.Merge(bs)
		return bsc
	}

	return NewCoord(k, deserialize, guf, cuf, gus)
}

func NewCoord(k int,
	des func(FirstRoundSerialized) FirstRoundSketch, guf func(UnionSketch, uint32) UnionFilter, cuf func(uf UnionFilter) UnionFilter, gus func(FirstRoundSketch) UnionSketch) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, nil, k, des, guf, cuf, gus}
}

type UnionSketch interface {
	Merge(disttopk.Sketch)
	GetInfo() string
}

type UnionFilter interface {
	PassesInt(int) bool
	ByteSize() int
	GetInfo() string
}

type Coord struct {
	*stream.HardStopChannelCloser
	input           chan disttopk.DemuxObject
	backPointers    []chan<- stream.Object
	lists           [][]disttopk.Item
	FinalList       []disttopk.Item
	k               int
	deserialize     func(FirstRoundSerialized) FirstRoundSketch
	getUnionFilter  func(UnionSketch, uint32) UnionFilter //disttopk.NewCountMinFilterFromSketch(ucm, uint32(localthresh)
	copyUnionFilter func(UnionFilter) UnionFilter         //disttopk.NewCountMinFilterFromSketch(ucm, uint32(localthresh)
	getUnionSketch  func(FirstRoundSketch) UnionSketch
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
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			fr := dobj.Obj.(FirstRound)
			il := fr.list
			items += len(il)
			m = il.AddToMap(m)
			mresp = il.AddToCountMap(mresp)

			cm := src.deserialize(fr.cm)
			sketchsize += fr.cm.ByteSize()
			//cm := fr.cm.(*disttopk.CountMinSketch)
			//sketchsize += cm.ByteSize()

			if ucm == nil {
				ucm = src.getUnionSketch(cm)
			} else {
				ucm.Merge(cm.(disttopk.Sketch))
			}
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
	localthresh := thresh

	bytesRound := items*disttopk.RECORD_SIZE + sketchsize
	fmt.Println(ucm.GetInfo())
	fmt.Println("Round 1 cm: got ", items, " items, thresh ", thresh, "(log ", uint32(math.Log(localthresh)), "), bytes ", bytesRound)
	bytes := bytesRound

	total_back_bytes := 0
	uf := src.getUnionFilter(ucm, uint32(localthresh))
	fmt.Println("Uf info: ", uf.GetInfo())

	for _, ch := range src.backPointers {
		//uf := src.getUnionFilter(ucm, uint32(localthresh))
		cuf := src.copyUnionFilter(uf)
		total_back_bytes += uf.ByteSize()
		select {
		case ch <- SecondRound{cuf}:
		case <-src.StopNotifier:
			return nil
		}
	}

	round2items := 0
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			il := dobj.Obj.(disttopk.ItemList)
			m = il.AddToMap(m)
			round2items += len(il)
			mresp = il.AddToCountMap(mresp)
		case <-src.StopNotifier:
			return nil
		}
	}

	bytesRound = round2items*disttopk.RECORD_SIZE + total_back_bytes
	fmt.Println("Round 2 cm: got ", round2items, " items, bytes", bytesRound)
	bytes += bytesRound
	fmt.Printf("Total bytes cm: %E\n", float64(bytes))

	il = disttopk.MakeItemList(m)
	il.Sort()
	//fmt.Println("Sorted Global List: ", il[:src.k])
	if disttopk.OUTPUT_RESP {
		for _, it := range il[:src.k] {
			fmt.Println("Resp: ", it.Id, it.Score, mresp[it.Id])
		}
	}
	src.FinalList = il
	return nil
}

/*
type ZipfSourceOp struct {
	*stream.HardStopChannelCloser
	*stream.BaseOut
	souce ZipfSource
}



func NewZipfSourceOperator(max uint32) ZipfSource {
	hcc := stream.NewHardStopChannelCloser()
	o := stream.NewBaseOut(stream.CHAN_SLACK)
	nrs := ZipfSource{hcc, o, max}
	return &nrs
}

func (src *ZipfSource) GenerateItem(rank int) Item {
	id := rand.Int()
	score := math.Pow(float64(rank), -src.zipParam) / src.zipNorm
	return Item{id, score}
}

func (src *ZipfSource) Run() error {
	defer close(src.Out())
	var count uint32
	count = 0

	slog.Logf(logger.Levels.Debug, "Generating up to %d %s", src.MaxItems, " tuples")
	for {
		rank := count + 1

		item := src.generateItem(rank)
		select {
		case src.Out <- item:
			count = count + 1
		case <-src.StopNotifier:
			return nil
		}

		if count >= src.MaxItems {
			slog.Logf(logger.Levels.Debug, "Generated all tuples %d, %d", count, src.MaxItems)
			return nil
		}
	}

}*/
