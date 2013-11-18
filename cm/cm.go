package cm

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	"fmt"
)

func NewPeer(list disttopk.ItemList, k int, eps float64, delta float64) *Peer {
	return &Peer{stream.NewHardStopChannelCloser(), nil, nil, list, k, 0, eps, delta}
}

type Peer struct {
	*stream.HardStopChannelCloser
	forward chan<- disttopk.DemuxObject
	back    <-chan stream.Object
	list    disttopk.ItemList
	k       int
	id      int
	eps     float64
	delta   float64
}

type FirstRound struct {
	list disttopk.ItemList
	cm   *disttopk.CountMinSketch
}

type SecondRound struct {
	thresh uint32
	ucm    *disttopk.CountMinSketch
}

func (src *Peer) Run() error {
	//defer close(src.forward)
	src.list.Sort()
	//fmt.Println("Sort", src.list[:10])

	localtop := src.list[:src.k]

	localcm := disttopk.NewCountMinSketchPb(src.eps, src.delta)
	for _, v := range src.list {
		localcm.AddInt(v.Id, uint32(v.Score))
	}

	select {
	case src.forward <- disttopk.DemuxObject{src.id, FirstRound{localtop, localcm}}:
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
		if index >= src.k && sr.ucm.QueryInt(v.Id) >= sr.thresh {
			exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
		}
	}

	select {
	case src.forward <- disttopk.DemuxObject{src.id, disttopk.ItemList(exactlist)}:
	case <-src.StopNotifier:
		return nil
	}

	return nil
}

func NewCoord(k int) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, nil, k}
}

type Coord struct {
	*stream.HardStopChannelCloser
	input        chan disttopk.DemuxObject
	backPointers []chan<- stream.Object
	lists        [][]disttopk.Item
	FinalList    []disttopk.Item
	k            int
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
	var ucm *disttopk.CountMinSketch
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			fr := dobj.Obj.(FirstRound)
			il := fr.list
			items += len(il)
			m = il.AddToMap(m)
			mresp = il.AddToCountMap(mresp)
			if ucm == nil {
				ucm = fr.cm
			} else {
				ucm.Merge(fr.cm)
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

	cmItems := ucm.Hashes * ucm.Columns

	bytesRound := items*disttopk.RECORD_SIZE + (nnodes * cmItems * 4)
	fmt.Println("Round 1 cm: got ", items, " items, thresh ", thresh, ", items in cm", cmItems, ", bytes ", bytesRound)
	bytes := bytesRound

	for _, ch := range src.backPointers {
		select {
		case ch <- SecondRound{uint32(localthresh), ucm}:
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

	bytesRound = round2items*disttopk.RECORD_SIZE + (nnodes * cmItems * 32)
	fmt.Println("Round 2 cm: got ", round2items, " items, bytes", bytesRound)
	bytes += bytesRound
	fmt.Println("Total bytes cm: ", bytes)

	il = disttopk.MakeItemList(m)
	il.Sort()
	//fmt.Println("Sorted Global List: ", il[:src.k])
	for _, it := range il[:src.k] {
		fmt.Println("Resp: ", it.Id, it.Score, mresp[it.Id])
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
