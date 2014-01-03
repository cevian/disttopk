package tworound

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	"fmt"
	"math"
	"runtime"
)

type ByteSlice []byte

func (b ByteSlice) ByteSize() int {
	return len([]byte(b))
}

func NewBloomPeer(list disttopk.ItemList, topk int, numpeer int, N_est int) *Peer {

	return NewPeer(list, &DefaultPeerAdaptor{topk, numpeer, N_est}, topk)
}

func NewBloomPeerGcs(list disttopk.ItemList, topk int, numpeer int, N_est int) *Peer {

	return NewPeer(list, &GcsPeerAdaptor{&DefaultPeerAdaptor{topk, numpeer, N_est}}, topk)
}

func NewPeer(list disttopk.ItemList, pa PeerAdaptor, k int) *Peer {
	return &Peer{stream.NewHardStopChannelCloser(), pa, nil, nil, list, k, 0}
}

type PeerAdaptor interface {
	createSketch() FirstRoundSketch
	serialize(FirstRoundSketch) Serialized
	deserializeSecondRound(Serialized) UnionFilter
	getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int) ([]disttopk.Item, *AccessAccounting)
}

type DefaultPeerAdaptor struct {
	topk    int
	numpeer int
	N_est   int
}

func (t *DefaultPeerAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int) ([]disttopk.Item, *AccessAccounting) {
	exactlist := make([]disttopk.Item, 0)
	for index, v := range list {
		if index >= cutoff_sent && uf.PassesInt(v.Id) == true {
			exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
		}
	}
	return exactlist, &AccessAccounting{serial_items: len(list), length: len(list)}
}

func (t *DefaultPeerAdaptor) createSketch() FirstRoundSketch {
	return disttopk.NewBloomSketch(t.topk, t.numpeer, t.N_est)
}

func (*DefaultPeerAdaptor) serialize(c FirstRoundSketch) Serialized {
	obj, ok := c.(*disttopk.BloomSketch)
	if !ok {
		panic("Unexpected")
	}
	b, err := disttopk.SerializeObject(obj)
	if err != nil {
		panic(err)
	}
	return ByteSlice(b)
	//return c
}

func (*DefaultPeerAdaptor) deserializeSecondRound(s Serialized) UnionFilter {
	bs := s.(ByteSlice)
	obj := &disttopk.BloomSketchCollection{}
	err := disttopk.DeserializeObject(obj, []byte(bs))
	if err != nil {
		panic(err)
	}
	return obj
}

type GcsPeerAdaptor struct {
	*DefaultPeerAdaptor
}

func (t *GcsPeerAdaptor) createSketch() FirstRoundSketch {
	return disttopk.NewBloomSketchGcs(t.topk, t.numpeer, t.N_est)
}

func (t *GcsPeerAdaptor) getRoundTwoList(uf UnionFilter, list disttopk.ItemList, cutoff_sent int) ([]disttopk.Item, *AccessAccounting) {
	//fmt.Println("entering get round two list")
	list_items := list.Len()

	bsc := uf.(*disttopk.BloomSketchCollection)
	hvf := disttopk.NewHashValueFilter()
	bsc.AddToHashValueFilter(hvf)

	if hvf.NumHashValues() < list_items {

		ht_bits := uint8(math.Ceil(math.Log2(float64(list_items))))
		//ht_bits = 26 //CHANGE ME
		ht := disttopk.NewHashTable(ht_bits)

		for _, v := range list {
			ht.Insert(v.Id, v.Score)
		}
		hvs_sent := disttopk.NewHashValueSlice() //hack wont store hash values
		for i := 0; i < cutoff_sent; i++ {
			hvs_sent.Insert(uint32(list[i].Id))
		}

		//fmt.Println("entering for loops get round two list")

		exactlist := make([]disttopk.Item, 0)
		items_tested := 0
		random_access := 0
		for mod_bits, hvslice := range hvf.GetFilters() {
			//println("Mod 2", mod_bits, hvslice.Len())
			for _, hv := range hvslice.GetSlice() {
				items_map, ra := ht.GetByHashValue(uint(hv), mod_bits)
				random_access += ra
				items_tested += len(items_map)
				for id, score := range items_map {
					if !hvs_sent.Contains(uint32(id)) && uf.PassesInt(id) == true {
						exactlist = append(exactlist, disttopk.Item{id, score})
						hvs_sent.Insert(uint32(id))
					}
				}
			}
		}

		//fmt.Println("Round two list items tested", items_tested, "random access", random_access, "total items", len(list))
		return exactlist, &AccessAccounting{serial_items: 0, random_access: random_access, random_items: items_tested, length: len(list)}
	} else {
		exactlist := make([]disttopk.Item, 0)
		for index, v := range list {
			if index >= cutoff_sent && uf.PassesInt(v.Id) == true {
				exactlist = append(exactlist, disttopk.Item{v.Id, v.Score})
			}
		}
		//fmt.Println("Round two list items used serial test, total items (all sequential tested)", len(list))
		return exactlist, &AccessAccounting{serial_items: len(list), random_access: 0, random_items: 0, length: len(list)}
	}
}

type Peer struct {
	*stream.HardStopChannelCloser
	PeerAdaptor
	forward chan<- disttopk.DemuxObject
	back    <-chan stream.Object
	list    disttopk.ItemList
	k       int
	id      int
}

type FirstRoundSketch interface {
	CreateFromList(list disttopk.ItemList)
	ByteSize() int
}

type Serialized interface {
	ByteSize() int
}

type FirstRound struct {
	list disttopk.ItemList
	cm   Serialized
}

type SecondRound struct {
	ufser Serialized
}

type SecondRoundPeerReply struct {
	list   disttopk.ItemList
	access *AccessAccounting
}

type AccessAccounting struct {
	serial_items  int
	random_access int
	random_items  int
	length        int
}

func (t *AccessAccounting) Merge(other AccessAccounting) {
	t.serial_items += other.serial_items
	t.random_access += other.random_access
	t.random_items += other.random_items
	t.length += other.length
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

	var uf UnionFilter
	select {
	case obj := <-src.back:
		uf = src.deserializeSecondRound(obj.(SecondRound).ufser)
	case <-src.StopNotifier:
		return nil
	}

	exactlist, round2Access := src.getRoundTwoList(uf, src.list, src.k)
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
	deserialize := func(frs Serialized) FirstRoundSketch {
		bs := frs.(ByteSlice)
		obj := &disttopk.BloomSketch{}
		err := disttopk.DeserializeObject(obj, []byte(bs))
		if err != nil {
			panic(err)
		}
		return obj
		//return frs.(FirstRoundSketch)
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

	suf := func(uf UnionFilter) Serialized {
		obj, ok := uf.(*disttopk.BloomSketchCollection)
		if !ok {
			panic("Unexpected")
		}
		b, err := disttopk.SerializeObject(obj)
		if err != nil {
			panic(err)
		}
		return ByteSlice(b)
	}

	gus := func(frs FirstRoundSketch) UnionSketch {
		bs := frs.(*disttopk.BloomSketch)
		bsc := disttopk.NewBloomSketchCollection()
		bsc.Merge(bs)
		return bsc
	}

	return NewCoord(k, deserialize, guf, cuf, suf, gus)
}

func NewCoord(k int,
	des func(Serialized) FirstRoundSketch, guf func(UnionSketch, uint32) UnionFilter, cuf func(uf UnionFilter) UnionFilter, suf func(UnionFilter) Serialized, gus func(FirstRoundSketch) UnionSketch) *Coord {
	return &Coord{stream.NewHardStopChannelCloser(), make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, nil, k, des, guf, cuf, suf, gus}
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
	input                chan disttopk.DemuxObject
	backPointers         []chan<- stream.Object
	lists                [][]disttopk.Item
	FinalList            []disttopk.Item
	k                    int
	deserialize          func(Serialized) FirstRoundSketch
	getUnionFilter       func(UnionSketch, uint32) UnionFilter //disttopk.NewCountMinFilterFromSketch(ucm, uint32(localthresh)
	copyUnionFilter      func(UnionFilter) UnionFilter         //disttopk.NewCountMinFilterFromSketch(ucm, uint32(localthresh)
	serializeUnionFilter func(UnionFilter) Serialized          //disttopk.NewCountMinFilterFromSketch(ucm, uint32(localthresh)
	getUnionSketch       func(FirstRoundSketch) UnionSketch
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
	fmt.Println("Round 1 tr: got ", items, " items, thresh ", thresh, "(log ", uint32(math.Log(localthresh)), "), bytes ", bytesRound)
	bytes := bytesRound

	total_back_bytes := 0
	uf := src.getUnionFilter(ucm, uint32(localthresh))
	fmt.Println("Uf info: ", uf.GetInfo())

	for _, ch := range src.backPointers {
		//uf := src.getUnionFilter(ucm, uint32(localthresh))
		cuf := src.copyUnionFilter(uf)
		ser := src.serializeUnionFilter(cuf)
		total_back_bytes += ser.ByteSize()
		select {
		case ch <- SecondRound{ser}:
		case <-src.StopNotifier:
			return nil
		}
	}

	round2items := 0
	round2Access := &AccessAccounting{}
	for cnt := 0; cnt < nnodes; cnt++ {
		select {
		case dobj := <-src.input:
			srpr := dobj.Obj.(SecondRoundPeerReply)
			il := srpr.list
			m = il.AddToMap(m)
			round2items += len(il)
			mresp = il.AddToCountMap(mresp)
			round2Access.Merge(*srpr.access)
		case <-src.StopNotifier:
			return nil
		}
	}

	bytesRound = round2items*disttopk.RECORD_SIZE + total_back_bytes
	fmt.Println("Round 2 tr: got ", round2items, " items, bytes record", round2items*disttopk.RECORD_SIZE, "bytes filter", total_back_bytes, " bytes", bytesRound)
	fmt.Printf("Round 2 tr: access %+v\n", round2Access)
	bytes += bytesRound
	fmt.Printf("Total bytes tr: %E\n", float64(bytes))

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
