package tworound

import "github.com/cevian/disttopk"

type PeerSketchAdaptor interface {
	createSketch(list disttopk.ItemList, localtop disttopk.ItemList) (sketch FirstRoundSketch, NumberOfSerialAccessesOverLocaltop int)
	serialize(FirstRoundSketch) Serialized
	deserialize(Serialized) FirstRoundSketch
}

type BloomHistogramPeerSketchAdaptor struct {
	topk    int
	numpeer int
	N_est   int
}

func NewBloomHistogramPeerSketchAdaptor(topk int, numpeer int, N_est int) PeerSketchAdaptor {
	return &BloomHistogramPeerSketchAdaptor{topk, numpeer, N_est}
}

func (t *BloomHistogramPeerSketchAdaptor) createSketch(list disttopk.ItemList, localtop disttopk.ItemList) (FirstRoundSketch, int) {
	s := disttopk.NewBloomSketch(t.topk, t.numpeer, t.N_est)
	accesses := s.CreateFromList(list)
	return s, accesses - len(localtop)
}

func (*BloomHistogramPeerSketchAdaptor) serialize(c FirstRoundSketch) Serialized {
	obj, ok := c.(*disttopk.BloomHistogram)
	if !ok {
		panic("Unexpected")
	}
	b, err := disttopk.SerializeObject(obj)
	if err != nil {
		panic(err)
	}
	return b
	//return c
}

func (t *BloomHistogramPeerSketchAdaptor) deserialize(frs Serialized) FirstRoundSketch {
	bs := frs
	obj := &disttopk.BloomHistogram{}
	err := disttopk.DeserializeObject(obj, []byte(bs))
	if err != nil {
		panic(err)
	}
	return obj
	//return frs.(FirstRoundSketch)
}

type BloomHistogramGcsPeerSketchAdaptor struct {
	*BloomHistogramPeerSketchAdaptor
}

func NewBloomHistogramGcsPeerSketchAdaptor(topk int, numpeer int, N_est int) PeerSketchAdaptor {
	return &BloomHistogramGcsPeerSketchAdaptor{&BloomHistogramPeerSketchAdaptor{topk, numpeer, N_est}}
}

func (t *BloomHistogramGcsPeerSketchAdaptor) createSketch(list disttopk.ItemList, localtop disttopk.ItemList) (FirstRoundSketch, int) {
	s := disttopk.NewBloomSketchGcs(t.topk, t.numpeer, t.N_est)
	return s, s.CreateFromList(list) - len(localtop)
}
