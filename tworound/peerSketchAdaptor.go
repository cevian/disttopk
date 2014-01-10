package tworound

import "github.com/cevian/disttopk"

type PeerSketchAdaptor interface {
	createSketch(list disttopk.ItemList) FirstRoundSketch
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

func (t *BloomHistogramPeerSketchAdaptor) createSketch(list disttopk.ItemList) FirstRoundSketch {
	s := disttopk.NewBloomSketch(t.topk, t.numpeer, t.N_est)
	s.CreateFromList(list)
	return s
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

func (t *BloomHistogramGcsPeerSketchAdaptor) createSketch(list disttopk.ItemList) FirstRoundSketch {
	s := disttopk.NewBloomSketchGcs(t.topk, t.numpeer, t.N_est)
	s.CreateFromList(list)
	return s
}

type CountMinPeerSketchAdaptor struct {
	topk int
	//numpeer int
	//N_est   int
}

func NewCountMinPeerSketchAdaptor(topk int) PeerSketchAdaptor {
	return &CountMinPeerSketchAdaptor{topk}
}

func (t *CountMinPeerSketchAdaptor) createSketch(list disttopk.ItemList) FirstRoundSketch {
	eps := 0.0001
	delta := 0.01
	s := disttopk.NewCountMinSketchPb(eps, delta)
	for _, v := range list {
		s.AddInt(v.Id, uint32(v.Score))
	}
	return s
}

func (*CountMinPeerSketchAdaptor) serialize(c FirstRoundSketch) Serialized {
	obj, ok := c.(*disttopk.CountMinSketch)
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

func (t *CountMinPeerSketchAdaptor) deserialize(frs Serialized) FirstRoundSketch {
	bs := frs
	obj := &disttopk.CountMinSketch{}
	err := disttopk.DeserializeObject(obj, []byte(bs))
	if err != nil {
		panic(err)
	}
	return obj
	//return frs.(FirstRoundSketch)
}

type NonePeerSketchAdaptor struct {
}

func NewNonePeerSketchAdaptor() PeerSketchAdaptor {
	return &NonePeerSketchAdaptor{}
}

func (t *NonePeerSketchAdaptor) createSketch(list disttopk.ItemList) FirstRoundSketch {
	return nil
}

func (*NonePeerSketchAdaptor) serialize(c FirstRoundSketch) Serialized {
	return nil
}

func (t *NonePeerSketchAdaptor) deserialize(frs Serialized) FirstRoundSketch {
	return nil
}
