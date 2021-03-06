package tworound

import "github.com/cevian/disttopk"

type PeerSketchAdaptor interface {
	createSketch(list disttopk.ItemList, localtop disttopk.ItemList) (sketch FirstRoundSketch, NumberOfSerialAccessesOverLocaltop int)
	serialize(FirstRoundSketch) Serialized
	deserialize(Serialized) FirstRoundSketch
}

type PeerAdditionalSketchAdaptor interface {
	getAdditionalSketch(uf UnionFilter, list disttopk.ItemList, prevSketch FirstRoundSketch) (sketch FirstRoundSketch, SerialAccess int)
	serializeAdditionalSketch(c FirstRoundSketch) Serialized
	deserializeAdditionalSketch(frs Serialized) FirstRoundSketch
}

type BloomHistogramPeerSketchAdaptor struct {
	topk    int
	numpeer int
	N_est   int
	EstimateParameter disttopk.EstimateParameter
}

func NewBloomHistogramPeerSketchAdaptor(topk int, numpeer int, N_est int, EstimateParameter disttopk.EstimateParameter) PeerSketchAdaptor {
	return &BloomHistogramPeerSketchAdaptor{topk, numpeer, N_est, EstimateParameter}
}

func (t *BloomHistogramPeerSketchAdaptor) createSketch(list disttopk.ItemList, localtop disttopk.ItemList) (FirstRoundSketch, int) {
	s := NewBloomHistogramSketchPlain(t.topk, t.numpeer, t.N_est)
	accesses := s.CreateFromList(list)
	return s, accesses - len(localtop)
}

func (*BloomHistogramPeerSketchAdaptor) serialize(c FirstRoundSketch) Serialized {
	obj, ok := c.(*BloomHistogramSketch)
	if !ok {
		panic("Unexpected")
	}
	b, err := disttopk.SerializeObject(obj.BloomHistogram)
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

func NewBloomHistogramGcsPeerSketchAdaptor(topk int, numpeer int, N_est int, EstimateParameter disttopk.EstimateParameter) PeerSketchAdaptor {
	return &BloomHistogramGcsPeerSketchAdaptor{ NewBloomHistogramPeerSketchAdaptor(topk, numpeer, N_est, EstimateParameter).(*BloomHistogramPeerSketchAdaptor)}
}

func (t *BloomHistogramGcsPeerSketchAdaptor) createSketch(list disttopk.ItemList, localtop disttopk.ItemList) (FirstRoundSketch, int) {
	s := NewBloomHistogramSketchGcs(t.topk, t.numpeer, t.N_est, t.EstimateParameter)
	return s, s.CreateFromList(list) - len(localtop)
}
