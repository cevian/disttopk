package tworound

import "github.com/cevian/disttopk"

//import "fmt"

const USE_THRESHOLD = true
const USE_SINGLEHASH = true

type CountMinPeerSketchAdaptor struct {
	topk    int
	numpeer int
	Columns int
	N_est   int
}

func NewCountMinPeerSketchAdaptor(topk int, numpeer int, N_est int) PeerSketchAdaptor {
	return &CountMinPeerSketchAdaptor{topk, numpeer, 0, N_est}
}

func (t *CountMinPeerSketchAdaptor) createSketch(list disttopk.ItemList, localtop disttopk.ItemList) FirstRoundSketch {
	//eps := 0.0001
	//delta := 0.01
	//s := disttopk.NewCountMinSketchPb(eps, delta)

	hashes := disttopk.CountMinHashesEst(0.01)
	if USE_SINGLEHASH {
		hashes = 1
	}

	if t.Columns == 0 {
		//eps := 0.0001
		//t.Columns = disttopk.CountMinColumnsEst(eps)
		t.Columns = t.N_est
	}

	s := disttopk.NewCountMinSketch(hashes, t.Columns)

	if USE_THRESHOLD {
		kscore := uint(list[t.topk].Score)
		cutoff := kscore / uint(t.numpeer)
		for _, v := range list {
			if uint(v.Score) > cutoff {
				s.AddWithCutoff(disttopk.IntKeyToByteKey(v.Id), uint(v.Score), cutoff)
			}
		}
	} else {
		for _, v := range list {
			s.Add(disttopk.IntKeyToByteKey(v.Id), uint32(v.Score))
		}

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

func (t *NonePeerSketchAdaptor) createSketch(list disttopk.ItemList, localtop disttopk.ItemList) FirstRoundSketch {
	return nil
}

func (*NonePeerSketchAdaptor) serialize(c FirstRoundSketch) Serialized {
	return nil
}

func (t *NonePeerSketchAdaptor) deserialize(frs Serialized) FirstRoundSketch {
	return nil
}
