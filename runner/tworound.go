 package runner

import (
	"github.com/cevian/disttopk"
	"github.com/cevian/disttopk/tworound"
	"github.com/cevian/go-stream/stream"
	//"github.com/cloudflare/go-stream/util/slog";
	//"fmt"
	//	"strconv"
)

type TwoRoundRunner struct{
	runnerGenerator func(l []disttopk.ItemList, numpeer int, Nest int, topk int,GroundTruth disttopk.ItemList) *tworound.ProtocolRunner
	Name string
	Exact bool
}

func NewTwoRoundRunner(gen func(l []disttopk.ItemList, numpeer int, Nest int, topk int, GroundTruth disttopk.ItemList) *tworound.ProtocolRunner, name string, exact bool) *TwoRoundRunner {
	return &TwoRoundRunner{gen, name, exact}
}

func (t *TwoRoundRunner) Run(l []disttopk.ItemList, topk int, GroundTruth disttopk.ItemList, Nest int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	//Nest := getNEst(l)

	pr := t.runnerGenerator(l, numpeer, Nest, topk, GroundTruth)
	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	coord := pr.NewCoord()
	runner.Add(coord)
	for i, list := range l {
		peers[i] = pr.NewPeer(list)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}
func (t *TwoRoundRunner) GetName() string {
	return t.Name
} 

func (t *TwoRoundRunner) IsExact() bool {
	return t.Exact
} 




func NewBloomSketchRunner() *TwoRoundRunner {
	gen := func(l []disttopk.ItemList, numpeer int, Nest int, topk int, GroundTruth disttopk.ItemList) *tworound.ProtocolRunner{
		return tworound.NewBloomPR(topk, numpeer, Nest, 1.0)
	}

	return NewTwoRoundRunner(gen, "Bloom Sketch", true)
}

/*
func NewSbr2rNoMergeRunner() *TwoRoundRunner {
	gen := func(l []disttopk.ItemList,numpeer int, Nest int, topk int ) *tworound.ProtocolRunner{
		return tworound.NewBloomGcsPR(topk, numpeer, Nest, 1.0)	
	}

	return NewTwoRoundRunner(gen, "2R Gcs", true)
}*/

func NewSbr2RRunner() *TwoRoundRunner {
	gen := func(l []disttopk.ItemList,numpeer int, Nest int, topk int, GroundTruth disttopk.ItemList ) *tworound.ProtocolRunner{
		return tworound.NewBloomGcsMergePR(topk, numpeer, Nest, 1.0)	
	}

	return NewTwoRoundRunner(gen, "SBR-2R", true)
}
/*
func NewCountMinRunner() *TwoRoundRunner {
	gen := func(l []disttopk.ItemList,numpeer int, Nest int, topk int) *tworound.ProtocolRunner{
		return tworound.NewCountMinPR(topk, numpeer, Nest)	
	}

	return NewTwoRoundRunner(gen, "CountMin", true)
}

func NewApproximateBloomFilterRunner() *TwoRoundRunner {
	gen := func(l []disttopk.ItemList, numpeer int, Nest int, topk int) *tworound.ProtocolRunner{
		return tworound.NewApproximateBloomFilterPR(topk, numpeer, Nest)	
	}

	return NewTwoRoundRunner(gen, "bloom", false)
}
*/
func NewSbrARunner() *TwoRoundRunner {
	gen := func(l []disttopk.ItemList, numpeer int, Nest int, topk int, GroundTruth disttopk.ItemList) *tworound.ProtocolRunner{
		pr := tworound.NewApproximateBloomGcsFilterPR(topk, numpeer, Nest)
		pr.GroundTruth = GroundTruth
		return pr
	}

	return NewTwoRoundRunner(gen, "SBR-A", false)
}

func NewSbrErNoSplitRunner() *TwoRoundRunner {
	gen := func(l []disttopk.ItemList,numpeer int, Nest int, topk int, GroundTruth disttopk.ItemList) *tworound.ProtocolRunner{
		return tworound.NewExtraRoundBloomGcsMergePR(topk, numpeer, Nest, 1.0)	
	}

	return NewTwoRoundRunner(gen, "SBR-ER NoSplit", true)
}

func NewSbrErUnderNestRunner() *TwoRoundRunner {
	gen := func(l []disttopk.ItemList, numpeer int, Nest int, topk int, GroundTruth disttopk.ItemList) *tworound.ProtocolRunner{
		return tworound.NewExtraRoundBloomGcsMergeSplitPR(topk, numpeer, Nest, 0.0)	
	}

	return NewTwoRoundRunner(gen, "SBR-ER Under", true)
}

func NewSbrErRunner() *TwoRoundRunner {
	return NewSbrErOverNestRunner()
}

func NewSbrErOverNestRunner() *TwoRoundRunner {
	gen := func(l []disttopk.ItemList, numpeer int, Nest int, topk int, GroundTruth disttopk.ItemList) *tworound.ProtocolRunner{
		return tworound.NewExtraRoundBloomGcsMergeSplitPR(topk, numpeer, Nest, 1.0)	
	}

	return NewTwoRoundRunner(gen, "SBR-ER", true)
}


func NewSbrErIdealNestRunner() *TwoRoundRunner {
	gen := func(l []disttopk.ItemList,numpeer int, Nest int, topk int, GroundTruth disttopk.ItemList) *tworound.ProtocolRunner{
		return tworound.NewExtraRoundBloomGcsMergeSplitPR(topk, numpeer, Nest, 1.0)	
	}

	return NewTwoRoundRunner(gen, "SBR-ER IdealNest", true)
}








