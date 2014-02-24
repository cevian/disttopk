package runner

import (
	"github.com/cevian/disttopk"
	//"github.com/cevian/disttopk/cm"
	//"github.com/cevian/disttopk/cmfilter"
	"github.com/cevian/disttopk/klee"
	"github.com/cevian/disttopk/naive"
	"github.com/cevian/disttopk/tput"
	"github.com/cevian/disttopk/tput-hash"
	"github.com/cevian/disttopk/tworound"
	"github.com/cevian/go-stream/stream"
	//"github.com/cloudflare/go-stream/util/slog";
	//"fmt"
	//	"strconv"
)

var GroundTruth disttopk.ItemList

func RunNaive(l []disttopk.ItemList, cutoff int) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*naive.NaivePeer, len(l))
	coord := naive.NewNaiveCoord(cutoff)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = naive.NewNaivePeer(list, cutoff)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

func RunNaiveK2(l []disttopk.ItemList, cutoff int) (disttopk.ItemList, disttopk.AlgoStats) {
	return RunNaive(l, 2*cutoff)
}
func RunNaiveExact(l []disttopk.ItemList, cutoff int) (disttopk.ItemList, disttopk.AlgoStats) {
	return RunNaive(l, 0)
}
func RunTput(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*tput.Peer, len(l))
	coord := tput.NewCoord(k)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tput.NewPeer(list, k)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

func RunTputHash(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
	return RunTputHashApproximateFlag(l, k, false)
}

func RunTputHashExtraRound(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
	return RunTputHashApproximateFlag(l, k, true)
}

func RunTputHashApproximateFlag(l []disttopk.ItemList, k int, approximate_t2 bool) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*tput_hash.Peer, len(l))
	coord := tput_hash.NewCoord(k, approximate_t2)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tput_hash.NewPeer(list, k)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

func RunKlee3(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
	return RunKlee(l, k, false)
}

func RunKlee4(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
	return RunKlee(l, k, true)
}

func RunKlee(l []disttopk.ItemList, k int, clRound bool) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*klee.Peer, len(l))
	coord := klee.NewCoord(k, clRound)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = klee.NewPeer(list, k, clRound)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

/*
func runCm(l []disttopk.ItemList, k int, eps float64, delta float64) disttopk.ItemList {
	runner := stream.NewRunner()
	peers := make([]*cm.Peer, len(l))
	coord := cm.NewCoord(k)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = cm.NewPeer(list, k, eps, delta)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList
}

func runCmFilter(l []disttopk.ItemList, k int, eps float64, delta float64) disttopk.ItemList {
	runner := stream.NewRunner()
	peers := make([]*cmfilter.Peer, len(l))
	coord := cmfilter.NewCoord(k)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = cmfilter.NewPeer(list, k, eps, delta)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList
}
*/
func getNEst(l []disttopk.ItemList) int {
	/*items := 0
	for _, list := range l {
		items += len(list)
	}
	return items*/
	ids := make(map[int]bool)
	for _, list := range l {
		for _, item := range list {
			ids[item.Id] = true
		}
	}
	return len(ids)
}

func RunProtocolRunner(pr *tworound.ProtocolRunner, l []disttopk.ItemList) (disttopk.ItemList, disttopk.AlgoStats) {
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

func RunBloomSketch(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	N_est := getNEst(l)
	return RunProtocolRunner(tworound.NewBloomPR(topk, numpeer, N_est, 0.0), l)
}

func RunBloomSketchGcs(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	N_est := getNEst(l)
	return RunProtocolRunner(tworound.NewBloomGcsPR(topk, numpeer, N_est, 0.0), l)
}

func RunBloomSketchGcsMerge(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	N_est := getNEst(l)
	return RunProtocolRunner(tworound.NewBloomGcsMergePR(topk, numpeer, N_est, 0.0), l)
}

func RunCountMin(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	N_est := getNEst(l)
	return RunProtocolRunner(tworound.NewCountMinPR(topk, numpeer, N_est), l)
}

func RunApproximateBloomFilter(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	N_est := getNEst(l)
	return RunProtocolRunner(tworound.NewApproximateBloomFilterPR(topk, numpeer, N_est), l)
}

func RunApproximateBloomGcsFilter(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	N_est := getNEst(l)
	pr := tworound.NewApproximateBloomGcsFilterPR(topk, numpeer, N_est)
	pr.GroundTruth = GroundTruth
	return RunProtocolRunner(pr, l)
}

func RunExtraRoundBloomGcsMergeFilter(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	N_est := getNEst(l)
	return RunProtocolRunner(tworound.NewExtraRoundBloomGcsMergePR(topk, numpeer, N_est, 0.0), l)
}

func RunExtraRoundBloomGcsMergeSplitUnderNest(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	N_est := getNEst(l)
	return RunProtocolRunner(tworound.NewExtraRoundBloomGcsMergeSplitPR(topk, numpeer, N_est, 0.0), l)
}

func RunExtraRoundBloomGcsMergeSplitIdealNest(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	N_est := getNEst(l)
	return RunProtocolRunner(tworound.NewExtraRoundBloomGcsMergeSplitPR(topk, numpeer, N_est, -1.0), l)
}

func RunExtraRoundBloomGcsMergeSplitOverNest(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	N_est := getNEst(l)
	return RunProtocolRunner(tworound.NewExtraRoundBloomGcsMergeSplitPR(topk, numpeer, N_est, 1.0), l)
}
