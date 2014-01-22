package paramtest

import (
	"fmt"
	"github.com/cevian/disttopk/tworound"
	"github.com/cevian/go-stream/stream"
	"math"
	"testing"
)

import "github.com/cevian/disttopk"

var _ = math.Ceil

func runBloomSketchGcsMerge(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	coord := tworound.NewBloomGcsMergeCoord(topk)
	numpeer := len(l)
	N_est := getNEst(l)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tworound.NewBloomGcsMergePeer(list, topk, numpeer, N_est)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

func TestBloomGcsMergeParameter(t *testing.T) {
	size_sum := 0
	count := 0
	res := ""
	for _, zipfParam := range []float64{2, 1, 0.7, 0.5, 0.3} {
		size := RunBloomGcsMergeParamTest(100000, 10, 10, zipfParam, 100)
		s := fmt.Sprintln("ZipfParam", zipfParam, "Size", size)
		fmt.Print(s)
		res += s
		size_sum += size
		count++
	}
	fmt.Println("************************************")
	fmt.Print(res)
	fmt.Println("Average size", float64(size_sum)/float64(count), "Size sum", size_sum)
}

func RunBloomGcsMergeParamTest(N, Nnodes, k int, zipParam float64, permParam int) (size int) {
	l := disttopk.GetFullOverlapOrderPermutedSimpleList(Nnodes, uint32(N), zipParam, permParam)

	//n := Getn(l[0], k, Nnodes)
	_, stats := runBloomSketchGcsMerge(l, k)
	return int(stats.Bytes_transferred)
}
