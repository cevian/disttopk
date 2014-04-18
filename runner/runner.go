package runner

import (
	"github.com/cevian/disttopk"
	//"github.com/cevian/disttopk/cm"
	//"github.com/cevian/disttopk/cmfilter"
	"github.com/cevian/disttopk/klee"
	"github.com/cevian/disttopk/magic"
	"github.com/cevian/disttopk/naive"
	"github.com/cevian/disttopk/tput"
	"github.com/cevian/disttopk/tput-hash"
	"github.com/cevian/go-stream/stream"
	//"github.com/cloudflare/go-stream/util/slog";
	//"fmt"
	//	"strconv"
)

type Runner interface {
	Run(l []disttopk.ItemList, hts []*disttopk.HashTable, topk int, GroundTruth disttopk.ItemList, Nest int) (disttopk.ItemList, disttopk.AlgoStats)
	GetName() string
	IsExact() bool
}

type PlainRunner struct {
	Runner func(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats)
	Name   string
	Exact  bool
}

func (t *PlainRunner) Run(l []disttopk.ItemList, hts []*disttopk.HashTable, topk int, GroundTruth disttopk.ItemList, Nest int) (disttopk.ItemList, disttopk.AlgoStats) {
	return t.Runner(l, topk)
}

func (t *PlainRunner) GetName() string {
	return t.Name
}

func (t *PlainRunner) IsExact() bool {
	return t.Exact
}

func NewNaiveK2Runner() *PlainRunner {
	runner := func(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
		return RunNaive(l, k*2)
	}

	return &PlainRunner{runner, "Naive (2k)", false}
}

func NewNaiveExactRunner() *PlainRunner {
	runner := func(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
		return RunNaive(l, 0)
	}
	return &PlainRunner{runner, "Naive-exact", true}
}

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

func NewTputRunner() *PlainRunner {
	return &PlainRunner{RunTput, "TPUT", true}
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

func NewTputHRunner() *TputHashRunner {
	return &TputHashRunner{false, "TPUT-H"}
}

func NewTputERRunner() *TputHashRunner {
	return &TputHashRunner{true, "TPUT-ER"}
}

type TputHashRunner struct {
	extraRound bool
	Name       string
}

func (t *TputHashRunner) Run(l []disttopk.ItemList, hts []*disttopk.HashTable, topk int, GroundTruth disttopk.ItemList, Nest int) (disttopk.ItemList, disttopk.AlgoStats) {
	return RunTputHashExtraRoundFlag(l, hts, topk, t.extraRound)
}

func (t *TputHashRunner) GetName() string {
	return t.Name
}

func (t *TputHashRunner) IsExact() bool {
	return true
}

func RunTputHashExtraRoundFlag(l []disttopk.ItemList, hts []*disttopk.HashTable, k int, extra_round bool) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*tput_hash.Peer, len(l))
	coord := tput_hash.NewCoord(k, extra_round) //if extra round true => approximate t2 is true
	runner.Add(coord)
	for i, list := range l {
		ht := hts[i]
		peers[i] = tput_hash.NewPeer(list, ht, k)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

func NewKlee3Runner() *PlainRunner {
	runner := func(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
		return RunKlee(l, k, false)
	}
	return &PlainRunner{runner, "KLEE3", false}
}

func NewKlee4Runner() *PlainRunner {
	runner := func(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
		return RunKlee(l, k, true)
	}
	return &PlainRunner{runner, "KLEE4", false}
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

type MagicRunner struct {
}

func NewMagicRunner() *MagicRunner {
	return &MagicRunner{}
}

func (t *MagicRunner) Run(l []disttopk.ItemList, hts []*disttopk.HashTable, topk int, GroundTruth disttopk.ItemList, Nest int) (disttopk.ItemList, disttopk.AlgoStats) {
	return RunMagic(l, topk, GroundTruth)
}

func (t *MagicRunner) GetName() string {
	return "Magic"
}

func (t *MagicRunner) IsExact() bool {
	return true
}

func RunMagic(l []disttopk.ItemList, k int, groundTruth disttopk.ItemList) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*magic.Peer, len(l))
	coord := magic.NewCoord(k)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = magic.NewPeer(list, k, groundTruth)
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
}*/
