package main

import (
	"github.com/cevian/disttopk"
	"github.com/cevian/disttopk/cm"
	"github.com/cevian/disttopk/cmfilter"
	"github.com/cevian/disttopk/naive"
	"github.com/cevian/disttopk/tput"
	"github.com/cloudflare/go-stream/stream"
	//"github.com/cloudflare/go-stream/util/slog";
	"fmt"
)

func runNaive(l [][]disttopk.Item, cutoff int) disttopk.ItemList {
	runner := stream.NewRunner()
	peers := make([]*naive.NaivePeer, 10)
	coord := naive.NewNaiveCoord(cutoff)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = naive.NewNaivePeer(list, cutoff)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList
}

func runTput(l [][]disttopk.Item, k int) disttopk.ItemList {
	runner := stream.NewRunner()
	peers := make([]*tput.Peer, 10)
	coord := tput.NewCoord(k)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tput.NewPeer(list, k)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList
}

func runCm(l [][]disttopk.Item, k int, eps float64, delta float64) disttopk.ItemList {
	runner := stream.NewRunner()
	peers := make([]*cm.Peer, 10)
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

func runCmFilter(l [][]disttopk.Item, k int, eps float64, delta float64) disttopk.ItemList {
	runner := stream.NewRunner()
	peers := make([]*cmfilter.Peer, 10)
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

func main() {
	l := disttopk.GetListSet(10, 10000, 0.8, 0.7)
	fmt.Println("List Head: ", l[0][:2], l[1][:2])
	fmt.Println("List Tail: ", l[0][len(l[0])-3:], l[1][len(l[1])-3:])

	l1norm := 0.0
	for _, list := range l {
		for _, item := range list {
			l1norm += item.Score
		}
	}

	k := 100
	eps := 0.001
	fmt.Println("L1 Norm is ", l1norm, "Error should be ", eps*l1norm)
	naivel := runNaive(l, 0)
	naivecutl := runNaive(l, k*5)
	tputl := runTput(l, k)
	cml := runCmFilter(l, k, eps, 0.01)

	_ = tputl
	_ = cml

	match := true

	for i := 0; i < k; i++ {
		compare := naivecutl[i]
		if naivel[i] != compare {
			fmt.Println("Lists do not match", naivel[i], compare)
			match = false
		}
	}
	if match == true {
		fmt.Println("Lists Match")
	}
}
