package main

import (
	"github.com/cevian/disttopk"
	"github.com/cevian/disttopk/cm"
	"github.com/cevian/disttopk/cmfilter"
	"github.com/cevian/disttopk/klee"
	"github.com/cevian/disttopk/naive"
	"github.com/cevian/disttopk/tput"
	"github.com/cevian/disttopk/tworound"
	"github.com/cevian/go-stream/stream"
	//"github.com/cloudflare/go-stream/util/slog";
	"fmt"
	"os"
	"runtime"
)

var _ = os.Exit

const BASE_DATA_PATH = "/home/arye/goprojects/src/github.com/cevian/disttopk/data/"

//const BASE_DATA_PATH = "/home/arye/go-stream/src/github.com/cevian/disttopk/data/"

func runNaive(l []disttopk.ItemList, cutoff int) disttopk.ItemList {
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
	return coord.FinalList
}

func runTput(l []disttopk.ItemList, k int) disttopk.ItemList {
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
	return coord.FinalList
}

func runKlee(l []disttopk.ItemList, k int) disttopk.ItemList {
	runner := stream.NewRunner()
	peers := make([]*klee.Peer, len(l))
	coord := klee.NewCoord(k)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = klee.NewPeer(list, k)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList
}

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

func runBloomSketch(l []disttopk.ItemList, topk int) disttopk.ItemList {
	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	coord := tworound.NewBloomCoord(topk)
	numpeer := 33
	N_est := 2700000
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tworound.NewBloomPeer(list, topk, numpeer, N_est)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList
}

func runBloomSketchGcs(l []disttopk.ItemList, topk int) disttopk.ItemList {
	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	coord := tworound.NewBloomCoord(topk)
	numpeer := 33
	N_est := 2700000
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tworound.NewBloomPeerGcs(list, topk, numpeer, N_est)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList
}

func getRecall(exact disttopk.ItemList, approx disttopk.ItemList, k int) float64 {
	em := exact.AddToMap(nil)
	found := 0
	for i := 0; i < k; i++ {
		item := approx[i]
		_, ok := em[item.Id]
		if ok {
			found += 1
		}
	}
	return float64(found) / float64(k)
}

func getScoreError(exact disttopk.ItemList, approx disttopk.ItemList, k int) float64 {
	err := 0.0
	for i := 0; i < k; i++ {
		aitem := approx[i]
		eitem := exact[i]
		e := 0.0
		if aitem.Score > eitem.Score {
			e = aitem.Score - eitem.Score
		} else {
			e = eitem.Score - aitem.Score
		}
		err += e
	}
	return err / float64(k)
}

func getScoreErrorRel(exact disttopk.ItemList, approx disttopk.ItemList, k int) float64 {
	err := 0.0
	for i := 0; i < k; i++ {
		aitem := approx[i]
		eitem := exact[i]
		e := 0.0
		if aitem.Score > eitem.Score {
			e = aitem.Score - eitem.Score
		} else {
			e = eitem.Score - aitem.Score
		}
		err += (e / eitem.Score)
	}
	return err / float64(k)
}

func main() {
	args := os.Args
	var source string
	if len(args) > 1 {
		source = args[1]
	} else {
		source = "zipf"
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("Reading from " + source)
	var l []disttopk.ItemList
	//l is a list of lists; each top-level list is the data from each peer.
	if source == "zipf" {
		l = disttopk.GetListSet(10, 10000, 0.8, 0.7)
	} else if source == "UCB" {
		fs := &disttopk.FileSource{&disttopk.UcbFileSourceAdaptor{KeyOnClient: false, ModServers: 10}}
		l = fs.ReadFilesAndCache(BASE_DATA_PATH+"ucb/UCB-home*", BASE_DATA_PATH+"cache")
	} else if source == "WC" {
		fs := &disttopk.FileSource{&disttopk.WcFileSourceAdaptor{KeyOnClient: true}}
		l = fs.ReadFilesAndCache(BASE_DATA_PATH+"wc/wc_day*", BASE_DATA_PATH+"cache")
	} else {
		fmt.Println("Source should be 'WC', 'zipf', or 'UCB'. Default is zipf.")
		os.Exit(1)
	}

	fmt.Println("List Head: ", l[0][:2], l[1][:2])
	fmt.Println("List Tail: ", l[0][len(l[0])-3:], l[1][len(l[1])-3:])

	l1norm := 0.0
	items := 0
	ids := make(map[int]bool)
	for _, list := range l {
		items += len(list)
		for _, item := range list {
			l1norm += item.Score
			ids[item.Id] = true
		}
	}

	k := 10
	eps := 0.0001
	fmt.Println("#Items (sum in lists) ", items, " (unique)", len(ids), ", #lists", len(l), " L1 Norm is ", l1norm, "Error should be ", eps*l1norm)
	ids = make(map[int]bool)
	naivel := runNaive(l, 0)
	/*
		info := ""
		for _, knaive := range []int{10, 50, 100} {
			for _, headroom := range []int{1, 2, 5, 10, 15} {
				naivecutl := runNaive(l, knaive*headroom)
				fmt.Println("Naiive k", knaive, " headroom", headroom, " recall ", getRecall(naivel, naivecutl, k), " Score err ", getScoreError(naivel, naivecutl, k), " Rel ", getScoreErrorRel(naivel, naivecutl, k))
				info += fmt.Sprintln(knaive, headroom, getRecall(naivel, naivecutl, k), getScoreError(naivel, naivecutl, k), getScoreErrorRel(naivel, naivecutl, k))
			}
		}
		fmt.Println(info)
		runtime.GC()
		runTput(l, k)
		runtime.GC()

		cml := runCmFilter(l, k, eps, 0.01)
	*/
	runTput(l, k)
	runtime.GC()
	//cml := runBloomSketch(l, k)
	//cml := runBloomSketchGcs(l, k)
	cml := runKlee(l, k)
	fmt.Println("Klee stats, recall:", getRecall(naivel, cml, k), getScoreError(naivel, cml, k), getScoreErrorRel(naivel, cml, k))
	//_ = naivecutl
	//_ = tputl
	_ = cml

	match := true

	for i := 0; i < k; i++ {
		compare := cml[i]
		if naivel[i] != compare {
			fmt.Println("Lists do not match", naivel[i], compare)
			match = false
		}
	}
	if match == true {
		fmt.Println("Lists Match")
	}
	fmt.Println("The K'th Item:", naivel[k-1])
}
