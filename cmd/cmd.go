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
	//	"strconv"
	"os"
	"runtime"
)

var _ = os.Exit

const BASE_DATA_PATH = "/home/arye/goprojects/src/github.com/cevian/disttopk/data/"

//const BASE_DATA_PATH = "/home/arye/go-stream/src/github.com/cevian/disttopk/data/"

func runNaive(l []disttopk.ItemList, cutoff int) (disttopk.ItemList, disttopk.AlgoStats) {
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

func runNaiveK2(l []disttopk.ItemList, cutoff int) (disttopk.ItemList, disttopk.AlgoStats) {
	return runNaive(l, 2*cutoff)
}
func runNaiveExact(l []disttopk.ItemList, cutoff int) (disttopk.ItemList, disttopk.AlgoStats) {
	return runNaive(l, 0)
}
func runTput(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
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

func runKlee3(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
	return runKlee(l, k, false)
}

func runKlee4(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
	return runKlee(l, k, true)
}

func runKlee(l []disttopk.ItemList, k int, clRound bool) (disttopk.ItemList, disttopk.AlgoStats) {
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

func runBloomSketchGcs(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
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
	return coord.FinalList, coord.Stats
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

var algo_names []string = []string{"Naive-exact", "Naive (2k)", "TPUT", "Klee3", "Klee4", "2R Exact"}

func analyze_dataset(data []disttopk.ItemList) map[string]disttopk.AlgoStats {
	l1norm := 0.0
	items := 0
	ids := make(map[int]bool)
	for _, list := range data {
		items += len(list)
		for _, item := range list {
			l1norm += item.Score
			ids[item.Id] = true
		}
	}

	k := 10
	eps := 0.0001
	fmt.Println("#Items (sum in lists) ", items, " (unique)", len(ids), ", #lists", len(data), " L1 Norm is ", l1norm, "Error should be ", eps*l1norm)
	ids = make(map[int]bool)
	naive_exact, _ := runNaive(data, 0)
	ground_truth := naive_exact

	//	var meths_to_run
	type rank_algorithm func([]disttopk.ItemList, int) (disttopk.ItemList, disttopk.AlgoStats)
	algos_to_run := []rank_algorithm{runNaiveExact, runNaiveK2, runTput, runKlee3, runKlee4, runBloomSketchGcs}

	//cml := runBloomSketch(l, k)
	//cml := (l, k)

	statsMap := make(map[string]disttopk.AlgoStats)
	for i, algorithm := range algos_to_run {
		fmt.Println("-----------------------")

		result, stats := algorithm(data, k) //, stats
		stats.Recall = getRecall(ground_truth, result, k)

		stats.Abs_err = getScoreError(ground_truth, result, k)
		stats.Rel_err = getScoreErrorRel(ground_truth, result, k)
		fmt.Printf("%v results: BW = %v Recall = %v Error = %v (rel. %e)\n", algo_names[i], stats.Bytes_transferred, stats.Recall, stats.Abs_err, stats.Rel_err)

		statsMap[algo_names[i]] = stats

		runtime.GC()

		match := true

		for i := 0; i < k; i++ {
			if ground_truth[i] != result[i] {
				fmt.Println("Lists do not match", ground_truth[i], result[i])
				match = false
			}
		}
		if match == true {
			fmt.Println("Lists Match")
		}
	} //end loop over algorithms

	return statsMap
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

	fmt.Println("Data source is " + source)
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

	datatable := make(map[string]map[string]disttopk.AlgoStats)
	/*
		for numlists := 5; numlists < 40; numlists += 10 {
			l = disttopk.GetListSet(numlists, 10000, 0.8, 0.7)
			datatable[strconv.Itoa(numlists)] = analyze_dataset(l)
		}*/
	datatable[source] = analyze_dataset(l)

	//table header
	fmt.Print(" ")
	for _, name := range algo_names {
		fmt.Printf("\t& %s", name)
	}
	fmt.Println()
	//now the table
	for dataset, row := range datatable {
		fmt.Print(dataset)
		for _,algo := range algo_names {
			fmt.Printf("\t& %d (r. err %.6G)", row[algo].Bytes_transferred, row[algo].Rel_err )
		}
		fmt.Println()
	}

}
