package main

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
	"fmt"
	//	"strconv"
	"os"
	"runtime"
)

var _ = os.Exit

const BASE_DATA_PATH = "/home/arye/goprojects/src/github.com/cevian/disttopk/data/"

//const BASE_DATA_PATH = "/home/arye/go-stream/src/github.com/cevian/disttopk/data/"

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

func RunBloomSketch(l []disttopk.ItemList, topk int) disttopk.ItemList {
	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	coord := tworound.NewBloomCoord(topk)
	numpeer := len(l)
	N_est := getNEst(l)
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

func RunBloomSketchGcs(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	coord := tworound.NewBloomGcsCoord(topk)
	numpeer := len(l)
	N_est := getNEst(l)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tworound.NewBloomGcsPeer(list, topk, numpeer, N_est)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

func RunBloomSketchGcsMerge(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
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

func RunCountMin(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	coord := tworound.NewCountMinCoord(topk)
	numpeer := len(l)
	N_est := getNEst(l)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tworound.NewCountMinPeer(list, topk, numpeer, N_est)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

func RunApproximateBloomFilter(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	coord := tworound.NewApproximateBloomFilterCoord(topk)
	numpeer := len(l)
	N_est := getNEst(l)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tworound.NewApproximateBloomFilterPeer(list, topk, numpeer, N_est)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

func RunApproximateBloomGcsFilter(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	N_est := getNEst(l)
	coord := tworound.NewApproximateBloomGcsFilterCoord(topk, N_est)
	coord.GroundTruth = GroundTruth
	numpeer := len(l)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tworound.NewApproximateBloomGcsFilterPeer(list, topk, numpeer, N_est)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

func RunExtraRoundBloomGcsMergeFilter(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	coord := tworound.NewExtraRoundBloomGcsMergeCoord(topk)
	numpeer := len(l)
	N_est := getNEst(l)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tworound.NewExtraRoundBloomGcsMergePeer(list, topk, numpeer, N_est)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

type Algorithm struct {
	name   string
	runner func([]disttopk.ItemList, int) (disttopk.ItemList, disttopk.AlgoStats)
}

var algorithms []Algorithm = []Algorithm{
	//	Algorithm{"Naive-exact",  runNaiveExact},
	// Approximate:
	//	Algorithm{"Naive (2k)",   runNaiveK2},
	Algorithm{"Klee3-2R", RunKlee3},
	Algorithm{"Klee4-3R", RunKlee4},
	Algorithm{"Approx bloom", RunApproximateBloomFilter},
	Algorithm{"App bloom GCS", RunApproximateBloomGcsFilter},
	// Extra-Round Exact
	Algorithm{"ER GCS-M", RunExtraRoundBloomGcsMergeFilter},
	Algorithm{"ER TPUT-hash", RunTputHashExtraRound},
	// Exact
	Algorithm{"TPUT   ", RunTput},
	Algorithm{"TPUT-hash", RunTputHash},
	Algorithm{"2R Gcs  ", RunBloomSketchGcs},
	Algorithm{"2R Gcs-Merge", RunBloomSketchGcsMerge},
	Algorithm{"Count Min", RunCountMin},
}

//var algo_names []string = []string{"Naive-exact", "Naive (2k)", "Klee3-2R", "Klee4-3R", "Approx bloom", "TPUT   ", "TPUT-hash", "2R Gcs  ", "2R Gcs-Merge", "Count Min"}

var GroundTruth disttopk.ItemList

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
	ids = make(map[int]bool)
	naive_exact, _ := RunNaive(data, 0)
	ground_truth := naive_exact
	GroundTruth = ground_truth
	fmt.Println("#Items (sum in lists) ", items, " (unique)", len(ids), ", #lists", len(data), "k-score", ground_truth[k-1].Score)

	//stats for count min:
	// eps := 0.0001
	//fmt.Println(" L1 Norm is ", l1norm, "Error should be ", eps*l1norm)

	//	var meths_to_run
	//type rank_algorithm func([]disttopk.ItemList, int) (disttopk.ItemList, disttopk.AlgoStats)
	//algos_to_run := []rank_algorithm{runNaiveExact, runNaiveK2, runKlee3, runKlee4, runApproximateBloomFilter, runTput, runTputHash, runBloomSketchGcs, runBloomSketchGcsMerge, runCountMin}

	//cml := runBloomSketch(l, k)
	//cml := (l, k)

	statsMap := make(map[string]disttopk.AlgoStats)
	allStats := ""
	for _, algorithm := range algorithms {
		name := algorithm.name
		fmt.Println("-----------------------")

		result, stats := algorithm.runner(data, k) //, stats
		stats.CalculatePerformance(ground_truth, result, k)

		stat_string := fmt.Sprintf("%v results: \t\t BW = %v \t Recall = %v (%v)\t Error = %v (rel. %e) \t Access: serial %v, random :%v (%v)\n", name, stats.Bytes_transferred, stats.Recall, stats.Edit_distance, stats.Abs_err, stats.Rel_err, stats.Serial_items, stats.Random_items, stats.Random_access)

		fmt.Print(stat_string)
		allStats += stat_string
		statsMap[name] = stats

		runtime.GC()

		match := true

		for i := 0; i < k; i++ {
			if ground_truth[i] != result[i] {
				fmt.Println("Lists do not match at position", i, ground_truth[i], "vs", result[i])
				match = false
			}
		}
		if match == true {
			fmt.Println("Lists Match")
		}
	} //end loop over algorithms

	fmt.Println("*******************************")
	fmt.Print(allStats)
	fmt.Println("*******************************")

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
		//really good GCS-m result. Why?
		l = disttopk.GetListSet(10, 10000, 0.8, 0.7)
	} else if source == "zipf-disjoint" {
		l = disttopk.GetDisjointSimpleList(10, 10000, 0.7)
	} else if source == "zipf-fo" {
		l = disttopk.GetFullOverlapSimpleList(10, 10000, 0.7)
	} else if source == "zipf-perm" {
		l = disttopk.GetFullOverlapOrderPermutedSimpleList(10, 100000, 0.7, 100)
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
	for _, algorithm := range algorithms {
		fmt.Printf("\t& %s", algorithm.name)
	}
	fmt.Println()
	//now the table
	for dataset, row := range datatable {
		fmt.Print(dataset)
		for _, algorithm := range algorithms {
			name := algorithm.name
			fmt.Printf("\t& %d (edit dist %.6G)", row[name].Bytes_transferred, row[name].Edit_distance)
		}
		fmt.Println()
	}

}
