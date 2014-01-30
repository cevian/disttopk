package main

import (
	"github.com/cevian/disttopk"
	//"github.com/cevian/disttopk/cm"
	//"github.com/cevian/disttopk/cmfilter"

	"github.com/cevian/disttopk/runner"

	//"github.com/cloudflare/go-stream/util/slog";
	"fmt"
	//	"strconv"
	"os"
	"runtime"
)

var _ = os.Exit

const BASE_DATA_PATH = "/home/arye/goprojects/src/github.com/cevian/disttopk/data/"

//const BASE_DATA_PATH = "/home/arye/go-stream/src/github.com/cevian/disttopk/data/"

type Algorithm struct {
	name   string
	runner func([]disttopk.ItemList, int) (disttopk.ItemList, disttopk.AlgoStats)
}

var algorithms []Algorithm = []Algorithm{
	//	Algorithm{"Naive-exact",  runNaiveExact},
	// Approximate:
	//	Algorithm{"Naive (2k)",   runNaiveK2},
	Algorithm{"Klee3-2R", runner.RunKlee3},
	Algorithm{"Klee4-3R", runner.RunKlee4},
	Algorithm{"Approx bloom", runner.RunApproximateBloomFilter},
	Algorithm{"App bloom GCS", runner.RunApproximateBloomGcsFilter},
	// Extra-Round Exact
	Algorithm{"ER GCS-M", runner.RunExtraRoundBloomGcsMergeFilter},
	Algorithm{"ER TPUT-hash", runner.RunTputHashExtraRound},
	// Exact
	Algorithm{"TPUT   ", runner.RunTput},
	Algorithm{"TPUT-hash", runner.RunTputHash},
	Algorithm{"2R Gcs  ", runner.RunBloomSketchGcs},
	Algorithm{"2R Gcs-Merge", runner.RunBloomSketchGcsMerge},
	Algorithm{"Count Min", runner.RunCountMin},
}

//var algo_names []string = []string{"Naive-exact", "Naive (2k)", "Klee3-2R", "Klee4-3R", "Approx bloom", "TPUT   ", "TPUT-hash", "2R Gcs  ", "2R Gcs-Merge", "Count Min"}

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
	naive_exact, _ := runner.RunNaive(data, 0)
	ground_truth := naive_exact
	runner.GroundTruth = ground_truth
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
		l = disttopk.GetFullOverlapOrderPermutedSimpleListSeedOverlap(10, 100000, 0.7, 10000, 99, 1.0)
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
