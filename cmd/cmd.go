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

func runTputHash(l []disttopk.ItemList, k int) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*tput_hash.Peer, len(l))
	coord := tput_hash.NewCoord(k)
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

func runBloomSketch(l []disttopk.ItemList, topk int) disttopk.ItemList {
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

func runBloomSketchGcs(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
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

func runCountMin(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
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

func runApproximateBloomFilter(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
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

func runApproximateBloomGcsMergeFilter(l []disttopk.ItemList, topk int) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	coord := tworound.NewApproximateBloomGcsMergeCoord(topk)
	numpeer := len(l)
	N_est := getNEst(l)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = tworound.NewApproximateBloomGcsMergePeer(list, topk, numpeer, N_est)
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

func getRecall(exact disttopk.ItemList, approx disttopk.ItemList, k int) float64 {
	em := exact[:k].AddToMap(nil)
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

func itemList2item(ilist disttopk.ItemList) []int {
	keys := make([]int, len(ilist))
	for i, item := range ilist {
		keys[i] = item.Id
	}
	return keys
}

func IMax(i, j int) int {
	if i > j {
		return i
	} else {
		return j
	}
}

func IMin(i, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}

func JWDistance(exact_list disttopk.ItemList, approx_list disttopk.ItemList, k int) float64 {
	// approximately the Jaro edit distance: 0 is no match, 1 is perfect match
	//Inspiration from https://code.google.com/p/duke/source/browse/src/main/java/no/priv/garshol/duke/JaroWinkler.java
	matches := 0.0
	transpositions := 0.0

	exact_keys := itemList2item(exact_list)[0:k]
	approx_keys := itemList2item(approx_list)

	if len(approx_keys) < k {
		fmt.Printf("XXX, this case not yet implemented in JWDistance")
		os.Exit(1)
		//perhaps should just append with nils?
	}

	search_window_width := len(approx_keys) / 2
	last_match_in_approx := -1
	for i := 0; i < k; i++ {
		to_match := exact_keys[i]
		search_start := IMax(0, i-search_window_width)
		search_end := IMin(i+search_window_width+1, len(approx_keys))
		for j := search_start; j < search_end; j++ {
			if to_match == approx_keys[j] {
				matches++
				if last_match_in_approx != -1 && j < last_match_in_approx {
					transpositions++ // moved back before earlier
				}
				last_match_in_approx = j
				break
			}
		}
	}

	fmt.Println("Edit distance debug: ", matches, "matches", transpositions, "transpositions, k= ", k, "algo output length", len(approx_keys))

	if matches == 0 {
		return 0
	} else {
		k_f := float64(k)
		return (matches/k_f + matches/k_f + (matches-transpositions)/matches) / 3.0

	}
}

type Algorithm struct {
	name   string
	runner func([]disttopk.ItemList, int) (disttopk.ItemList, disttopk.AlgoStats)
}

var algorithms []Algorithm = []Algorithm{
	//	Algorithm{"Naive-exact",  runNaiveExact},
	//	Algorithm{"Naive (2k)",   runNaiveK2},
	Algorithm{"Klee3-2R", runKlee3},
	Algorithm{"Klee4-3R", runKlee4},
	Algorithm{"Approx bloom", runApproximateBloomFilter},
	Algorithm{"Approx GCS-M", runApproximateBloomGcsMergeFilter},
	Algorithm{"TPUT   ", runTput},
	Algorithm{"TPUT-hash", runTputHash},
	Algorithm{"2R Gcs  ", runBloomSketchGcs},
	Algorithm{"2R Gcs-Merge", runBloomSketchGcsMerge},
	Algorithm{"Count Min", runCountMin},
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
	naive_exact, _ := runNaive(data, 0)
	ground_truth := naive_exact
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
		stats.Recall = getRecall(ground_truth, result, k)

		stats.Abs_err = getScoreError(ground_truth, result, k)
		stats.Rel_err = getScoreErrorRel(ground_truth, result, k)
		stats.Edit_distance = JWDistance(ground_truth, result, k)
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
		l = disttopk.GetListSet(10, 10000, 0.8, 0.7)
	} else if source == "zipf-disjoint" {
		l = disttopk.GetDisjointSimpleList(10, 10000, 0.7)
	} else if source == "zipf-fo" {
		l = disttopk.GetFullOverlapSimpleList(10, 10000, 0.7)
	} else if source == "zipf-perm" {
		l = disttopk.GetFullOverlapOrderPermutedSimpleList(10, 10000, 0.7, 100)
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
