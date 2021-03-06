package main

import (
	"flag"
	"io/ioutil"
	"strings"

	"github.com/cevian/disttopk"
	//"github.com/cevian/disttopk/cm"
	//"github.com/cevian/disttopk/cmfilter"
	"log"
	"runtime/pprof"

	"github.com/cevian/disttopk/cmd/printers"
	"github.com/cevian/disttopk/runner"

	//"github.com/cloudflare/go-stream/util/slog";
	"fmt"
	//	"strconv"
	"os"
	"runtime"
)

var _ = os.Exit

const BASE_DATA_PATH = "/home/arye/goprojects/src/github.com/cevian/disttopk/data/"

var suite = flag.String("suite", "Distribution", "suite to run")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var keyClient = flag.Bool("keyclient", false, "key on client")
var modServers = flag.Int("modServers", 10, "mod the servers by (UCB trace)")

func Run(l []disttopk.ItemList, protos []runner.Runner, k int) map[string]disttopk.AlgoStats {
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	hts := disttopk.MakeHashTables(l)
	NestIdeal := getNEst(l)
	naive_exact, _ := runner.RunNaive(l, 0)
	ground_truth := naive_exact

	if ground_truth[k-1] == ground_truth[k] {
		fmt.Println("WARNING_ERROR: no difference between the kth and k+1st element")
		return make(map[string]disttopk.AlgoStats)

	}

	runtime.GC()

	results := make(map[string]disttopk.AlgoStats)
	for _, proto := range protos {
		runtime.GC()
		//mem := &runtime.MemStats{}
		//runtime.ReadMemStats(mem)
		//fmt.Printf("Start Memstats %e %e %e %e %e %e\n", float64(mem.Alloc), float64(mem.TotalAlloc), float64(mem.Sys), float64(mem.Lookups), float64(mem.Mallocs), float64(mem.Frees))

		fmt.Println("---- Running:", proto.GetName())
		proto_list, res := proto.Run(l, hts, k, ground_truth, NestIdeal)
		res.CalculatePerformance(ground_truth, proto_list, k)
		if proto.IsExact() && res.Abs_err != 0.0 {
			printers.PrintDiff(ground_truth, proto_list, k)
			panic(fmt.Sprintf("Protocol %v should be exact but has error %v", proto.GetName(), res.Abs_err))
		}
		results[proto.GetName()] = res
		fmt.Println("Result:", proto.GetName(), "Bytes", res.Bytes_transferred, "Rounds", res.Rounds)
		//runtime.ReadMemStats(mem)
		//fmt.Printf("Memstats %e %e %e %e %e %e\n", float64(mem.Alloc), float64(mem.TotalAlloc), float64(mem.Sys), float64(mem.Lookups), float64(mem.Mallocs), float64(mem.Frees))

		if *memprofile != "" {
			f, err := os.Create(fmt.Sprintf("%s.%s", *memprofile, proto.GetName()))
			if err != nil {
				log.Fatal(err)
			}
			pprof.WriteHeapProfile(f)
			f.Close()

		}
	}

	return results
}

func getNEst(l []disttopk.ItemList) int {
	ids := make(map[int]bool)
	for _, list := range l {
		for _, item := range list {
			ids[item.Id] = true
		}
	}
	return len(ids)
}
func processClueWeb() {
	_, results := disttopk.CwGetItemLists("clueweb/webtrack2012topics.xml", "clueweb/results.txt")

	queryData, err := ioutil.ReadFile("clueweb/webtrack2012topics.xml")
	if err != nil {
		panic("snh")
	}
	topics := disttopk.CwGetTopics(queryData)

	for topic_id, topic := range topics[:1] {
		lists := make([]disttopk.ItemList, 0)
		for _, word := range strings.Split(topic, " ") {
			wordListMap := results[word]
			if len(wordListMap) < 1 {
				//this is a stop word
				continue
			}

			list := disttopk.MakeItemList(wordListMap)
			list.Sort()
			lists = append(lists, list)
		}

		if len(lists) == 0 {
			panic("snh")
		}

		fmt.Println("#lists:", len(lists))

		runners := getRunners()
		stats := Run(lists, runners, 10)
		desc := printers.ExportPrinter(&printers.CwRowDesc{topic_id}, runners, stats)
		fmt.Println(desc)
	}

}

func getRunners() []runner.Runner {
	return []runner.Runner{
		//runner.NewMagicRunner(),
		runner.NewKlee3Runner(),
		runner.NewKlee4Runner(),
		runner.NewSbrARunner(),
		runner.NewSbr2RRunner(),
		//runner.NewSbrErNoSplitRunner(),
		runner.NewSbrErRunner(),
		//runner.NewSbrErIdealNestRunner(),
		runner.NewTputRunner(),
		runner.NewTputHRunner(),
		runner.NewTputERRunner(),
	}

}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("Data source is ", *suite)
	var l []disttopk.ItemList
	var rd printers.RowDesc
	if *suite == "UCB" {
		rd = &printers.UcbRowDesc{KeyOnClient: *keyClient, ModServers: *modServers}
		fs := rd.GetFs()
		l = fs.ReadFilesAndCache(BASE_DATA_PATH+"ucb/UCB-home*", BASE_DATA_PATH+"cache")
	} else if *suite == "WC" {
		rd = &printers.WcRowDesc{KeyOnClient: *keyClient}
		fs := rd.GetFs()
		l = fs.ReadFilesAndCache(BASE_DATA_PATH+"wc/wc_day*", BASE_DATA_PATH+"cache")
	} else if *suite == "clueweb" {
		processClueWeb()
		return
	} else {
		fmt.Println("Source should be 'WC', 'zipf', or 'UCB'. Default is zipf.")
		os.Exit(1)
	}

	fmt.Println("List Head: ", l[0][:2], l[1][:2])
	fmt.Println("List Tail: ", l[0][len(l[0])-3:], l[1][len(l[1])-3:])

	runners := []runner.Runner{
		//runner.NewMagicRunner(),
		runner.NewKlee3Runner(),
		runner.NewKlee4Runner(),
		runner.NewSbrARunner(),
		runner.NewSbr2RRunner(),
		//runner.NewSbrErNoSplitRunner(),
		runner.NewSbrErRunner(),
		//runner.NewSbrErIdealNestRunner(),
		runner.NewTputRunner(),
		runner.NewTputHRunner(),
		runner.NewTputERRunner(),
	}

	stats := Run(l, runners, 10)
	desc := printers.ExportPrinter(rd, runners, stats)
	fmt.Println(desc)
}
