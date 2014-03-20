package main

import (
	"flag"
	"github.com/cevian/disttopk"
	//"github.com/cevian/disttopk/cm"
	//"github.com/cevian/disttopk/cmfilter"
	"log"
	"runtime/pprof"

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
		proto_list, res := proto.Run(l, k, ground_truth, NestIdeal)
		res.CalculatePerformance(ground_truth, proto_list, k)
		if proto.IsExact() && res.Abs_err != 0.0 {
			PrintDiff(ground_truth, proto_list, k)
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
func PrintDiff(ground_truth, result disttopk.ItemList, k int) {
	for i := 0; i < k; i++ {
		if ground_truth[i] != result[i] {
			fmt.Println("Lists do not match at position", i, "Ground truth:", ground_truth[i], "vs", result[i])
		}
	}
}




func ExportPrinterHeaders(rd RowDesc) string {
	s := "--------------Start Export----------\nExport\t"+rd.GetHeaders()
	s += "\tProtocol Name\tExact\tRounds\tSize\tRel Err\tRecall\tDistance\tScore K"
	for i := 0; i <= 3; i++ {
		rs := fmt.Sprintf("Round %d", i+1)
		s += fmt.Sprintf("\t%s Sketch Bytes\t%s Serial Items sum\t%s Serial Items max\t%s Random Items sum\t%s Random Items max\t%s Random Access sum\t%s Random Access max\t%s Transferred Items sum", rs, rs, rs, rs, rs, rs, rs, rs)
	}
	s += "\n"
	return s
}




func ExportPrinter(rd RowDesc, runners []runner.Runner, res map[string]disttopk.AlgoStats) string {
	s := ExportPrinterHeaders(rd)
	for _, proto := range runners {
		s += fmt.Sprintf("Export\t%s", rd.GetRowData())
		stats := res[proto.GetName()]
		s += fmt.Sprintf("\t%s\t%t\t%d\t%d\t%f\t%f\t%f\t%d", proto.GetName(), proto.IsExact(), stats.Rounds, stats.Bytes_transferred, stats.Rel_err, stats.Recall, stats.Edit_distance, stats.TrueScoreK)
		if len(stats.RoundStats) > 4 {
			panic("Too many rounds")
		}
		for i := 0; i <= 3; i++ {
			roundStat := disttopk.AlgoStatsRoundUnion{}
			if i < len(stats.RoundStats) {
				roundStat = stats.RoundStats[i]
			}
			s += fmt.Sprintf("\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d", roundStat.Bytes_sketch_sum, roundStat.Serial_items_sum, roundStat.Serial_items_max, roundStat.Random_items_sum, roundStat.Random_items_max, roundStat.Random_access_sum, roundStat.Random_access_max, roundStat.Transferred_items_sum)

		}
		s += "\n"
	}
    return s
}


type RowDesc interface{
 GetFs() *disttopk.FileSource 
 GetHeaders() string
GetRowData() string
}

type UcbRowDesc struct{
	KeyOnClient bool
	ModServers int
}

func (t *UcbRowDesc) GetFs() *disttopk.FileSource {
	return &disttopk.FileSource{&disttopk.UcbFileSourceAdaptor{KeyOnClient: t.KeyOnClient, ModServers: t.ModServers}}
}

func (t *UcbRowDesc) GetHeaders() string {
	return "Type\tKeyOnClient\tModServers"
}

func (t *UcbRowDesc) GetRowData() string {
	return fmt.Sprintf("%s\t%t\t%d", "UCB", t.KeyOnClient, t.ModServers)
}

type WcRowDesc struct{
	KeyOnClient bool
}

func (t *WcRowDesc) GetFs() *disttopk.FileSource {
	return &disttopk.FileSource{&disttopk.WcFileSourceAdaptor{KeyOnClient: t.KeyOnClient}}
}

func (t *WcRowDesc) GetHeaders() string {
	return "Type\tKeyOnClient"
}

func (t *WcRowDesc) GetRowData() string {
	return fmt.Sprintf("%s\t%t", "WC", t.KeyOnClient)
}




func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("Data source is ", *suite)
	var l []disttopk.ItemList
	var rd RowDesc
	if *suite == "UCB" {
		rd =  &UcbRowDesc{KeyOnClient: *keyClient, ModServers: *modServers}
		fs := rd.GetFs()
		l = fs.ReadFilesAndCache(BASE_DATA_PATH+"ucb/UCB-home*", BASE_DATA_PATH+"cache")
	} else if *suite == "WC" {
		rd  =  &WcRowDesc{KeyOnClient: *keyClient}
		fs := rd.GetFs()
		l = fs.ReadFilesAndCache(BASE_DATA_PATH+"wc/wc_day*", BASE_DATA_PATH+"cache")
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
	desc := ExportPrinter(rd, runners,stats)
	fmt.Println(desc)
}
