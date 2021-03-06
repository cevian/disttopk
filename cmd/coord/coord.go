package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/cevian/disttopk"
	"github.com/cevian/disttopk/cmd/common"
	"github.com/cevian/disttopk/cmd/printers"
	"github.com/cevian/disttopk/runner"
)

var suite = flag.String("suite", "Distribution", "suite to run")
var peers = flag.Int("peers", 0, "Num Peers")
var coord_ip = flag.String("coordIp", "", "IP of coord")
var modServers = flag.Int("modServers", 33, "Number of servers for UCB")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var keyClient = flag.Bool("keyclient", false, "key on client")
var numRuns = flag.Int("numRuns", 1, "Number Of Runs")

const BASE_DATA_PATH = "/home/arye/goprojects/src/github.com/cevian/disttopk/data/"

func Run(ip string, l []disttopk.ItemList, protos []runner.Runner, k int, numRuns int) []map[string]disttopk.AlgoStats {

	hts := disttopk.MakeHashTables(l)
	NestIdeal := printers.GetNEst(l)
	naive_exact, _ := runner.RunNaive(l, 0)
	ground_truth := naive_exact

	if ground_truth[k-1] == ground_truth[k] {
		fmt.Println("WARNING_ERROR: no difference between the kth and k+1st element")
		return make([]map[string]disttopk.AlgoStats, 0)

	}

	runtime.GC()

	resultsColl := make([]map[string]disttopk.AlgoStats, 0)
	for w := 0; w < numRuns; w++ {
		results := make(map[string]disttopk.AlgoStats)
		for i, proto := range protos {
			runtime.GC()
			//mem := &runtime.MemStats{}
			//runtime.ReadMemStats(mem)
			//fmt.Printf("Start Memstats %e %e %e %e %e %e\n", float64(mem.Alloc), float64(mem.TotalAlloc), float64(mem.Sys), float64(mem.Lookups), float64(mem.Mallocs), float64(mem.Frees))

			fmt.Println("---- Running:", proto.GetName())
			//proto_list, res := proto.RunCoord(l, hts, k, ground_truth, NestIdeal)

			if *cpuprofile != "" {
				profname := fmt.Sprintf("%s.%s", *cpuprofile, proto.GetName())
				f, err := os.Create(profname)
				if err != nil {
					log.Fatal(err)
				}
				pprof.StartCPUProfile(f)
			}
			proto_list, res := proto.(runner.NetworkRunner).RunCoord(fmt.Sprintf("%s:%d", ip, 7000+i), l, hts, k, ground_truth, NestIdeal)
			if *cpuprofile != "" {

				pprof.StopCPUProfile()
			}

			res.CalculatePerformance(ground_truth, proto_list, k)
			if proto.IsExact() && res.Abs_err != 0.0 {
				printers.PrintDiff(ground_truth, proto_list, k)
				panic(fmt.Sprintf("Protocol %v should be exact but has error %v", proto.GetName(), res.Abs_err))
			}
			results[proto.GetName()] = res
			fmt.Println("Result:", proto.GetName(), "Bytes", res.Bytes_transferred, "Rounds", res.Rounds, "Execution Time", res.Took)
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
		resultsColl = append(resultsColl, results)
	}

	return resultsColl
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("Data source is ", *suite)
	var l []disttopk.ItemList
	var rd printers.RowDesc
	if *suite == "WC" {
		rd = &printers.WcRowDesc{KeyOnClient: *keyClient}
		fs := rd.GetFs()
		l = fs.ReadFilesAndCache(BASE_DATA_PATH+"wc/wc_day*", BASE_DATA_PATH+"cache")
	} else if *suite == "UCB" {
		rd = &printers.UcbRowDesc{KeyOnClient: *keyClient, ModServers: *modServers}
		fs := rd.GetFs()
		l = fs.ReadFilesAndCache(BASE_DATA_PATH+"ucb/UCB-home*", BASE_DATA_PATH+"cache")
	} else if *suite == "SYN" {
		r := &printers.SynRowDesc{10, 10, 10000000, 2.0, 10, 1.0, 1, 100}
		l = disttopk.GetFullOverlapOrderPermutedSimpleListSeedOverlap(r.Nodes, uint32(r.N), r.Zip, r.Perms, r.Seed, r.Overlap)
		rd = r
	} else {
		fmt.Println("Source should be 'WC', 'zipf', or 'UCB'. Default is zipf.")
		os.Exit(1)
	}

	if *peers > 0 {
		l = l[0:*peers]
	}

	for i, il := range l {
		fmt.Println("Peer length: ", i, len(il))
	}

	fmt.Println("Num Peers: ", len(l))
	fmt.Println("List Head: ", l[0][:2], l[1][:2])
	fmt.Println("List Tail: ", l[0][len(l[0])-3:], l[1][len(l[1])-3:])

	runners := common.GetRunners()

	statsCol := Run(*coord_ip, l, runners, 10, *numRuns)

	desc := printers.ExportPrinterHeaders(rd)
	for _, stats := range statsCol {
		desc += printers.ExportPrinterNoHeader(rd, runners, stats)
	}
	fmt.Println(desc)
}
