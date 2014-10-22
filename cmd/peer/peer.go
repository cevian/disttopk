package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/cevian/disttopk"
	"github.com/cevian/disttopk/cmd/common"
	"github.com/cevian/disttopk/cmd/printers"
	"github.com/cevian/disttopk/runner"
)

var suite = flag.String("suite", "Distribution", "suite to run")
var data_path = flag.String("dataPath", BASE_DATA_PATH, "base data path")
var coord_ip = flag.String("coordIp", "127.0.0.1", "Ip of coordinator")
var index = flag.Int("index", 0, "index of peer")
var modServers = flag.Int("modServers", 33, "Number of servers for UCB")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var keyClient = flag.Bool("keyclient", false, "key on client")

const BASE_DATA_PATH = "/home/arye/goprojects/src/github.com/cevian/disttopk/data/"

func Run(ip string, index int, l []disttopk.ItemList, protos []runner.Runner, k int) {
	my_l := l[index]

	ht := my_l.MakeHashTable()

	NestIdeal := printers.GetNEst(l)
	//naive_exact, _ := runner.RunNaive(l, 0)
	//ground_truth := naive_exact

	/*if ground_truth[k-1] == ground_truth[k] {
		fmt.Println("WARNING_ERROR: no difference between the kth and k+1st element")
		return make(map[string]disttopk.AlgoStats)

	}*/

	runtime.GC()

	//results := make(map[string]disttopk.AlgoStats)
	for i, proto := range protos {
		time.Sleep(100 * time.Millisecond) //give coord time to open next conn
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
		proto.(runner.NetworkRunner).RunPeer(fmt.Sprintf("%s:%d", ip, 7000+i), len(l), my_l, ht, k, nil, NestIdeal)
		if *cpuprofile != "" {
			pprof.StopCPUProfile()
		}
		//res.CalculatePerformance(ground_truth, proto_list, k)
		/*if proto.IsExact() && res.Abs_err != 0.0 {
			printers.PrintDiff(ground_truth, proto_list, k)
			panic(fmt.Sprintf("Protocol %v should be exact but has error %v", proto.GetName(), res.Abs_err))
		}*/
		//results[proto.GetName()] = res
		//fmt.Println("Result:", proto.GetName(), "Bytes", res.Bytes_transferred, "Rounds", res.Rounds)
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

	return
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("Running peer, data source is ", *suite, "index is ", *index)
	var l []disttopk.ItemList
	var rd printers.RowDesc
	if *suite == "WC" {
		rd = &printers.WcRowDesc{KeyOnClient: *keyClient}
		fs := rd.GetFs()
		l = fs.ReadFilesAndCache(*data_path+"wc/wc_day*", *data_path+"cache")
	} else if *suite == "UCB" {
		rd = &printers.UcbRowDesc{KeyOnClient: *keyClient, ModServers: *modServers}
		fs := rd.GetFs()
		l = fs.ReadFilesAndCache(BASE_DATA_PATH+"ucb/UCB-home*", BASE_DATA_PATH+"cache")
	} else if *suite == "SYN" {
		r := &printers.SynRowDesc{10, 10, 1000, 2.0, 10, 1.0, 1, 100}
		l = disttopk.GetFullOverlapOrderPermutedSimpleListSeedOverlap(r.Nodes, uint32(r.N), r.Zip, r.Perms, r.Seed, r.Overlap)
		rd = r
	} else {
		fmt.Println("Source should be 'WC', 'zipf', or 'UCB'. Default is zipf.")
		os.Exit(1)
	}

	fmt.Println("List Head: ", l[0][:2], l[1][:2])
	fmt.Println("List Tail: ", l[0][len(l[0])-3:], l[1][len(l[1])-3:])

	runners := common.GetRunners()
	Run(*coord_ip, *index, l, runners, 10)
	fmt.Println("Done")
}
