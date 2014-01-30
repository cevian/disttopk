package main

import (
	"fmt"
	"math"
	"runtime"
	"testing"
)

import "github.com/cevian/disttopk"
import "github.com/cevian/disttopk/runner"

var _ = math.Ceil

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func TestBloomGcsMergeParameter(t *testing.T) {
	size_sum := 0
	count := 0
	res := ""
	for _, listSize := range []int{500000, 100000, 10000, 1000} {
		for _, zipfParam := range []float64{2, 1, 0.7, 0.5, 0.3} {
			size_gcs, size_tputHash := RunBloomGcsMergeParamTest(listSize, 10, 10, zipfParam, 100)
			improvement := (float64(size_tputHash) - float64(size_gcs)) / float64(size_tputHash)
			s := fmt.Sprintf("N %4.2E\tZipfParam %2.1f\tSize GCS %4.2E\tSize Tput Hash %4.2E\tImprovement %3.2f%%\n", float64(listSize), zipfParam, float64(size_gcs), float64(size_tputHash), improvement*100)
			fmt.Print(s)
			res += s
			size_sum += size_gcs
			count++
			runtime.GC()
		}
	}
	fmt.Println("************************************")
	fmt.Print(res)
	fmt.Println("Average size gcs", float64(size_sum)/float64(count), "Size sum gcs", size_sum)
}

func RunBloomGcsMergeParamTest(N, Nnodes, k int, zipParam float64, permParam int) (size_gcs int, size_tputhash int) {
	l := disttopk.GetFullOverlapOrderPermutedSimpleList(Nnodes, uint32(N), zipParam, permParam)

	runtime.GC()
	//n := Getn(l[0], k, Nnodes)
	_, stats := runner.RunBloomSketchGcsMerge(l, k)
	_, stats_tput := runner.RunTputHash(l, k)
	return int(stats.Bytes_transferred), int(stats_tput.Bytes_transferred)
}

/*
func RunAll(N, Nnodes, k int, zipParam float64, permParam int, protos []Protocol, seed int64, overlap float64) map[string]disttopk.AlgoStats {
	l := disttopk.GetFullOverlapOrderPermutedSimpleListSeedOverlap(Nnodes, uint32(N), zipParam, permParam, seed, overlap)

	naive_exact, _ := runner.RunNaive(l, 0)
	ground_truth := naive_exact

	runtime.GC()
	//n := Getn(l[0], k, Nnodes)
	results := make(map[string]disttopk.AlgoStats)
	for _, proto := range protos {
		fmt.Println("---- Running:", proto.Name, " Seed:", seed, "N ", N, "ZipParam", zipParam, "Nnodes", Nnodes, "k", k, "Permparam", permParam, "Overlap", overlap)
		proto_list, res := proto.Runner(l, k)
		res.CalculatePerformance(ground_truth, proto_list, k)
		if proto.isExact && res.Abs_err != 0.0 {
			PrintDiff(ground_truth, proto_list, k)
			panic(fmt.Sprintf("Protocol %v should be exact but has error %v", proto.Name, res.Abs_err))
		}
		results[proto.Name] = res
		runtime.GC()
	}

	return results
}*/

func TestDistributionsAll(t *testing.T) {

	//protocols := []Protocol{GcsMerge}

	printers := defaultPrinters
	for _, p := range printers {
		p.Start()
	}

	k := 10
	nodes := 10
	overlap := 1.0
	for _, perms := range []int{0, k, 5 * k, 10 * k, 100 * k} {
		for _, listSize := range []int{1000, 10000, 100000, 200000} {
			for _, zipfParam := range []float64{0.2, 0.4, 0.6, 0.8, 1, 2} {
				for _, seed := range []int64{1, 2, 3, 4, 5} {
					rd := RowDescription{k, nodes, listSize, zipfParam, perms, overlap, seed}
					results := Run(rd, protocols)
					for _, p := range printers {
						row := p.EnterRow(rd, results)
						fmt.Print("Res ", row, "\n")
					}
				}
			}
			for _, p := range printers {
				p.EnterNewN()
			}

			fmt.Println("=====================================")
		}
	}
	fmt.Println("***********************************")

	for _, p := range printers {
		fmt.Print(p.Summary())
		fmt.Println("*******************************************************************************")
	}

}

func TestOverlap(t *testing.T) {
	printers := defaultPrinters

	for _, p := range printers {
		p.Start()
	}

	nodes := 10
	k := 10
	listSize := 10000
	for _, zipfParam := range []float64{1.0, 0.7, 0.5, 0.3} {
		for _, perms := range []int{0, k, 5 * k, 10 * k, 100 * k} {
			for _, overlap := range []float64{1.0, 0.75, 0.25, 0.1, 0} {
				for _, seed := range []int64{1, 2, 3, 4, 5} {
					rd := RowDescription{k, nodes, listSize, zipfParam, perms, overlap, seed}
					results := Run(rd, protocols)
					for _, p := range printers {
						row := p.EnterRow(rd, results)
						fmt.Print("Res ", row, "\n")
					}

					fmt.Println("=====================================")
				}
			}
		}
	}
	fmt.Println("***********************************")

	for _, p := range printers {
		fmt.Print(p.Summary())
		fmt.Println("*******************************************************************************")
	}

}

func TestKlee3vsKlee4(t *testing.T) {
	printers := defaultPrinters

	protocols := []Protocol{Klee4, Klee3}
	for _, p := range printers {
		p.Start()
	}

	nodes := 10
	k := 10
	for _, zipfParam := range []float64{0.7, 1.0} {
		for _, listSize := range []int{10000, 100000} {
			for _, perms := range []int{0, k, 5 * k, 10 * k, 100 * k} {
				for _, overlap := range []float64{1.0, 0.75, 0.5, 0.25, 0.1, 0} {
					for _, seed := range []int64{1} {
						rd := RowDescription{k, nodes, listSize, zipfParam, perms, overlap, seed}
						results := Run(rd, protocols)
						for _, p := range printers {
							row := p.EnterRow(rd, results)
							fmt.Print("Res ", row, "\n")
						}

						fmt.Println("=====================================")
					}
				}
			}
		}
	}
	fmt.Println("***********************************")

	for _, p := range printers {
		fmt.Print(p.Summary())
		fmt.Println("*******************************************************************************")
	}

}

func TestSeedsAll(t *testing.T) {
	printers := defaultPrinters

	for _, p := range printers {
		p.Start()
	}

	listSize := 10000
	zipfParam := 0.3
	perms := 100
	overlap := 1.0
	k := 10
	nodes := 10
	for seed := 0; seed < 10; seed++ {
		rd := RowDescription{k, nodes, listSize, zipfParam, perms, overlap, int64(seed)}
		results := Run(rd, protocols)
		for _, p := range printers {
			row := p.EnterRow(rd, results)
			fmt.Print("Res ", row, "\n")
		}

		fmt.Println("=====================================")
	}
	fmt.Println("***********************************")

	for _, p := range printers {
		fmt.Print(p.Summary())
		fmt.Println("*******************************************************************************")
	}

}

func TestGenerateList(t *testing.T) {
	Nnodes := 10
	N := 10000
	zipParam := 0.7
	permParam := 100
	overlap := 1.0

	for n := 0; n < 100; n++ {
		seed := n
		disttopk.GetFullOverlapOrderPermutedSimpleListSeedOverlap(Nnodes, uint32(N), zipParam, permParam, int64(seed), overlap)
	}
}
