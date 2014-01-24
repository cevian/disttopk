package paramtest

import (
	"fmt"
	"math"
	"runtime"
	"testing"
)

import "github.com/cevian/disttopk"
import cmd "github.com/cevian/disttopk/cmd"

var _ = math.Ceil

func TestBloomGcsMergeParameter(t *testing.T) {
	size_sum := 0
	count := 0
	res := ""
	for _, listSize := range []int{ /*500000,*/ 100000, 10000, 1000} {
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
	_, stats := cmd.RunBloomSketchGcsMerge(l, k)
	_, stats_tput := cmd.RunTputHash(l, k)
	return int(stats.Bytes_transferred), int(stats_tput.Bytes_transferred)
}

type Protocol struct {
	Name   string
	Runner func([]disttopk.ItemList, int) (disttopk.ItemList, disttopk.AlgoStats)
}

func RunAll(N, Nnodes, k int, zipParam float64, permParam int, protos []Protocol) map[string]disttopk.AlgoStats {
	l := disttopk.GetFullOverlapOrderPermutedSimpleList(Nnodes, uint32(N), zipParam, permParam)

	runtime.GC()
	//n := Getn(l[0], k, Nnodes)
	results := make(map[string]disttopk.AlgoStats)
	for _, proto := range protos {
		_, res := proto.Runner(l, k)
		results[proto.Name] = res
	}

	return results
}

var protocols []Protocol = []Protocol{
	//	Protocol{"Naive-exact",  runNaiveExact},
	// Approximate:
	//	Protocol{"Naive (2k)",   runNaiveK2},
	Protocol{"Klee3", cmd.RunKlee3},
	Protocol{"Klee4", cmd.RunKlee4},
	Protocol{"bloom", cmd.RunApproximateBloomFilter},
	// Extra-Round Exact
	Protocol{"ER GCS", cmd.RunApproximateBloomGcsMergeFilter},
	Protocol{"ER TP", cmd.RunTputHashExtraRound},
	// Exact
	Protocol{"Tput", cmd.RunTput},
	Protocol{"TputH", cmd.RunTputHash},
	//	Protocol{"2R Gcs  ", cmd.RunBloomSketchGcs},
	Protocol{"2R Gcs-Merge", cmd.RunBloomSketchGcsMerge},
	//	Protocol{"Count Min", cmd.RunCountMin},
}

func TestAll(t *testing.T) {
	res_all := MakeTitles(protocols) + "\n"
	for _, listSize := range []int{ /*500000, 100000, */ 10000, 1000} {
		for _, zipfParam := range []float64{2, 1, 0.7, 0.5, 0.3} {
			results := RunAll(listSize, 10, 10, zipfParam, 100, protocols)
			row := MakeRow(listSize, zipfParam, protocols, results)
			fmt.Print("Res ", row, "\n")
			res_all += row + "\n"
		}
		res_all += "---------------------------------------------------------------------------------------" + "\n"
		fmt.Println("=====================================")
	}
	fmt.Println("***********************************")
	fmt.Print(res_all)
}

func MakeTitles(protos []Protocol) string {
	s := "N\tZip"
	for _, proto := range protos {
		s += "\t" + proto.Name
	}
	return s
}
func MakeRow(N int, zip float64, protos []Protocol, res map[string]disttopk.AlgoStats) string {
	s := fmt.Sprintf("%4.1E\t%2.1f", float64(N), float64(zip))
	for _, proto := range protos {
		stats := res[proto.Name]
		s += fmt.Sprintf("\t%4.1E", float64(stats.Bytes_transferred))
	}
	return s
}
