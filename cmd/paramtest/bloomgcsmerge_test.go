package paramtest

import (
	"fmt"
	"math"
	"testing"
)

import "github.com/cevian/disttopk"
import cmd "github.com/cevian/disttopk/cmd"

var _ = math.Ceil

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
		}
	}
	fmt.Println("************************************")
	fmt.Print(res)
	fmt.Println("Average size gcs", float64(size_sum)/float64(count), "Size sum gcs", size_sum)
}

func RunBloomGcsMergeParamTest(N, Nnodes, k int, zipParam float64, permParam int) (size_gcs int, size_tputhash int) {
	l := disttopk.GetFullOverlapOrderPermutedSimpleList(Nnodes, uint32(N), zipParam, permParam)

	//n := Getn(l[0], k, Nnodes)
	_, stats := cmd.RunBloomSketchGcsMerge(l, k)
	_, stats_tput := cmd.RunTputHash(l, k)
	return int(stats.Bytes_transferred), int(stats_tput.Bytes_transferred)
}
