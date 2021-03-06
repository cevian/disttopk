package main

import (
	"fmt"
	"github.com/cevian/disttopk/tworound"
	"github.com/cevian/go-stream/stream"
	"math"
	"testing"
)

import "github.com/cevian/disttopk"

var _ = math.Ceil

//import cmd "github.com/cevian/disttopk/cmd"
func getNEst(l []disttopk.ItemList) int {
	ids := make(map[int]bool)
	for _, list := range l {
		for _, item := range list {
			ids[item.Id] = true
		}
	}
	return len(ids)
}

func RunCountMinExplicitColumns(l []disttopk.ItemList, topk int, columns int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	N_est := getNEst(l)
	pr := tworound.NewCountMinPR(topk, numpeer, N_est)

	runner := stream.NewRunner()
	peers := make([]*tworound.Peer, len(l))
	coord := pr.NewCoord()
	runner.Add(coord)
	for i, list := range l {
		peers[i] = pr.NewPeer(list)
		peers[i].PeerSketchAdaptor.(*tworound.CountMinPeerSketchAdaptor).Columns = columns
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.FinalList, coord.Stats
}

//import "github.com/cevian/disttopk/cmd"

//import "math/rand"

func Getn(list disttopk.ItemList, topk int, nnodes int) int {
	kscore := uint(list[topk].Score)
	cutoff := kscore / uint(nnodes)
	items := 0
	for _, v := range list {
		items += 1
		if uint(v.Score) <= cutoff {
			break
		}
	}
	return items
}

func TestCountMinParameter(t *testing.T) {
	err_sum := 0.0
	count := 0
	res := ""
	for _, zipfParam := range []float64{1, 0.7, 0.3} {
		exp_size, _, optsize, _ := RunCountMinParamTest(100000, 10, 10, zipfParam, 100)
		error_rate := math.Abs((float64(exp_size) - float64(optsize)) / float64(optsize))
		s := fmt.Sprintln("ZipfParam", zipfParam, "Error Rate", error_rate, "Expected Size", exp_size, "Opt size", optsize)
		fmt.Print(s)
		res += s
		err_sum += error_rate
		count++
	}
	fmt.Println("***************************************")
	fmt.Print(res)
	fmt.Println("Average error", err_sum/float64(count))
}

func RunCountMinParamTest(N, Nnodes, k int, zipParam float64, permParam int) (expSize int, expValue int, optSize int, optValue int) {
	l := disttopk.GetFullOverlapOrderPermutedSimpleList(Nnodes, uint32(N), zipParam, permParam)

	n := Getn(l[0], k, Nnodes)
	//n := 398
	//eps_est := disttopk.EstimateEpsCm(N, n, disttopk.RECORD_SIZE*8, 1)

	//Very good results are obtained when running with the exact number of filtered items
	//But it is impossible to know this locally. So we approximate it
	//exact_filter_est := map[float64]int{0.3: 400, 0.7: 15, 1.0: 10}

	//n_filter_approx := Getn(l[0], k, 2)

	//This is a simple approximation but seems to work quite well.
	n_filter_approx := n / Nnodes

	/*if n_filter_approx < n/10 {
		n_filter_approx = n / 10
	}*/

	fmt.Println("approximating n_filter", n_filter_approx, k, n)
	eps_est := disttopk.EstimateEpsCmNew(N, n, n_filter_approx, disttopk.RECORD_SIZE*8)
	columns_est := disttopk.CountMinColumnsEstBloomPow2(n, eps_est)
	fmt.Println("eps est", eps_est, columns_est)
	_, stats_est := RunCountMinExplicitColumns(l, k, columns_est)

	//columns := disttopk.CountMinColumnsEstPow2(0.001)
	columns := 10000
	stats_str := ""

	lowest_value := uint64(0)
	lowest_value_columns := 0
	left_column := columns
	right_column := 0
	scale := 2
	for i := 0; i < 20; i++ {
		_, stats := RunCountMinExplicitColumns(l, k, columns)
		stats_str += fmt.Sprintf("columns: %4.2e eps: %4.2e\tbytes %v left %v right %v lowest (cols:%e, eps:%e bytes: %v) \n", float64(columns), float64(n)/float64(columns), stats.Bytes_transferred, left_column, right_column, float64(lowest_value_columns), float64(n)/float64(lowest_value_columns), lowest_value)

		if right_column == 0 {
			//initialization stage
			if lowest_value == 0 {
				lowest_value = stats.Bytes_transferred
				lowest_value_columns = columns
				columns = columns * scale
				continue
			}
			if stats.Bytes_transferred < lowest_value {
				left_column = lowest_value_columns
				lowest_value = stats.Bytes_transferred
				lowest_value_columns = columns
				columns = columns * scale
			} else {
				right_column = columns
				columns = (left_column + right_column) / 2
			}
			continue
		}

		if stats.Bytes_transferred < lowest_value {
			if lowest_value_columns < columns {
				left_column = lowest_value_columns
			} else {
				right_column = lowest_value_columns
			}
			lowest_value = stats.Bytes_transferred
			lowest_value_columns = columns
		} else {
			if columns < lowest_value_columns {
				left_column = columns
			} else {
				right_column = columns
			}
		}
		columns = (left_column + right_column) / 2
		if columns == lowest_value_columns {
			columns = left_column + ((right_column - left_column) / 3)
		}

	}

	/*
	   	lower := uint64(0)
	   	lower_column := 0
	   	higher := uint64(0)
	   	higher_column := 0
	   	//last_column := 0
	   	scale := 10
	   	for i := 0; i < 10; i++ {
	   		_, stats := RunCountMinExplicitColumns(l, k, columns)
	   		stats_str += fmt.Sprintf("columns: %6e \t\tbytes %v\n", float64(columns), stats.Bytes_transferred)

	   		if lower == 0 {
	   i			lower = stats.Bytes_transferred
	   			lower_column = columns
	   			columns = columns * scale
	   			continue
	   		}




	   		if last == 0 {
	   			last = stats.Bytes_transferred
	   			last_column = columns
	   		}

	   		if last < stats.Bytes_transferred {
	   			columns = (columns + last_column) / 2
	   			scale /= 2
	   		} else {
	   			last = stats.Bytes_transferred
	   			last_column = columns
	   			columns = columns * scale
	   		}

	   	}*/
	fmt.Println(stats_str)
	fmt.Println("Estimate ", eps_est, columns_est, stats_est.Bytes_transferred, int(lowest_value)-int(stats_est.Bytes_transferred), (float64(lowest_value)-float64(stats_est.Bytes_transferred))/float64(lowest_value))
	return int(stats_est.Bytes_transferred), columns_est, int(lowest_value), lowest_value_columns
}
