package main

import (
	"flag"
	"fmt"
	"math"
	"runtime"
)
import "github.com/cevian/disttopk/runner"
import "github.com/cevian/disttopk"

var suite = flag.String("suite", "Distribution", "suite to run")
var partition = flag.Int("partition", 0, "Partition to run")
var totalPartitions = flag.Int("totalpartitions", 0, "Total number of partitions")

func main() {
	flag.Parse()
	fmt.Printf("Running suite: %s partition: %d out of %d\n", *suite, *partition, *totalPartitions)

	var s Suite
	if *suite == "Distribution" {
		s = &Distribution{}
	} else if *suite == "Overlap" {
		s = &Overlap{&Distribution{}}
	} else {
		panic(fmt.Sprint("Unknown suite", *suite))
	}

	rds := GetRowDescriptionPartition(s.GetRowDescription(), *partition, *totalPartitions)

	printers := defaultPrinters
	for _, p := range printers {
		p.Start()
	}

	protos := s.GetProtocols()
	for _, rd := range rds {
		stat := Run(rd, protos)
		for _, p := range printers {
			row := p.EnterRow(rd, stat)
			fmt.Print("Res ", row, "\n")
		}

	}

	fmt.Println("***********************************")

	for _, p := range printers {
		fmt.Print(p.Summary())
		fmt.Println("*******************************************************************************")
	}

}

func GetRowDescriptionPartition(rds []RowDescription, partition int, totalPartitions int) []RowDescription {
	if totalPartitions == 0 {
		return rds
	}

	incr := float64(len(rds)) / float64(totalPartitions)
	//ex: len = 11, totalpartitions = 4, incr = 2 or 3
	// [0,1][2,3][4,5][6-10]
	// [0,1,2][3,4,5][6,7,8][9,10] *better
	// for len = 9; incr 2 or 3
	//[0,1][2,3][4,5][6,7,8] *better
	//[0,1,2][3,4,5][6,7,8][nil]
	// so need to round:
	whole, part := math.Modf(incr)
	inc := int(whole)
	if part > 0.5 {
		inc++
	}
	if inc == 0 {
		inc = 1
	}

	if inc*partition > len(rds)-1 {
		return nil
	}

	if inc*(partition+1) > len(rds) {
		return rds[inc*partition : len(rds)]
	}
	return rds[inc*partition : inc*(partition+1)]
}

type Suite interface {
	GetRowDescription() []RowDescription
	GetProtocols() []Protocol
}

type Distribution struct {
}

func (t *Distribution) GetRowDescription() []RowDescription {
	rds := make([]RowDescription, 0)
	k := 10
	nodes := 10
	overlap := 1.0
	for _, perms := range []int{0, k, 5 * k, 10 * k, 100 * k} {
		for _, listSize := range []int{1000, 10000, 100000, 200000} {
			for _, zipfParam := range []float64{0.2, 0.4, 0.6, 0.8, 1, 2} {
				for _, seed := range []int64{1, 2, 3, 4, 5} {
					rd := RowDescription{k, nodes, listSize, zipfParam, perms, overlap, seed}
					rds = append(rds, rd)
				}
			}
		}
	}
	return rds
}

func (t *Distribution) GetProtocols() []Protocol {
	return protocols
}

type Overlap struct {
	*Distribution
}

func (t *Overlap) GetRowDescription() []RowDescription {
	rds := make([]RowDescription, 0)
	k := 10
	nodes := 10
	listSize := 10000
	for _, perms := range []int{0, k, 5 * k, 10 * k, 100 * k} {
		for _, overlap := range []float64{1.0, 0.75, 0.25, 0.1, 0} {
			for _, zipfParam := range []float64{0.2, 0.4, 0.6, 0.8, 1, 2} {
				for _, seed := range []int64{1, 2, 3, 4, 5} {
					rd := RowDescription{k, nodes, listSize, zipfParam, perms, overlap, seed}
					rds = append(rds, rd)
				}
			}
		}
	}
	return rds
}

func Run(rd RowDescription, protos []Protocol) map[string]disttopk.AlgoStats {
	l := disttopk.GetFullOverlapOrderPermutedSimpleListSeedOverlap(rd.nodes, uint32(rd.N), rd.zip, rd.perms, rd.seed, rd.overlap)

	naive_exact, _ := runner.RunNaive(l, 0)
	ground_truth := naive_exact

	runtime.GC()
	//n := Getn(l[0], k, Nnodes)
	results := make(map[string]disttopk.AlgoStats)
	for _, proto := range protos {
		fmt.Println("---- Running:", proto.Name, rd.String())
		proto_list, res := proto.Runner(l, rd.k)
		res.CalculatePerformance(ground_truth, proto_list, rd.k)
		if proto.isExact && res.Abs_err != 0.0 {
			PrintDiff(ground_truth, proto_list, rd.k)
			panic(fmt.Sprintf("Protocol %v should be exact but has error %v", proto.Name, res.Abs_err))
		}
		results[proto.Name] = res
		runtime.GC()
	}

	return results
}
