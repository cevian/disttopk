package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
)
import "github.com/cevian/disttopk/runner"
import "github.com/cevian/disttopk"

var suite = flag.String("suite", "Distribution", "suite to run")
var partition = flag.Int("partition", 0, "Partition to run")
var totalPartitions = flag.Int("totalpartitions", 0, "Total number of partitions")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	flag.Parse()
	fmt.Printf("Running suite: %s partition: %d out of %d\n", *suite, *partition, *totalPartitions)

	var s Suite
	if *suite == "Distribution" {
		s = &Distribution{}
	} else if *suite == "Overlap" {
		s = &Overlap{&Distribution{}}
	} else if *suite == "Test" {
		s = &Test{}
	} else if *suite == "OneListSize" {
		s = &OneListSize{}
	} else {
		panic(fmt.Sprint("Unknown suite", *suite))
	}

	rds := GetRowDescriptionPartition(s.GetRowDescription(), *partition, *totalPartitions)

	protos := s.GetProtocols()
	printers := GetDefaultPrinters(protos)
	for _, p := range printers {
		p.Start()
	}

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

	start_index := inc * partition
	end_index := inc * (partition + 1)
	if end_index > len(rds) || partition == (totalPartitions-1) {
		end_index = len(rds)
	}
	fmt.Println("Covering partition from", start_index, "To", end_index, " Length: ", len(rds))
	return rds[start_index:end_index]
}

type Suite interface {
	GetRowDescription() []RowDescription
	GetProtocols() []Protocol
}

func PermuteList(rds []RowDescription) []RowDescription {
	rand.Seed(int64(1))

	for i := 0; i < len(rds)/2; i++ {
		new_pos := rand.Intn(len(rds) - 1)
		old := rds[new_pos]
		rds[new_pos] = rds[i]
		rds[i] = old
	}
	return rds
}

type Distribution struct {
}

func (t *Distribution) GetRowDescription() []RowDescription {
	rds := make([]RowDescription, 0)
	k := 10
	nodes := 10
	for _, perms := range []int{0, k, 5 * k, 10 * k, 100 * k} {
		for _, overlap := range []float64{1.0, 0.75, 0.25, 0.1, 0} {
			for _, zipfParam := range []float64{0.2, 0.4, 0.6, 0.8, 1, 2} {
				for _, seed := range []int64{1, 2, 3, 4, 5} {
					for _, listSize := range []int{1000, 10000, 100000, 200000} {
						rd := RowDescription{k, nodes, listSize, zipfParam, perms, overlap, seed}
						rds = append(rds, rd)
					}
				}
			}
		}
	}
	//return PermuteList(rds)
	return rds
}

func (t *Distribution) GetProtocols() []Protocol {
	return protocols
}

type OneListSize struct {
}

func (t *OneListSize) GetRowDescription() []RowDescription {
	rds := make([]RowDescription, 0)
	k := 10
	nodes := 10
	listSize := 10000
	for _, perms := range []int{0, k, 5 * k, 10 * k, 100 * k} {
		for _, overlap := range []float64{1.0, 0.75, 0.25, 0.1, 0} {
			for _, zipfParam := range []float64{0.2, 0.4, 0.6, 0.8, 1, 2} {
				for _, seed := range []int64{1, 2} {
					rd := RowDescription{k, nodes, listSize, zipfParam, perms, overlap, seed}
					rds = append(rds, rd)
				}
			}
		}
	}
	return PermuteList(rds)
}

func (t *OneListSize) GetProtocols() []Protocol {
	return []Protocol{ErGms, ErGmsIdealNest, ErGmsUnderNest}
}

type Nestimate struct {
}

func (t *Nestimate) GetRowDescription() []RowDescription {
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
	return PermuteList(rds)
}

func (t *Nestimate) GetProtocols() []Protocol {
	return []Protocol{ErGms, ErGmsIdealNest, ErGmsUnderNest}
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

type Test struct {
}

func (t *Test) GetRowDescription() []RowDescription {
	/*
		bad performance for split bh send, ergcs
		k := 10
		nodes := 10
		listSize := 10000
		zipfParam := 0.4
		perms := 100
		overlap := 0.2
		seed := int64(1)*/
	k := 10
	nodes := 10
	listSize := 10000
	zipfParam := 0.2
	perms := 10
	overlap := 0.0
	seed := int64(1)
	rd := RowDescription{k, nodes, listSize, zipfParam, perms, overlap, seed}
	return []RowDescription{rd}
}

func (t *Test) GetProtocols() []Protocol {
	return []Protocol{ErGcs, ErGms, GcsMerge, TputHash, Klee3, Klee4, BloomGcs}
}

func Run(rd RowDescription, protos []Protocol) map[string]disttopk.AlgoStats {
	l := disttopk.GetFullOverlapOrderPermutedSimpleListSeedOverlap(rd.nodes, uint32(rd.N), rd.zip, rd.perms, rd.seed, rd.overlap)

	naive_exact, _ := runner.RunNaive(l, 0)
	ground_truth := naive_exact

	if ground_truth[rd.k-1] == ground_truth[rd.k] {
		fmt.Println("WARNING_ERROR: no difference between the kth and k+1st element")
		return make(map[string]disttopk.AlgoStats)

	}

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
		fmt.Println("Result:", proto.Name, "Bytes", res.Bytes_transferred)

		if *memprofile != "" {
			f, err := os.Create(fmt.Sprintf("%s.%s", *memprofile, proto.Name))
			if err != nil {
				log.Fatal(err)
			}
			pprof.WriteHeapProfile(f)
			f.Close()

		}
		runtime.GC()
	}

	return results
}
