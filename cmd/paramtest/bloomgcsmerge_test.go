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
	_, stats := cmd.RunBloomSketchGcsMerge(l, k)
	_, stats_tput := cmd.RunTputHash(l, k)
	return int(stats.Bytes_transferred), int(stats_tput.Bytes_transferred)
}
func PrintDiff(ground_truth, result disttopk.ItemList, k int) {
	for i := 0; i < k; i++ {
		if ground_truth[i] != result[i] {
			fmt.Println("Lists do not match at position", i, "Ground truth:", ground_truth[i], "vs", result[i])
		}
	}
}

type Protocol struct {
	Name    string
	Runner  func([]disttopk.ItemList, int) (disttopk.ItemList, disttopk.AlgoStats)
	isExact bool
}

func RunAll(N, Nnodes, k int, zipParam float64, permParam int, protos []Protocol, seed int64) map[string]disttopk.AlgoStats {
	l := disttopk.GetFullOverlapOrderPermutedSimpleListSeed(Nnodes, uint32(N), zipParam, permParam, seed)

	naive_exact, _ := cmd.RunNaive(l, 0)
	ground_truth := naive_exact

	runtime.GC()
	//n := Getn(l[0], k, Nnodes)
	results := make(map[string]disttopk.AlgoStats)
	for _, proto := range protos {
		fmt.Println("---- Running:", proto.Name)
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
}

var NaiveEx = Protocol{"Naive-exact", cmd.RunNaiveExact, true}

//approx
var Naive2k = Protocol{"Naive (2k)", cmd.RunNaiveK2, false}
var Klee3 = Protocol{"Klee3", cmd.RunKlee3, false}
var Klee4 = Protocol{"Klee4", cmd.RunKlee4, false}
var Bloom = Protocol{"bloom", cmd.RunApproximateBloomFilter, false}

// Extra-Round Exact
var ErGcs = Protocol{"ER GCS", cmd.RunApproximateBloomGcsMergeFilter, true}
var ErTput = Protocol{"ER TP", cmd.RunTputHashExtraRound, true}

// Exact
var Tput = Protocol{"Tput", cmd.RunTput, true}
var TputHash = Protocol{"TputH", cmd.RunTputHash, true}

//var Gcs	= Protocol{"2R Gcs  ", cmd.RunBloomSketchGcs, true}
var GcsMerge = Protocol{"2R GcsM", cmd.RunBloomSketchGcsMerge, true}
var CountMin = Protocol{"Count Min", cmd.RunCountMin, true}

var protocols []Protocol = []Protocol{
	Klee3,
	Klee4,
	Bloom,
	ErGcs,
	ErTput,
	Tput,
	TputHash,
	GcsMerge,
	//CountMin
}

func ApproximateProtocols() []Protocol {
	ret := make([]Protocol, 0)
	for _, protocol := range protocols {
		if !protocol.isExact {
			ret = append(ret, protocol)
		}
	}
	return ret
}

func ExactProtocols() []Protocol {
	ret := make([]Protocol, 0)
	for _, protocol := range protocols {
		if protocol.isExact {
			ret = append(ret, protocol)
		}
	}
	return ret
}

func TestAll(t *testing.T) {
	printers := []Printer{&OverviewPrinter{protocols, ""},
		&ApproxPrinter{&OverviewPrinter{ApproximateProtocols(), ""}},
		&ExactPrinter{&OverviewPrinter{ExactProtocols(), ""}},
		&GcsTputPrinter{&OverviewPrinter{protocols, ""}},
	}
	for _, p := range printers {
		p.Start()
	}

	k := 10
	nodes := 10
	for _, perms := range []int{k, 5 * k, 10 * k, 100 * k} {
		for _, listSize := range []int{ /*500000, 100000, */ 10000, 1000} {
			for _, zipfParam := range []float64{2, 1, 0.7, 0.5, 0.3} {
				results := RunAll(listSize, nodes, k, zipfParam, perms, protocols, 99)
				for _, p := range printers {
					row := p.EnterRow(RowDescription{listSize, zipfParam, perms}, results)
					fmt.Print("Res ", row, "\n")
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

func TestSeedsAll(t *testing.T) {
	printers := []Printer{&OverviewPrinter{protocols, ""},
		&ApproxPrinter{&OverviewPrinter{ApproximateProtocols(), ""}},
		&ExactPrinter{&OverviewPrinter{ExactProtocols(), ""}},
		&GcsTputPrinter{&OverviewPrinter{protocols, ""}},
	}
	for _, p := range printers {
		p.Start()
	}

	listSize := 10000
	zipfParam := 0.3
	perms := 100

	for seed := 0; seed < 10; seed++ {
		results := RunAll(listSize, 10, 10, zipfParam, perms, protocols, int64(seed))
		for _, p := range printers {
			row := p.EnterRow(RowDescription{listSize, zipfParam, perms}, results)
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

type RowDescription struct {
	N     int
	zip   float64
	perms int
}

type Printer interface {
	Start()
	EnterRow(rd RowDescription, res map[string]disttopk.AlgoStats) string
	EnterNewN()
	Summary() string
}

type OverviewPrinter struct {
	protocols []Protocol
	s         string
}

func (t *OverviewPrinter) EnterNewN() {
	t.s += "---------------------------------------------------------------------------\n"
}

func (t *OverviewPrinter) RowDescriptionHeaders() string {
	return "N\tZip\tPerm"
}

func (t *OverviewPrinter) Start() {
	t.s = t.RowDescriptionHeaders()
	for _, proto := range t.protocols {
		t.s += "\t" + proto.Name
	}
	t.s += "\n"
}

func (t *OverviewPrinter) GetRowDescription(rd RowDescription) string {
	return fmt.Sprintf("%4.1E\t%2.1f\t%d", float64(rd.N), float64(rd.zip), rd.perms)
}

func (t *OverviewPrinter) EnterRow(rd RowDescription, res map[string]disttopk.AlgoStats) string {
	s := t.GetRowDescription(rd)
	for _, proto := range t.protocols {
		stats := res[proto.Name]
		s += fmt.Sprintf("\t%4.1E", float64(stats.Bytes_transferred))
	}
	t.s += s + "\n"
	return s
}
func (t *OverviewPrinter) Summary() string {
	return t.s
}

type ApproxPrinter struct {
	*OverviewPrinter
}

func (t *ApproxPrinter) Start() {
	t.s = t.RowDescriptionHeaders()
	for _, proto := range t.protocols {
		t.s += "\t" + proto.Name + "\tRelE.\tRecall"
	}
	t.s += "\tBest BW\tBest Err\n"
}

func (t *ApproxPrinter) EnterRow(rd RowDescription, res map[string]disttopk.AlgoStats) string {
	s := t.GetRowDescription(rd)
	for _, proto := range t.protocols {
		stats := res[proto.Name]
		s += fmt.Sprintf("\t%4.1E\t%4.1E\t%2.1f", float64(stats.Bytes_transferred), float64(stats.Rel_err), float64(stats.Recall))
	}
	s += "\t" + t.BestProtoBytes(res) + "\t" + t.BestProtoErr(res)
	t.s += s + "\n"
	return s
}

func (t *OverviewPrinter) BestProtoBytes(res map[string]disttopk.AlgoStats) string {
	bestValue := uint64(0)
	bestName := ""
	for _, proto := range t.protocols {
		stats := res[proto.Name]
		if bestValue == 0.0 || stats.Bytes_transferred < bestValue {
			bestValue = stats.Bytes_transferred
			bestName = proto.Name
		}
	}
	return bestName
}

func (t *OverviewPrinter) BestProtoErr(res map[string]disttopk.AlgoStats) string {
	bestValue := 0.0
	bestName := ""
	first := true
	for _, proto := range t.protocols {

		stats := res[proto.Name]
		if first || stats.Abs_err < bestValue {
			bestValue = stats.Abs_err
			bestName = proto.Name
			first = false
		}
	}
	return bestName
}

type ExactPrinter struct {
	*OverviewPrinter
}

func (t *ExactPrinter) Start() {
	t.s = t.RowDescriptionHeaders()
	for _, proto := range t.protocols {
		t.s += "\t" + proto.Name
	}

	t.s += "\tBest\n"
}

func (t *ExactPrinter) EnterRow(rd RowDescription, res map[string]disttopk.AlgoStats) string {
	s := t.GetRowDescription(rd)
	for _, proto := range t.protocols {
		stats := res[proto.Name]
		s += fmt.Sprintf("\t%4.1E", float64(stats.Bytes_transferred))
	}
	s += "\t" + t.BestProtoBytes(res)
	t.s += s + "\n"
	return s
}

type GcsTputPrinter struct {
	*OverviewPrinter
}

func (t *GcsTputPrinter) Start() {
	t.s = t.RowDescriptionHeaders()
	t.s += "\tGcsM\tTputH\tImprovement\n"
}

func (t *GcsTputPrinter) EnterRow(rd RowDescription, res map[string]disttopk.AlgoStats) string {
	s := t.GetRowDescription(rd)
	size_tputHash := res[TputHash.Name].Bytes_transferred
	size_gcs := res[GcsMerge.Name].Bytes_transferred
	improvement := (float64(size_tputHash) - float64(size_gcs)) / float64(size_tputHash)
	s += fmt.Sprintf("\t%4.1E\t%4.1E\t%3.2f", float64(size_gcs), float64(size_tputHash), improvement*100)
	t.s += s + "\n"
	return s
}
