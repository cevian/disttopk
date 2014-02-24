package main

import "github.com/cevian/disttopk"

import (
	"fmt"
)

func PrintDiff(ground_truth, result disttopk.ItemList, k int) {
	for i := 0; i < k; i++ {
		if ground_truth[i] != result[i] {
			fmt.Println("Lists do not match at position", i, "Ground truth:", ground_truth[i], "vs", result[i])
		}
	}
}

/*var defaultPrinters = []Printer{&OverviewPrinter{protocols, ""},
	&ApproxPrinter{&OverviewPrinter{ApproximateProtocols(), ""}},
	&ExactPrinter{&OverviewPrinter{ExactProtocols(), ""}},
	&GcsTputPrinter{&OverviewPrinter{protocols, ""}},
	&ExportPrinter{&OverviewPrinter{protocols, ""}},
}*/

func GetDefaultPrinters(protos []Protocol) []Printer{
   return []Printer{&OverviewPrinter{protos, ""},
	&ApproxPrinter{&OverviewPrinter{ApproximateProtocols(), ""}},
	&ExactPrinter{&OverviewPrinter{ExactProtocols(), ""}},
	&GcsTputPrinter{&OverviewPrinter{protocols, ""}},
	&ExportPrinter{&OverviewPrinter{protos, ""}},
}
	
}

type RowDescription struct {
	k       int
	nodes   int
	N       int
	zip     float64
	perms   int
	overlap float64
	seed    int64
}

func (rd *RowDescription) String() string {
	return fmt.Sprintf("k=%v nodes=%v N=%v zip=%v perms=%v overap=%v seed=%v", rd.k, rd.nodes, rd.N, rd.zip, rd.perms, rd.overlap, rd.seed)
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
	return "N\tZip\tPerm\tOverlap\tSeed"
}

func (t *OverviewPrinter) Start() {
	t.s = t.RowDescriptionHeaders()
	for _, proto := range t.protocols {
		t.s += "\t" + proto.Name
	}
	t.s += "\n"
}

func (t *OverviewPrinter) GetRowDescription(rd RowDescription) string {
	return fmt.Sprintf("%4.1E\t%2.1f\t%d\t%2.2f\t%d", float64(rd.N), float64(rd.zip), rd.perms, rd.overlap, rd.seed)
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

type ExportPrinter struct {
	*OverviewPrinter
}

func (t *ExportPrinter) EnterNewN() {
}
func (t *ExportPrinter) Start() {
	t.s = "--------------Start Export----------\nExport\t" + t.RowDescriptionHeaders()
	t.s += "\tProtocol Name\tExact\tRounds\tSize\tRel Err\tRecall\tDistance\tSerial Items\tRandom Access\tRandom Items\n"
}

func (t *ExportPrinter) GetRowDescription(rd RowDescription) string {
	return fmt.Sprintf("%f\t%f\t%d\t%f\t%d", float64(rd.N), float64(rd.zip), rd.perms, rd.overlap, rd.seed)
}

func (t *ExportPrinter) EnterRow(rd RowDescription, res map[string]disttopk.AlgoStats) string {
	s := ""
	for _, proto := range t.protocols {
		s += "Export\t" + t.GetRowDescription(rd)
		stats := res[proto.Name]
		s += fmt.Sprintf("\t%s\t%t\t%d\t%d\t%f\t%f\t%f\t%d\t%d\t%d\n", proto.Name, proto.isExact, stats.Rounds, stats.Bytes_transferred, stats.Rel_err, stats.Recall, stats.Edit_distance, stats.Serial_items, stats.Random_access, stats.Random_items)
	}
	t.s += s
	return ""
}
