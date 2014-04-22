package printers

import (
	"fmt"

	"github.com/cevian/disttopk"
	"github.com/cevian/disttopk/runner"
)

func GetNEst(l []disttopk.ItemList) int {
	ids := make(map[int]bool)
	for _, list := range l {
		for _, item := range list {
			ids[item.Id] = true
		}
	}
	return len(ids)
}
func PrintDiff(ground_truth, result disttopk.ItemList, k int) {
	for i := 0; i < k; i++ {
		if ground_truth[i] != result[i] {
			fmt.Println("Lists do not match at position", i, "Ground truth:", ground_truth[i], "vs", result[i])
		}
	}
}

func ExportPrinterHeaders(rd RowDesc) string {
	s := "--------------Start Export----------\nExport\t" + rd.GetHeaders()
	s += "\tProtocol Name\tExact\tRounds\tSize\tExecution Time\tRel Err\tRecall\tDistance\tScore K\tError k"
	for i := 0; i <= 3; i++ {
		rs := fmt.Sprintf("Round %d", i+1)
		s += fmt.Sprintf("\t%s Sketch Bytes\t%s Serial Items sum\t%s Serial Items max\t%s Random Items sum\t%s Random Items max\t%s Random Access sum\t%s Random Access max\t%s Transferred Items sum", rs, rs, rs, rs, rs, rs, rs, rs)
	}
	s += "\n"
	return s
}

func ExportPrinter(rd RowDesc, runners []runner.Runner, res map[string]disttopk.AlgoStats) string {
	s := ExportPrinterHeaders(rd)
	for _, proto := range runners {
		s += fmt.Sprintf("Export\t%s", rd.GetRowData())
		stats := res[proto.GetName()]
		s += fmt.Sprintf("\t%s\t%t\t%d\t%d\t%d\t%f\t%f\t%f\t%d\t%f", proto.GetName(), proto.IsExact(), stats.Rounds, stats.Bytes_transferred, stats.Took, stats.Rel_err, stats.Recall, stats.Edit_distance, stats.TrueScoreK, stats.K_err)
		if len(stats.RoundStats) > 4 {
			panic("Too many rounds")
		}
		for i := 0; i <= 3; i++ {
			roundStat := disttopk.AlgoStatsRoundUnion{}
			if i < len(stats.RoundStats) {
				roundStat = stats.RoundStats[i]
			}
			s += fmt.Sprintf("\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d", roundStat.Bytes_sketch_sum, roundStat.Serial_items_sum, roundStat.Serial_items_max, roundStat.Random_items_sum, roundStat.Random_items_max, roundStat.Random_access_sum, roundStat.Random_access_max, roundStat.Transferred_items_sum)

		}
		s += "\n"
	}
	return s
}

type RowDesc interface {
	GetFs() *disttopk.FileSource
	GetHeaders() string
	GetRowData() string
}

type UcbRowDesc struct {
	KeyOnClient bool
	ModServers  int
}

func (t *UcbRowDesc) GetFs() *disttopk.FileSource {
	return &disttopk.FileSource{&disttopk.UcbFileSourceAdaptor{KeyOnClient: t.KeyOnClient, ModServers: t.ModServers}}
}

func (t *UcbRowDesc) GetHeaders() string {
	return "Type\tKeyOnClient\tModServers"
}

func (t *UcbRowDesc) GetRowData() string {
	return fmt.Sprintf("%s\t%t\t%d", "UCB", t.KeyOnClient, t.ModServers)
}

type WcRowDesc struct {
	KeyOnClient bool
}

func (t *WcRowDesc) GetFs() *disttopk.FileSource {
	return &disttopk.FileSource{&disttopk.WcFileSourceAdaptor{KeyOnClient: t.KeyOnClient}}
}

func (t *WcRowDesc) GetHeaders() string {
	return "Type\tKeyOnClient"
}

func (t *WcRowDesc) GetRowData() string {
	return fmt.Sprintf("%s\t%t", "WC", t.KeyOnClient)
}

type CwRowDesc struct {
	Topic int
}

func (t *CwRowDesc) GetFs() *disttopk.FileSource {
	return nil
}

func (t *CwRowDesc) GetHeaders() string {
	return "Topic"
}

func (t *CwRowDesc) GetRowData() string {
	return fmt.Sprintf("%d", t.Topic)
}

type SynRowDesc struct {
	K          int
	Nodes      int
	N          int
	Zip        float64
	Perms      int
	Overlap    float64
	Seed       int64
	RecordSize int
}

func (t *SynRowDesc) GetFs() *disttopk.FileSource {
	return nil
}

func (t *SynRowDesc) GetHeaders() string {
	return "N\tZip\tPerm\tOverlap\tSeed\tRecord Size"
}

func (t *SynRowDesc) GetRowData() string {
	return fmt.Sprintf("%4.1E\t%2.1f\t%d\t%2.2f\t%d\t%d", float64(t.N), float64(t.Zip), t.Perms, t.Overlap, t.Seed, t.RecordSize)
}
