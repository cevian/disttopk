package main

import "github.com/cevian/disttopk/runner"
import "github.com/cevian/disttopk"

type Protocol struct {
	Name    string
	Runner  func([]disttopk.ItemList, int) (disttopk.ItemList, disttopk.AlgoStats)
	isExact bool
}

var NaiveEx = Protocol{"Naive-exact", runner.RunNaiveExact, true}

//approx
var Naive2k = Protocol{"Naive (2k)", runner.RunNaiveK2, false}
var Klee3 = Protocol{"Klee3", runner.RunKlee3, false}
var Klee4 = Protocol{"Klee4", runner.RunKlee4, false}
var Bloom = Protocol{"bloom", runner.RunApproximateBloomFilter, false}
var BloomGcs = Protocol{"SBR-A", runner.RunApproximateBloomGcsFilter, false}

// Extra-Round Exact
var ErGcs = Protocol{"ER GCS", runner.RunExtraRoundBloomGcsMergeFilter, true}
var ErGms = Protocol{"SBR-ER", runner.RunExtraRoundBloomGcsMergeSplitUnderNest, true}
var ErGmsOverNest = Protocol{"SBR-ER OverNest", runner.RunExtraRoundBloomGcsMergeSplitOverNest, true}
var ErGmsIdealNest = Protocol{"SBR-ER IdealNest", runner.RunExtraRoundBloomGcsMergeSplitIdealNest, true}
var ErTput = Protocol{"TPUT-ER", runner.RunTputHashExtraRound, true}

// Exact
var Tput = Protocol{"TPUT", runner.RunTput, true}
var TputHash = Protocol{"TPUT-H", runner.RunTputHash, true}

//var Gcs	= Protocol{"2R Gcs  ", runner.RunBloomSketchGcs, true}
var GcsMerge = Protocol{"SBR-2R", runner.RunBloomSketchGcsMerge, true}
var CountMin = Protocol{"Count Min", runner.RunCountMin, true}

var protocols []Protocol = []Protocol{
	Klee3,
	Klee4,
	Bloom,
	BloomGcs,
	ErGcs,
	ErGms,
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
