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
var BloomGcs = Protocol{"bloomGcs", runner.RunApproximateBloomGcsFilter, false}

// Extra-Round Exact
var ErGcs = Protocol{"ER GCS", runner.RunExtraRoundBloomGcsMergeFilter, true}
var ErGms = Protocol{"ER GMS", runner.RunExtraRoundBloomGcsMergeSplit, true}
var ErTput = Protocol{"ER TP", runner.RunTputHashExtraRound, true}

// Exact
var Tput = Protocol{"Tput", runner.RunTput, true}
var TputHash = Protocol{"TputH", runner.RunTputHash, true}

//var Gcs	= Protocol{"2R Gcs  ", runner.RunBloomSketchGcs, true}
var GcsMerge = Protocol{"2R GcsM", runner.RunBloomSketchGcsMerge, true}
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
