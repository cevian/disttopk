package main

import "github.com/cevian/disttopk/runner"
//import "github.com/cevian/disttopk"


func GetRunners() []runner.Runner {
   return []runner.Runner{
	runner.NewKlee3Runner(),
	runner.NewKlee4Runner(),
	runner.NewSbrARunner(),
	runner.NewSbr2RRunner(),
	runner.NewSbrErNoSplitRunner(),
	runner.NewSbrErRunner(),
	runner.NewSbrErIdealNestRunner(),
	runner.NewTputRunner(),
	runner.NewTputHRunner(),
	runner.NewTputERRunner(),
}


}

func ApproximateRunners() []runner.Runner {
	ret := make([]runner.Runner, 0)
	for _, runner := range GetRunners() {
		if !runner.IsExact() {
			ret = append(ret, runner)
		}
	}
	return ret
}

func ExactRunners() []runner.Runner {
	ret := make([]runner.Runner, 0)
	for _, runner := range GetRunners() {
		if runner.IsExact() {
			ret = append(ret, runner)
		}
	}
	return ret
}


