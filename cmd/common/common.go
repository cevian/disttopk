package common

import "github.com/cevian/disttopk/runner"

func GetRunners() []runner.Runner {
	return []runner.Runner{
		runner.NewTputRunner(),
		runner.NewTputHRunner(),
		runner.NewTputERRunner(),
		runner.NewSbrErRunner(),
		runner.NewSbr2RRunner(),
		runner.NewSbrARunner(),
		runner.NewNaiveK2Runner(),
		runner.NewNaiveExactRunner(),
		runner.NewKlee3Runner(),
		runner.NewKlee4Runner(),
	}
}
