package tput_hash

import "github.com/cevian/disttopk"

import "testing"

//import "github.com/cevian/disttopk/tput-hash"
import (
	//"bytes"
	"fmt"

//"math/rand"
)

func TestCmVsCha(t *testing.T) {
	lists := disttopk.GetFullOverlapOrderPermutedSimpleList(10, 10000, 0.7, 100)
	list := lists[0]

	cm := disttopk.NewCountMinSketch(1, 100000)
	for _, item := range list {
		cm.Add(disttopk.IntKeyToByteKey(item.Id), uint32(item.Score))
	}

	cha := NewCountHashArray(100000)

	for _, item := range list {
		cha.Add(disttopk.IntKeyToByteKey(item.Id), uint(item.Score))
	}

	if !cha.Data.Equal(cm.Data[0]) {
		t.Error("Counts should be equal")
	}

	cm_ser, err := disttopk.SerializeObject(cm)
	if err != nil {
		panic(err)
	}

	cha_ser, err := disttopk.SerializeObject(cha)
	if err != nil {
		panic(err)
	}

	fmt.Println("Lengths: ", len(cm_ser), len(cha_ser))
	fmt.Println("Lengths: ", len(disttopk.CompressBytes(cm_ser)), len(disttopk.CompressBytes(cha_ser)))

}
