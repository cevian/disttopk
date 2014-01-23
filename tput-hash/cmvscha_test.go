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

/*
func TestCountArraySerializeProblem(t *testing.T) {
	l := disttopk.GetFullOverlapOrderPermutedSimpleList(10, uint32(1000), 0.3, 100)
	k := 10
	for _, list := range l {
		cha := NewCountHashArray(10000)

		//fmt.Println("Peer ", src.id, " got ", thresh, " index ", index, "k", src.k, "list[index+1].score", src.list[index+1].Score)
		//v.Score >= thresh included
		last_index_to_send := len(list) - 1
		items_looked_at := uint(0)
		if last_index_to_send >= k {
			for _, list_item := range list[k : last_index_to_send+1] {
				items_looked_at += 1
				cha.Add(disttopk.IntKeyToByteKey(list_item.Id), uint(list_item.Score))
			}
		}

		t.Log("89 before serialize", cha.Data.Get(89))
		cha_ser, err := disttopk.SerializeObject(cha)
		if err != nil {
			panic(err)
		}
		//	cha_comp := disttopk.CompressBytes(cha_ser)
		//	cha_got_ser := disttopk.DecompressBytes(cha_comp)
		//bytes_cha += len(cha_got_ser)
		cha_got := &CountHashArray{}
		if err := disttopk.DeserializeObject(cha_got, cha_ser); err != nil {
			panic(err)
		}
		t.Log("89 after deserialize", cha_got.Data.Get(89))
		//break
	}

}*/
