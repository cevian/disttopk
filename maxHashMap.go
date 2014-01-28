package disttopk

import (
	"fmt"
	"math"
	"sort"
)

type MaxHashMap struct {
	data         map[uint32]int64  //the over-approximation should be data[hash] + cutoff. maps hashValue => mapValue (max-cutoff)
	data_under   map[uint32]uint64 //the unse-approximation
	cutoff       uint32
	modulus_bits uint32
}

func NewMaxHashMap() *MaxHashMap {
	return &MaxHashMap{make(map[uint32]int64), make(map[uint32]uint64), 0, 0}
}

func (t *MaxHashMap) GetInfo() string {
	return fmt.Sprintf("MaxHashMap, %v items, cutoff: %v, modulus_bits: %v", len(t.data), t.cutoff, t.modulus_bits)
}

func (t *MaxHashMap) GetModulusBits() uint {
	return uint(t.modulus_bits)
}

func (t *MaxHashMap) Cutoff() uint {
	return uint(t.cutoff)
}

func (t *MaxHashMap) addData(hashValue uint, max uint, min uint, cutoff uint) {
	if math.MaxInt64-t.data[uint32(hashValue)] < int64(max-cutoff) {
		panic("Overflow")
	}

	t.data[uint32(hashValue)] += int64(max - cutoff)
	t.data_under[uint32(hashValue)] += uint64(min)

}

func (t *MaxHashMap) Add(hashValue uint, modulus_bits uint, max uint, min uint, cutoff uint) {
	if max < min {
		panic(fmt.Sprintf("Max < min", max, min))
	}

	//fmt.Println("Adding ", hashValue, modulus_bits, max, cutoff)
	if t.modulus_bits == 0 {
		t.modulus_bits = uint32(modulus_bits)
	}
	/*if max <= cutoff { //this can happen when merging in exact values from top-k
		panic(fmt.Sprintf("Wrong input max < cutoff %v %v", max, cutoff))
	}*/

	if uint32(modulus_bits) < t.modulus_bits {
		rcv_modulus := (1 << modulus_bits)
		mhm_modulus := (1 << t.modulus_bits)
		count := 0
		for int(hashValue) < mhm_modulus {
			count += 1
			t.addData(hashValue, max, min, cutoff)
			hashValue += uint(rcv_modulus)
		}

		return
		//fmt.Println("#values", count, max-cutoff, max, cutoff)
		//panic(fmt.Sprint("Only greater modulus supported got", modulus_bits, " mhm ", t.modulus_bits))
	}

	if uint32(modulus_bits) > t.modulus_bits {
		hashValue = hashValue % uint(t.modulus_bits)
	}

	t.addData(hashValue, max, min, cutoff)
}

func (t *MaxHashMap) AddCutoff(c uint) {
	t.cutoff += uint32(c)
}

func (t *MaxHashMap) GetFilter(thresh uint) *Gcs {
	if uint32(thresh) <= t.cutoff {
		fmt.Printf("WARNING: in MaxHashMap thresh(%v) <= cutoff(%v). Sending no filter, everything will be sent", thresh, t.cutoff)
		return nil
	}

	mapValueThresh := int64(thresh) - int64(t.cutoff)

	values := make([]uint32, 0)
	count := 0
	for hashValue, mapValue := range t.data {
		if mapValue >= mapValueThresh {
			//fmt.Println("Diff", mapValue-mapValueThresh, mapValue, mapValueThresh, count)
			values = append(values, hashValue)
			count += 1
		}
	}

	//n := len(values)

	m := (1 << (uint(t.modulus_bits)))
	//fmt.Printf("Get Filter. m %v (%v), thresh %v, mvthresh %v, #hash values %v, #hash values above thresh %v", m, t.modulus_bits, thresh, mapValueThresh, len(t.data), len(values))
	gcs := NewGcs(m)

	for _, value := range values {
		gcs.Data.Insert(value)
	}
	return gcs

}

func (t *MaxHashMap) GetThreshApprox(maxNumberHashValues int, gamma float64) uint {
	mapValuesSortedUnder := make([]int, 0, len(t.data_under))
	for _, mapValue := range t.data_under {
		mapValuesSortedUnder = append(mapValuesSortedUnder, int(mapValue))
	}
	sort.Ints(mapValuesSortedUnder)

	underApprox := mapValuesSortedUnder[len(mapValuesSortedUnder)-maxNumberHashValues]

	mapValuesSorted := make([]int, 0, len(t.data))
	for _, mapValue := range t.data {
		mapValuesSorted = append(mapValuesSorted, int(mapValue))
	}
	sort.Ints(mapValuesSorted)

	overApprox := mapValuesSorted[len(mapValuesSorted)-maxNumberHashValues] + int(t.cutoff)

	if overApprox < underApprox {
		panic(fmt.Sprintln("UnderApprox", underApprox, "OverApprox", overApprox, "Gamma", gamma, "cutoff", t.cutoff, mapValuesSorted[len(mapValuesSorted)-maxNumberHashValues:], mapValuesSortedUnder[len(mapValuesSortedUnder)-maxNumberHashValues:]))
	}
	approxThresh := underApprox + int(float64(overApprox-underApprox)*gamma)

	return uint(approxThresh)
}

/*
func (t *MaxHashMap) GetThreshApprox(maxNumberHashValues int) uint {
	mapValuesSorted := make([]int, 0, len(t.data))
	for _, mapValue := range t.data {
		mapValuesSorted = append(mapValuesSorted, int(mapValue))
	}
	sort.Ints(mapValuesSorted)

	approxThresh := mapValuesSorted[len(mapValuesSorted)-maxNumberHashValues]
	approxThresh = approxThresh - int(t.cutoff) //this is a correction. Not going from mapValues to query values.

	return (uint(approxThresh) + uint(t.cutoff)) //this goes from domain of mapValues to Scores
}
*/
