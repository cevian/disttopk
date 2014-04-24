package disttopk

import (
	"fmt"
	"math"
)

import typesort "github.com/cevian/disttopk/sort"

type MaxHashMap struct {
	data             map[uint32]int64 //the over-approximation should be data[hash] + cutoff. maps hashValue => mapValue (max-cutoff)
	data_under       map[uint32]int64 //the unse-approximation
	cutoff           uint32
	cutoff_map       map[uint32]bool
	modulus_bits     uint32
	min_modulus_bits uint32
}

func NewMaxHashMap(length_hint int) *MaxHashMap {
	return &MaxHashMap{make(map[uint32]int64, length_hint), make(map[uint32]int64, length_hint), 0, make(map[uint32]bool, length_hint), 0, 0}
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

	if max < min {
		panic("snh")
	}

	if !t.cutoff_map[uint32(hashValue)] {
		t.data[uint32(hashValue)] += int64(max - cutoff)
		t.cutoff_map[uint32(hashValue)] = true
	} else {
		t.data[uint32(hashValue)] += int64(max)

	}
	t.data_under[uint32(hashValue)] += int64(min)
}

func (t *MaxHashMap) SetModulusBits(bits int) {
	t.modulus_bits = uint32(bits)
	t.min_modulus_bits = t.modulus_bits
}

func (t *MaxHashMap) Add(hashValue uint, modulus_bits uint, max uint, min uint, cutoff uint) {
	if max < min {
		panic(fmt.Sprintf("Max < min", max, min))
	}

	if uint32(modulus_bits) < t.min_modulus_bits {
		t.min_modulus_bits = uint32(modulus_bits)
	}

	//fmt.Println("Adding ", hashValue, modulus_bits, max, cutoff)
	if t.modulus_bits == 0 {
		t.SetModulusBits(int(modulus_bits))
	}
	/*if max <= cutoff { //this can happen when merging in exact values from top-k
		panic(fmt.Sprintf("Wrong input max < cutoff %v %v", max, cutoff))
	}*/

	mhm_modulus := (1 << t.modulus_bits)

	if uint32(modulus_bits) < t.modulus_bits {
		rcv_modulus := (1 << modulus_bits)
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
		hashValue = hashValue % uint(mhm_modulus)
	}

	t.addData(hashValue, max, min, cutoff)
}

func (t *MaxHashMap) AddCutoff(c uint) {
	if math.MaxUint32-t.cutoff < uint32(c) {
		panic("Overflow")
	}
	t.cutoff += uint32(c)
	t.cutoff_map = make(map[uint32]bool)
}

func (t *MaxHashMap) GetFilter(thresh int64) (*Gcs, int64) {
	if thresh <= int64(t.cutoff) {
		fmt.Printf("WARNING: in MaxHashMap thresh(%v) <= cutoff(%v). Sending no filter, everything will be sent", thresh, t.cutoff)
		return nil, 0
	}

	mapValueThresh := thresh - int64(t.cutoff)

	m := (1 << (uint(t.modulus_bits)))
	gcs := NewGcs(m)

	maxNotIncluded := int64(0)
	for hashValue, mapValue := range t.data {
		if mapValue >= mapValueThresh {
			//fmt.Println("Diff", mapValue-mapValueThresh, mapValue, mapValueThresh, count)
			gcs.Data.Insert(hashValue)
		} else {
			value := mapValue + int64(t.cutoff)
			if value > maxNotIncluded {
				maxNotIncluded = value
			}
		}
	}
	if maxNotIncluded == 0 {
		maxNotIncluded = thresh - 1
	}
	//fmt.Println("Better thresh", thresh,  maxNotIncluded+1, t.cutoff, mapValueThresh, len(t.data))

	return gcs, maxNotIncluded + 1

}

func (t *MaxHashMap) GetCountHashesWithCutoff(thresh int64, cutoff int64, filterThresh int64) (int, int64) {

	mapValueThresh := thresh - cutoff

	//this is the value the filter will use to send stuff already
	mapValueFilter := filterThresh - int64(t.cutoff)

	count := 0
	minover := int64(0)
	for _, mapValue := range t.data {
		if mapValue >= mapValueThresh && (minover == 0 || mapValue < minover) {
			minover = mapValue
		}
		if mapValue >= mapValueThresh && mapValue < mapValueFilter {
			count += 1
		}

	}
	next := int64(0)
	if minover > 0 && minover < thresh {
		next = thresh - (minover + 1)
	}
	return count, next
}

func (t *MaxHashMap) GetMaxCutoff(thresh int64) int64 {
	max := int64(0)
	for _, mapValue := range t.data {
		if mapValue > max {
			max = mapValue
		}
	}
	//thresh - x = max
	//x = thresh-max

	return (max - 1) + int64(t.cutoff)
}

func (t *MaxHashMap) GetMinModulusBitsMap(m map[uint32]int64) map[uint32]int64 {
	min_modulus := uint32(1 << t.min_modulus_bits)
	res := make(map[uint32]int64)
	for hv, count := range m {
		//max and not addition is the right thing here.
		//it prevent double counting thing that were a smaller modulus and then copied to several times in the larger modulus
		//not sure this is conservative, things that should have been added, already were in the larger modulus
		if res[hv%min_modulus] < count {
			res[hv%min_modulus] = count
		}
	}
	return res
}

func (t *MaxHashMap) UnderApprox(maxNumberHashValues int) int64 {
	//we want maxNumberHashValues items to be represented
	//that means we need to use the min modulus bits representation
	//otherwise we may double count items as having two hash values
	data := t.GetMinModulusBitsMap(t.data_under)
	mapValuesSortedUnder := make([]int64, 0, len(data))
	for _, mapValue := range data {
		mapValuesSortedUnder = append(mapValuesSortedUnder, mapValue)
	}
	typesort.Int64s(mapValuesSortedUnder)

	underApprox := mapValuesSortedUnder[len(mapValuesSortedUnder)-maxNumberHashValues]
	return underApprox
}

func (t *MaxHashMap) OverApprox(maxNumberHashValues int) int64 {
	data := t.GetMinModulusBitsMap(t.data)
	mapValuesSorted := make([]int64, 0, len(data))
	for _, mapValue := range data {
		mapValuesSorted = append(mapValuesSorted, mapValue)
	}
	typesort.Int64s(mapValuesSorted)

	overApprox := mapValuesSorted[len(mapValuesSorted)-maxNumberHashValues] + int64(t.cutoff)
	return overApprox
}

/*
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
}*/

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
