package disttopk

import "fmt"

type MaxHashMap struct {
	data         map[uint32]uint32 //the over-approximation should be data[hash] + cutoff
	cutoff       uint32
	modulus_bits uint32
}

func NewMaxHashMap() *MaxHashMap {
	return &MaxHashMap{make(map[uint32]uint32), 0, 0}
}

func (t *MaxHashMap) GetInfo() string {
	return fmt.Sprintf("MaxHashMap, %v items, cutoff: %v, modulus_bits: %v", len(t.data), t.cutoff, t.modulus_bits)
}

func (t *MaxHashMap) Add(hashValue uint, modulus_bits uint, max uint, cutoff uint) {
	//fmt.Println("Adding ", hashValue, modulus_bits, max, cutoff)
	if t.modulus_bits == 0 {
		t.modulus_bits = uint32(modulus_bits)
	}

	if uint32(modulus_bits) != t.modulus_bits {
		panic("Only one modulus supported")
	}
	if max <= cutoff {
		panic("Wrong input")
	}

	t.data[uint32(hashValue)] += uint32(max - cutoff)
}

func (t *MaxHashMap) AddCutoff(c uint) {
	t.cutoff += uint32(c)
}

func (t *MaxHashMap) GetFilter(thresh uint) *Gcs {
	if uint32(thresh) < t.cutoff {
		panic("error")
	}

	mapValueThresh := uint32(thresh) - t.cutoff

	values := make([]uint32, 0)
	for hashValue, mapValue := range t.data {
		if mapValue >= mapValueThresh {
			values = append(values, hashValue)
		}
	}

	//n := len(values)

	m := (1 << (uint(t.modulus_bits)))
	//fmt.Printf("Get Filter. m %v (%v), thresh %v, mvthresh %v, len %v %v", m, t.modulus_bits, thresh, mapValueThresh, len(t.data), len(values))
	gcs := NewGcs(m)

	for _, value := range values {
		gcs.Data.Insert(value)
	}
	return gcs

}