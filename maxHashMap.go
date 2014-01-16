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
	if max <= cutoff {
		panic(fmt.Sprintf("Wrong input max < cutoff %v %v", max, cutoff))
	}

	if uint32(modulus_bits) < t.modulus_bits {
		rcv_modulus := (1 << modulus_bits)
		mhm_modulus := (1 << t.modulus_bits)
		count := 0
		for int(hashValue) < mhm_modulus {
			count += 1
			t.data[uint32(hashValue)] += uint32(max - cutoff)
			hashValue += uint(rcv_modulus)
		}

		return
		//fmt.Println("#values", count, max-cutoff, max, cutoff)
		//panic(fmt.Sprint("Only greater modulus supported got", modulus_bits, " mhm ", t.modulus_bits))
	}

	if uint32(modulus_bits) > t.modulus_bits {
		hashValue = hashValue % uint(t.modulus_bits)
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
