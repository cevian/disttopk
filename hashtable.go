package disttopk

type HashEntry struct {
	data map[int]float64
}

type HashTable struct {
	*CountMinHash
	buckets     map[int]*HashEntry
	bucket_bits uint8
}

func NewHashTable(bucket_bits uint8) *HashTable {
	tot_buckets := (1 << bucket_bits)
	return &HashTable{NewCountMinHash(1, tot_buckets), make(map[int]*HashEntry), bucket_bits}
}

func (t *HashTable) Insert(key int, score float64) {
	bucket := t.GetIndexNoOffset(IntKeyToByteKey(key), 0)
	entry, ok := t.buckets[int(bucket)]
	if !ok {
		entry = &HashEntry{make(map[int]float64)}
		t.buckets[int(bucket)] = entry
	}
	entry.data[key] = score
}

func (t *HashTable) addEntry(entry *HashEntry, val map[int]float64) {
	if entry == nil {
		return
	}
	if len(entry.data) == 0 {
		panic("should never happen")
	}
	for k, v := range entry.data {
		val[k] = v
	}
}

func (t *HashTable) GetByHashValue(hash_value uint, modulus_bits uint8) (values map[int]float64, num_random_accesses int) {
	ret := make(map[int]float64)
	if modulus_bits == t.bucket_bits {
		t.addEntry(t.buckets[int(hash_value)], ret)
		return ret, 1
	} else if modulus_bits > t.bucket_bits {
		t.addEntry(t.buckets[int(hash_value%uint(1<<t.bucket_bits))], ret)
		return ret, 1
	} else {
		//bucket_bits > modulus_bits
		original_modulus := (1 << modulus_bits)
		bucket_modulus := (1 << t.bucket_bits)
		ra := 0
		for int(hash_value) < bucket_modulus {
			t.addEntry(t.buckets[int(hash_value)], ret)
			hash_value += uint(original_modulus)
			ra++
		}
		return ret, ra
	}
}
