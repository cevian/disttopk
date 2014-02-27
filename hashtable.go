package disttopk

type HashEntry struct {
	id int
	score float64
	Next *HashEntry
}

type HashTable struct {
	*CountMinHash
	buckets     []HashEntry
	bucket_bits uint8
}

func NewHashTable(bucket_bits uint8) *HashTable {
	tot_buckets := (1 << bucket_bits)
	return &HashTable{NewCountMinHash(1, tot_buckets), make([]HashEntry, tot_buckets, tot_buckets), bucket_bits}
}

func (t *HashTable) Insert(key int, score float64) {
	bucket := t.GetIndexNoOffset(IntKeyToByteKey(key), 0)
	entry := &t.buckets[int(bucket)]
	if entry.id == 0 {
		entry.id = key
		entry.score = score
		return
	}
	for entry.Next != nil {
		entry = entry.Next
	}
	entry.Next = &HashEntry{key, score, nil}
}

func (t *HashTable) VisitHashValue(hash_value uint, visitor func(uint, uint)) {
	entry := &t.buckets[int(hash_value)]
	for entry != nil && entry.id != 0 {
		visitor(uint(entry.id), uint(entry.score))
		entry = entry.Next
	}
}

func (t *HashTable) addEntry(entry *HashEntry, val map[int]float64) {
	for entry != nil && entry.id != 0 {
		val[entry.id] = entry.score
		entry = entry.Next
	}
}

func (t *HashTable) GetTableHashValues(hash_value uint, modulus_bits uint8) []uint {
	if modulus_bits == t.bucket_bits {
		return []uint{hash_value}
	} else if modulus_bits > t.bucket_bits {
		return []uint{hash_value % uint(1<<t.bucket_bits)}
	} else {
		ret := make([]uint, 0)
		original_modulus := (1 << modulus_bits)
		bucket_modulus := (1 << t.bucket_bits)
		for int(hash_value) < bucket_modulus {
			ret = append(ret, hash_value)
			hash_value += uint(original_modulus)
		}
		return ret
	}
}

func (t *HashTable) GetByHashValue(hash_value uint, modulus_bits uint8) (values map[int]float64, num_random_accesses int) {
	ret := make(map[int]float64)
	if modulus_bits == t.bucket_bits {
		t.addEntry(&t.buckets[int(hash_value)], ret)
		return ret, 1
	} else if modulus_bits > t.bucket_bits {
		t.addEntry(&t.buckets[int(hash_value%uint(1<<t.bucket_bits))], ret)
		return ret, 1
	} else {
		//bucket_bits > modulus_bits
		original_modulus := (1 << modulus_bits)
		bucket_modulus := (1 << t.bucket_bits)
		ra := 0
		for int(hash_value) < bucket_modulus {
			t.addEntry(&t.buckets[int(hash_value)], ret)
			hash_value += uint(original_modulus)
			ra++
		}
		return ret, ra
	}
}
