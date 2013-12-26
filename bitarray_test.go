package disttopk

import "testing"
import "math/rand"

func TestBitArray(t *testing.T) {
	n := 10000
	a := NewBitArray(uint(n))

	set := make(map[uint]bool)

	for i := 0; i < n/10; i++ {
		j := rand.Int() % n
		set[uint(j)] = true
		a.Set(uint(j))
	}

	for i := 0; i < n; i++ {
		j := uint(i)
		expected := false
		_, ok := set[j]
		if ok {
			expected = true
		}
		actual := a.Check(j)
		if expected != actual {
			t.Fail()
		}
	}

}

func TestBitArraySerialize(t *testing.T) {
	n := 10000
	a := NewBitArray(uint(n))

	for i := 0; i < n/10; i++ {
		j := rand.Int() % n
		a.Set(uint(j))
	}

	b, err := SerializeObject(a)

	if err != nil {
		panic(err)
	}

	var obj BitArray

	err = DeserializeObject(&obj, b)

	if err != nil {
		panic(err)
	}

	if !a.Equal(&obj) {
		t.Fail()
	}

}
