package disttopk

import "testing"
import (
	"bytes"
	//"fmt"
	"math/rand"
)

func TestCountArray(t *testing.T) {
	n := 10000
	a := NewCountArray(int(n))

	set := make(map[int]uint)

	for i := 0; i < n/10; i++ {
		j := int(rand.Int() % n)
		set[j] = 2
		a.Set(j, 2)
	}

	for i := 0; i < n; i++ {
		j := int(i)
		expected, _ := set[j]
		actual := a.Get(j)
		if expected != actual {
			t.Fail()
		}
	}

}

func TestCountArraySerialize(t *testing.T) {
	n := 10000
	a := NewCountArray(int(n))

	for i := 0; i < n/10; i++ {
		j := rand.Int() % n
		v := rand.Int() % n
		a.Set(int(j), uint(v))
	}

	b, err := SerializeObject(a)

	if err != nil {
		panic(err)
	}

	var obj CountArray

	err = DeserializeObject(&obj, b)

	if err != nil {
		panic(err)
	}

	if !a.Equal(&obj) {
		t.Fail()
	}

}

func TestCountArraySerializeWithBag(t *testing.T) {
	n := 10000
	a := NewCountArray(int(n))

	for i := 0; i < n/10; i++ {
		j := rand.Int() % n
		v := rand.Int() % n
		a.Set(int(j), uint(v))
	}

	buf := new(bytes.Buffer)
	if err := a.SerializeWithBag(buf); err != nil {
		panic(err)
	}
	b := buf.Bytes()

	var obj CountArray

	bufr := bytes.NewReader(b)
	err := obj.DeserializeWithBag(bufr)

	if err != nil {
		panic(err)
	}

	a.transformLog()
	a.untransformLog()

	if !a.Equal(&obj) {
		t.Fail()
	}

}
