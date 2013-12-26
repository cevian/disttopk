package disttopk

import "testing"
import "bytes"
import "io"
import "math/rand"
import "sort"

func TestGolumbReaderWriter(t *testing.T) {
	var b bytes.Buffer
	n := 1000

	inta := make([]uint, 0, n)

	w := NewGolumbEncoder(&b, 5) //m ==32

	for i := 0; i < n; i++ {
		num := uint(rand.Int() % 64)
		inta = append(inta, num)
		w.Write(num)
	}
	w.Close()

	r := NewGolumbDecoder(&b, 5)

	for i := 0; i < n; i++ {
		test, err := r.Read()
		if err != nil {
			t.Fail()
		}
		if test != inta[i] {
			t.Fail()
		}
	}

	_, err := r.Read()
	if err != io.EOF {
		t.Fail()
	}

}

func TestGolumbSlice(t *testing.T) {
	n := 1000

	inta := make([]int, 0, n)

	for i := 0; i < n; i++ {
		num := int(rand.Int() % 10000)
		inta = append(inta, num)
	}

	compressed := GolumbEncode(inta)

	newa, err := GolumbDecode(compressed)

	if err != nil {
		t.Fail()
	}

	sort.Ints(inta)

	for i := 0; i < n; i++ {
		if newa[i] != inta[i] {
			t.Fail()
		}
	}
}
