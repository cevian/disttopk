package disttopk

import "testing"
import "bytes"
import "math/rand"

func TestBit(t *testing.T) {
	var b bytes.Buffer
	n := 1000

	bita := make([]bool, 0, n)

	w := NewBitWriter(&b)

	for i := 0; i < n; i++ {
		num := uint(rand.Int() % 2)
		bit := (num == 1)
		bita = append(bita, bit)
		w.AddBits(num, 1)
	}
	w.Close(false)

	r := NewBitReader(&b)

	for i := 0; i < n; i++ {
		test, _ := r.ReadBit()
		if test != bita[i] {
			t.Fail()
		}
	}

}
