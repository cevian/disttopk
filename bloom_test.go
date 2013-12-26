package disttopk

import "testing"
import "math/rand"

func TestBloom(t *testing.T) {
	n := 100
	eps := 0.000001
	nTest := 10 * int(1/eps)

	m := EstimateMSimple(n, eps)

	b := NewBloomSimpleEst(m, n)

	println("nTest = ", nTest, "n", n, "m", m, "k", b.Hashes)

	member := make(map[int]bool)

	for i := 0; i < n; i++ {
		j := rand.Int()
		member[j] = true
		b.AddInt(j)
	}

	fp := 0
	for i := 0; i < nTest; i++ {
		j := rand.Int()
		is_member, _ := member[j]
		if !is_member && b.QueryInt(j) == true {
			fp += 1
		}
	}

	fp_rate := float64(fp) / float64(nTest)
	println("FP rate = ", fp_rate, "Expected", eps)

}

func TestBloomSerialize(t *testing.T) {
	n := 100
	eps := 0.000001
	m := EstimateMSimple(n, eps)
	bloom := NewBloomSimpleEst(m, n)

	for i := 0; i < n; i++ {
		j := rand.Int()
		bloom.AddInt(j)
	}

	b, err := SerializeObject(bloom)

	if err != nil {
		panic(err)
	}

	if len(b) != bloom.ByteSize() {
		t.Error("Wrong len,", len(b), bloom.ByteSize())
	}

	var obj Bloom

	err = DeserializeObject(&obj, b)

	if err != nil {
		panic(err)
	}

	if !bloom.Equal(&obj) {
		t.Fail()
	}
}

/*
func TestBloomSerialize(t *testing.T) {
	n := 100
	eps := 0.000001
	m := EstimateMSimple(n, eps)
	bloom := NewBloomSimpleEst(m, n)

	//println("nTest = ", nTest, "n", n, "m", m, "k", b.Hashes)

	//member := make(map[int]bool)

	for i := 0; i < n; i++ {
		j := rand.Int()
		//member[j] = true
		bloom.AddInt(j)
	}

	b, err := GobBytesEncode(bloom)

	if err != nil {
		panic(err)
	}

	var obj Bloom

	err = GobBytesDecode(&obj, b)

	if err != nil {
		panic(err)
	}

}*/
