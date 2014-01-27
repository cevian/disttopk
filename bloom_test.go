package disttopk

import "testing"
import "math/rand"
import "math"
import "fmt"

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

func TestBloomVsBloomGcsSize(t *testing.T) {
	err_sum := 0.0
	abs_err_sum := 0.0
	for _, eps := range []float64{0.1, 0.01, 0.001, 0.0001, 0.00001, 0.000001, 0.0000001} {
		ns := []int{1, 10, 100, 1000, 10000, 100000}
		if eps < 0.0001 {
			ns = []int{1, 10, 100, 1000}
		}

		for _, n := range ns {
			serialized, compressed := RunBloomGcs(eps, n)
			expected := GetExpectedBloomGcsSize(eps, n)
			err := float64(expected-(compressed-11)) / float64(expected)
			abs_err := math.Abs(float64(expected - (compressed - 11)))
			err_sum += math.Abs(err)
			abs_err_sum += abs_err
			t.Log(eps, n, "Serialized", serialized, "Compressed", compressed, "Compressed No Overhead", compressed-11, "Expected", expected, "diff", float64(expected-(compressed-11))/float64(expected))
		}
	}
	fmt.Println("error sum", err_sum, abs_err_sum)
}

func GetExpectedBloomGcsSize(eps float64, n int) int {
	//overhead is 4 in gcs 5 in golumb

	t := GetExpectedGcsSize(eps, n) + 4 + 5
	return int(t)
}

func GetExpectedGcsSize(eps float64, n int) int {
	// for k=opt : m = n *  1.44 * log_2(1/eps) = n * 1.44 * 1/ln(2) * ln (1/eps)
	return int(math.Ceil((float64(n) * 1.44 * 0.8 * math.Log2(1.0/eps)) / 8.0))
}

func RunBloomGcs(eps float64, n int) (int, int) {
	src := NewSimpleZipfSource(uint32(n), 0.7, 1)

	m := EstimateMGcs(n, eps)

	gcs := NewGcs(m)

	list := src.GetList()
	for k, item := range list {

		score := uint32(item.Score)
		//println("score", score, eps, n)
		if score == 0 {
			panic(fmt.Sprint(score, n, eps, k))
		}

		gcs.Add(IntKeyToByteKey(item.Id))
	}
	//println("Here", eps, n)

	ser, err := SerializeObject(gcs)
	if err != nil {
		panic(err)
	}
	comp := CompressBytes(ser)
	return len(ser), len(comp)
}
