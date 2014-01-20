package disttopk

import "testing"
import (
	"fmt"
	"math"
)

/*
func TestCountMinHashSerialize(t *testing.T) {
	hash := NewCountMinHash(2, 2)

	b, err := GobBytesEncode(hash)

	if err != nil {
		panic(err)
	}

	if len(b) != hash.ByteSize() {
		t.Error("len is", len(b), "but bytesize is", hash.ByteSize())
	}

	var obj CountMinHash

	err = GobBytesDecode(&obj, b)

	if err != nil {
		panic(err)
	}

	if obj.Columns != hash.Columns || obj.Hashes != hash.Hashes {
		t.Fail()
	}

	if len(obj.hasha) != hash.Hashes {
		t.Fail()
	}
}*/

func TestCountMinHashComp(t *testing.T) {
	err_sum := 0.0
	for _, eps := range []float64{0.1, 0.01, 0.001, 0.0001} {
		for _, n := range []int{1, 10, 100, 1000} {
			serialized, compressed, valueBits := RunCountMinHashComp(eps, n)
			expected := GetExpected(eps, n, valueBits)
			err := float64(expected-(compressed-11)) / float64(expected)
			err_sum += err
			t.Log(eps, n, "Serialized", serialized, "Compressed", compressed, "Compressed No Overhead", compressed-11, "Expected", expected, "diff", float64(expected-(compressed-11))/float64(expected))
		}
	}
}

func GetExpected(eps float64, n int, valueBits uint8) int {

	//overhead 8 in count min, 9 in count array
	hash_array := GetExpectedHashArray(eps, n) + 8 + 9
	values := GetExpectedValuesEncoding(n, valueBits)
	//println("Bytes", hash_array, values, math.Log2(1.0/eps))
	return int(hash_array + values)
}

func GetExpectedHashArray(eps float64, n int) int {
	// for k=opt : m = n *  1.44 * log_2(1/eps) = n * 1.44 * 1/ln(2) * ln (1/eps)

	return int(math.Ceil((float64(n) * 1.44 * 0.7 * math.Log2(1.0/eps)) / 8.0))
}

func GetExpectedValuesEncoding(n int, valueBits uint8) int {
	return int(math.Ceil((float64(n) * float64(valueBits)) / 8))
}

func RunCountMinHashComp(eps float64, n int) (int, int, uint8) {
	src := NewSimpleZipfSource(uint32(n), 0.7)
	// for k=1 : m  ~ n/eps
	hash := NewCountMinSketch(1, int(float64(n)/float64(eps)))

	list := src.GetList()
	for k, item := range list {

		score := uint32(item.Score)
		//println("score", score, eps, n)
		if score == 0 {
			panic(fmt.Sprint(score, n, eps, k))
		}

		//	println("score", uint32(item.Score))
		hash.Add(IntKeyToByteKey(item.Id), score)
		//hash.Add(IntKeyToByteKey(item.Id), 8)
	}
	//println("Here", eps, n)

	ser, err := SerializeObject(hash)
	if err != nil {
		panic(err)
	}
	comp := CompressBytes(ser)
	return len(ser), len(comp), hash.GetValueBits(0)
}

func RunCountMinHashCompHashArrayOnly(eps float64, n int) (int, int, uint8) {
	src := NewSimpleZipfSource(uint32(n), 0.7)
	// for k=1 : m  ~ n/eps
	hash := NewCountMinSketch(1, int(float64(n)/float64(eps)))

	list := src.GetList()
	for _, item := range list {
		hash.Add(IntKeyToByteKey(item.Id), uint32(item.Score))
		//hash.Add(IntKeyToByteKey(item.Id), 8)
	}

	ser, err := SerializeObject(hash)
	if err != nil {
		panic(err)
	}
	comp := CompressBytes(ser)
	return len(ser), len(comp), hash.GetValueBits(0)
}
