package disttopk

import "testing"
import "math/rand"

func TestBloom(t *testing.T) {
  n := 100
  eps := 0.000001
  nTest := 10* int(1/eps)
  println("nTest = ", nTest)

  m := EstimateMSimple(n, eps)

  b := NewBloomSimpleEst(m, n)

  member := make(map[int]bool)

  for i := 0; i<n; i++ {
        j := rand.Int()
        member[j] = true
	b.AddInt(j)
  }

  fp := 0
  for i:= 0; i<nTest; i++{
   j := rand.Int()
   is_member, _ := member[j]
   if !is_member && b.QueryInt(j) == true {
	fp += 1
	}
  }

  fp_rate := float64(fp)/float64(nTest)
  println("FP rate = ", fp_rate, "Expected", eps)


}
