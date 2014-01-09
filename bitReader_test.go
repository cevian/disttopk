package disttopk

import "testing"
import (
	"bytes"
	"fmt"
)

func TestSimpleEncod(t *testing.T) {

	buf := new(bytes.Buffer)
	bw := NewBitWriter(buf)
	if err := bw.AddBits(uint(6403), uint(15)); err != nil {
		panic(err)
	}
	bw.Close(true)

	b := buf.Bytes()

	fmt.Println("bytes", b)

	bufr := bytes.NewReader(b)
	br := NewBitReader(bufr)
	index, err := br.ReadBits64(uint(15))

	if err != nil {
		panic(err)
	}

	if index != 6403 {
		t.Fail()
	}

}
