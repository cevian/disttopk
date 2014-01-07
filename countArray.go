package disttopk

import (
	"encoding/binary"
	"io"
	"math"
)

type CountArray struct {
	Data []uint32
}

func NewCountArray(size int) *CountArray {
	return &CountArray{make([]uint32, size)}
}

func (t *CountArray) Len() int {
	return len(t.Data)
}

func (t *CountArray) Set(idx int, value uint) {
	t.Data[idx] = uint32(value)
}

func (t *CountArray) Get(idx int) uint {
	return uint(t.Data[idx])
}

func (t *CountArray) Serialize(w io.Writer) error {
	length := uint32(len(t.Data))
	if err := binary.Write(w, binary.BigEndian, &length); err != nil {
		return err
	}

	max := uint32(0)
	for _, v := range t.Data {
		if v > max {
			max = v
		}
	}

	bits := uint8(math.Ceil(math.Log2(float64(max))))
	if err := binary.Write(w, binary.BigEndian, &bits); err != nil {
		return err
	}

	bw := NewBitWriter(w)
	for _, v := range t.Data {
		if err := bw.AddBits(uint(v), uint(bits)); err != nil {
			return err
		}
	}

	return bw.Close(true)
}

func (t *CountArray) Deserialize(r io.Reader) error {
	length := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return err
	}
	bits := uint8(0)
	if err := binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}

	t.Data = make([]uint32, length)

	br := NewBitReader(r)
	for k, _ := range t.Data {
		val, err := br.ReadBits64(uint(bits))
		if err != nil {
			return err
		}
		t.Data[k] = uint32(val)
	}
	return nil
}
