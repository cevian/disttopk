package disttopk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type BitArray struct {
	data     []byte
	num_bits uint
}

func NewBitArray(num_bits uint) *BitArray {
	n_bytes := getNumBytes(num_bits)
	return &BitArray{make([]byte, n_bytes), uint(num_bits)}
}

func getNumBytes(num_bits uint) uint {
	n_bytes := uint(0)
	if num_bits%8 == 0 {
		n_bytes = num_bits / 8
	} else {
		n_bytes = (num_bits / 8) + 1
	}
	return n_bytes
}

func (b *BitArray) NumBits() uint {
	return b.num_bits
}

func (b *BitArray) ByteSize() int {
	return len(b.data) + 4
}

func (t *BitArray) CountSetBit() uint {
	c := uint(0)
	for _, b := range t.data {
		c += t.countSetBitByte(b)
	}
	return c
}

func (t *BitArray) countSetBitByte(v byte) uint {
	c := uint(0) // c accumulates the total bits set in v
	for ; v > 0; c++ {
		v &= v - 1 // clear the least significant bit set
	}
	return c
}
func (b *BitArray) Set(idx uint) {
	old := b.data[idx/8]
	mask := byte(1 << (idx % 8))
	b.data[idx/8] = (old | mask)
}

func (b *BitArray) Clear(idx uint) {
	old := b.data[idx/8]
	mask := byte(1 << (idx % 8))
	b.data[idx/8] = (old &^ mask)
}

func (b *BitArray) SetValue(idx uint, v bool) {
	if v {
		b.Set(idx)
	} else {
		b.Clear(idx)
	}
}

func (b *BitArray) Check(idx uint) bool {
	old := b.data[idx/8]
	mask := byte(1 << (idx % 8))
	v := (old & mask)
	return v != 0
}

func (b *BitArray) Serialize(w io.Writer) error {
	var n uint32 = uint32(b.num_bits)
	if err := binary.Write(w, binary.BigEndian, &n); err != nil {
		return err
	}
	if _, err := w.Write(b.data); err != nil {
		return err
	}
	return nil
}

func (b *BitArray) Deserialize(r io.Reader) error {
	var num_bits uint32 = uint32(0)
	if err := binary.Read(r, binary.BigEndian, &num_bits); err != nil {
		return err
	}
	b.num_bits = uint(num_bits)
	num_bytes := getNumBytes(uint(num_bits))
	b.data = make([]byte, num_bytes)
	if wrote, err := r.Read(b.data); err != nil || wrote != int(num_bytes) {
		if err == nil {
			return errors.New(fmt.Sprintln("Less bytes than expected", wrote, num_bytes))
		}
		return err
	}
	return nil
}

func (b *BitArray) MergeOr(arg *BitArray) {
	if b.num_bits != arg.num_bits {
		panic("Must have same num bits to merge")
	}

	for k, _ := range b.data {
		b.data[k] |= arg.data[k]
	}
}

func (b *BitArray) Equal(arg *BitArray) bool {
	if b.num_bits != arg.num_bits {
		return false
	}

	if len(b.data) != len(arg.data) {
		return false
	}

	for k, v := range b.data {
		if arg.data[k] != v {
			return false
		}
	}
	return true
}
