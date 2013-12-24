package disttopk

import (
	"bufio"
	"io"
)

// BitReader wraps an io.Reader and provides the ability to read values,
// bit-by-bit, from it. Its Read* methods don't return the usual error
// because the error handling was verbose. Instead, any error is kept and can
// be checked afterwards.
type BitReader struct {
	r    io.ByteReader
	n    uint64
	bits uint
	err  error
}

// newBitReader returns a new BitReader reading from r. If r is not
// already an io.ByteReader, it will be converted via a bufio.Reader.
func NewBitReader(r io.Reader) *BitReader {
	byter, ok := r.(io.ByteReader)
	if !ok {
		byter = bufio.NewReader(r)
	}
	return &BitReader{r: byter}
}

// ReadBits64 reads the given number of bits and returns them in the
// least-significant part of a uint64. In the event of an error, it returns 0
// and the error can be obtained by calling Err().
func (br *BitReader) ReadBits64(bits uint) (n uint64, err error) {
	for bits > br.bits {
		b, err := br.r.ReadByte()
		if err != nil {
			return 0, err
		}
		br.n <<= 8
		br.n |= uint64(b)
		br.bits += 8
	}

	// br.n looks like this (assuming that br.bits = 14 and bits = 6):
	// Bit: 111111
	//      5432109876543210
	//
	//         (6 bits, the desired output)
	//        |-----|
	//        V     V
	//      0101101101001110
	//        ^            ^
	//        |------------|
	//           br.bits (num valid bits)
	//
	// This the next line right shifts the desired bits into the
	// least-significant places and masks off anything above.
	n = (br.n >> (br.bits - bits)) & ((1 << bits) - 1)
	br.bits -= bits
	return n, nil
}

func (br *BitReader) ReadBits(bits uint) (int, error) {
	n64, err := br.ReadBits64(bits)
	return int(n64), err
}

func (br *BitReader) ReadBit() (bool, error) {
	n, err := br.ReadBits(1)
	return (n != 0), err
}

func (br *BitReader) TryReadBit() (bit byte, ok bool) {
	if br.bits > 0 {
		br.bits--
		return byte(br.n>>br.bits) & 1, true
	}
	return 0, false
}

func (br *BitReader) Err() error {
	return br.err
}
