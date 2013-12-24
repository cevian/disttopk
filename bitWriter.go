package disttopk

import (
	"bufio"
	"encoding/binary"
	"io"
)

const egWordBits = 64 //a lot of stuff depends on this being 64 #do not change

type BitWriter struct {
	data     uint64
	bitsleft uint
	out      byteWriter
	outbuf   []byte
}

//A Bit Writer has functions to write bits to a writer
func NewBitWriter(w io.Writer) *BitWriter {
	ww := makeWriter(w)
	return &BitWriter{0, egWordBits, ww, make([]byte, 8)}
}

func (s *BitWriter) AddBit(b bool) error {
	if b {
		return s.AddBits(1, 1)
	} else {
		return s.AddBits(0, 1)
	}
}

func (s *BitWriter) AddBits(bits uint, nbits uint) error {
	if nbits > egWordBits {
		panic("Cannot write more than a word at a time with the current code")
	}

	if nbits < s.bitsleft {
		s.data |= (uint64(bits) << (s.bitsleft - nbits))
		s.bitsleft -= nbits
		return nil
	} else {
		s.data |= uint64(bits >> (nbits - s.bitsleft))
		nbits -= s.bitsleft
		// This next line only matters in the future
		//bits &= ((1 << nbits)-1) // zero out the bits we just consumed
		if err := s.emitBits(); err != nil {
			return err
		}
	}

	// This code will never be executed when using 64 bit words.
	//for ; nbits > egWordBits; nbits -= egWordBits {
	//      s.data = uint64(bits >> (nbits - egWordBits))
	//      s.emitBits()
	//}
	s.data = uint64(bits) << (egWordBits - nbits)
	s.bitsleft = egWordBits - nbits
	return nil
}

func (s *BitWriter) AddOneBits(nones uint) error {
	for i := uint(0); i < nones; i++ {
		if err := s.AddBit(true); err != nil {
			return err
		}
	}
	return nil
}

func (s *BitWriter) AddZeroBits(nzeros uint) error {
	// Split into three chunks:  Number of zeros we can add
	// to the current byte;  number of intermediate zero bytes
	// we should emit;  number of zeros to add to the new byte
	// if any.
	if nzeros < s.bitsleft {
		s.bitsleft -= nzeros
		return nil
	} else {
		nzeros -= s.bitsleft
		if err := s.emitBits(); err != nil {
			return err
		}
	}
	// We now have a zero byte at bitpos 0.
	for ; nzeros >= egWordBits; nzeros -= egWordBits {
		if err := s.emitBits(); err != nil {
			return err
		}
	}
	s.bitsleft -= nzeros
	return nil
}

func (s *BitWriter) Close(padding_bit bool) error {
	if s.bitsleft != egWordBits {
		err := s.emitPartialBits(padding_bit)
		if err != nil {
			return err
		}
	}
	return s.out.Flush()
}

type byteWriter interface {
	io.Writer
	//WriteByte(c byte) error
	Flush() error
}

func makeWriter(w io.Writer) byteWriter {
	if ww, ok := w.(byteWriter); ok {
		return ww
	}
	return bufio.NewWriter(w)
}

func (s *BitWriter) emitPartialBits(padding_bit bool) error {
	bits_padding := s.bitsleft % 8
	for i := uint(0); i < bits_padding; i++ {
		s.AddBit(padding_bit)
	}

	var b [8]byte
	var bs = b[:8]
	// The slowness here makes me crave an optimized htonll function.
	binary.BigEndian.PutUint64(bs, s.data)
	nbytes := ((egWordBits - s.bitsleft) + 7) / 8
	if nbytes > 0 {
		_, err := s.out.Write(bs[:nbytes])
		if err != nil {
			return err
		}
	}
	s.data = 0
	s.bitsleft = egWordBits
	return nil
}

func (s *BitWriter) emitBits() error {
	// The overhead of allocating and freeing the outbuf slice
	// makes it worth pre-allocating in the struct.
	binary.BigEndian.PutUint64(s.outbuf, s.data)
	_, err := s.out.Write(s.outbuf)
	if err != nil {
		return err
	}
	s.data = 0
	s.bitsleft = egWordBits
	return nil
}
