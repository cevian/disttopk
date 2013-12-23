package disttopk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

var _ = fmt.Println
var start_offset = int(0)

type GolumbEncoder struct {
	w      *BitWriter
	m      uint
	m_log  uint //log2 of m
	m_mask uint
}

func NewGolumbEncoder(w io.Writer, m_bits uint) *GolumbEncoder {
	if m_bits < 1 || m_bits > 63 {
		panic("m_bits needs to be between 1 and 63")
	}

	m := uint(1 << m_bits)
	m_mask := m - 1 // if m = 8 => 7 => 0111
	bw := NewBitWriter(w)
	return &GolumbEncoder{bw, m, m_bits, m_mask}
}

func (g *GolumbEncoder) Write(i uint) error {
	q := i / g.m
	g.w.AddOneBits(q)
	g.w.AddBit(false)

	//assume M is a power of 2
	remainder := i & g.m_mask
	remainder_bits := g.m_log
	return g.w.AddBits(remainder, remainder_bits)
}

//the end is padded with 1s
func (g *GolumbEncoder) Close() error {
	return g.w.Close(true)
}

type GolumbDecoder struct {
	r     *BitReader
	m     uint
	m_log uint
}

func NewGolumbDecoder(r io.Reader, m_bits uint) *GolumbDecoder {
	if m_bits < 1 || m_bits > 63 {
		panic("m_bits needs to be between 1 and 63")
	}
	m := uint(1 << m_bits)
	br := NewBitReader(r)
	return &GolumbDecoder{br, m, m_bits}

}

func (g *GolumbDecoder) Read() (uint, error) {
	for {
		q := uint(0)
		var err error
		var bit bool
		for bit, err = g.r.ReadBit(); bit && err == nil; bit, err = g.r.ReadBit() {
			q++
		}
		if err != nil { // this may happen if fallen off the end
			return 0, err
		}
		remainder, err := g.r.ReadBits64(g.m_log)
		if err != nil {
			return 0, err
		}

		return (q * g.m) + uint(remainder), nil

	}
}

func GolumbEncode(unsorted []int) []byte {
	sort.Ints(unsorted)
	return GolumbEncodeSorted(unsorted)
}

func GolumbParameter(sum uint, num_samples uint) uint {
	a := uint(49)
	b := uint(128)
	k := uint(0)
	for ; ((num_samples * b) << k) <= b*sum+num_samples*a; k++ {

	}
	return k
}

func GolumbEncodeSorted(sorted []int) []byte {
	if sorted[len(sorted)-1] < start_offset {
		panic("Illegal input")
	}
	sum_of_increments := uint((sorted[len(sorted)-1] - start_offset)) // (a1-start)+(a2-a1)+(a3-a2) = a3-start
	num_increments := uint(len(sorted))

	m_bits := GolumbParameter(sum_of_increments, num_increments)

	bytestream := &bytes.Buffer{}
	binary.Write(bytestream, binary.BigEndian, uint32(m_bits))
	egs := NewGolumbEncoder(bytestream, m_bits)

	prev := start_offset
	for _, i := range sorted {
		delta := i - prev
		prev = i
		egs.Write(uint(delta))
	}
	egs.Close()

	return bytestream.Bytes()
}

func GolumbDecode(compressed []byte) ([]int, error) {
	r := bytes.NewReader(compressed)
	var m_bits uint32
	err := binary.Read(r, binary.BigEndian, &m_bits)

	if err != nil {
		return nil, err
	}
	decoder := NewGolumbDecoder(r, uint(m_bits))

	val := start_offset
	res := make([]int, 0)
	for {
		n, err := decoder.Read()
		if err == nil {
			val = val + int(n)
			res = append(res, val)
		}
		if err != nil {
			if err == io.EOF {
				return res, nil
			}
			return res, err
		}
	}
}
