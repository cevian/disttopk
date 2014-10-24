package disttopk

import (
	"encoding/binary"
	//	"fmt"
	"io"
	"math"
)

type CountArray struct {
	Data []uint32
}

func NewCountArray(size int) *CountArray {
	return &CountArray{make([]uint32, size)}
}

func (t *CountArray) Equal(other *CountArray) bool {
	if t.Len() != other.Len() {
		return false
	}
	for k, v := range t.Data {
		if v != other.Data[k] {
			return false
		}
	}
	return true
}

func (t *CountArray) CountNonZero() int {
	c := 0
	for _, v := range t.Data {
		if v > 0 {
			c += 1
		}
	}
	return c
}

func (t *CountArray) Len() int {
	return len(t.Data)
}

func (t *CountArray) Max() uint {
	max := t.Data[0]
	for _, v := range t.Data[1:] {
		if v > max {
			max = v
		}
	}
	return uint(max)
}

func (t *CountArray) Min() uint {
	min := t.Data[0]
	for _, v := range t.Data[1:] {
		if v < min {
			min = v
		}
	}
	return uint(min)
}

func (t *CountArray) MinNonZero() uint {
	min := uint32(0)
	for _, v := range t.Data {
		if (v != 0 && v < min) || min == 0 {
			min = v
		}
	}
	return uint(min)
}

func (t *CountArray) Set(idx int, value uint) {
	t.Data[idx] = uint32(value)
}

func (t *CountArray) Get(idx int) uint {
	return uint(t.Data[idx])
}

func (t *CountArray) GetValueBits() uint8 {
	max := uint32(0)
	for _, v := range t.Data {
		if v > max {
			max = v
		}
	}
	bits := uint8(math.Floor(math.Log2(float64(max)))) + 1
	return bits
}

func (t *CountArray) Serialize(w io.Writer) error {
	return t.SerializeBits(w, t.GetValueBits())
	//return t.SerializeSimple(w)
}

func (t *CountArray) Deserialize(r io.Reader) error {
	return t.DeserializeBits(r)
	//return t.DeserializeSimple(r)
}

func (t *CountArray) SerializeSimple(w io.Writer) error {
	length := uint32(len(t.Data))
	if err := binary.Write(w, binary.BigEndian, &length); err != nil {
		return err
	}
	for _, v := range t.Data {
		if err := binary.Write(w, binary.BigEndian, &v); err != nil {
			return err
		}
	}
	return nil
}

func (t *CountArray) DeserializeSimple(r io.Reader) error {
	length := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return err
	}

	t.Data = make([]uint32, length)

	for k, _ := range t.Data {
		val := uint32(0)
		if err := binary.Read(r, binary.BigEndian, &val); err != nil {
			return err
		}

		t.Data[k] = val
	}
	return nil
}

func (t *CountArray) SerializeBits(w io.Writer, bits uint8) error {
	length := uint32(len(t.Data))
	if err := binary.Write(w, binary.BigEndian, &length); err != nil {
		return err
	}

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

func (t *CountArray) DeserializeBits(r io.Reader) error {
	length := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return err
	}
	bits := uint8(0)
	if err := binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}

	t.Data = make([]uint32, length)

	br := NewBitReaderSize(r, int(length))
	for k, _ := range t.Data {
		val, err := br.ReadBits64(uint(bits))
		if err != nil {
			return err
		}
		t.Data[k] = uint32(val)
	}
	return nil
}
func (orig *CountArray) LogNormalize(scale float64) {
	for i := 0; i < orig.Len(); i++ {
		v := orig.Get(i)
		if v > 0 {
			newval := uint(math.Ceil(math.Log(float64(v)) * scale))
			orig.Set(i, newval+1)
		}
	}
}

func (orig *CountArray) LogDenormalize(scale float64) {
	for i := 0; i < orig.Len(); i++ {
		v := orig.Get(i)
		if v > 0 {
			newval := uint(math.Exp(float64(v-1) / scale))
			orig.Set(i, newval)
		}
	}
}

////////////////////////////////////////////better compression///////////////////////////////////

/* The idea here is to use a Bag for high value cells to allow you to reduce the #bits/cell for the rest
   of the structure. So you end up writing out [array with less bits per cell][bag with index, value pairs for value that are bigger than can fit into the array]

   Turns out that when using huffman codes (like zlib or gzip) then this is ineffective. Huffman codes do almost as good a job without this level of complexity.
*/

func (orig *CountArray) subtractCountArray(min uint) {
	for i := 0; i < orig.Len(); i++ {
		v := orig.Get(i)
		if v > 0 {
			newval := uint(v - min)
			orig.Set(i, newval)
		}
	}
}

func (orig *CountArray) unsubtractCountArray(min uint) {
	for i := 0; i < orig.Len(); i++ {
		v := orig.Get(i)
		if v > 0 {
			newval := uint(v + min)
			orig.Set(i, newval)
		}
	}
}

func (orig *CountArray) getBagMapForCountArray(min uint) map[uint32]uint32 {
	bm := make(map[uint32]uint32)
	for i := 0; i < orig.Len(); i++ {
		v := orig.Get(i)
		if v > min {
			bm[uint32(i)] = uint32(v)
			//fmt.Println("ser idx ", i, v)
		}
	}
	//fmt.Println("ser len ", len(bm), orig.Len())
	return bm
}

func (orig *CountArray) integrateBag(bm map[uint32]uint32) {
	//fmt.Println("len ", len(bm), orig.Len())
	for index, value := range bm {
		//fmt.Println("idx ", index, value)
		orig.Set(int(index), uint(value))
	}
}

func (orig_do_not_change *CountArray) SerializeWithBag(w io.Writer) error {
	new_data := make([]uint32, orig_do_not_change.Len())
	copy(new_data, orig_do_not_change.Data)
	orig := &CountArray{new_data}

	//orig.transformLog()
	max := orig.Max()
	min := orig.MinNonZero()

	rang := max - min

	fullRangeBits := uint8(math.Floor(math.Log2(float64(rang))) + 1)
	keyBits := uint8(math.Ceil(math.Log2(float64(orig.Len()))))
	NonBagBits := fullRangeBits

	cdata := make([]uint32, orig.Len())
	copy(cdata, orig.Data)
	data := IntSlice(cdata)
	data.Sort()

	for NonBagBits > 0 {
		proposeBits := NonBagBits - 1
		proposeMax := uint32(math.Exp2(float64(proposeBits))-1) + uint32(min-1)
		numberItemsLeftOut := data.ItemsMoreThan(proposeMax)

		bagBits := numberItemsLeftOut * uint32(keyBits) * uint32(fullRangeBits) //store index into array and 4 bytes for value (32 bits)
		if bagBits < (uint32(fullRangeBits-proposeBits) * uint32(orig.Len())) {
			NonBagBits = proposeBits //accept proposal
		} else {
			break
		}
	}

	//maxValue := uint32(math.Exp2(float64(NonBagBits))-1) + uint32(min-1)
	//fmt.Println("bits: ", fullRangeBits, NonBagBits, data.ItemsMoreThan(maxValue), data.ItemsMoreThan(maxValue)*uint32(keyBits)*uint32(fullRangeBits))
	//NoBagBits is right now

	orig.subtractCountArray(min - 1)

	orig.SerializeBits(w, NonBagBits)

	min_write := uint32(min)
	if err := binary.Write(w, binary.BigEndian, &min_write); err != nil {
		return err
	}

	MaxNotInBag := uint32(math.Exp2(float64(NonBagBits)) - 1)

	bag_map := orig.getBagMapForCountArray(uint(MaxNotInBag))
	bag_len := uint32(len(bag_map))
	if err := binary.Write(w, binary.BigEndian, &bag_len); err != nil {
		return err
	}

	if bag_len > 0 {
		if err := binary.Write(w, binary.BigEndian, &keyBits); err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, &fullRangeBits); err != nil {
			return err
		}
		bw := NewBitWriter(w)
		for k, v := range bag_map {
			if err := bw.AddBits(uint(k), uint(keyBits)); err != nil {
				return err
			}
			if err := bw.AddBits(uint(v), uint(fullRangeBits)); err != nil {
				return err
			}
		}
		bw.Close(true)
	}
	return nil
}

func (ca *CountArray) DeserializeWithBag(r io.Reader) error {
	ca.Deserialize(r)

	min := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &min); err != nil {
		return err
	}

	bag_len := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &bag_len); err != nil {
		return err
	}

	if bag_len > 0 {

		keyBits := uint8(0)
		if err := binary.Read(r, binary.BigEndian, &keyBits); err != nil {
			return err
		}
		fullRangeBits := uint8(0)
		if err := binary.Read(r, binary.BigEndian, &fullRangeBits); err != nil {
			return err
		}

		bagMap := make(map[uint32]uint32)
		br := NewBitReader(r)
		for i := uint32(0); i < bag_len; i++ {
			index, err := br.ReadBits64(uint(keyBits))
			if err != nil {
				return err
			}
			value, err := br.ReadBits64(uint(fullRangeBits))
			if err != nil {
				return err
			}
			bagMap[uint32(index)] = uint32(value)
		}
		ca.integrateBag(bagMap)
	}
	ca.unsubtractCountArray(uint(min - 1))
	//ca.untransformLog()
	return nil

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////GCS SERIALIZE//////////////////////////

func (t *CountArray) SerializeGcs(w io.Writer) error {
	return t.SerializeBitsGcs(w, t.GetValueBits())
}

func (t *CountArray) SerializeBitsGcs(w io.Writer, bits uint8) error {
	length := uint32(len(t.Data))
	if err := binary.Write(w, binary.BigEndian, &length); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, &bits); err != nil {
		return err
	}

	nonzero := uint32(t.CountNonZero())
	if err := binary.Write(w, binary.BigEndian, &nonzero); err != nil {
		return err
	}

	bw := NewBitWriter(w)
	cellIndexes := make([]int, 0, nonzero)
	for cellIndex, v := range t.Data {
		if v > 0 {
			if err := bw.AddBits(uint(v), uint(bits)); err != nil {
				return err
			}
			cellIndexes = append(cellIndexes, cellIndex)
		}
	}
	if err := bw.Close(true); err != nil {
		return err
	}

	//println("Cellindexes", len(cellIndexes))
	return GolumbEncodeWriter(w, cellIndexes)
}

/*
func (t *CountArray) SerializeHashArray(w io.Writer) error {
	nonzero := uint32(t.CountNonZero())
	cellIndexes := make([]int, 0, nonzero)
	for cellIndex, v := range t.Data {
		if v > 0 {
			cellIndexes = append(cellIndexes, cellIndex)
		}
	}

	return GolumbEncodeWriter(w, cellIndexes)
}*/

func (t *CountArray) DeserializeGcs(r io.Reader) error {
	length := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return err
	}
	bits := uint8(0)
	if err := binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	nonzero := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &nonzero); err != nil {
		return err
	}

	t.Data = make([]uint32, length)

	values := make([]uint32, nonzero)
	br := NewBitReader(r)
	for k, _ := range values {
		val, err := br.ReadBits64(uint(bits))
		if err != nil {
			return err
		}
		values[k] = uint32(val)
	}

	cellIndexes, err := GolumbDecodeReader(r)
	if err != nil {
		return err
	}

	for k, cellIndex := range cellIndexes {
		t.Data[cellIndex] = values[k]
	}

	return nil
}
