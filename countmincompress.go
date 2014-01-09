package disttopk

import (
	"sort"
)

/*
func TransformLog(orig *CountArray) {
	for i := 0; i < orig.Len(); i++ {
		v := orig.Get(i)
		if v > 0 {
			newval = uint32(math.Ceil(math.Log(float64(v)) * 10))
			orig.Set(i, newval)
		}
	}
}

func SubtractCountArray(orig *CountArray, uint min) {
	for i := 0; i < orig.Len(); i++ {
		v := orig.Get(i)
		if v > 0 {
			newval = uint32(v - min)
			orig.Set(i, newval)
		}
	}
}

func getBagMapForCountArray(orig *CountArray, min int) map[uint32]uint32 {
	bm := make(map[uint32]uint32)
	for i := 0; i < orig.Len(); i++ {
		v := orig.Get(i)
		if v > min {
			bm[i] = v
		}
	}
	return bm
}

func CompressCountArray(orig *CountArray, w io.Writer) error {
	TransformLog(orig)
	max := orig.Max()
	min := orig.Min()

	rang := max - min

	fullRangeBits := uint8(math.Ceil(math.Log2(float64(rang))))
	keyBits := uint8(math.Ceil(math.Log2(float64(len(data)))))
	NonBagBits := fullRangeBits

	cdata := make([]uint32, orig.Len())
	copy(cdata, orig.Data)
	data := IntSlice(cdata)
	data.Sort()

	for NonBagBits > 0 {
		proposeBits := NonBagBits - 1
		proposeMax := uint32(math.Exp2(float64(proposeBits))-1) + min
		numberItemsLeftOut := data.ItemsMoreThan(proposeMax)

		bagBits := numberItemsLeftOut * keyBits * fullRangeBits //store index into array and 4 bytes for value (32 bits)
		if bagBits < ((fullRangeBits - propose) * uint32(orig.Len())) {
			NoBagBits = propose //accept proposal
		} else {
			break
		}
	}

	//NoBagBits is right now

	SubtractCountArray(orig, min)

	orig.SerializeBits(w, NoBagBits)

	min_write := uint32(min)
	if err := binary.Write(w, binary.BigEndian, &min_write); err != nil {
		return err
	}

	MaxNotInBag := uint32(math.Exp2(float64(NoBagBits)) - 1)

	bag_map = getBagMapForCountArray(orig, MaxNotInBag)
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
	}
}

func DecompressCountArray(r io.Reader) (error, *CountArray) {
	ca := &CountArray{}
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

		bagMap = make(map[uint32]uint32)
		br := NewBitReader(r)
		for i := uint32(0); i < bag_len; i++ {
			index, err := br.ReadBits64(uint(keyBits))
			if err != nil {
				return err
			}
			value, err := br.ReadBits64(uint(keyBits))
			if err != nil {
				return err
			}
			bagMap[index] = value
		}
	}

}

type CMCompress struct {
	*CountMinHash
	data  []byte
	bag   map[string][]byte
	min   uint32
	cells int
}

func (c *CMCompress) ByteSize() int {
	size := 0
	for k, v := range c.bag {
		size += len([]byte(k)) + len(v)
	}
	size += len(c.data)
	size += 4 + 4
	return size
}

func Compress(orig *CountMinSketch) *CMCompress {
	for k, v := range orig.Data {
		if v > 0 {
			orig.Data[k] = uint32(math.Ceil(math.Log(float64(v)) * 10))
			//println(orig.Data[k])
		}
	}
	cdata := make([]uint32, len(orig.Data))
	copy(cdata, orig.Data)
	data := IntSlice(cdata)
	data.Sort()

	i := len(data) - 1
	for data[i] == 0 {
		i--
	}
	min := data[i]
	max := data[0]

	allBytes := uint32(math.Ceil(math.Ceil(math.Log2(float64(max-min))) / 8.0))
	useBytes := uint32(allBytes)
	keyBytes := uint32(math.Ceil(math.Ceil(math.Log2(float64(len(data)))) / 8.0))
	for useBytes > 0 {
		propose := useBytes - 1
		proposeMax := uint32(math.Exp2(float64(propose*8.0))-1) + min
		numberItemsLeftOut := data.ItemsMoreThan(proposeMax)

		bagBytes := numberItemsLeftOut * keyBytes * allBytes //store index into array and 4 bytes for value (32 bits)
		if bagBytes < ((allBytes - propose) * uint32(len(data))) {
			useBytes -= 1
		} else {
			break
		}

	}
	//println("After, ", min, max, allBytes, useBytes)

	MaxCM := uint32(math.Exp2(float64(useBytes*8.0))-1) + min

	comp := CMCompress{orig.CountMinHash, make([]byte, int(useBytes)*len(data)), make(map[string][]byte), min, len(orig.Data)}

	for index, value := range orig.Data {
		if value <= MaxCM {
			bIndex := uint32(index) * useBytes
			if value > 0 {
				bValue := value - min
				//println(bValue)
				writeValue(bValue, comp.data[bIndex:], useBytes)
			}
		} else {
			key := make([]byte, keyBytes)
			val := make([]byte, allBytes)
			writeValue(uint32(index), key, keyBytes)
			writeValue(value-min, val, allBytes)
			comp.bag[string(key)] = val
		}
	}

	return &comp
}

func Decompress(comp *CMCompress) *CountMinSketch {
	cells := uint32(comp.cells)
	useBytes := uint32(len(comp.data)) / cells

	data := make([]uint32, cells)

	//println("Decomp", cells, useBytes, len(comp.data), len(comp.bag), comp.min)

	for ki, _ := range data {
		k := uint32(ki)
		byteSlice := comp.data[k*useBytes : (k+1)*useBytes]
		val := readValue(byteSlice)
		if val > 0 {
			data[k] = val + comp.min
			//println(val)
		}
	}

	for sk, bval := range comp.bag {
		bk := []byte(sk)
		index := readValue(bk)
		val := readValue(bval)
		data[index] = val + comp.min
	}

	for k, v := range data {
		if v > 0 {
			data[k] = uint32(math.Exp(float64(v) / 10.0))
		}
	}

	return &CountMinSketch{comp.CountMinHash, data}
}

func writeValue(value uint32, data []byte, nBytes uint32) {
	if value > uint32(math.Exp2(float64(nBytes)*8.0)-1) {
		panic("Can't write value ")
	}
	//origValue := value
	for i := uint32(0); i < nBytes; i++ {
		data[i] = byte(value)
		value = value >> 8
	}
	/*check := readValue(data[:nBytes])
	if check != origValue {
		println("Value wrong", check, origValue, data[nBytes])
		panic("Value is not saved ok")
	}*/
/*}

func readValue(data []byte) uint32 {
	result := uint32(0)
	for k, v := range data {
		result |= uint32((uint32(v) << (uint32(k) * 8)))
	}
	return result
}*/

type IntSlice []uint32

func (p IntSlice) Len() int           { return len(p) }
func (p IntSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p IntSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Sort is a convenience method.
func (p IntSlice) Sort() { sort.Sort(sort.Reverse(p)) }
func (p IntSlice) ItemsMoreThan(x uint32) uint32 {
	i := uint32(0)
	for p[i] > x {
		i++
	}
	return i
}
