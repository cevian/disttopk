package disttopk

import (
	"encoding/binary"
	"fmt"
	"io"
)

type BloomHistogramKleeEntry interface {
	IsComplete() bool
	ByteSize() int

	Serialize(w io.Writer) error
	Deserialize(r io.Reader) error
	GetLowerBound() uint32 //needed for max_size_candidate_list
	GetUpperBound() uint32 //needed for max_size_candidate_list
	GetFreq() uint32       //needed for max_size_candidate_list
}

type BloomHistogramKleeEntryComplete struct {
	*BloomHistogramKleeEntryPartial
	filter BloomFilter
}

type BloomHistogramKleeEntryPartial struct {
	avg  uint32
	freq uint32
	max  uint32 //?the paper says no but then need this for getting max_size_candidate list
	min  uint32 //?
}

func (c *BloomHistogramKleeEntryComplete) GetInfo() string {
	return fmt.Sprintf("BloomKleeEntry: k %v, %+v", "k ", c.filter.NumberHashes(), c)
}

func (c *BloomHistogramKleeEntryComplete) IsComplete() bool {
	return true
}

func (c *BloomHistogramKleeEntryComplete) ByteSize() int {
	return c.filter.ByteSize() + 4 + 4 + 4 + 4
}

func (c *BloomHistogramKleeEntryPartial) GetUpperBound() uint32 {
	return c.max
}

func (c *BloomHistogramKleeEntryPartial) GetLowerBound() uint32 {
	return c.min
}
func (c *BloomHistogramKleeEntryPartial) GetFreq() uint32 {
	return c.freq
}

func (c *BloomHistogramKleeEntryPartial) IsComplete() bool {
	return false
}

func (c *BloomHistogramKleeEntryPartial) ByteSize() int {
	return 4 + 4
}

type KleeFilterAdaptor interface {
	CreateBloomEntryFilter(n int) (BloomFilter, float64)
	CreateBloomFilterToDeserialize() BloomFilter
}

type PlainKleeFilterAdaptor struct{}

func (p PlainKleeFilterAdaptor) CreateBloomEntryFilter(n int) (BloomFilter, float64) {
	eps := 0.004
	m := EstimateMSimple(n, eps)
	//fmt.Println("create bloom entry filter", n, eps, m)
	entry := NewBloomSimpleEst(m, n)
	return entry, eps
}

func (p PlainKleeFilterAdaptor) CreateBloomFilterToDeserialize() BloomFilter {
	return &Bloom{}
}

type BloomHistogramKlee struct {
	KleeFilterAdaptor
	Data    []BloomHistogramKleeEntry
	c_index uint32
}

func NewBloomHistogramKlee() *BloomHistogramKlee {
	return &BloomHistogramKlee{PlainKleeFilterAdaptor{}, nil, 0}
}

func (b *BloomHistogramKlee) ByteSize() int {
	sz := 0
	for _, v := range b.Data {
		sz += v.ByteSize()
	}
	return sz + 4
}

func (b *BloomHistogramKlee) Len() int {
	return len(b.Data)
}

func (c *BloomHistogramKlee) GetInfo() string {
	//ret := fmt.Sprintf("BloomSketch: buckets", len(c.Data))

	ret := ""
	ret = fmt.Sprintln("bloom histogram klee", len(c.Data), " c idx ", c.c_index)
	/*
		for _, v := range c.Data {
			ret += v.GetInfo()
		}*/
	return ret

}

func (b *BloomHistogramKlee) CreateFromList(list ItemList, c float64) {
	max := uint32(list[0].Score)
	min := uint32(list[len(list)-1].Score)
	rang := max - min

	n := uint32(100) //I have no idea how the pick this. the paper says typically <100

	incr := rang / n
	if incr < 1 {
		incr = 1
	}

	//fmt.Println("overall", max, min, incr)

	total_sum := uint32(0)
	for _, item := range list {
		total_sum += uint32(item.Score)
	}

	cutoff_sum := uint32(float64(total_sum) * c)

	b.Data = make([]BloomHistogramKleeEntry, 0)
	start_index := 0
	before_cutoff := true
	cum_sum := uint32(0)
	for start_index < len(list) {
		start_max := uint32(list[start_index].Score)
		minscore := uint32(0)
		if start_max > incr { //prevent underflo
			minscore = start_max - incr
		}
		last_index := start_index
		sum := uint32(0)
		for i, item := range list[start_index:] {
			if uint32(item.Score) > minscore {
				last_index = i + start_index
				sum += uint32(item.Score)
			} else {
				break
			}
		}
		cum_sum += sum

		num_items := (last_index - start_index) + 1
		//fmt.Println("cum_sum", cum_sum, sum, start_index, last_index, start_max, incr, minscore, rang, num_items)
		//fmt.Println("range", start_max, minscore, num_items, last_index, start_index)

		var entry BloomHistogramKleeEntry
		if before_cutoff {
			var filter BloomFilter
			filter, _ = b.CreateBloomEntryFilter(num_items)
			for _, item := range list[start_index : last_index+1] {
				filter.Add(IntKeyToByteKey(item.Id))
			}
			partialentry := &BloomHistogramKleeEntryPartial{sum / uint32(num_items), uint32(num_items), uint32(list[start_index].Score), uint32(list[last_index].Score)}
			entry = &BloomHistogramKleeEntryComplete{partialentry, filter}
			//fmt.Printf("Putting in entry %+v\n", entry.(*BloomHistogramKleeEntryComplete).BloomHistogramKleeEntryPartial)
		} else {
			entry = &BloomHistogramKleeEntryPartial{sum / uint32(num_items), uint32(num_items), uint32(list[start_index].Score), uint32(list[last_index].Score)}
			//fmt.Printf("Putting in entry %+v\n", entry.(*BloomHistogramKleeEntryPartial))
		}

		if entry.GetFreq() == 0 {
			fmt.Println("cum_sum", cum_sum, sum, start_index, last_index, start_max, incr, minscore, rang, num_items)
			panic("snh")
		}

		b.Data = append(b.Data, entry)

		start_index = last_index + 1

		if before_cutoff {
			b.c_index = uint32(len(b.Data) - 1)
		}

		//fmt.Println("Cuttoff_sum", cutoff_sum, "cum sum", cum_sum, "total_sum", total_sum, incr)

		if cum_sum > cutoff_sum {
			before_cutoff = false
		}
	}
	//fmt.Println("Cindex", b.c_index, "len", len(b.Data), cutoff_sum, total_sum, cum_sum)
}

func (b *BloomHistogramKlee) GetPartialAvg() uint32 {
	cum_freq := uint32(0)
	cum_sum := uint32(0)
	if int(b.c_index) == len(b.Data)-1 {
		//this can happen if almost all the mass is in the last histo cell. That means the (0.1) * mass point can be there too
		return 0
	}

	for _, entry := range b.Data[b.c_index+1:] {
		partial := entry.(*BloomHistogramKleeEntryPartial)
		cum_freq += partial.freq
		cum_sum += (partial.freq * partial.avg)
	}
	if cum_freq == 0 {
		fmt.Println("Error", cum_freq, cum_sum, len(b.Data), b.c_index)
	}

	return uint32(cum_sum / cum_freq)
}

func (b *BloomHistogramKlee) MaxSizeCandidateList(thresh uint32) uint32 {
	size := uint32(0)
	for _, entry := range b.Data {
		//fmt.Printf("In mscl %v %v %v %+v\n", entry.GetLowerBound(), entry.GetFreq(), thresh, entry)
		if entry.GetLowerBound() < thresh {
			return size
		}
		size += entry.GetFreq()
	}

	return size
}

func (b *BloomHistogramKlee) GetUpperBoundByIndex(idx int) uint32 {
	return b.Data[idx].GetUpperBound()
}

func (s *BloomHistogramKlee) HistoCellIndex(value uint32) int {
	for k, entry := range s.Data {
		if entry.GetUpperBound() >= value && entry.GetLowerBound() <= value {
			return k
		}
	}
	panic(fmt.Sprintln("Shouldn't be here", value))
	//return -1
}

func (s *BloomHistogramKlee) Query(key []byte) uint32 {
	for _, entry := range s.Data[:s.c_index+1] {
		complete := entry.(*BloomHistogramKleeEntryComplete)
		if complete.filter.Query(key) {
			return complete.avg
		}
	}
	return s.GetPartialAvg()
}

/////////////////////////serialization stuff//////////////////////

func (p *BloomHistogramKleeEntryComplete) Serialize(w io.Writer) error {
	if err := p.filter.Serialize(w); err != nil {
		return err
	}

	if err := p.BloomHistogramKleeEntryPartial.Serialize(w); err != nil {
		return err
	}
	return nil
}

func (p *BloomHistogramKleeEntryComplete) Deserialize(r io.Reader) error {
	if p.filter == nil {
		panic("Have to initialize filter beforehand")
	}
	if err := p.filter.Deserialize(r); err != nil {
		return err
	}

	p.BloomHistogramKleeEntryPartial = &BloomHistogramKleeEntryPartial{}
	return p.BloomHistogramKleeEntryPartial.Deserialize(r)
}

func (p *BloomHistogramKleeEntryPartial) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, &p.avg); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &p.freq); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &p.max); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &p.min); err != nil {
		return err
	}

	return nil
}

func (p *BloomHistogramKleeEntryPartial) Deserialize(r io.Reader) error {
	if err := binary.Read(r, binary.BigEndian, &p.avg); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &p.freq); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &p.max); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &p.min); err != nil {
		return err
	}

	return nil
}

func (p *BloomHistogramKlee) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, &p.c_index); err != nil {
		return err
	}
	datal := uint32(len(p.Data))
	if err := binary.Write(w, binary.BigEndian, &datal); err != nil {
		return err
	}

	for _, v := range p.Data {
		if err := v.Serialize(w); err != nil {
			return err
		}
	}

	return nil
}

func (p *BloomHistogramKlee) Deserialize(r io.Reader) error {
	if err := binary.Read(r, binary.BigEndian, &p.c_index); err != nil {
		return err
	}

	filter := PlainKleeFilterAdaptor{}
	p.KleeFilterAdaptor = filter

	datal := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &datal); err != nil {
		return err
	}
	p.Data = make([]BloomHistogramKleeEntry, datal)
	for i := uint32(0); i < datal; i++ {
		var entry BloomHistogramKleeEntry
		if i <= p.c_index {
			entry = &BloomHistogramKleeEntryComplete{filter: p.CreateBloomFilterToDeserialize()}
		} else {
			entry = &BloomHistogramKleeEntryPartial{}
		}
		if err := entry.Deserialize(r); err != nil {
			return err
		}
		p.Data[i] = entry
	}

	return nil
}
