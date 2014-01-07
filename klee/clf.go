package klee

import (
	"fmt"
	"io"
)
import "github.com/cevian/disttopk"

var _ = fmt.Println

type ClfRow struct {
	*disttopk.CountMinHash
	Data *disttopk.CountArray
}

func NewClfRow(size int) *ClfRow {
	s := ClfRow{
		disttopk.NewCountMinHash(1, size),
		disttopk.NewCountArray(size),
	}
	return &s
}

func (b *ClfRow) CreateNew() *ClfRow {
	return NewClfRow(b.Data.Len())
}

func (b *ClfRow) ByteSize() int {
	return (b.Data.Len() * 4) + 4
}

func (b *ClfRow) GetIndex(id []byte) uint32 {
	return b.GetIndexNoOffset(id, 0)
}

func (b *ClfRow) Add(id []byte, histo_cell uint32) {
	store_histo_cell := histo_cell + 1
	index := b.GetIndexNoOffset(id, 0)
	old_val := uint32(b.Data.Get(int(index)))
	if old_val == 0 || old_val > store_histo_cell {
		b.Data.Set(int(index), uint(store_histo_cell))
	}
}

func (t *ClfRow) HasHistoCellIndex(idx int) bool {
	return t.Data.Get(idx) != 0
}

func (s *ClfRow) QueryHistoCellIndex(idx int) uint32 {
	return uint32(s.Data.Get(idx)) - 1
}

func (t *ClfRow) HasHistoCell(key []byte) bool {
	index := int(t.GetIndexNoOffset(key, 0))
	return t.Data.Get(index) != 0
}

func (s *ClfRow) QueryHistoCell(key []byte) uint32 {
	index := int(s.GetIndexNoOffset(key, 0))
	return uint32(s.Data.Get(index)) - 1
}

func (p *ClfRow) Serialize(w io.Writer) error {
	if err := p.CountMinHash.Serialize(w); err != nil {
		return err
	}
	return p.Data.Serialize(w)
}

func (p *ClfRow) Deserialize(r io.Reader) error {
	p.CountMinHash = &disttopk.CountMinHash{}
	if err := p.CountMinHash.Deserialize(r); err != nil {
		return err
	}
	p.Data = &disttopk.CountArray{}
	return p.Data.Deserialize(r)

}

/*func (p *ClfRow) Equal(obj *ClfRow) bool {
	return p.CountMinHash.Equal(obj.CountMinHash) && p.Data.Equal(obj.Data)
}*/
