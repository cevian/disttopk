package disttopk

import (
	"encoding/binary"
	"fmt"
	"os"
)

var _ = fmt.Println

type Request struct {
	Timestamp uint32
	ClientID  uint32
	ObjectID  uint32
	Size      uint32
	Method    uint8
	Status    uint8
	TypeName  uint8
	Server    uint8
}

func ReadWCFile(filename string) []ItemList {
	file, err := os.Open(filename) // For read access.
	if err != nil {
		panic(err)
	}

	m := make(map[uint8]ItemList)
	for err == nil {
		r := &Request{}
		err = binary.Read(file, binary.LittleEndian, r)
		s := r.Server
		i := Item{int(r.ObjectID), 1}
		m[s] = append(m[s], i)
		//fmt.Println("here", *r)
	}
	il := make([]ItemList, 0, len(m))
	for _, v := range m {
		m := v.AddToMap(nil)
		l := MakeItemList(m)
		l.Sort()
		//fmt.Println(l)
		il = append(il, l)
	}
	return il
}
