package disttopk

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
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
	files, err := filepath.Glob(filename)
	if err != nil {
		panic(err)
	}

	mn := make(map[uint8]map[int]float64)
	for _, f := range files {
		enterWCFile(f, mn)
	}

	il := make([]ItemList, 0, len(mn))
	for _, v := range mn {
		//m := v.AddToMap(nil)
		l := MakeItemList(v)
		l.Sort()
		//fmt.Println(l)
		il = append(il, l)
	}
	return il
}

func enterWCFile(filename string, m map[uint8]map[int]float64) {
	fmt.Println("Processing file", filename)
	file, err := os.Open(filename) // For read access.
	if err != nil {
		panic(err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		//return
		panic(fmt.Sprintln(err, filename))
	}
	defer gz.Close()

	for err == nil {
		r := &Request{}
		err = binary.Read(gz, binary.LittleEndian, r)
		s := r.Server
		mi, ok := m[s]
		if !ok {
			m[s] = make(map[int]float64)
			mi = m[s]
		}
		//mi[int(r.ObjectID)] += 1
		mi[int(r.ClientID)] += 1
	}

}
