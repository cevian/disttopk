package disttopk

import (
	"compress/gzip"
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

type WcFileSourceAdaptor struct {
	KeyOnClient bool
}

func (this *WcFileSourceAdaptor) CacheFileNameSuffix() string {
	s := ".wc"
	if this.KeyOnClient {
		s += ".client"
	} else {
		s += ".object"
	}
	return s
}

func (this *WcFileSourceAdaptor) FillMapFromFile(filename string, m map[uint32]map[int]float64) {

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
		s := uint32(r.Server)
		mi, ok := m[s]
		if !ok {
			m[s] = make(map[int]float64)
			mi = m[s]
		}
		if this.KeyOnClient {
			mi[int(r.ClientID)] += 1
		} else {
			mi[int(r.ObjectID)] += 1
		}
	}

}
