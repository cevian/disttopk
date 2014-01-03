package disttopk

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
)

type FileSourceAdaptor interface {
	FillMapFromFile(filename string, m map[uint32]map[int]float64)
}

type FileSource struct {
	FileSourceAdaptor
}

func (this *FileSource) ReadFiles(fileglob string) []ItemList {
	files, err := filepath.Glob(fileglob)
	if err != nil {
		panic(err)
	}

	mn := make(map[uint32]map[int]float64)
	for _, f := range files {
		this.FillMapFromFile(f, mn)
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

func (this *FileSource) ReadFilesAndCache(fileglob string, cachefilename string) []ItemList {
	if _, err := os.Stat(cachefilename); os.IsNotExist(err) {
		fmt.Println("Generating cache", cachefilename)
		l := this.ReadFiles(fileglob)
		f, err := os.Create(cachefilename)
		if err != nil {
			panic(err)
		}
		enc := gob.NewEncoder(f)
		enc.Encode(l)
		f.Close()
		fmt.Println("Finished Generating cache", cachefilename)
	}
	f, err := os.Open(cachefilename)
	if err != nil {
		panic(err)
	}
	dec := gob.NewDecoder(f)
	var l []ItemList
	if err = dec.Decode(&l); err != nil {
		panic(err)
	}
	return l
}
