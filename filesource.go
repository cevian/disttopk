package disttopk

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type FileSourceAdaptor interface {
	FillMapFromFile(filename string, m map[uint32]map[int]float64)
	CacheFileNameSuffix() string
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

	if len(files) == 0 {
		panic(fmt.Sprintln("Cannot find files in", fileglob))
	}

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

func (this *FileSource) ReadFilesAndCache(fileglob string, cachebase string) []ItemList {
	cachefilename := cachebase + this.CacheFileNameSuffix()
	if _, err := os.Stat(cachefilename); os.IsNotExist(err) {
		fmt.Println("Generating cache", cachefilename)
		l := this.ReadFiles(fileglob)
		f, err := os.Create(cachefilename)
		if err != nil {
			panic(err)
		}
		enc := gob.NewEncoder(f)
		for _, itemlist := range l {
			if err := enc.Encode(len(itemlist)); err != nil {
				panic(err)
			}
			for _, item := range itemlist {
				if err := enc.Encode(item.Id); err != nil {
					panic(err)
				}
				if err := enc.Encode(item.Score); err != nil {
					panic(err)
				}

			}
		}
		f.Close()
		fmt.Println("Finished Generating cache", cachefilename)
	}
	f, err := os.Open(cachefilename)
	if err != nil {
		panic(err)
	}
	dec := gob.NewDecoder(f)
	var l []ItemList
	for {
		length := 0
		err = dec.Decode(&length)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		il := NewItemList()
		for i := 0; i < length; i++ {
			item := Item{}
			if err := dec.Decode(&item.Id); err != nil {
				panic(err)
			}
			if err := dec.Decode(&item.Score); err != nil {
				panic(err)
			}

			il = append(il, item)
		}
		l = append(l, il)
	}
	return l
}
