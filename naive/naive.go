package naive

import "github.com/cevian/go-stream/stream"
import "github.com/cevian/disttopk"

import (
	"errors"
	"fmt"
)

func NewNaivePeer(list []disttopk.Item, cutoff int) *NaivePeer {
	return &NaivePeer{stream.NewHardStopChannelCloser(), nil, nil, list, 0, cutoff}
}

type NaivePeer struct {
	*stream.HardStopChannelCloser
	forward chan<- disttopk.DemuxObject
	back    <-chan stream.Object
	list    disttopk.ItemList
	id      int
	cutoff  int
}

func (src *NaivePeer) Run() error {
	//defer close(src.forward)
	var rcv <-chan stream.Object
	rcv = nil
	sent := false

	src.list.Sort()
	list := src.list
	if src.cutoff > 0 {
		list = src.list[:src.cutoff]
	}

	for {
		if sent == false {
			rcv = nil
		} else {
			rcv = src.back
		}
		select {
		case src.forward <- disttopk.DemuxObject{src.id, list}:
			return nil
		case <-rcv:
			return errors.New("No second round in naive implementation")
		case <-src.StopNotifier:
			return nil
		}
	}
}

func NewNaiveCoord(cutoff int) *NaiveCoord {
	return &NaiveCoord{stream.NewHardStopChannelCloser(), make(chan disttopk.DemuxObject, 3), make([]chan<- stream.Object, 0), nil, nil, cutoff}
}

type NaiveCoord struct {
	*stream.HardStopChannelCloser
	input        chan disttopk.DemuxObject
	backPointers []chan<- stream.Object
	lists        []disttopk.ItemList
	FinalList    []disttopk.Item
	cutoff       int
}

func (src *NaiveCoord) Add(p *NaivePeer) {
	id := len(src.backPointers)
	back := make(chan stream.Object, 3)
	src.backPointers = append(src.backPointers, back)
	p.id = id
	p.back = back
	p.forward = src.input
}

func (src *NaiveCoord) Run() error {
	defer func() {
		for _, ch := range src.backPointers {
			close(ch)
		}
	}()

	src.lists = make([]disttopk.ItemList, len(src.backPointers))
	cnt := 0
	items := 0
	for {
		select {
		case dobj := <-src.input:
			cnt++
			list := dobj.Obj.(disttopk.ItemList)
			src.lists[dobj.Id] = list
			items += len(list)
			if cnt == len(src.backPointers) {
				m := make(map[int]float64)
				for _, l := range src.lists {
					il := disttopk.ItemList(l)
					il.AddToMap(m)
				}
				/*
					for _, l := range src.lists {
						for _, item := range l {
							score := m[item.Id]
							m[item.Id] = score + item.Score
						}
					}*/

				il := disttopk.MakeItemList(m)

				il.Sort()
				/*
					il := make(disttopk.ItemList, len(m))
					i := 0
					for k, v := range m {
						il[i] = disttopk.Item{k, v}
						i++
					}
					sort.Sort(sort.Reverse(il))*/
				//fmt.Println("Sorted Global List: ", il[:10])
				fmt.Printf("Total bytes naive (cutoff=%d): %E\n", src.cutoff, float64(items*disttopk.RECORD_SIZE))
				if disttopk.OUTPUT_RESP {
					for _, it := range il[:10] {
						fmt.Println("Resp: ", it.Id, it.Score) //, mresp[it.Id])
					}
				}
				src.FinalList = il
				return nil
			}
		case <-src.StopNotifier:
			return nil
		}
	}
}

/*
type ZipfSourceOp struct {
	*stream.HardStopChannelCloser
	*stream.BaseOut
	souce ZipfSource
}



func NewZipfSourceOperator(max uint32) ZipfSource {
	hcc := stream.NewHardStopChannelCloser()
	o := stream.NewBaseOut(stream.CHAN_SLACK)
	nrs := ZipfSource{hcc, o, max}
	return &nrs
}

func (src *ZipfSource) GenerateItem(rank int) Item {
	id := rand.Int()
	score := math.Pow(float64(rank), -src.zipParam) / src.zipNorm
	return Item{id, score}
}

func (src *ZipfSource) Run() error {
	defer close(src.Out())
	var count uint32
	count = 0

	slog.Logf(logger.Levels.Debug, "Generating up to %d %s", src.MaxItems, " tuples")
	for {
		rank := count + 1

		item := src.generateItem(rank)
		select {
		case src.Out <- item:
			count = count + 1
		case <-src.StopNotifier:
			return nil
		}

		if count >= src.MaxItems {
			slog.Logf(logger.Levels.Debug, "Generated all tuples %d, %d", count, src.MaxItems)
			return nil
		}
	}

}*/
