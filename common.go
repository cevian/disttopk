package disttopk

import "github.com/cevian/go-stream/stream"

const RECORD_SIZE = 10000

type DemuxObject struct {
	Id  int
	Obj stream.Object
}
