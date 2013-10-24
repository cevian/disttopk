package disttopk

import "github.com/cevian/go-stream/stream"

const RECORD_SIZE = 100
const OUTPUT_RESP = false

type DemuxObject struct {
	Id  int
	Obj stream.Object
}
