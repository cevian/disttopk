package disttopk

import "github.com/cevian/go-stream/stream"

const RECORD_SIZE = 100
const OUTPUT_RESP = false
const SAVE_DEBUG = false
const PRINT_BUCKETS = false

type DemuxObject struct {
	Id  int
	Obj stream.Object
}
