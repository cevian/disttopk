package disttopk

import "github.com/cevian/go-stream/stream"

var RECORD_SIZE = 100
var RECORD_ID_SIZE = 4
const OUTPUT_RESP = false
const SAVE_DEBUG = false
const PRINT_BUCKETS = false

type DemuxObject struct {
	Id  int
	Obj stream.Object
}
