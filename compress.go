package disttopk

import (
	"bytes"
	"compress/zlib"
	"io"
)

const USE_COMPRESSION = true

func CompressBytes(in []byte) []byte {
	if USE_COMPRESSION {
		var b bytes.Buffer
		w := zlib.NewWriter(&b)
		if _, err := w.Write(in); err != nil {
			panic(err)
		}
		if err := w.Close(); err != nil {
			panic(err)
		}
		return b.Bytes()
	}
	return in
}

func DecompressBytes(in []byte) []byte {
	if USE_COMPRESSION {
		inbufr := bytes.NewReader(in)
		r, err := zlib.NewReader(inbufr)
		if err != nil {
			panic(err)
		}

		var outbuf bytes.Buffer
		if _, err := io.Copy(&outbuf, r); err != nil {
			panic(err)
		}
		if err := r.Close(); err != nil {
			panic(err)
		}
		return outbuf.Bytes()
	}
	return in
}
