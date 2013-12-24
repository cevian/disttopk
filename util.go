package disttopk

import "encoding/binary"

func IntKeyToByteKey(key int) []byte {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	return tmp
}
