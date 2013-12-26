package disttopk

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
)

func IntKeyToByteKey(key int) []byte {
	tmp := make([]byte, 16)
	binary.PutUvarint(tmp, uint64(key))
	return tmp
}

type Serializer interface {
	Serialize(w io.Writer) error
	Deserialize(r io.Reader) error
}

func SerializeObject(obj Serializer) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := obj.Serialize(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DeserializeObject(into Serializer, b []byte) error {
	buf := bytes.NewReader(b)
	return into.Deserialize(buf)
}

func GobBytesEncode(obj interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)
	if err := e.Encode(obj); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func GobBytesDecode(into interface{}, b []byte) error {
	buf := bytes.NewReader(b)
	e := gob.NewDecoder(buf)
	err := e.Decode(into)
	return err
}

func SerializeIntAsU32(w io.Writer, v *int) error {
	writev := uint32(*v)
	return binary.Write(w, binary.BigEndian, &writev)
}
func DeserializeIntAsU32(r io.Reader, v *int) error {
	readv := uint32(0)
	err := binary.Read(r, binary.BigEndian, &readv)
	if err != nil {
		return err
	}
	*v = int(readv)
	return nil
}
