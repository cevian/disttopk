package disttopk

//import "testing"

/*
func TestCountMinHashSerialize(t *testing.T) {
	hash := NewCountMinHash(2, 2)

	b, err := GobBytesEncode(hash)

	if err != nil {
		panic(err)
	}

	if len(b) != hash.ByteSize() {
		t.Error("len is", len(b), "but bytesize is", hash.ByteSize())
	}

	var obj CountMinHash

	err = GobBytesDecode(&obj, b)

	if err != nil {
		panic(err)
	}

	if obj.Columns != hash.Columns || obj.Hashes != hash.Hashes {
		t.Fail()
	}

	if len(obj.hasha) != hash.Hashes {
		t.Fail()
	}
}*/
