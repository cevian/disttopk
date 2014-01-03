package disttopk

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

//http://ita.ee.lbl.gov/html/contrib/UCB.home-IP-HTTP.html

type UcbTraceFixed struct {
	//Version uint8  /* Trace record version */
	Crs  uint32 /* client request seconds */
	Cru  uint32 /* client request microseconds */
	Srs  uint32 /* server first response byte seconds */
	Sru  uint32 /* server first response byte microseconds */
	Sls  uint32 /* server last response byte seconds */
	Slu  uint32 /* server last response byte microseconds */
	Cip  uint32 /* client IP address */
	Cpt  uint16 /* client port */
	Sip  uint32 /* server IP address */
	Spt  uint16 /* server port */
	Cprg uint8  /* client headers/pragmas */
	Sprg uint8  /* server headers/pragmas */
	/* If a date is FFFFFFFF, it was missing/unreadable/unapplicable in trace */
	Cims   uint32 /* client IF-MODIFIED-SINCE date, if applicable */
	Sexp   uint32 /* server EXPIRES date, if applicable */
	Slmd   uint32 /* server LAST-MODIFIED, if applicable */
	Rhl    uint32 /* response HTTP header length */
	Rdl    uint32 /* response data length, not including header */
	Urllen uint16 /* url length, not including NULL term */
}

type UcbTrace struct {
	*UcbTraceFixed
	Url string /* request url, e.g. "GET / HTTP/1.0", + '\0' */
}

type UcbFileSourceAdaptor struct {
	KeyOnClient bool
}

func (this *UcbFileSourceAdaptor) FillMapFromFile(filename string, m map[uint32]map[int]float64) {
	fmt.Println("Processing UCB file", filename)
	file, err := os.Open(filename) // For read access.
	if err != nil {
		panic(err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		panic(fmt.Sprintln(err, filename))
	}
	defer gz.Close()

	for err == nil {
		r := &UcbTrace{&UcbTraceFixed{}, ""}
		err = binary.Read(gz, binary.BigEndian, r.UcbTraceFixed)
		if err == io.EOF {
			return
		}
		if err != nil {
			panic(fmt.Sprintln("error: ", err))
		}
		//fmt.Println("strlne:", r.Urllen)
		buf := make([]byte, r.Urllen)
		readb := 0
		for err == nil && readb < len(buf) {
			thisreadb := 0
			thisreadb, err = gz.Read(buf[readb:])
			readb += thisreadb
		}

		if err != nil {
			panic(fmt.Sprintln("error: ", err, readb, len(buf), r.UcbTraceFixed))
		}
		r.Url = string(buf)

		s := r.Sip%512
		mi, ok := m[s]
		if !ok {
			m[s] = make(map[int]float64)
			mi = m[s]
		}
		if this.KeyOnClient {
			//fmt.Println("data:", r.Sip, r.Cip)
			mi[int(r.Cip)] += 1
		} else {
			parts := strings.SplitN(r.Url, " ", 3)
			stringid := strings.SplitN(parts[1], ".", 2)[0]
			if len(stringid) > 19 {
				stringid = stringid[:18]
			}
			objectid, err := strconv.ParseUint(stringid, 10, 64)
			if err != nil {
				panic(err)
			}
			//fmt.Println("data:", r.Sip, objectid, int(objectid))
			mi[int(objectid)] += 1
		}
	}
	fmt.Println("end err:", err)

}
