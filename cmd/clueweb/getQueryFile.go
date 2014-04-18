package main

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
)

import "github.com/cevian/disttopk"

func main() {
	data, err := ioutil.ReadFile("webtrack2012topics.xml")
	if err != nil {
		fmt.Printf("error: %v", err)
		return
	}

	topics := disttopk.CwGetTopics(data)
	word_array := disttopk.CwGetWords(topics)

	type Query struct {
		Type   string `xml:"type"`
		Number int    `xml:"number"`
		Text   string `xml:"text"`
	}

	type Parameters struct {
		XMLName xml.Name `xml:"parameters"`
		Query   []Query  `xml:"query"`
	}

	params := make([]*Parameters, 0, 5)
	out := &Parameters{}
	count := 0
	for i, w := range word_array {
		count++
		if count > 29 {
			params = append(params, out)
			out = &Parameters{}
			count = 0
		}
		q := Query{"indri", i, w}
		out.Query = append(out.Query, q)
	}
	params = append(params, out)

	for i, p := range params {
		output, err := xml.MarshalIndent(p, "  ", "    ")
		if err != nil {
			fmt.Printf("error: %v\n", err)
		}
		err = ioutil.WriteFile(fmt.Sprintf("queries_%d.txt", i), output, 0644)
	}
}
