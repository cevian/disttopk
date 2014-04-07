package main

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
)

func main() {
	//	Where string `xml:"where,attr"`
	type Topic struct {
		Description string `xml:"description"`
	}
	type Result struct {
		XMLName xml.Name `xml:"webtrack2012"`
		Topic   []Topic  `xml:"topic"`
	}
	v := Result{}

	data, err := ioutil.ReadFile("webtrack2012topics.xml")
	if err != nil {
		fmt.Printf("error: %v", err)
		return
	}

	err = xml.Unmarshal(data, &v)
	if err != nil {
		fmt.Printf("error: %v", err)
		return
	}

	words := make(map[string]bool)
	for _, topic := range v.Topic {
		desc := strings.Replace(strings.Trim(topic.Description, "?!\n\t.\" "), "\"", "", -1)
		//fmt.Printf("topic %v\n", desc)
		for _, word := range strings.Split(desc, " ") {
			words[word] = true
		}
	}

	word_array := make([]string, 0, len(words))
	for v, _ := range words {
		word_array = append(word_array, v)
	}
	sort.Strings(word_array)

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
