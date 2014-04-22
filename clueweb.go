package disttopk

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

func CwGetTopics(data []byte) []string {
	type Topic struct {
		Description string `xml:"description"`
	}
	type Result struct {
		XMLName xml.Name `xml:"webtrack2012"`
		Topic   []Topic  `xml:"topic"`
	}
	v := Result{}

	err := xml.Unmarshal(data, &v)
	if err != nil {
		fmt.Printf("error: %v", err)
		return nil
	}

	topics := make([]string, 0, len(v.Topic))
	for _, topic := range v.Topic {
		desc := strings.Replace(strings.Trim(topic.Description, "?!\n\t.\" "), "\"", "", -1)
		topics = append(topics, desc)
	}

	return topics
}

func CwGetWords(topics []string) []string {
	words := make(map[string]bool)
	for _, topic := range topics {
		for _, word := range strings.Split(topic, " ") {
			words[word] = true
		}
	}

	word_array := make([]string, 0, len(words))
	for v, _ := range words {
		word_array = append(word_array, v)
	}
	sort.Strings(word_array)

	return word_array
}

func CwGetItemLists(topicsFilename string, resultsFilename string) (doc_map map[string]int, results map[string]map[int]float64) {
	queryData, err := ioutil.ReadFile("clueweb/webtrack2012topics.xml")
	topics := CwGetTopics(queryData)
	word_array := CwGetWords(topics)

	maxDocId := 0
	doc_map = make(map[string]int)             // doc_name => doc_id
	results = make(map[string]map[int]float64) //word => doc_id => score

	filename := "clueweb/results.txt"
	f, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 4*1024)

	minScore := float64(0)
	for {
		line, isPrefix, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil || isPrefix {
			panic("Readline Error")
		}

		sline := string(line)

		if !strings.Contains(sline, "Q0") {
			continue
		}
		parts := strings.Split(sline, " ")

		if len(parts) != 6 {
			fmt.Printf("parts %+v\n", parts)
			panic("snh")
		}

		word_idx, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			panic("snh")
		}

		doc_name := parts[2]
		scoreStr := parts[4]

		doc_id, ok := doc_map[doc_name]
		if !ok {
			doc_map[doc_name] = maxDocId
			maxDocId++
			doc_id = doc_map[doc_name]
		}

		word := word_array[word_idx]

		word_map, ok := results[word]
		if !ok {
			results[word] = make(map[int]float64)
			word_map = results[word]
		}
		scoreFloat, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			panic("snh parse")
		}
		if scoreFloat < minScore {
			minScore = scoreFloat
		}
		word_map[doc_id] = scoreFloat
	}

	corrector := (-minScore) + 1
	if corrector < 0 {
		panic("snh")
	}
	for _, wordMap := range results {
		for doc_id, score := range wordMap {
			newScore := (score + corrector) * 1000
			if newScore < 1 {
				panic("snh")
			}
			wordMap[doc_id] = float64(int32(newScore))
		}
	}

	//fmt.Printf("corrector %v results %+v\n", corrector, results)
	//panic("test")
	//fmt.Printf("doc %+v\n", doc_map)
	return
}
