package runner

import (
	"fmt"
	"time"

	"github.com/cevian/disttopk/klee"
	"github.com/cevian/disttopk/naive"
	"github.com/cevian/disttopk/tput"

	"github.com/cevian/disttopk"
	"github.com/cevian/disttopk/netchan"
	"github.com/cevian/go-stream/stream"
)

type Peer interface {
	stream.Operator
	SetNetwork(readCh chan stream.Object, writeCh chan stream.Object)
}
type Coord interface {
	stream.Operator
	Add(stream.Operator)
	AddNetwork(channel chan stream.Object)
	GetFinalList() disttopk.ItemList
	GetStats() disttopk.AlgoStats
	InputChannel() chan stream.Object
}
type SmartRunner struct {
	NewPeer     func(list disttopk.ItemList, k int, ht *disttopk.HashTable) Peer
	NewCoord    func(k int) Coord
	RegisterGob func()
	Name        string
	Exact       bool
}

func (t *SmartRunner) Run(l []disttopk.ItemList, hts []*disttopk.HashTable, topk int, GroundTruth disttopk.ItemList, Nest int) (disttopk.ItemList, disttopk.AlgoStats) {
	runner := stream.NewRunner()
	peers := make([]Peer, len(l))
	coord := t.NewCoord(topk)
	runner.Add(coord)
	for i, list := range l {
		peers[i] = t.NewPeer(list, topk, hts[i])
		coord.Add(peers[i])
		runner.Add(peers[i])
	}
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	return coord.GetFinalList(), coord.GetStats()
}

func (t *SmartRunner) RunPeer(addr string, numpeer int, l disttopk.ItemList, ht *disttopk.HashTable, topk int, GroundTruth disttopk.ItemList, Nest int) {
	//numpeer := len(l)
	//Nest := getNEst(l)

	t.RegisterGob()

	//pr := t.runnerGenerator(nil, numpeer, Nest, topk, GroundTruth)
	runner := stream.NewRunner()

	client := netchan.NewClient(addr)
	defer client.Close()
	fmt.Println("Connecting")
	var err error
	for i := 0; i < 3; i++ {
		time.Sleep(1000 * time.Millisecond)
		err = client.Connect()
		if err == nil {
			break
		}
	}
	if err != nil {
		panic(err)
	}

	//ht := hts[index]
	peer := t.NewPeer(l, topk, ht)
	peer.SetNetwork(client.Reader.Channel(), client.Writer.Channel())
	runner.Add(peer)
	defer close(client.Writer.Channel())

	fmt.Println("Starting Protocol")
	runner.AsyncRunAll()
	runner.WaitGroup().Wait()
	fmt.Println("Returned Protocol")
	return
}

func (t *SmartRunner) GetName() string {
	return t.Name
}

func (t *SmartRunner) IsExact() bool {
	return t.Exact
}

func (t *SmartRunner) RunCoord(addr string, l []disttopk.ItemList, hts []*disttopk.HashTable, topk int, GroundTruth disttopk.ItemList, Nest int) (disttopk.ItemList, disttopk.AlgoStats) {
	numpeer := len(l)
	//Nest := getNEst(l)

	t.RegisterGob()

	//pr := t.runnerGenerator(l, numpeer, Nest, topk, GroundTruth)
	runnerCoord := stream.NewRunner()
	coord := t.NewCoord(topk)
	runnerCoord.Add(coord)

	server := netchan.NewServer(addr)
	//defer server.Close()
	err := server.Listen()
	if err != nil {
		panic(err)
	}

	serverConnChannel := server.NewConnChannel()

	for i := 0; i < numpeer; i++ {
		fmt.Println("Getting serverch", i)
		serverConn := <-serverConnChannel
		serverConn.Reader.SetChannel(coord.InputChannel())
		serverConn.Start()
		fmt.Println("started server conn")
		coord.AddNetwork(serverConn.Writer.Channel())
	}
	fmt.Println("Starting Protocol")
	runnerCoord.AsyncRunAll()
	runnerCoord.WaitGroup().Wait()
	fmt.Println("Returned coord Protocol")
	server.Close()
	fmt.Println("Returned Protocol")
	return coord.GetFinalList(), coord.GetStats()
}

func (t *SmartRunner) RunNetwork(addr string, l []disttopk.ItemList, hts []*disttopk.HashTable, topk int, GroundTruth disttopk.ItemList, Nest int) (disttopk.ItemList, disttopk.AlgoStats) {
	//numpeer := len(l)
	//Nest := getNEst(l)

	t.RegisterGob()

	//pr := t.runnerGenerator(l, numpeer, Nest, topk, GroundTruth)
	runner := stream.NewRunner()
	runnerCoord := stream.NewRunner()
	peers := make([]Peer, len(l))
	coord := t.NewCoord(topk)
	runnerCoord.Add(coord)

	server := netchan.NewServer(addr)
	//defer server.Close()
	err := server.Listen()
	if err != nil {
		panic(err)
	}

	serverConnChannel := server.NewConnChannel()

	for i, list := range l {
		client := netchan.NewClient(addr)
		defer client.Close()
		//fmt.Println("Connecting")
		err := client.Connect()
		if err != nil {
			panic(err)
		}
		//fmt.Println("Getting serverch")
		serverConn := <-serverConnChannel
		serverConn.Reader.SetChannel(coord.InputChannel())
		serverConn.Start()
		//fmt.Println("started server conn")

		ht := hts[i]
		peers[i] = t.NewPeer(list, topk, ht)
		coord.AddNetwork(serverConn.Writer.Channel())
		peers[i].SetNetwork(client.Reader.Channel(), client.Writer.Channel())
		runner.Add(peers[i])
		defer close(client.Writer.Channel())
	}
	//fmt.Println("Starting Protocol")
	runnerCoord.AsyncRunAll()
	runner.AsyncRunAll()
	runnerCoord.WaitGroup().Wait()
	//fmt.Println("Returned coord Protocol")
	server.Close()
	runner.WaitGroup().Wait()
	//fmt.Println("Returned Protocol")
	return coord.GetFinalList(), coord.GetStats()
}

func NewNaiveK2Runner() *SmartRunner {
	return NewNaiveRunner("Naive-2k", 2, false)
}

func NewNaiveExactRunner() *SmartRunner {
	return NewNaiveRunner("Naive-Exact", 0, true)
}

func NewNaiveRunner(name string, cutoff int, exact bool) *SmartRunner {
	/*	NewPeer     func(list disttopk.ItemList, k int, ht *disttopk.HashTable) Peer
		NewCoord    func(k int) Coord
		RegisterGob func()
		Name        string
		Exact       bool*/

	newPeer := func(list disttopk.ItemList, k int, ht *disttopk.HashTable) Peer {
		return naive.NewNaivePeer(list, cutoff*k)
	}
	newCoord := func(k int) Coord {
		return naive.NewNaiveCoord(cutoff)
	}
	registerGob := func() {
		naive.RegisterGob()
	}

	return &SmartRunner{newPeer, newCoord, registerGob, name, exact}
}

func NewTputRunner() *SmartRunner {
	/*	NewPeer     func(list disttopk.ItemList, k int, ht *disttopk.HashTable) Peer
		NewCoord    func(k int) Coord
		RegisterGob func()
		Name        string
		Exact       bool*/

	newPeer := func(list disttopk.ItemList, k int, ht *disttopk.HashTable) Peer {
		return tput.NewPeer(list, k)
	}
	newCoord := func(k int) Coord {
		return tput.NewCoord(k)
	}
	registerGob := func() {
		tput.RegisterGob()
	}

	return &SmartRunner{newPeer, newCoord, registerGob, "TPUT", true}
}

func NewTputHRunner() *SmartRunner {
	return NewTputERFlagRunner("TPUT-H", false)
}

func NewTputERRunner() *SmartRunner {
	return NewTputERFlagRunner("TPUT-ER", true)
}

func NewTputERFlagRunner(name string, extra_round bool) *SmartRunner {
	/*	NewPeer     func(list disttopk.ItemList, k int, ht *disttopk.HashTable) Peer
		NewCoord    func(k int) Coord
		RegisterGob func()
		Name        string
		Exact       bool*/

	newPeer := func(list disttopk.ItemList, k int, ht *disttopk.HashTable) Peer {
		return tput_hash.NewPeer(list, ht, k)
	}
	newCoord := func(k int) Coord {
		return tput_hash.NewCoord(k, extra_round)
	}
	registerGob := func() {
		tput_hash.RegisterGob()
	}

	return &SmartRunner{newPeer, newCoord, registerGob, name, true}
}

func NewKlee3Runner() *SmartRunner {
	return NewKleeRunner("KLEE3", false)
}

func NewKlee4Runner() *SmartRunner {
	return NewKleeRunner("KLEE4", true)
}

func NewKleeRunner(name string, clRound bool) *SmartRunner {
	/*	NewPeer     func(list disttopk.ItemList, k int, ht *disttopk.HashTable) Peer
		NewCoord    func(k int) Coord
		RegisterGob func()
		Name        string
		Exact       bool*/

	newPeer := func(list disttopk.ItemList, k int, ht *disttopk.HashTable) Peer {
		return klee.NewPeer(list, k, clRound)
	}
	newCoord := func(k int) Coord {
		return klee.NewCoord(k, clRound)
	}
	registerGob := func() {
		klee.RegisterGob()
	}

	return &SmartRunner{newPeer, newCoord, registerGob, name, false}
}
