package netchan

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/cevian/go-stream/stream"
)

type Client struct {
	addr   string
	Writer *Writer
	Reader *Reader
	c      *Conn
	//conn   net.Conn
}

func NewClient(addr string) *Client {
	return &Client{addr: addr}
}

func (t *Client) Connect() error {
	conn, err := net.Dial("tcp", t.addr)
	if err != nil {
		fmt.Println("Cannot establish a connection with %s %v", t.addr, err)
		return err
	}

	c := NewConn(conn, "Client")
	t.Writer = NewWriter(c)
	t.Reader = NewReader(c)
	t.c = c

	go t.Writer.Run()
	go t.Reader.Run()
	return nil
}

func (t *Client) Close() error {
	closed := false
	select {
	case _, ok := <-t.Writer.channel:
		if !ok {
			closed = true
		}
	default:
	}
	if !closed {
		panic("Should not call closed before closing writer")
	}

	//fmt.Println("Client Closed")
	t.c.Close()
	return nil
}

type Server struct {
	addr           string
	newConnChannel chan *ServerConn
	closing        bool
	listener       net.Listener
	wg             *sync.WaitGroup
}

func NewServer(addr string) *Server {
	return &Server{addr, make(chan *ServerConn, 3), false, nil, &sync.WaitGroup{}}
}

func (t *Server) NewConnChannel() chan *ServerConn {
	return t.newConnChannel
}

func (t *Server) Listen() error {
	ln, err := net.Listen("tcp", t.addr)
	if err != nil {
		fmt.Println("Error listening %v", err)
		return err
	}
	t.listener = ln

	f := func() {
		for {
			conn, err := t.listener.Accept()
			if err != nil {
				if !t.closing {
					fmt.Println("Error Accepting %v", err)
					return
				}
				return
			}

			sc := NewServerConn(conn, t.wg)
			t.newConnChannel <- sc
		}
	}

	go f()
	return nil
}

func (t *Server) Close() error {
	//fmt.Println("Listener closed")
	t.wg.Wait()
	t.closing = true
	return t.listener.Close()
}

type ServerConn struct {
	Writer *Writer
	Reader *Reader
	wg     *sync.WaitGroup
	//conn   net.Conn
}

func NewServerConn(conn net.Conn, wg *sync.WaitGroup) *ServerConn {
	c := NewConn(conn, "Server")
	return &ServerConn{NewWriter(c), NewReader(c), wg}
}

func (t ServerConn) Start() error {
	t.wg.Add(2)
	go func() {
		defer t.wg.Done()
		t.Writer.Run()
	}()
	go func() {
		defer t.wg.Done()
		t.Reader.Run()
	}()
	return nil
}

type Conn struct {
	conn     net.Conn
	isClosed bool
	typ      string
}

func NewConn(c net.Conn, typ string) *Conn {
	return &Conn{c, false, typ}
}

func (t *Conn) Close() error {
	t.isClosed = true
	t.conn.Close()
	return nil
}

func (t *Conn) IsClosed() bool {
	return t.isClosed
}

type Writer struct {
	writer  io.Writer
	channel chan stream.Object
	conn    *Conn
}

func NewWriter(c *Conn) *Writer {
	return &Writer{c.conn, make(chan stream.Object, 3), c}
}

func (t *Writer) Run() error {
	encoder := gob.NewEncoder(t.writer)

	for {
		obj, ok := <-t.channel
		//fmt.Println("writer: Got Message")
		if !ok {
			//channel closed
			//fmt.Println("Writer Closed", t.conn.typ)
			t.conn.Close()
			return nil
		}

		//fmt.Println("writer: encoding Message")
		err := encoder.Encode(&obj)
		if err != nil {
			fmt.Println("Encoder error", err)
			return err
		}
	}
}

type ParallelDeserializer interface {
	ParallelDeserialize() interface{}
}

type Reader struct {
	reader       io.Reader
	channel      chan stream.Object
	closeChannel bool
	conn         *Conn
}

func NewReader(c *Conn) *Reader {
	return &Reader{c.conn, make(chan stream.Object, 3), true, c}
}

func (t *Reader) Run() error {
	decoder := gob.NewDecoder(t.reader)

	if t.closeChannel {
		defer close(t.channel)
	}

	for {
		var v stream.Object
		err := decoder.Decode(&v)
		//fmt.Println("reader: Got Message")
		if err != nil {
			if err == io.EOF || t.conn.IsClosed() {
				return nil
			}
			fmt.Println("Decoder error 1", t.conn.typ, err)
			return err
		}

		pd, isPd := v.(ParallelDeserializer)
		if isPd {
			t.channel <- pd.ParallelDeserialize()
		} else {
			//fmt.Println("reader: sent Message")
			t.channel <- v
		}
	}
}

func (t *Reader) SetChannel(ch chan stream.Object) {
	t.channel = ch
	t.closeChannel = false
}

func (t *Reader) Channel() chan stream.Object {
	return t.channel
}
func (t *Writer) Channel() chan stream.Object {
	return t.channel
}

/*
func (t *Writer) Encode(obj interface{}) ([]byte, error) {
	return disttopk.GobBytesEncode(obj)
}

func (t Writer) writeMsg(msg []byte) error {
	err := binary.Write(t.writer, binary.LittleEndian, uint32(len(msg)))
	if err != nil {
		return err
	}
	_, err = t.writer.Write(msg)
	return err
}*/
