package netchan

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"

	"github.com/cevian/go-stream/stream"
)

type Client struct {
	addr   string
	Writer *Writer
	Reader *Reader
	conn   net.Conn
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

	c := NewConn(conn)
	t.Writer = NewWriter(c)
	t.Reader = NewReader(c)
	t.conn = conn

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

	t.conn.Close()
	return nil
}

type Server struct {
	addr           string
	newConnChannel chan *ServerConn
	closing        bool
	listener       net.Listener
}

func NewServer(addr string) *Server {
	return &Server{addr, make(chan *ServerConn, 3), false, nil}
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

			sc := NewServerConn(conn)
			t.newConnChannel <- sc
		}
	}

	go f()
	return nil
}

func (t *Server) Close() error {
	t.closing = true
	return t.listener.Close()
}

type ServerConn struct {
	Writer *Writer
	Reader *Reader
	conn   net.Conn
}

func NewServerConn(conn net.Conn) *ServerConn {
	c := NewConn(conn)
	return &ServerConn{NewWriter(c), NewReader(c), conn}
}

func (t ServerConn) Start() error {
	go t.Writer.Run()
	go t.Reader.Run()
	return nil
}

type Conn struct {
	conn     net.Conn
	isClosed bool
}

func NewConn(c net.Conn) *Conn {
	return &Conn{c, false}
}

func (t *Conn) Close() error {
	t.conn.Close()
	t.isClosed = true
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
			fmt.Println("Decoder error 1", err)
			return err
		}
		//fmt.Println("reader: sent Message")
		t.channel <- v
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
