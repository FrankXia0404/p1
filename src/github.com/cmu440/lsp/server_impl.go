// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"strconv"
	"log"
	"os"
)

const (
	BUFFER_SIZE = 1024
	INIT_SEQ    = -1
)

var ltrace *log.Logger = log.New(os.Stderr, "TRACE: ", log.Ldate|log.Ltime|log.Lshortfile)

type clientInfo struct {
	connId      int
	nextSeqNum  int
	clientAddr  *lspnet.UDPAddr
	receiveChan chan Message
	sendChan    chan Message
}

type server struct {
	serverConn            *lspnet.UDPConn
	clientConnChan        chan *lspnet.UDPAddr
	serverReadChan        chan Message
	serverWriteChan       chan Message
	serverWriteOkChan     chan bool
	serverCloseConnChan   chan int
	serverCloseConnOkChan chan bool
	clients               map[int]*clientInfo
	count                 int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	laddr, err := lspnet.ResolveUDPAddr("udp", lspnet.JoinHostPort("localhost", strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}

	serverConn, err := lspnet.ListenUDP("udp", laddr)
	s := &server{
		serverConn:            serverConn,
		clientConnChan:        make(chan *lspnet.UDPAddr),
		serverReadChan:        make(chan Message),
		serverWriteChan:       make(chan Message),
		serverWriteOkChan:     make(chan bool),
		serverCloseConnChan:   make(chan int),
		serverCloseConnOkChan: make(chan bool),
		clients:               make(map[int]*clientInfo),
		count:                 0,
	}
	go s.readFromClients()
	go s.handleServerEvents()
	return s, nil
}

func (s *server) readFromClients() {
	buf := make([]byte, BUFFER_SIZE)
	var msg Message
	for {
		n, clientAddr, err := s.serverConn.ReadFromUDP(buf)
		if err != nil {
			ltrace.Println(err)
			return
		}

		json.Unmarshal(buf[:n], &msg)
		ltrace.Println(msg.String())

		switch msg.Type {
		case MsgConnect:
			s.clientConnChan <- clientAddr
		default:
			if c, ok := s.clients[msg.ConnID]; ok {
				c.receiveChan <- msg
			}
		}
	}
}

func (s *server) handleServerEvents() {
	for {
		select {
		case clientAddr := <-s.clientConnChan:
			s.addClient(clientAddr)
		case msg := <-s.serverWriteChan:
			if c, ok := s.clients[msg.ConnID]; ok {
				s.serverWriteOkChan <- true
				if msg.SeqNum == INIT_SEQ {
					msg.SeqNum = c.nextSeqNum
					c.nextSeqNum++
				}
				c.sendChan <- msg
			} else {
				s.serverWriteOkChan <- false
			}
		case connId := <-s.serverCloseConnChan:
			if _, ok := s.clients[connId]; ok {
				s.serverCloseConnOkChan <- true
				delete(s.clients, connId)
			} else {
				s.serverCloseConnOkChan <- false
			}
		}
	}
}

func (s *server) addClient(clienAddr *lspnet.UDPAddr) {
	c := clientInfo{
		connId:      s.count,
		nextSeqNum:  1,
		clientAddr:  clienAddr,
		receiveChan: make(chan Message),
		sendChan:    make(chan Message),
	}
	s.clients[c.connId] = &c
	s.count++
	print("New connection: ", c.connId)

	go s.handleClientEvents(c.connId)

	c.sendChan <- *NewAck(c.connId, 0)
}

func (s *server) handleClientEvents(connId int) {
	c := s.clients[connId]
	for {
		select {
		case msg := <-c.sendChan:
			ltrace.Println("Sending:", msg.String())
			buf, _ := json.Marshal(msg)
			s.serverConn.WriteToUDP(buf, c.clientAddr)
		case msg := <-c.receiveChan:
			switch msg.Type {
			case MsgData:
				s.serverReadChan <- msg
				ackMsg := NewAck(msg.ConnID, msg.SeqNum)
				buf, _ := json.Marshal(ackMsg)
				ltrace.Println("Sending: ", msg.String())
				s.serverConn.WriteToUDP(buf, c.clientAddr)
			}
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	if msg, open := <-s.serverReadChan; open {
		return msg.ConnID, msg.Payload, nil
	} else {
		return -1, nil, errors.New("Channel closed.")
	}
}

func (s *server) Write(connID int, payload []byte) error {
	// TODO add hash
	msg := NewData(connID, INIT_SEQ, payload, nil)

	s.serverWriteChan <- *msg
	if ok, open := <-s.serverWriteOkChan; open {
		switch ok {
		case true:
			return nil
		case false:
			return errors.New(fmt.Sprintf("connID does not exist: %v", connID))
		}
	}

	return errors.New("Channel closed")
}

func (s *server) CloseConn(connID int) error {
	s.serverCloseConnChan <- connID
	if ok, open := <-s.serverCloseConnOkChan; open {
		switch ok {
		case true:
			return nil
		case false:
			return errors.New(fmt.Sprintf("connID does not exist: %v", connID))
		}
	}

	return errors.New("Channel closed")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}
