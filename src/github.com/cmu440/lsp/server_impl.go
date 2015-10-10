// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"log"
	"os"
	"strconv"
)

const (
	SERVER_BUFFER_SIZE = 1024
	UNINIT_SEQ_NUM     = -1
	INIT_SEQ_NUM       = 0
)

var (
	ltrace *log.Logger = log.New(os.Stderr, "TRACE: ", log.Ldate|log.Ltime|log.Lshortfile)
)

type clientInfo struct {
	connID     int
	nextSeqNum int
	clientAddr *lspnet.UDPAddr
	inMsgChan  chan Message
	outMsgChan chan Message
}

type server struct {
	serverConn           *lspnet.UDPConn
	clients              map[int]*clientInfo
	newClientAddrChan    chan *lspnet.UDPAddr
	recvMsgChan          chan Message
	respMsgChan          chan Message
	respErrChan          chan error
	closeConnIdChan      chan int
	connIdCloseErrorChan chan error
	connIDGenerator      chan int
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
	if err != nil {
		return nil, err
	}

	s := &server{
		serverConn:           serverConn,
		newClientAddrChan:    make(chan *lspnet.UDPAddr),
		recvMsgChan:          make(chan Message),
		respMsgChan:          make(chan Message),
		respErrChan:          make(chan error),
		closeConnIdChan:      make(chan int),
		connIdCloseErrorChan: make(chan error),
		clients:              make(map[int]*clientInfo),
		connIDGenerator:      connIDs(),
	}
	go s.readFromClients()
	go s.handleServerEvents()
	return s, nil
}

func (s *server) readFromClients() {
	buf := make([]byte, SERVER_BUFFER_SIZE)
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
			s.newClientAddrChan <- clientAddr
		default:
			if c, ok := s.clients[msg.ConnID]; ok {
				c.inMsgChan <- msg
			} else {
				ltrace.Println("ConnID not found: ", c.connID)
			}
		}
	}
}

func (s *server) handleServerEvents() {
	for {
		select {
		case clientAddr := <-s.newClientAddrChan:
			s.addClient(clientAddr)
		case msg := <-s.respMsgChan:
			if c, ok := s.clients[msg.ConnID]; ok {
				s.respErrChan <- nil
				if msg.SeqNum == UNINIT_SEQ_NUM {
					msg.SeqNum = c.nextSeqNum
					c.nextSeqNum++
				}
				c.outMsgChan <- msg
			} else {
				err := errors.New(fmt.Sprintf("connID does not exist: %v", msg.ConnID))
				s.respErrChan <- err
			}
		case connId := <-s.closeConnIdChan:
			if _, ok := s.clients[connId]; ok {
				s.connIdCloseErrorChan <- nil
				s.removeClient(connId)
			} else {
				err := errors.New(fmt.Sprintf("connID does not exist: %v", connId))
				s.connIdCloseErrorChan <- err
			}
		}
	}
}

func (s *server) removeClient(connId int) {
	c := s.clients[connId]
	close(c.inMsgChan)
	close(c.outMsgChan)
	delete(s.clients, connId)
}

func (s *server) addClient(clienAddr *lspnet.UDPAddr) {
	c := clientInfo{
		connID:     s.generateConnID(),
		nextSeqNum: INIT_SEQ_NUM + 1,
		clientAddr: clienAddr,
		inMsgChan:  make(chan Message),
		outMsgChan: make(chan Message),
	}
	s.clients[c.connID] = &c
	ltrace.Println("New connection: ", c.connID)

	go s.handleClientEvents(c.connID)

	c.outMsgChan <- *NewAck(c.connID, INIT_SEQ_NUM)
}

func (s *server) handleClientEvents(connId int) {
	c := s.clients[connId]
	for {
		select {
		case msg := <-c.outMsgChan:
			ltrace.Println("Sending:", msg.String())
			buf, _ := json.Marshal(msg)
			s.serverConn.WriteToUDP(buf, c.clientAddr)
		case msg := <-c.inMsgChan:
			switch msg.Type {
			case MsgData:
				s.recvMsgChan <- msg
				ackMsg := NewAck(msg.ConnID, msg.SeqNum)
				buf, _ := json.Marshal(ackMsg)
				ltrace.Println("Sending: ", msg.String())
				s.serverConn.WriteToUDP(buf, c.clientAddr)
			}
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	if msg, open := <-s.recvMsgChan; open {
		return msg.ConnID, msg.Payload, nil
	}

	return -1, nil, errors.New("Channel closed.")
}

func (s *server) Write(connID int, payload []byte) error {
	// TODO add hash
	msg := NewData(connID, UNINIT_SEQ_NUM, payload, nil)

	s.respMsgChan <- *msg
	if err, open := <-s.respErrChan; open {
		return err
	}

	return errors.New("Channel closed")
}

func (s *server) CloseConn(connID int) error {
	s.closeConnIdChan <- connID
	if err, open := <-s.connIdCloseErrorChan; open {
		return err
	}

	return errors.New("Channel closed")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}

func connIDs() chan int {
	yield := make(chan int)
	count := 0
	go func() {
		for {
			yield <- count
			count++
		}
	}()
	return yield
}

func (s *server) generateConnID() int {
	return <-s.connIDGenerator
}
