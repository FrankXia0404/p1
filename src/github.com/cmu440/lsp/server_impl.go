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
	"time"
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
	seqOrg     *SeqOrganizor
	wind       *slidingWindow
	params     Params
	resetEpochCountChan chan bool
	epochCountChan      chan int
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
	params               Params
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
		params:               *params,
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
		if !IsMsgIntegrated(&msg) {
			ltrace.Println("Message corrupted: ", msg)
			continue
		}

		switch msg.Type {
		case MsgConnect:
			s.newClientAddrChan <- clientAddr
		default:
			if c, ok := s.clients[msg.ConnID]; ok {
				ltrace.Printf("Server revc from C%d: %v", c.connID, msgString(msg))
				c.resetEpochCountChan <- true
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
				switch msg.Type {
				case MsgData:
					msg.SeqNum = c.nextSeqNum
					c.nextSeqNum++
					msg = *NewDataWithHash(msg.ConnID, msg.SeqNum, msg.Payload)
					c.wind.AddMsg(msg)
				default:
					ltrace.Fatal(msgString(msg))
				}
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
		params: s.params,
		resetEpochCountChan: make(chan bool),
		epochCountChan:      make(chan int),
	}
	// sequence
	seqOrg, err := NewSeqOrganizor(s.recvMsgChan, INIT_SEQ_NUM+1)
	if err != nil {
		ltrace.Fatal(err)
	}
	c.seqOrg = seqOrg

	// Sliding window
	wind, err := newWindow(c.outMsgChan, c.params.WindowSize, INIT_SEQ_NUM + 1, "Server C" + strconv.Itoa(c.connID))
	if err != nil {
		ltrace.Fatal(err)
	}
	c.wind = wind

	s.clients[c.connID] = &c

	go s.handleClientEvents(c.connID)
	go epochServer(c.connID, c.resetEpochCountChan, time.Duration(s.params.EpochMillis)*time.Millisecond, s.fireEpoch, c.epochCountChan)

	ltrace.Printf("Server new conn %d: %v", c.connID, clienAddr)
	c.outMsgChan <- *NewAck(c.connID, INIT_SEQ_NUM)
}

func (s *server) handleClientEvents(connId int) {
	c := s.clients[connId]
	for {
		select {
		case msg := <-c.outMsgChan:
			ltrace.Printf("Server C%d wirte: %v", c.connID, msgString(msg))
			buf, _ := json.Marshal(msg)
			s.serverConn.WriteToUDP(buf, c.clientAddr)
		case msg := <-c.inMsgChan:
			switch msg.Type {
			case MsgData:
				go c.seqOrg.AddMsg(msg)
				ackMsg := NewAck(msg.ConnID, msg.SeqNum)
				ltrace.Printf("Server C%d wirte: %v", c.connID, msgString(*ackMsg))
				buf, _ := json.Marshal(ackMsg)
				s.serverConn.WriteToUDP(buf, c.clientAddr)
			case MsgAck:
				go c.wind.Ack(msg)
			case MsgConnect:
				ltrace.Fatal(msgString(msg))
			}
		case epochCount :=<-c.epochCountChan:
			ltrace.Printf("Server Epoch C%d: %v Count: %d/%d", c.connID, c.params, epochCount, c.params.EpochLimit)
			if epochCount >= c.params.EpochLimit {
				c.resetEpochCountChan <- false
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

func msgString(m Message) string {
	var name, payload, hash string
	switch m.Type {
	case MsgConnect:
		name = "Connect"
	case MsgData:
		name = "Data"
		payload = " " + string(m.Payload)
	case MsgAck:
		name = "Ack"
	}
	return fmt.Sprintf("[%s %d %d %s %v]", name, m.ConnID, m.SeqNum, payload, hash)
}

func (s *server) fireEpoch(connId int) {
	c := s.clients[connId]
	c.wind.FireEpoch()
}