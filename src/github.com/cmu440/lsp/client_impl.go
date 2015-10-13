// Contains the implementation of a LSP client.

package lsp

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"reflect"
	"strconv"
)

const (
	CLIENT_BUFFER_SIZE = 1024
)

type client struct {
	connID          int
	nextSeqNum      int
	connectChan     chan bool
	writeBufferChan chan Message
	outMsgChan      chan Message
	inMsgChan       chan Message
	readBufferChan  chan Message
	conn            *lspnet.UDPConn
	serverAddr      *lspnet.UDPAddr
	seqOrg          *SeqOrganizor
	wind            *slidingWindow
	params          *Params
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	raddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}

	conn, err := lspnet.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}

	client := &client{
		connectChan:     make(chan bool),
		writeBufferChan: make(chan Message),
		outMsgChan:      make(chan Message),
		inMsgChan:       make(chan Message),
		readBufferChan:  make(chan Message),
		conn:            conn,
		serverAddr:      raddr,
		params:          params,
	}

	go client.writeToServer()
	go client.readFromServer()
	go client.handleConnection()

	return client.connect()
}

func (c *client) initSeq() {
	seqOrg, err := NewSeqOrganizor(c.readBufferChan, INIT_SEQ_NUM+1)
	if err != nil {
		ltrace.Fatal(err)
	}
	c.seqOrg = seqOrg
}

func (c *client) initWindow() {
	wind, err := newWindow(c.outMsgChan, c.params.WindowSize, INIT_SEQ_NUM+1)
	if err != nil {
		ltrace.Fatal(err)
	}
	c.wind = wind
}

func (c *client) connect() (Client, error) {
	msg := NewConnect()
	ltrace.Println("Attempt to connect")

	c.outMsgChan <- *msg
	if isConnected, open := <-c.connectChan; open {
		if isConnected {
			c.initSeq()
			c.initWindow()
			return c, nil
		} else {
			return nil, errors.New("Connection failed")
		}
	} else {
		return nil, errors.New("Connection closed")
	}
}

func (c *client) writeToServer() {
	for msg := range c.outMsgChan {
		buf, err := json.Marshal(msg)
		if err != nil {
			ltrace.Println(err)
		}
		ltrace.Printf("Client%d Write: %v", c.connID, msgString(msg))
		c.conn.Write(buf)
	}
}

func (c *client) readFromServer() {
	var msg Message
	buf := make([]byte, CLIENT_BUFFER_SIZE)
	for {
		n, err := c.conn.Read(buf)
		if err != nil {
			ltrace.Println(err)
			continue
		}

		json.Unmarshal(buf[:n], &msg)
		if !IsMsgIntegrated(&msg) {
			ltrace.Println("Message corrupted: ", msg)
			continue
		}

		ltrace.Printf("Client%d Read: %v", c.connID, msgString(msg))
		c.inMsgChan <- msg
	}
}

func (c *client) handleConnection() {
	for {
		select {
		case msg := <-c.inMsgChan:
			switch msg.Type {
			case MsgAck:
				if msg.SeqNum == INIT_SEQ_NUM {
					c.connID = msg.ConnID
					c.nextSeqNum = INIT_SEQ_NUM + 1
					c.connectChan <- true
				} else {
					c.wind.Ack(msg)
				}
			case MsgData:
				c.seqOrg.AddMsg(msg)
				ackMsg := NewAck(msg.ConnID, msg.SeqNum)
				c.outMsgChan <- *ackMsg
			case MsgConnect:
				ltrace.Fatal(msgString(msg))
			}
		case msg := <-c.writeBufferChan:
			c.wind.AddMsg(msg)
		}
	}
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	if msg, open := <-c.readBufferChan; open {

		return msg.Payload, nil
	} else {
		return nil, errors.New("Channel closed")
	}
}

func (c *client) Write(payload []byte) error {
	msg := NewDataWithHash(c.connID, c.nextSeqNum, payload)

	c.nextSeqNum++
	c.writeBufferChan <- *msg
	return nil

}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}

func NewDataWithHash(connID, seqNum int, payload []byte) *Message {
	md5 := md5.New()
	md5.Write([]byte(strconv.Itoa(connID)))
	md5.Write([]byte(strconv.Itoa(seqNum)))
	md5.Write(payload)
	hash := md5.Sum(make([]byte, 0))
	return NewData(connID, seqNum, payload, hash)
}

func IsMsgIntegrated(msg *Message) bool {
	switch msg.Type {
	case MsgData:
		newMsg := NewDataWithHash(msg.ConnID, msg.SeqNum, msg.Payload)
		return reflect.DeepEqual(msg.Hash, newMsg.Hash)
	default:
		return true
	}
}
