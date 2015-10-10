// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
)

type client struct {
	connID         int
	nextSeqNum     int
	connectChan    chan bool
	writeChan      chan []byte
	sendChan       chan []byte
	receiveChan    chan Message
	readBufferChan chan Message
	conn           *lspnet.UDPConn
	addr           *lspnet.UDPAddr
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
		connectChan:    make(chan bool),
		writeChan:      make(chan []byte),
		sendChan:       make(chan []byte),
		receiveChan:    make(chan Message),
		readBufferChan: make(chan Message),
		nextSeqNum:     1,
		conn:           conn,
		addr:           raddr,
	}

	go client.writeToServer()
	go client.readFromServer()
	go client.handleConnection()

	return client.connect()
}

func (c *client) connect() (Client, error) {
	msg := NewConnect()
	ltrace.Println("Attempt to connect")
	buf, _ := json.Marshal(msg)
	c.writeChan <- buf
	for _ = range c.connectChan {
		return c, nil
	}
	return nil, errors.New("Connection closed")
}

func (c *client) writeToServer() {
	for msg := range c.sendChan {
		c.conn.Write(msg)
	}
}

func (c *client) readFromServer() {
	var msg Message
	buf := make([]byte, BUFFER_SIZE)
	for {
		n, err := c.conn.Read(buf)
		if err != nil {
			ltrace.Println(err)
			continue
		}

		json.Unmarshal(buf[:n], &msg)
		ltrace.Println("Receive message: ", msg.String())

		c.receiveChan <- msg
	}
}

func (c *client) handleConnection() {
	for {
		select {
		case msg := <-c.receiveChan:
			switch msg.Type {
			case MsgAck:
				if msg.SeqNum == 0 {
					c.connID = msg.ConnID
					c.connectChan <- true
				}
			case MsgData:
				c.readBufferChan <- msg
				ackMsg := NewAck(msg.ConnID, msg.SeqNum)
				ltrace.Println("Send message:", ackMsg)
				buf, _ := json.Marshal(ackMsg)
				c.sendChan <- buf
			}
		case msg := <-c.writeChan:
			c.sendChan <- msg
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
	// TODO add hash
	msg := NewData(c.connID, c.nextSeqNum, payload, nil)
	ltrace.Println("Send message: ", msg)

	buf, _ := json.Marshal(msg)
	c.nextSeqNum++
	c.writeChan <- buf
	return nil

}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}
