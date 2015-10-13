package lsp

type slidingWindow struct {
	packets    []packet
	outMsgChan chan<- Message
	minSeqNum  int
	winSize    int
	reqChan    chan windRequest
}

type packet struct {
	msg     Message
	isSent  bool
	isAcked bool
}

type WindReqType int

const (
	WindAppend = iota
	WindAck
	WindUpdate
)

type windRequest struct {
	reqType WindReqType
	param   Message
	retChan chan error
}

func newWindow(outMsgChan chan Message, winSize, initSeq int) (*slidingWindow, error) {
	w := &slidingWindow{
		packets:    make([]packet, 0),
		outMsgChan: outMsgChan,
		minSeqNum:  initSeq,
		winSize:    winSize,
		reqChan:    make(chan windRequest),
	}
	go w.runWindow()
	return w, nil
}

func (w *slidingWindow) runWindow() {
	for {
		select {
		case req := <-w.reqChan:
			switch req.reqType {
			case WindAppend:
				p := packet{
					msg:     req.param,
					isSent:  false,
					isAcked: false,
				}
				w.packets = append(w.packets, p)
				req.retChan <- nil
			case WindAck:
				ackMsg := req.param
				if ackMsg.SeqNum < w.minSeqNum {
					req.retChan <- nil
					break
				}
				offset := ackMsg.SeqNum - w.minSeqNum
				w.packets[offset].isAcked = true
				req.retChan <- nil
			case WindUpdate:
				w.updateWindow()
				req.retChan <- nil
			}
		}
	}
}

func (w *slidingWindow) Ack(acMsg Message) error {
	errChan := make(chan error)
	w.reqChan <- windRequest{reqType: WindAck, param: acMsg, retChan: errChan}
	return <-errChan
}

func (w *slidingWindow) AddMsg(msg Message) error {
	errChan := make(chan error)
	w.reqChan <- windRequest{reqType: WindAppend, param: msg, retChan: errChan}

	if err := <-errChan; err != nil {
		return err
	}

	w.reqChan <- windRequest{reqType: WindUpdate, retChan: errChan}
	return <-errChan
}

func (w *slidingWindow) sendMsgs() {
	for i := 0; i < len(w.packets) && i < w.winSize; i++ {
		if !w.packets[i].isSent {
			w.outMsgChan <- w.packets[i].msg
			w.packets[i].isSent = true
		}
	}
}

func (w *slidingWindow) updateWindow() {
	offset := 0
	for offset < len(w.packets) && offset < w.winSize {
		if !w.packets[offset].isAcked {
			break
		}
		offset++
	}
	w.packets = w.packets[offset:]
	w.minSeqNum += offset
	w.sendMsgs()

	ltrace.Printf("Window size: %d %v", len(w.packets), w.packets)
}
