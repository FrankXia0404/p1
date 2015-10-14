package lsp

type slidingWindow struct {
	debugStr string
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
	WindEpoch
)

type windRequest struct {
	reqType WindReqType
	param   Message
	retChan chan error
}

func newWindow(outMsgChan chan Message, winSize, initSeq int, debugStr string) (*slidingWindow, error) {
	w := &slidingWindow{
		packets:    make([]packet, 0),
		outMsgChan: outMsgChan,
		minSeqNum:  initSeq,
		winSize:    winSize,
		reqChan:    make(chan windRequest),
		debugStr: debugStr,
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
				if (req.param.SeqNum < w.minSeqNum) {
					req.retChan <- nil
					break
				}
				p := packet{
					msg:     req.param,
					isSent:  false,
					isAcked: false,
				}
				w.packets = append(w.packets, p)
				w.updateWindow()
				req.retChan <- nil
			case WindAck:
				ackMsg := req.param
				if ackMsg.SeqNum < w.minSeqNum {
					req.retChan <- nil
					break
				}
				i := ackMsg.SeqNum - w.minSeqNum
				w.packets[i].isAcked = true
				w.updateWindow()
				req.retChan <- nil
			case WindUpdate:
				w.updateWindow()
				req.retChan <- nil
			case WindEpoch:
				w.fireEpoch()
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
}

func (w *slidingWindow) fireEpoch() {
	for i := 0; i < len(w.packets) && i < w.winSize; i++ {
		if w.packets[i].isSent && !w.packets[i].isAcked {
			w.outMsgChan <- w.packets[i].msg
		}
	}
}

func (w *slidingWindow) FireEpoch() error{
	errChan := make(chan error)
	w.reqChan <- windRequest{reqType: WindEpoch, retChan: errChan}
	return <-errChan
}