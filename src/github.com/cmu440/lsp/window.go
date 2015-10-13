package lsp

type slidingWindow struct {
	msgWindow   []msgRecord
	outMsgChan  chan<- Message
	minSeqNum   int
	winSize     int
}

type msgRecord struct {
	msg     Message
	isSent  bool
	isAcked bool
}

func newWindow(outMsgChan chan Message, winSize, initSeq int) (*slidingWindow, error) {
	w := &slidingWindow{
		msgWindow:   make([]msgRecord, 0),
		outMsgChan:  outMsgChan,
		minSeqNum:   initSeq,
		winSize:     winSize,
	}
	return w, nil
}

func (win *slidingWindow) addMsg(msg Message) {
	record := msgRecord{
		msg:     msg,
		isSent:  false,
		isAcked: false,
	}
	win.msgWindow = append(win.msgWindow, record)
	win.updateWindow()
}

func (win *slidingWindow) sendMsgs() {
	for i := 0; i < len(win.msgWindow) && i < win.winSize; i++ {
		if !win.msgWindow[i].isSent {
			win.outMsgChan <- win.msgWindow[i].msg
			win.msgWindow[i].isSent = true
		}
	}
}

func (win *slidingWindow) ack(ackMsg Message) {
	if ackMsg.SeqNum < win.minSeqNum {
		return
	}

	ltrace.Println("Window: ", ackMsg.SeqNum, win.minSeqNum, win.msgWindow)
	win.msgWindow[ackMsg.SeqNum-win.minSeqNum].isAcked = true
}

func (win *slidingWindow) updateWindow() {
	offset := 0
	for offset < len(win.msgWindow) && offset < win.winSize {
		if !win.msgWindow[offset].isAcked {
			break
		}
		offset++
	}
	win.msgWindow = win.msgWindow[offset:]
	win.minSeqNum += offset
	win.sendMsgs()
}
