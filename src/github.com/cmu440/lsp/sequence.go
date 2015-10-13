package lsp

type SeqOrganizer struct {
	outMsgChan     chan<- Message
	msgMap         map[int]Message
	expSeqNum      int
	reqChan        chan seqRequest
	cleanCloseChan chan bool
	forceCloseChan chan bool
	mapSize        int
}

type SeqReqType int

const (
	SeqAdd = iota
)

type seqRequest struct {
	reqType SeqReqType
	param   Message
	retChan chan error
}

func (seq *SeqOrganizer) Close() {
	if seq.mapSize != 0 {
		zeroLen := make(chan bool)
		go func() {
			for {
				if seq.mapSize == 0 {
					zeroLen <- true
					return
				}

				select {
				case <- seq.forceCloseChan:
					return
					default:
				}
			}
		}()
		<-zeroLen

		seq.cleanCloseChan <- true
		close(seq.cleanCloseChan)
		close(seq.forceCloseChan)
	}
}

func (seq *SeqOrganizer) ForceClose() {
	close(seq.forceCloseChan)
}

func NewSeqOrganizer(outMsgChan chan Message, initSeqNum int) (*SeqOrganizer, error) {
	seq := &SeqOrganizer{
		outMsgChan:     outMsgChan,
		expSeqNum:      initSeqNum,
		reqChan:        make(chan seqRequest),
		msgMap:         make(map[int]Message),
		cleanCloseChan: make(chan bool),
		forceCloseChan: make(chan bool),
	}

	go seq.runSeqOrg()
	return seq, nil
}

func (seq *SeqOrganizer) AddMsg(msg Message) error {
	errChan := make(chan error)
	seq.reqChan <- seqRequest{reqType: SeqAdd, param: msg, retChan: errChan}
	return <-errChan
}

func (seq *SeqOrganizer) addMsg(msg Message) error {
	if msg.SeqNum < seq.expSeqNum {
		return nil
	}

	if _, ok := seq.msgMap[msg.SeqNum]; ok {
		return nil
	}

	if msg.SeqNum == seq.expSeqNum {
		seq.outMsgChan <- msg
		seq.expSeqNum++
		seq.updateMsgMap()
		return nil
	}

	seq.msgMap[msg.SeqNum] = msg
	return nil
}

func (seq *SeqOrganizer) updateMsgMap() {
	for {
		if msg, ok := seq.msgMap[seq.expSeqNum]; ok {
			delete(seq.msgMap, seq.expSeqNum)
			seq.mapSize = len(seq.msgMap)
			seq.outMsgChan <- msg
			seq.expSeqNum++
		} else {
			return
		}
	}
}

func (seq *SeqOrganizer) runSeqOrg() {
	for {
		select {
		case req := <-seq.reqChan:
			switch req.reqType {
			case SeqAdd:
				err := seq.addMsg(req.param)
				req.retChan <- err
			}
		case <-seq.cleanCloseChan:
			return
		case <-seq.forceCloseChan:
			return
		}
	}
}
