package lsp

type SeqOrganizor struct {
	outMsgChan chan<- Message
	msgMap     map[int]Message
	expSeqNum  int
	reqChan    chan seqRequest
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

func NewSeqOrganizor(outMsgChan chan Message, initSeqNum int) (*SeqOrganizor, error) {
	seq := new(SeqOrganizor)
	seq.outMsgChan = outMsgChan
	seq.expSeqNum = initSeqNum
	seq.reqChan = make(chan seqRequest)
	seq.msgMap = make(map[int]Message)
	go seq.runSeqOrg()
	return seq, nil
}

func (seq *SeqOrganizor) AddMsg(msg Message) error {
	errChan := make(chan error)
	seq.reqChan <- seqRequest{reqType: SeqAdd, param: msg, retChan: errChan}
	return <-errChan
}

func (seq *SeqOrganizor) addMsg(msg Message) error {
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

func (seq *SeqOrganizor) updateMsgMap() {
	for {
		if msg, ok := seq.msgMap[seq.expSeqNum]; ok {
			delete(seq.msgMap, seq.expSeqNum)
			seq.outMsgChan <- msg
			seq.expSeqNum++
		} else {
			return
		}
	}
}

func (seq *SeqOrganizor) runSeqOrg() {
	for req := range seq.reqChan {
		switch req.reqType {
		case SeqAdd:
			err := seq.addMsg(req.param)
			req.retChan <- err
		}
	}
}
