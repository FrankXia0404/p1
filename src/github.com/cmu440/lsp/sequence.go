package lsp

type SeqOrganizor struct {
	outMsgChan chan<- Message
	msgMap     map[int]Message
	expSeqNum  int
}

func NewSeqOrganizor(outMsgChan chan Message, initSeqNum int) (*SeqOrganizor, error) {
	seq := new(SeqOrganizor)
	seq.outMsgChan = outMsgChan
	seq.expSeqNum = initSeqNum

	seq.msgMap = make(map[int]Message)
	return seq, nil
}

func (seq *SeqOrganizor) AddMsg(msg Message) error {
	if msg.SeqNum < seq.expSeqNum {
		return nil
	}

	if _, ok := seq.msgMap[msg.SeqNum]; ok {
		return nil
	}

	if msg.SeqNum == seq.expSeqNum {
		seq.outMsgChan <-msg
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