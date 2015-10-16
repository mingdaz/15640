package lsp

import (
	"container/list"
	// "fmt"
	"github.com/cmu440/lspnet"
)

type ReqType int

const (
	ReqRead ReqType = iota
	ReqWrite
	ReqClose
	ReqCloseConn
	ReqConnect
	ReqEpoch
	ReqReceiveMsg
	ReqConnId
)

type request struct {
	Type         ReqType
	Value        interface{}
	ReplyChannel chan *autoret
}

type autoret struct {
	Value interface{}
	Err   error
}

// wrap list specific use for store Message
type MsgBuffer struct {
	Buf *list.List
}

func NewMsgBuffer() *MsgBuffer {
	return &MsgBuffer{list.New()}
}

func (buf *MsgBuffer) Length() int {
	return buf.Buf.Len()
}

func (buf *MsgBuffer) Insert(msg *Message) {
	Buf := buf.Buf
	if Buf.Len() == 0 || msg.SeqNum > Buf.Back().Value.(*Message).SeqNum {
		Buf.PushBack(msg)
	} else {
		sn := msg.SeqNum
		var temp *list.Element
		temp = nil
		for e := Buf.Front(); e != nil; e = e.Next() {

			if e.Value.(*Message).SeqNum >= sn {
				temp = e
				break
			}
		}
		// if equal not insert
		if sn < temp.Value.(*Message).SeqNum {
			Buf.InsertBefore(msg, temp)
		}
	}
}

func (buf *MsgBuffer) Front() *Message {
	// BUf := buf.Buf
	return buf.Buf.Front().Value.(*Message)
}

func (buf *MsgBuffer) GetAll() []*Message {
	Buf := buf.Buf
	list := make([]*Message, Buf.Len())
	ind := 0
	for e := Buf.Front(); e != nil; e = e.Next() {
		list[ind] = e.Value.(*Message)
		ind++
	}
	return list
}

func (buf *MsgBuffer) Pop() *Message {
	head := buf.Buf.Front()
	msg := head.Value.(*Message)
	buf.Buf.Remove(head)
	return msg
}

func (buf *MsgBuffer) Delete(sn int) bool {
	Buf := buf.Buf
	for e := Buf.Front(); e != nil; e = e.Next() {
		if e.Value.(*Message).SeqNum == sn {
			Buf.Remove(e)
			return true
		}
	}
	return false

}

func (buf *MsgBuffer) Fitwindow(wsize int) {
	Buf := buf.Buf
	maxsn := Buf.Back().Value.(*Message).SeqNum
	minsn := maxsn - wsize + 1
	e := Buf.Front()
	for e != nil {
		next := e.Next()
		if e.Value.(*Message).SeqNum < minsn {
			Buf.Remove(e)
		} else {
			break
		}
		e = next
	}

}

// Net work related
type Packet struct {
	msg   *Message
	raddr *lspnet.UDPAddr
}
