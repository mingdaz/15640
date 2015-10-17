package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"time"
)

type client struct {
	params                *Params
	uaddr                 *lspnet.UDPAddr
	uconn                 *lspnet.UDPConn
	requestChannel        chan *request
	closeSignal           chan int
	connLostSignal        chan int
	connEstablishedSignal chan int
	unackmsgBuffer        *MsgBuffer
	sendackBuffer         *MsgBuffer
	readBuffer            *MsgBuffer
	writeBuffer           *MsgBuffer
	delayRead             *list.List
	delayClose            *list.List
	connId                int
	seqNum                int
	waitSeqNum            int
	countEpoch            int
	clientRunning         bool
	connLost              bool
}

func NewClient(hostport string, params *Params) (Client, error) {
	c := &client{
		params:                params,
		requestChannel:        make(chan *request, 100),
		closeSignal:           make(chan int),
		connLostSignal:        make(chan int, 1),
		connEstablishedSignal: make(chan int, 1),
		writeBuffer:           NewMsgBuffer(),
		unackmsgBuffer:        NewMsgBuffer(),
		sendackBuffer:         NewMsgBuffer(),
		readBuffer:            NewMsgBuffer(),
		delayRead:             list.New(),
		delayClose:            list.New(),
		connId:                0,
		seqNum:                0,
		waitSeqNum:            1,
		countEpoch:            0,
		clientRunning:         true,
		connLost:              false,
	}
	c.uaddr, _ = lspnet.ResolveUDPAddr("udp", hostport)
	c.uconn, _ = lspnet.DialUDP("udp", nil, c.uaddr)

	go func() {
		var req *request
		for c.clientRunning {
			if c.connId == 0 && c.connLost {
				return
			}
			if c.delayRead.Len() > 0 && ((c.readBuffer.Length() > 0 && c.readBuffer.Front().SeqNum == c.waitSeqNum) || c.connLost || c.delayClose.Len() > 0) {
				req = c.delayRead.Front().Value.(*request)
				c.delayRead.Remove(c.delayRead.Front())
			} else if c.delayClose.Len() > 0 && ((c.writeBuffer.Length() == 0 && c.unackmsgBuffer.Length() == 0) || c.connLost) {
				req = c.delayClose.Front().Value.(*request)
				c.delayClose.Remove(c.delayClose.Front())
			} else {
				req = <-c.requestChannel
				if req.Type == ReqRead && (c.readBuffer.Length() == 0 || c.readBuffer.Front().SeqNum != c.waitSeqNum) {
					c.delayRead.PushBack(req)
					continue
				}
				if req.Type == ReqClose && (c.writeBuffer.Length() > 0 || c.unackmsgBuffer.Length() > 0) {
					c.delayClose.PushBack(req)
					continue
				}
			}
			switch req.Type {
			case ReqRead:
				c.myRead(req)
			case ReqWrite:
				c.myWrite(req)
			case ReqClose:
				c.myClose(req)
			case ReqConnId:
				c.myConnId(req)
			case ReqConnect:
				c.handleConnect(req)
			case ReqEpoch:
				c.handleEpoch()
			case ReqReceiveMsg:
				c.handleReceivedMsg(req)
			}
		}
	}()

	go func() {
		buf := make([]byte, 1500)
		for {
			select {
			case <-c.closeSignal:
				return
			default:
				n, addr, err := c.uconn.ReadFromUDP(buf[0:])
				if err == nil {
					msg := &Message{}
					err = json.Unmarshal(buf[:n], msg)
					if err == nil {
						packet := &Packet{msg, addr}
						req := &request{ReqReceiveMsg, packet, make(chan *autoret)}
						c.requestChannel <- req
					}
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-time.After(time.Millisecond * time.Duration(params.EpochMillis)):
				req := &request{ReqEpoch, nil, make(chan *autoret)}
				c.requestChannel <- req
			case <-c.closeSignal:
				return
			}
		}
	}()

	err := c.connect()
	if err != nil {
		return nil, err
	}
	select {
	case <-c.connEstablishedSignal:
		return c, nil
	case <-c.connLostSignal:
		return nil, errors.New("c1")
	}

}

func (c *client) ConnID() int {
	ret, _ := c.handleRequest(ReqConnId, nil)
	return ret.(int)
}

func (c *client) Read() ([]byte, error) {
	ret, err := c.handleRequest(ReqRead, nil)
	if err != nil {
		return nil, err
	} else {
		return ret.([]byte), nil
	}
}

func (c *client) Write(payload []byte) error {
	_, err := c.handleRequest(ReqWrite, payload)
	return err
}

func (c *client) Close() error {
	_, err := c.handleRequest(ReqClose, nil)
	return err
}

func (c *client) connect() error {
	_, err := c.handleRequest(ReqConnect, nil)
	return err
}

func (c *client) handleRequest(op ReqType, val interface{}) (interface{}, error) {
	req := &request{op, val, make(chan *autoret)}
	c.requestChannel <- req
	retV := <-req.ReplyChannel
	return retV.Value, retV.Err
}

func (c *client) myRead(req *request) {
	if c.delayClose.Len() > 0 {
		req.ReplyChannel <- &autoret{nil, errors.New("c2")}
		return
	}
	if c.readBuffer.Length() == 0 || c.readBuffer.Front().SeqNum != c.waitSeqNum {
		req.ReplyChannel <- &autoret{nil, errors.New("c3")}
		return
	}
	readMsg := c.readBuffer.Front()
	c.readBuffer.Pop()
	c.waitSeqNum++
	req.ReplyChannel <- &autoret{readMsg.Payload, nil}
}

func (c *client) myWrite(req *request) {
	if c.connLost {
		req.ReplyChannel <- &autoret{nil, errors.New("c4")}
		return
	}
	if c.delayClose.Len() > 0 {
		req.ReplyChannel <- &autoret{nil, errors.New("c5")}
		return
	}
	payload := req.Value.([]byte)
	c.seqNum++
	sentMsg := NewData(c.connId, c.seqNum, payload, nil)
	windowSize := c.params.WindowSize
	if c.writeBuffer.Length() == 0 &&
		(c.unackmsgBuffer.Length() == 0 || sentMsg.SeqNum-windowSize < c.unackmsgBuffer.Front().SeqNum) {
		c.unackmsgBuffer.Insert(sentMsg)
		buf, _ := json.Marshal(sentMsg)
		c.uconn.Write(buf)
	} else {
		c.writeBuffer.Insert(sentMsg)
	}
	req.ReplyChannel <- &autoret{nil, nil}
}

func (c *client) handleEpoch() {
	c.countEpoch++
	if c.countEpoch >= c.params.EpochLimit {
		c.connLost = true
		c.connLostSignal <- 1
		c.shutDown()
		return
	}
	if c.connId > 0 && c.sendackBuffer.Length() == 0 && c.delayClose.Len() == 0 {
		ackMsg := NewAck(c.connId, 0)
		buf, _ := json.Marshal(ackMsg)
		c.uconn.Write(buf)
	}

	unAckMsg := c.unackmsgBuffer.GetAll()
	for _, msg := range unAckMsg {
		buf, _ := json.Marshal(msg)
		c.uconn.Write(buf)
	}

	if c.delayClose.Len() == 0 {
		latestAck := c.sendackBuffer.GetAll()
		for _, msg := range latestAck {
			buf, _ := json.Marshal(msg)
			c.uconn.Write(buf)
		}
	}
}

func (c *client) handleReceivedMsg(req *request) {
	c.countEpoch = 0
	receivedMsg := req.Value.(*Packet).msg
	switch receivedMsg.Type {
	case MsgAck:
		msgExist := c.unackmsgBuffer.Delete(receivedMsg.SeqNum)
		if msgExist && c.writeBuffer.Length() > 0 {
			if c.unackmsgBuffer.Length() == 0 {
				sentMsg := c.writeBuffer.Pop()
				c.unackmsgBuffer.Insert(sentMsg)
				buf, _ := json.Marshal(sentMsg)
				c.uconn.Write(buf)
			}
			wLeft := c.unackmsgBuffer.Front().SeqNum
			wSize := c.params.WindowSize
			for c.writeBuffer.Length() != 0 {
				if c.writeBuffer.Front().SeqNum-wSize >= wLeft {
					break
				} else {
					sentMsg := c.writeBuffer.Pop()
					c.unackmsgBuffer.Insert(sentMsg)
					buf, _ := json.Marshal(sentMsg)
					c.uconn.Write(buf)
				}
			}
		}
		if msgExist && receivedMsg.SeqNum == 0 {
			c.connId = receivedMsg.ConnID
			c.connEstablishedSignal <- 1
		}
	case MsgData:
		if c.connId == 0 || c.delayClose.Len() > 0 {
			return
		}
		if receivedMsg.SeqNum >= c.waitSeqNum {
			c.readBuffer.Insert(receivedMsg)
			ackMsg := NewAck(c.connId, receivedMsg.SeqNum)
			buf, _ := json.Marshal(ackMsg)
			c.uconn.Write(buf)
			c.sendackBuffer.Insert(ackMsg)
			c.sendackBuffer.Fitwindow(c.params.WindowSize)
		}
	}
}

func (c *client) myConnId(req *request) {
	req.ReplyChannel <- &autoret{c.connId, nil}
}

func (c *client) myClose(req *request) {
	if !c.connLost {
		c.shutDown()
	}
	if c.writeBuffer.Length() > 0 || c.unackmsgBuffer.Length() > 0 {
		req.ReplyChannel <- &autoret{nil, errors.New("c6")}
	} else {
		req.ReplyChannel <- &autoret{nil, nil}
	}

	for e := c.delayClose.Front(); e != nil; e = e.Next() {
		req = e.Value.(*request)
		if c.writeBuffer.Length() > 0 || c.unackmsgBuffer.Length() > 0 {
			req.ReplyChannel <- &autoret{nil, errors.New("c7")}
		} else {
			req.ReplyChannel <- &autoret{nil, nil}
		}
	}
	c.clientRunning = false
}

func (c *client) handleConnect(req *request) {
	msg := NewConnect()
	c.unackmsgBuffer.Insert(msg)
	buf, _ := json.Marshal(msg)
	c.uconn.Write(buf)
	req.ReplyChannel <- &autoret{nil, nil}
}

func (c *client) shutDown() {
	if c.uconn != nil {
		c.uconn.Close()
	}
	c.closeSignal <- 1
}
