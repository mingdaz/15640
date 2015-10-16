// Contains the implementation of a LSP client.
// @author: Chun Chen

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	// "strconv"
	// "fmt"
	"time"
)

type client struct {
	params *Params
	// networkUtility        *networkUtility
	uaddr                 *lspnet.UDPAddr
	uconn                 *lspnet.UDPConn
	requestChannel        chan *request
	closeSignal           chan struct{}
	connLostSignal        chan struct{}
	connEstablishedSignal chan struct{}
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
	c := &client{
		params:                params,
		requestChannel:        make(chan *request, 100),
		closeSignal:           make(chan struct{}),
		connLostSignal:        make(chan struct{}, 1),
		connEstablishedSignal: make(chan struct{}, 1),
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
	// c.networkUtility = NewNetworkUtility(c.requestChannel)
	// err := c.networkUtility.dial(hostport)
	// if err != nil {
	// 	return nil, err
	// }
	go func() {
		var req *request
		for c.clientRunning {
			// if the connection lost when trying establishing the connection
			if c.connId == 0 && c.connLost {
				return
			}
			// unblock deferred read if the read buffer is ready or the connection is closed or lost
			if c.delayRead.Len() > 0 && ((c.readBuffer.Length() > 0 && c.readBuffer.Front().SeqNum == c.waitSeqNum) || c.connLost || c.delayClose.Len() > 0) {
				req = c.delayRead.Front().Value.(*request)
				c.delayRead.Remove(c.delayRead.Front())
			} else if c.delayClose.Len() > 0 && ((c.writeBuffer.Length() == 0 && c.unackmsgBuffer.Length() == 0) || c.connLost) {
				// unblock deferred close if all pending messages are sent and acked or connection is lost
				req = c.delayClose.Front().Value.(*request)
				c.delayClose.Remove(c.delayClose.Front())
			} else {
				// if there is no need to unblock any deferred request, get new request from reqeust channel
				req = <-c.requestChannel
				// defer read or close request if necessary
				if req.Type == ReqRead && (c.readBuffer.Length() == 0 || c.readBuffer.Front().SeqNum != c.waitSeqNum) {
					c.delayRead.PushBack(req)
					continue
				}
				if req.Type == ReqClose && (c.writeBuffer.Length() > 0 || c.unackmsgBuffer.Length() > 0) {
					c.delayClose.PushBack(req)
					continue
				}
			}

			// handle request according to its type
			switch req.Type {
			case ReqRead:
				c.handleRead(req)
			case ReqWrite:
				c.handleWrite(req)
			case ReqClose:
				c.handleClose(req)
			case ReqConnId:
				c.handleConnId(req)
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

	// waiting for signal whether the connection is established or lost
	select {
	case <-c.connEstablishedSignal:
		return c, nil
	case <-c.connLostSignal:
		return nil, errors.New("c1")
	}

}

func (c *client) ConnID() int {
	ret, _ := c.doRequest(ReqConnId, nil)
	return ret.(int)
}

func (c *client) Read() ([]byte, error) {
	ret, err := c.doRequest(ReqRead, nil)
	if err != nil {
		return nil, err
	} else {
		return ret.([]byte), nil
	}
}

func (c *client) Write(payload []byte) error {
	_, err := c.doRequest(ReqWrite, payload)
	return err
}

func (c *client) Close() error {
	_, err := c.doRequest(ReqClose, nil)
	return err
}

func (c *client) connect() error {
	_, err := c.doRequest(ReqConnect, nil)
	return err
}

// submit the request to reqeust channel (requestChannel). waiting for the event handler to handle
func (c *client) doRequest(op ReqType, val interface{}) (interface{}, error) {
	req := &request{op, val, make(chan *autoret)}
	c.requestChannel <- req
	retV := <-req.ReplyChannel
	return retV.Value, retV.Err
}

// handle user read request
func (c *client) handleRead(req *request) {
	// if client is closed, return an error
	if c.delayClose.Len() > 0 {
		req.ReplyChannel <- &autoret{nil, errors.New("c2")}
		return
	}
	// if connection is lost and there is nothing to read in the read buffer, return an error
	if c.readBuffer.Length() == 0 || c.readBuffer.Front().SeqNum != c.waitSeqNum {
		req.ReplyChannel <- &autoret{nil, errors.New("c3")}
		return
	}

	// return the expected message in the read buffer
	readMsg := c.readBuffer.Front()
	c.readBuffer.Pop()
	c.waitSeqNum++
	req.ReplyChannel <- &autoret{readMsg.Payload, nil}
}

// handle user write request
func (c *client) handleWrite(req *request) {
	// if connection is lost, return an error
	if c.connLost {
		req.ReplyChannel <- &autoret{nil, errors.New("c4")}
		return
	}
	// if client is closed, return an error
	if c.delayClose.Len() > 0 {
		req.ReplyChannel <- &autoret{nil, errors.New("c5")}
		return
	}

	// if there is space in unackmsgBuffer, insert the message into the buffer and send it out via network
	// otherwise insert it into write buffer
	payload := req.Value.([]byte)
	c.seqNum++
	sentMsg := NewData(c.connId, c.seqNum, payload, nil)
	// fmt.Println("[client]" + sentMsg.String())
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

// do corresponding actions when epoch fires
func (c *client) handleEpoch() {
	c.countEpoch++
	// detect if connection is lost
	if c.countEpoch >= c.params.EpochLimit {
		c.connLost = true
		c.connLostSignal <- struct{}{}
		c.shutDown()
		return
	}
	// if the connection is established but no data messages have been received, send a ack message with seq number 0
	if c.connId > 0 && c.sendackBuffer.Length() == 0 && c.delayClose.Len() == 0 {
		ackMsg := NewAck(c.connId, 0)
		// fmt.Println("[c]" + ackMsg.String())
		buf, _ := json.Marshal(ackMsg)
		c.uconn.Write(buf)
	}

	// resend unacknowledged messages in the buffer
	unAckMsg := c.unackmsgBuffer.GetAll()
	for _, msg := range unAckMsg {
		// fmt.Println("[c]" + msg.String())
		buf, _ := json.Marshal(msg)
		c.uconn.Write(buf)
	}

	// resend latest sent acknowledgements
	if c.delayClose.Len() == 0 {
		latestAck := c.sendackBuffer.GetAll()
		for _, msg := range latestAck {
			// fmt.Println("[c]" + msg.String())
			buf, _ := json.Marshal(msg)
			c.uconn.Write(buf)
		}
	}
}

// do corresponding actions when a new message comes from network handler
func (c *client) handleReceivedMsg(req *request) {
	// update the latest active epoch interval
	c.countEpoch = 0
	receivedMsg := req.Value.(*Packet).msg
	switch receivedMsg.Type {
	case MsgAck:
		// remove the message that has received acknowledgement from the buffer
		msgExist := c.unackmsgBuffer.Delete(receivedMsg.SeqNum)

		// if some messages in the buffer receive ack, check whether messages in the write buffer
		// can be moved into the unackmsgBuffer according to the sliding window size
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
		// check if the ack message is an ack for the connection message
		if msgExist && receivedMsg.SeqNum == 0 {
			c.connId = receivedMsg.ConnID
			c.connEstablishedSignal <- struct{}{}
		}
	case MsgData:
		// if connection is not established or the client is closed
		if c.connId == 0 || c.delayClose.Len() > 0 {
			return
		}

		// ignore messages whose seq num is smaller than expected seq num
		// epoch handler will resend the acks that haven't been received on the other side (if their seq num is smaller than expected seq num)
		// and the same size of sliding window on both sending and receiving side guarantee the correctness, otherwise we may have to send ack
		// for every data message we receive no matter whether its seq num is larger or smaller than the expected seq num
		if receivedMsg.SeqNum >= c.waitSeqNum {
			c.readBuffer.Insert(receivedMsg)
			// send ack for the data message and store the ack to latest sent ack buffer
			ackMsg := NewAck(c.connId, receivedMsg.SeqNum)
			// send ack message out
			buf, _ := json.Marshal(ackMsg)
			c.uconn.Write(buf)

			c.sendackBuffer.Insert(ackMsg)
			c.sendackBuffer.Fitwindow(c.params.WindowSize)
		}
	}
}

// handle user get conn id request
func (c *client) handleConnId(req *request) {
	req.ReplyChannel <- &autoret{c.connId, nil}
}

// handle user close client request
func (c *client) handleClose(req *request) {
	// if connection doesn't get lost when user tries to close the client, shut down the epoch timer and network handler go routines
	if !c.connLost {
		c.shutDown()
	}
	// if there is any pending message that is not sent or acked, this implies the connection get lost when user tries to close the client
	// then return an error
	if c.writeBuffer.Length() > 0 || c.unackmsgBuffer.Length() > 0 {
		req.ReplyChannel <- &autoret{nil, errors.New("c6")}
	} else {
		req.ReplyChannel <- &autoret{nil, nil}
	}

	// unblock all close function
	for e := c.delayClose.Front(); e != nil; e = e.Next() {
		req = e.Value.(*request)
		if c.writeBuffer.Length() > 0 || c.unackmsgBuffer.Length() > 0 {
			req.ReplyChannel <- &autoret{nil, errors.New("c7")}
		} else {
			req.ReplyChannel <- &autoret{nil, nil}
		}
	}
	// set the running flag as false so the event handler will return from infinite loop
	c.clientRunning = false
}

// handle connect request when creating new client, will send a connect message to server
func (c *client) handleConnect(req *request) {
	msg := NewConnect()
	c.unackmsgBuffer.Insert(msg)
	buf, _ := json.Marshal(msg)
	c.uconn.Write(buf)
	req.ReplyChannel <- &autoret{nil, nil}
}

// shut down the network handler and epoch timer go routines
func (c *client) shutDown() {
	if c.uconn != nil {
		c.uconn.Close()
	}
	c.closeSignal <- struct{}{}
}

// go routine which handles multiple requests (notification) from reqeust channel (requestChannel),
// including reqeusts from user and notifications from epoch timer and network handler
