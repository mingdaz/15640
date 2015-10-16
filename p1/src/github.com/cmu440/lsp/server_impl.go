// Contains the implementation of a LSP server.
// @author: Chun Chen

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	// "fmt"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type server struct {
	params            *Params
	requestChannel    chan *request
	connClose         chan int
	unackmsgBuffer    map[int]*MsgBuffer
	sendackBuffer     map[int]*MsgBuffer
	readBuffer        map[int]*MsgBuffer
	writeBuffer       map[int]*MsgBuffer
	delayRead         *list.List
	delayClose        *list.List
	sconn             *lspnet.UDPConn
	saddr             *lspnet.UDPAddr
	hostportConnIdMap map[string]int
	connIdHostportMap map[int]*lspnet.UDPAddr
	activeConn        map[int]bool
	waitSeqNum        map[int]int
	seqNum            map[int]int
	latestActiveEpoch map[int]int
	currEpoch         int
	connId            int
	serverRunning     bool
	connLostInClosing bool
}

type IdPayload struct {
	connId  int
	payload []byte
}

func NewServer(port int, params *Params) (Server, error) {
	Server := new(server)
	Server.params = params
	Server.requestChannel = make(chan *request, 100)
	Server.connClose = make(chan int)
	Server.readBuffer = make(map[int]*MsgBuffer)
	Server.writeBuffer = make(map[int]*MsgBuffer)
	Server.unackmsgBuffer = make(map[int]*MsgBuffer)
	Server.sendackBuffer = make(map[int]*MsgBuffer)
	Server.delayRead = list.New()
	Server.delayClose = list.New()
	Server.hostportConnIdMap = make(map[string]int)
	Server.connIdHostportMap = make(map[int]*lspnet.UDPAddr)
	Server.activeConn = make(map[int]bool)
	Server.waitSeqNum = make(map[int]int)
	Server.seqNum = make(map[int]int)
	Server.latestActiveEpoch = make(map[int]int)
	Server.currEpoch = 0
	Server.connId = 0
	Server.serverRunning = true
	Server.connLostInClosing = false
	Server.saddr, _ = lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	Server.sconn, _ = lspnet.ListenUDP("udp", Server.saddr)

	go func() {
		buf := make([]byte, 1500)
		for {
			select {
			case <-Server.connClose:
				return
			default:
				n, addr, err := Server.sconn.ReadFromUDP(buf[0:])
				if err == nil {
					msg := &Message{}
					err = json.Unmarshal(buf[:n], msg)
					if err == nil {
						packet := &Packet{msg, addr}
						req := &request{ReqReceiveMsg, packet, make(chan *autoret)}
						Server.requestChannel <- req
					}
				}
			}
		}
	}()

	go func() {
		for Server.serverRunning {
			req := <-Server.requestChannel
			switch req.Type {
			case ReqReceiveMsg:
				Server.handleReceivedMsg(req)
			case ReqEpoch:
				Server.handleEpoch()
			case ReqRead:
				Server.handleRead(req)
			case ReqWrite:
				Server.handleWrite(req)
			case ReqCloseConn:
				Server.handleCloseConn(req)
			case ReqClose:
				Server.handleClose(req)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-time.After(time.Millisecond * time.Duration(params.EpochMillis)):
				req := &request{ReqEpoch, nil, make(chan *autoret)}
				Server.requestChannel <- req
			case <-Server.connClose:
				return
			}
		}
	}()
	return Server, nil
}

func (s *server) Read() (int, []byte, error) {
	retVal, err := s.doRequest(ReqRead, nil)
	bundle := retVal.(*IdPayload)
	return bundle.connId, bundle.payload, err
}

func (s *server) Write(connID int, payload []byte) error {
	bundle := &IdPayload{connID, payload}
	_, err := s.doRequest(ReqWrite, bundle)
	return err
}

func (s *server) CloseConn(connID int) error {
	_, err := s.doRequest(ReqCloseConn, connID)
	return err
}

func (s *server) Close() error {
	_, err := s.doRequest(ReqClose, nil)
	return err
}

// submit the request to reqeust channel (requestChannel). waiting for the event handler to handle
func (s *server) doRequest(op ReqType, val interface{}) (interface{}, error) {
	req := &request{op, val, make(chan *autoret)}
	s.requestChannel <- req
	retVal := <-req.ReplyChannel
	return retVal.Value, retVal.Err
}

// do corresponding actions when a new message comes from network handler
func (s *server) handleReceivedMsg(req *request) {
	clientAddr := req.Value.(*Packet).raddr
	receivedMsg := req.Value.(*Packet).msg
	// fmt.Println(receivedMsg.String())
	switch receivedMsg.Type {
	case MsgConnect:
		// if the hostport was never seen, or the connection is lost/closed, and the server is not closed,
		// establish a connection and initiate related resources
		if connId := s.hostportConnIdMap[clientAddr.String()]; s.delayClose.Len() == 0 && connId == 0 {
			s.connId += 1
			s.hostportConnIdMap[clientAddr.String()] = s.connId
			s.connIdHostportMap[s.connId] = clientAddr
			ackMsg := NewAck(s.connId, 0)
			buf, _ := json.Marshal(ackMsg)
			s.sconn.WriteToUDP(buf, clientAddr)
			s.readBuffer[s.connId] = NewMsgBuffer()
			s.writeBuffer[s.connId] = NewMsgBuffer()
			s.unackmsgBuffer[s.connId] = NewMsgBuffer()
			s.sendackBuffer[s.connId] = NewMsgBuffer()
			s.waitSeqNum[s.connId] = 1
			s.seqNum[s.connId] = 0
			s.latestActiveEpoch[s.connId] = s.currEpoch
			s.activeConn[s.connId] = true
		}
	case MsgAck:
		// if the connection with this client is established before, and not lost
		if clientConnId := s.hostportConnIdMap[clientAddr.String()]; clientConnId > 0 {
			// set the latest active epoch of the specified connection
			s.latestActiveEpoch[clientConnId] = s.currEpoch

			unackmsgBuffer := s.unackmsgBuffer[clientConnId]
			msgExist := unackmsgBuffer.Delete(receivedMsg.SeqNum)

			// move messages from write MsgBuffer to unAckedMsg MsgBuffer and send them out via network
			// if their seq nums are in the sliding window
			writeBuffer := s.writeBuffer[clientConnId]
			if msgExist && writeBuffer.Length() > 0 {
				if unackmsgBuffer.Length() == 0 {
					sentMsg := writeBuffer.Pop()
					buf, _ := json.Marshal(sentMsg)
					s.sconn.WriteToUDP(buf, clientAddr)
					unackmsgBuffer.Insert(sentMsg)
				}

				wLeft := unackmsgBuffer.Front().SeqNum
				wSize := s.params.WindowSize
				for writeBuffer.Length() != 0 {
					if writeBuffer.Front().SeqNum-wSize >= wLeft {
						break
					} else {
						sentMsg := writeBuffer.Pop()
						unackmsgBuffer.Insert(sentMsg)
						buf, _ := json.Marshal(sentMsg)
						s.sconn.WriteToUDP(buf, clientAddr)
					}
				}
			}

			// if the connection is closed/the server is closed and all pending messages are sent and acked,
			// clean up all resources that relates to the connection
			if msgExist && unackmsgBuffer.Length() == 0 && (!s.activeConn[clientConnId] || s.delayClose.Len() > 0) {
				delete(s.hostportConnIdMap, clientAddr.String())
				delete(s.connIdHostportMap, clientConnId)
				delete(s.readBuffer, clientConnId)
				delete(s.writeBuffer, clientConnId)
				delete(s.unackmsgBuffer, clientConnId)
				delete(s.sendackBuffer, clientConnId)
				delete(s.latestActiveEpoch, clientConnId)
				delete(s.waitSeqNum, clientConnId)
				delete(s.seqNum, clientConnId)
			}
		}
	case MsgData:
		// don't receive data message if the connection is closed/lost or the server is closed
		if clientConnId := s.hostportConnIdMap[clientAddr.String()]; clientConnId > 0 && s.activeConn[clientConnId] && s.delayClose.Len() == 0 {
			// set the latest active epoch of the specified connection
			s.latestActiveEpoch[clientConnId] = s.currEpoch

			readBuffer := s.readBuffer[clientConnId]

			// ignore data messages whose seq num is smaller than the expected seq num
			// epoch handler will resend the acks that haven't been received on the other side (if their seq num is smaller than expected seq num)
			// and the same size of sliding window on both sending and receiving side guarantee the correctness, otherwise we may have to send ack
			// for every data message we receive no matter whether its seq num is larger or smaller than the expected seq num
			if receivedMsg.SeqNum >= s.waitSeqNum[clientConnId] {
				readBuffer.Insert(receivedMsg)
				// send ack for the data message and put the ack into latestAck MsgBuffer
				sendackBuffer := s.sendackBuffer[clientConnId]
				ackMsg := NewAck(clientConnId, receivedMsg.SeqNum)
				buf, _ := json.Marshal(ackMsg)
				s.sconn.WriteToUDP(buf, clientAddr)

				sendackBuffer.Insert(ackMsg)
				// adjust the MsgBuffer to conform sliding window size
				sendackBuffer.Fitwindow(s.params.WindowSize)
				// fmt.Printf("%d\n", sendackBuffer.Length())
				// wake a deferred read reqeusts up if the seq num of incoming message equals the expected seq num of the connection
				if receivedMsg.SeqNum == s.waitSeqNum[clientConnId] && s.delayRead.Len() > 0 {
					readReq := s.delayRead.Front().Value.(*request)
					s.delayRead.Remove(s.delayRead.Front())
					s.handleRead(readReq)
				}
			}
		}
	}
}

// do corresponding actions when epoch fires
func (s *server) handleEpoch() {
	s.currEpoch += 1

	// detect connection lost
	for connId, latestActiveEpoch := range s.latestActiveEpoch {
		if s.currEpoch-latestActiveEpoch >= s.params.EpochLimit {
			if s.delayClose.Len() > 0 {
				s.connLostInClosing = true
			}
			delete(s.activeConn, connId)
			// wake one deferred read up if no message is ready in read MsgBuffer of the connection
			// if condition is matched, then clean up all realted resources of the connection including the read MsgBuffer and expected seq num
			// otherwise clean up all related resources except read MsgBuffer and expected seq num since we allow further Read on a lost connection
			if (s.readBuffer[connId].Length() == 0 || s.readBuffer[connId].Front().SeqNum != s.waitSeqNum[connId]) &&
				s.delayRead.Len() > 0 {
				// since the connection has nothing to read and unblocks a deferred read, clean up its readMsgBuffer and waitSeqNum
				delete(s.readBuffer, connId)
				delete(s.waitSeqNum, connId)
				readReq := s.delayRead.Front().Value.(*request)
				s.delayRead.Remove(s.delayRead.Front())
				retVal := &autoret{&IdPayload{connId, nil}, errors.New("s1")}
				readReq.ReplyChannel <- retVal
			}
			// clean up all remaining related resources of this connection
			clientAddr := s.connIdHostportMap[connId]
			delete(s.hostportConnIdMap, clientAddr.String())
			delete(s.connIdHostportMap, connId)
			delete(s.writeBuffer, connId)
			delete(s.unackmsgBuffer, connId)
			delete(s.sendackBuffer, connId)
			delete(s.latestActiveEpoch, connId)
			delete(s.seqNum, connId)
		}
	}

	// don't resend latest ack if the server is closed
	if s.delayClose.Len() == 0 {
		for connId, sendackBuffer := range s.sendackBuffer {
			// if the connection is not closed
			if s.activeConn[connId] {
				// if the connection hasn't received any data message after connection is established, send a ack with seq num 0
				if sendackBuffer.Length() == 0 {
					ackMsg := NewAck(connId, 0)
					clientAddr := s.connIdHostportMap[connId]
					buf, _ := json.Marshal(ackMsg)
					s.sconn.WriteToUDP(buf, clientAddr)
				} else {
					clientAddr := s.connIdHostportMap[connId]
					latestAckMsg := sendackBuffer.GetAll()
					for _, msg := range latestAckMsg {
						buf, _ := json.Marshal(msg)
						s.sconn.WriteToUDP(buf, clientAddr)
					}
				}
			}
		}
	}

	// resend sent but unacked data messages, even though the connection is closed or the server is closed
	nonEmptyBuffer := 0
	for connId, unackmsgBuffer := range s.unackmsgBuffer {
		if unackmsgBuffer.Length() > 0 || s.writeBuffer[connId].Length() > 0 {
			nonEmptyBuffer += 1
		}

		clientAddr := s.connIdHostportMap[connId]
		unAckMsg := unackmsgBuffer.GetAll()
		for _, msg := range unAckMsg {
			buf, _ := json.Marshal(msg)
			s.sconn.WriteToUDP(buf, clientAddr)
		}
	}

	// if there is deferred Close() request and all pending messages are sent and acked, wake the pending Close request up
	if s.delayClose.Len() > 0 && nonEmptyBuffer == 0 {
		for e := s.delayClose.Front(); e != nil; e = e.Next() {
			closeReq := e.Value.(*request)
			var retVal *autoret
			if s.connLostInClosing {
				retVal = &autoret{nil, errors.New("s2")}
			} else {
				retVal = &autoret{nil, nil}
			}
			closeReq.ReplyChannel <- retVal
		}
		// stop the main handler routine
		s.serverRunning = false
		s.shutDown()
	}
}

// handle user read request
func (s *server) handleRead(req *request) {
	// return an error if the server is closed
	if s.delayClose.Len() > 0 {
		req.ReplyChannel <- &autoret{nil, errors.New("s3")}
		return
	}

	// check if there is any data message ready for read in any read MsgBuffer
	for connId, readBuffer := range s.readBuffer {
		if readBuffer.Length() > 0 && readBuffer.Front().SeqNum == s.waitSeqNum[connId] {
			readMsg := readBuffer.Pop()
			s.waitSeqNum[connId] += 1
			retVal := &autoret{&IdPayload{connId, readMsg.Payload}, nil}
			req.ReplyChannel <- retVal
			return
		}
	}

	// if nothing can be read from any read MsgBuffer, check if there is any lost connection in them
	for connId, _ := range s.readBuffer {
		// if the connection is lost and has no data message for reading
		if !s.activeConn[connId] {
			retVal := &autoret{&IdPayload{connId, nil}, errors.New("s4")}
			req.ReplyChannel <- retVal
			// clean up the realted resource of the lost connection
			delete(s.readBuffer, connId)
			delete(s.waitSeqNum, connId)
			return
		}
	}

	// if nothing can be read from any read MsgBuffer and no lost connection with no data ready for reading, defer the Read
	s.delayRead.PushBack(req)
}

// handle user write request
func (s *server) handleWrite(req *request) {
	connId := req.Value.(*IdPayload).connId
	payload := req.Value.(*IdPayload).payload
	// return an error if the connection is closed/lost, or the server is closed
	if s.delayClose.Len() > 0 || !s.activeConn[connId] {
		req.ReplyChannel <- &autoret{nil, errors.New("s5")}
	} else {
		// if the conn id exists
		if clientAddr := s.connIdHostportMap[connId]; clientAddr != nil {
			s.seqNum[connId] += 1
			seqNum := s.seqNum[connId]
			windowSize := s.params.WindowSize
			sentMsg := NewData(connId, seqNum, payload, nil)
			unackmsgBuffer := s.unackmsgBuffer[connId]
			writeBuffer := s.writeBuffer[connId]
			if writeBuffer.Length() == 0 &&
				(unackmsgBuffer.Length() == 0 || seqNum-windowSize < unackmsgBuffer.Front().SeqNum) {
				unackmsgBuffer.Insert(sentMsg)
				buf, _ := json.Marshal(sentMsg)
				s.sconn.WriteToUDP(buf, clientAddr)
			} else {
				writeBuffer.Insert(sentMsg)
			}
			req.ReplyChannel <- &autoret{nil, nil}
		} else {
			req.ReplyChannel <- &autoret{nil, errors.New("s6")}
		}
	}
}

// handle user close a specified connection
func (s *server) handleCloseConn(req *request) {
	connId := req.Value.(int)
	// if the connection is not closed or lost
	if s.activeConn[connId] {
		// unblock one delay read
		if s.delayRead.Len() > 0 {
			readReq := s.delayClose.Front().Value.(*request)
			s.delayRead.Remove(s.delayRead.Front())
			retVal := &autoret{&IdPayload{connId, nil}, errors.New("s7")}
			readReq.ReplyChannel <- retVal
		}

		clientAddr := s.connIdHostportMap[connId]
		// if there is no pending messages to be resent and acked, clean up resources that are used for resending unAcked messages
		if s.unackmsgBuffer[connId].Length() == 0 && s.writeBuffer[connId].Length() == 0 {
			delete(s.writeBuffer, connId)
			delete(s.unackmsgBuffer, connId)
			delete(s.hostportConnIdMap, clientAddr.String())
			delete(s.connIdHostportMap, connId)
		}

		delete(s.readBuffer, connId)
		delete(s.sendackBuffer, connId)
		delete(s.latestActiveEpoch, connId)
		delete(s.waitSeqNum, connId)
		delete(s.seqNum, connId)

		delete(s.activeConn, connId)
		req.ReplyChannel <- &autoret{nil, nil}
	} else {
		req.ReplyChannel <- &autoret{nil, errors.New("s8")}
	}
}

// handle user close the server
func (s *server) handleClose(req *request) {
	// unblcok all delay reads
	for s.delayRead.Len() != 0 {
		readReq := s.delayClose.Front().Value.(*request)
		s.delayRead.Remove(s.delayRead.Front())
		retVal := &autoret{&IdPayload{0, nil}, errors.New("s9")}
		readReq.ReplyChannel <- retVal
	}

	nonEmptyBuffer := 0
	for connId, unackmsgBuffer := range s.unackmsgBuffer {
		if unackmsgBuffer.Length() > 0 || s.writeBuffer[connId].Length() > 0 {
			nonEmptyBuffer += 1
			break
		}
	}
	// if there is no sent but unacked messages, shut down the whole server immediately
	if nonEmptyBuffer == 0 {
		s.shutDown()
		req.ReplyChannel <- &autoret{nil, nil}
		s.serverRunning = false
	} else {
		// otherwise defer the Close request
		s.delayClose.PushBack(req)
	}
}

// shut down the network handler and epoch timer go routines
func (s *server) shutDown() {
	if s.sconn != nil {
		s.sconn.Close()
	}
	s.connClose <- 1
}

// go routine which handles multiple requests (notification) from reqeust channel (requestChannel),
// including reqeusts from user and notifications from epoch timer and network handler
