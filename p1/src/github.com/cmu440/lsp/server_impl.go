package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
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
				Server.myRead(req)
			case ReqWrite:
				Server.myWrite(req)
			case ReqCloseConn:
				Server.myCloseConn(req)
			case ReqClose:
				Server.myClose(req)
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
	v, err := s.handleRequest(ReqRead, nil)
	p := v.(*IdPayload)
	return p.connId, p.payload, err
}

func (s *server) Write(connID int, payload []byte) error {
	p := &IdPayload{connID, payload}
	_, err := s.handleRequest(ReqWrite, p)
	return err
}

func (s *server) CloseConn(connID int) error {
	_, err := s.handleRequest(ReqCloseConn, connID)
	return err
}

func (s *server) Close() error {
	_, err := s.handleRequest(ReqClose, nil)
	return err
}

func (s *server) handleRequest(r ReqType, val interface{}) (interface{}, error) {
	req := &request{r, val, make(chan *autoret)}
	s.requestChannel <- req
	v := <-req.ReplyChannel
	return v.Value, v.Err
}

func (s *server) handleReceivedMsg(req *request) {
	clientAddr := req.Value.(*Packet).raddr
	receivedMsg := req.Value.(*Packet).msg
	switch receivedMsg.Type {
	case MsgConnect:
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
		if clientConnId := s.hostportConnIdMap[clientAddr.String()]; clientConnId > 0 {
			s.latestActiveEpoch[clientConnId] = s.currEpoch
			unackmsgBuffer := s.unackmsgBuffer[clientConnId]
			haveMsg := unackmsgBuffer.Delete(receivedMsg.SeqNum)
			writeBuffer := s.writeBuffer[clientConnId]
			if haveMsg && writeBuffer.Length() > 0 {
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
			if haveMsg && unackmsgBuffer.Length() == 0 && (!s.activeConn[clientConnId] || s.delayClose.Len() > 0) {
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
		if clientConnId := s.hostportConnIdMap[clientAddr.String()]; clientConnId > 0 && s.activeConn[clientConnId] && s.delayClose.Len() == 0 {
			s.latestActiveEpoch[clientConnId] = s.currEpoch
			readBuffer := s.readBuffer[clientConnId]
			if receivedMsg.SeqNum >= s.waitSeqNum[clientConnId] {
				readBuffer.Insert(receivedMsg)
				sendackBuffer := s.sendackBuffer[clientConnId]
				ackMsg := NewAck(clientConnId, receivedMsg.SeqNum)
				buf, _ := json.Marshal(ackMsg)
				s.sconn.WriteToUDP(buf, clientAddr)
				sendackBuffer.Insert(ackMsg)
				sendackBuffer.Fitwindow(s.params.WindowSize)
				if receivedMsg.SeqNum == s.waitSeqNum[clientConnId] && s.delayRead.Len() > 0 {
					readReq := s.delayRead.Front().Value.(*request)
					s.delayRead.Remove(s.delayRead.Front())
					s.myRead(readReq)
				}
			}
		}
	}
}

func (s *server) handleEpoch() {
	s.currEpoch += 1

	for connId, latestActiveEpoch := range s.latestActiveEpoch {
		if s.currEpoch-latestActiveEpoch >= s.params.EpochLimit {
			if s.delayClose.Len() > 0 {
				s.connLostInClosing = true
			}
			delete(s.activeConn, connId)
			if (s.readBuffer[connId].Length() == 0 || s.readBuffer[connId].Front().SeqNum != s.waitSeqNum[connId]) &&
				s.delayRead.Len() > 0 {
				delete(s.readBuffer, connId)
				delete(s.waitSeqNum, connId)
				readReq := s.delayRead.Front().Value.(*request)
				s.delayRead.Remove(s.delayRead.Front())
				v := &autoret{&IdPayload{connId, nil}, errors.New("s1")}
				readReq.ReplyChannel <- v
			}
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

	if s.delayClose.Len() == 0 {
		for connId, sendackBuffer := range s.sendackBuffer {
			if s.activeConn[connId] {
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

	if s.delayClose.Len() > 0 && nonEmptyBuffer == 0 {
		for e := s.delayClose.Front(); e != nil; e = e.Next() {
			closeReq := e.Value.(*request)
			var v *autoret
			if s.connLostInClosing {
				v = &autoret{nil, errors.New("s2")}
			} else {
				v = &autoret{nil, nil}
			}
			closeReq.ReplyChannel <- v
		}
		s.serverRunning = false
		s.shutDown()
	}
}

func (s *server) myRead(req *request) {
	if s.delayClose.Len() > 0 {
		req.ReplyChannel <- &autoret{nil, errors.New("s3")}
		return
	}

	for connId, readBuffer := range s.readBuffer {
		if readBuffer.Length() > 0 && readBuffer.Front().SeqNum == s.waitSeqNum[connId] {
			readMsg := readBuffer.Pop()
			s.waitSeqNum[connId] += 1
			v := &autoret{&IdPayload{connId, readMsg.Payload}, nil}
			req.ReplyChannel <- v
			return
		}
	}

	for connId, _ := range s.readBuffer {
		if !s.activeConn[connId] {
			v := &autoret{&IdPayload{connId, nil}, errors.New("s4")}
			req.ReplyChannel <- v
			delete(s.readBuffer, connId)
			delete(s.waitSeqNum, connId)
			return
		}
	}

	s.delayRead.PushBack(req)
}

func (s *server) myWrite(req *request) {
	connId := req.Value.(*IdPayload).connId
	payload := req.Value.(*IdPayload).payload
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

func (s *server) myCloseConn(req *request) {
	connId := req.Value.(int)
	if s.activeConn[connId] {
		if s.delayRead.Len() > 0 {
			readReq := s.delayClose.Front().Value.(*request)
			s.delayRead.Remove(s.delayRead.Front())
			v := &autoret{&IdPayload{connId, nil}, errors.New("s7")}
			readReq.ReplyChannel <- v
		}
		clientAddr := s.connIdHostportMap[connId]
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

func (s *server) myClose(req *request) {
	for s.delayRead.Len() != 0 {
		readReq := s.delayClose.Front().Value.(*request)
		s.delayRead.Remove(s.delayRead.Front())
		v := &autoret{&IdPayload{0, nil}, errors.New("s9")}
		readReq.ReplyChannel <- v
	}
	nonEmptyBuffer := 0
	for connId, unackmsgBuffer := range s.unackmsgBuffer {
		if unackmsgBuffer.Length() > 0 || s.writeBuffer[connId].Length() > 0 {
			nonEmptyBuffer += 1
			break
		}
	}
	if nonEmptyBuffer == 0 {
		s.shutDown()
		req.ReplyChannel <- &autoret{nil, nil}
		s.serverRunning = false
	} else {
		s.delayClose.PushBack(req)
	}
}

func (s *server) shutDown() {
	if s.sconn != nil {
		s.sconn.Close()
	}
	s.connClose <- 1
}
