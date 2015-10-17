package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	// "log"
	"math"
	"os"
	"strconv"
)

type MinerJob struct {
	clientid int
	msg      *bitcoin.Message
}

type CLientState struct {
	jobleft int
	minHash uint64
	nonce   uint64
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./server <port>")
		return
	}

	// TODO: implement this!
	const thres = 10000
	// const name = "log.txt"
	// const flag = os.O_RDWR | os.O_CREATE
	// const perm = os.FileMode(0666)
	// var FLOG *log.Logger

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("port Error")
		return
	}

	param := lsp.NewParams()
	Server, err := lsp.NewServer(port, param)
	if err != nil {
		fmt.Println("Error create server")
		return
	}

	clients := make(map[int]*CLientState)
	workminers := make(map[int]*MinerJob)
	idleMiner := make(map[int]bool)
	queue := list.New()

	// file, err := os.OpenFile(name, flag, perm)
	// if err != nil {
	// 	return
	// }
	// defer file.Close()
	// FLOG = log.New(file, "", log.Lshortfile|log.Lmicroseconds)

	for {
		// assign job to miner
		if queue.Len() > 0 {
			for connId, _ := range idleMiner {
				for queue.Len() > 0 {
					job := queue.Front().Value.(*MinerJob)
					if clients[job.clientid] != nil {
						buf, err := json.Marshal(job.msg)
						if err == nil {
							err = Server.Write(connId, buf)
						}
						if err == nil {
							queue.Remove(queue.Front())
							workminers[connId] = job
							delete(idleMiner, connId)
						}
						break
					} else {
						queue.Remove((queue.Front()))
					}
				}
				if queue.Len() == 0 {
					break
				}
			}
		}
		// read data
		connId, data, err := Server.Read()
		var msg *bitcoin.Message
		if err != nil {
			// FLOG.Println("handle failure: ", connId)

			// handle failure
			if job, ok := workminers[connId]; ok {
				// miner client
				if job != nil {
					queue.PushFront(job)
				}
				delete(idleMiner, connId)
				delete(workminers, connId)
			} else if clients[connId] != nil {
				// client
				delete(clients, connId)
			}
		} else {
			//handle incoming message
			msg = &bitcoin.Message{}
			err = json.Unmarshal(data, msg)

			if err != nil {
				fmt.Println("Error unmarshal")
				continue
			}
			// FLOG.Println("handle income message: ", msg)

			switch msg.Type {
			case bitcoin.Join:
				workminers[connId] = nil
				idleMiner[connId] = true
			case bitcoin.Request:
				if clients[connId] == nil {
					//partion job in queue
					data := msg.Data
					lower := msg.Lower
					upper := msg.Upper
					if upper-lower < thres {
						newjob := &MinerJob{}
						newjob.clientid = connId
						newjob.msg = bitcoin.NewRequest(data, lower, upper)
						queue.PushBack(newjob)
						newcli := &CLientState{}
						newcli.jobleft = 1
						newcli.minHash = math.MaxUint64
						newcli.nonce = 0
						clients[connId] = newcli
					} else {
						numminers := len(workminers)
						if numminers == 0 {
							numminers = 1
						}
						newcli := &CLientState{}
						newcli.jobleft = 1
						newcli.minHash = math.MaxUint64
						newcli.nonce = 0
						clients[connId] = newcli
						interval := (upper - lower + 1) / uint64(numminers)
						for i := 0; i < numminers; i++ {
							tmplower := lower + uint64(i)*interval
							var tmphigher uint64
							if i == numminers-1 {
								tmphigher = uint64(numminers)
							} else {
								tmphigher = tmplower + interval - 1
							}
							newjob := &MinerJob{}
							newjob.clientid = connId
							newjob.msg = bitcoin.NewRequest(data, tmplower, tmphigher)
							queue.PushBack(newjob)
						}
					}
				}
			case bitcoin.Result:

				//update result
				clientconnid := workminers[connId].clientid
				clist := clients[clientconnid]
				idleMiner[connId] = true
				workminers[connId] = nil
				if clist != nil {
					clist.jobleft--
					if msg.Hash < clist.minHash {
						clist.minHash = msg.Hash
						clist.nonce = msg.Nonce
					}

					if clist.jobleft == 0 {
						resultmsg := bitcoin.NewResult(clist.minHash, clist.nonce)
						buf, err := json.Marshal(resultmsg)
						if err == nil {
							// FLOG.Println("send back result: ", resultmsg.Hash, resultmsg.Nonce)

							Server.Write(clientconnid, buf)
						}
						delete(clients, clientconnid)
					}
				}
			}
		}
	}
}
