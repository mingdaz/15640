package p0

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
)

type client struct {
	reader *bufio.Reader
	writer *bufio.Writer
	re     chan string
	wr     chan string
	conn   net.Conn
	id     int
}
type multiEchoServer struct {
	flag    bool
	quit    chan int
	qu      chan string
	remove  chan int
	ct      chan int
	ls      net.Listener
	clients map[int]client
}

// New creates and returns (but does not start) a new MultiEchoServer.
func New() MultiEchoServer {
	mes := new(multiEchoServer)
	mes.flag = false
	mes.quit = make(chan int)
	mes.qu = make(chan string)
	mes.remove = make(chan int)
	mes.ct = make(chan int, 1)
	mes.clients = make(map[int]client)
	mes.ls = nil
	mes.ct <- 1
	go func() {
		for {
			select {
			// wait for new message
			case newMes := <-mes.qu:
				mes.broad(newMes)
			// delete client
			case xx := <-mes.remove:
				removeclient(mes, xx)
			// close server
			case <-mes.quit:
				fmt.Errorf("Close the Server\n")
				return
				// wait for the new client
			}

		}
		fmt.Println("exit multi listen")
	}()
	return mes
}

func addClient(mes *multiEchoServer, idd int, con net.Conn) client {
	fmt.Println("add client:", idd)
	v := client{}
	v.writer = bufio.NewWriter(con)
	v.reader = bufio.NewReader(con)
	v.id = idd
	v.conn = con
	v.re = make(chan string)
	v.wr = make(chan string, 75)
	select {
	case <-mes.ct:
		mes.clients[idd] = v
		mes.ct <- 1
	}
	go func() {
		for {
			line, err := v.reader.ReadString('\n')
			if err != nil {
				mes.remove <- v.id
				return
			}
			if len(line) == 1 {
				v.re <- line
			} else {
				v.re <- (line[0:len(line)-1] + line)
			}

		}
		fmt.Println("exit client read")
	}()
	go func() {
		for data := range v.wr {
			_, err := v.writer.WriteString(data)
			if err != nil {
				mes.remove <- v.id
				return
			}
			v.writer.Flush()
		}
		fmt.Println("exit client write")
	}()
	return v
}
func (mes *multiEchoServer) broad(data string) {
	select {
	case <-mes.ct:
		for _, client := range mes.clients {
			if len(client.wr) == 75 {
				continue
			} else {
				client.wr <- data
			}
		}
		mes.ct <- 1
	}
}
func removeclient(mes *multiEchoServer, id int) {
	fmt.Println("remove client", id)
	select {
	case <-mes.ct:
		hh := mes.clients[id]
		hh.conn.Close()
		delete(mes.clients, id)
		mes.ct <- 1
	}
}
func (mes *multiEchoServer) Start(port int) error {
	if mes.flag == true {
		return fmt.Errorf("the Server has already started\n")
	}
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return fmt.Errorf("Error on listen: ", err, "\n")
	}
	mes.ls = ln
	mes.flag = true
	connNumber := 0
	fmt.Println("begin")
	go func() {
		for {
			//fmt.Println("Waiting for a connection via Accept:",connNumber)
			conn, err := ln.Accept()
			if err == nil {
				var hold = addClient(mes, connNumber, conn)
				connNumber++
				go func() {
					for {
						mes.qu <- <-hold.re
					}
					fmt.Println("exit get brading")
				}()
			}
		}
		fmt.Println("exit waiting client")
	}()
	fmt.Println("end")
	return nil
}

func (mes *multiEchoServer) Close() {
	select {
	case <-mes.ct:
		for _, v := range mes.clients {
			v.conn.Close()
		}
		mes.ct <- 1
	}
	mes.ls.Close()
	mes.quit <- 0
	mes.flag = false
}

func (mes *multiEchoServer) Count() int {
	if mes != nil && mes.flag == true {
		select {
		case <-mes.ct:
			c := len(mes.clients)
			mes.ct <- 1
			return c
		}
	} else {
		fmt.Println("This Server does not start or has already closed")
		return 0
	}
}
