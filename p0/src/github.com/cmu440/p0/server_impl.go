// Implementation of a MultiEchoServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
)

type client struct {
	id         int
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	msg_buffer chan string
	read       chan string
}

type multiEchoServer struct {
	// TODO: implement this!
	ls   net.Listener
	conn map[int]client
	str  chan string
	kill chan int
	quit chan int
	sem  chan int
	flag bool
}

// New creates and returns (but does not start) a new MultiEchoServer.
func New() MultiEchoServer {
	// TODO: implement this!
	Server := new(multiEchoServer)
	Server.ls = nil
	Server.flag = false
	Server.conn = make(map[int]client)
	Server.str = make(chan string)
	Server.kill = make(chan int)
	Server.quit = make(chan int)
	Server.sem = make(chan int, 1)

	Server.sem <- 1
	go func() {
		for {
			select {
			case msg1 := <-Server.str:
				Server.broadcast(msg1)
			case msg2 := <-Server.kill:
				deletect(Server, msg2)
			case <-Server.quit:
				return
			}
		}
	}()
	return Server
}

func (mes *multiEchoServer) Start(port int) error {
	// TODO: implement this!
	if mes.flag == true {
		return fmt.Errorf("already started\n")
	}
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return fmt.Errorf("Error;", err, "\n")
	}
	mes.ls = ln
	mes.flag = true
	count := 0
	fmt.Println("begin")
	go func() {
		for {
			conn, err := ln.Accept()
			if err == nil {
				var ct = addct(mes, count, conn)
				count++
				go func() {
					for {
						mes.str <- <-ct.read
					}
				}()
			}
		}
	}()
	fmt.Println("end")
	return nil
}

func (mes *multiEchoServer) Close() {
	// TODO: implement this!
	select {
	case <-mes.sem:
		for _, ct := range mes.conn {
			ct.conn.Close()
		}
		mes.sem <- 1
	}
	mes.ls.Close()
	mes.quit <- 0
	mes.flag = false
}

func (mes *multiEchoServer) Count() int {
	// TODO: implement this!
	if mes != nil && mes.flag == true {
		select {
		case <-mes.sem:
			c := len(mes.conn)
			mes.sem <- 1
			return c
		}
	} else {
		fmt.Println("server flag false")
		return 0
	}

}

// TODO: add additional methods/functions below!
func (mes *multiEchoServer) broadcast(data string) {
	select {
	case <-mes.sem:
		// fmt.Println("broadcast string:", data)
		for _, client := range mes.conn {
			if len(client.msg_buffer) == 75 {
				continue
			} else {
				client.msg_buffer <- data
			}
		}
		mes.sem <- 1
	}
}

func addct(mes *multiEchoServer, id int, conn net.Conn) client {
	fmt.Println("add client", id)
	ct := client{}
	ct.id = id
	ct.conn = conn
	ct.msg_buffer = make(chan string, 75)
	ct.read = make(chan string)
	ct.writer = bufio.NewWriter(conn)
	ct.reader = bufio.NewReader(conn)
	select {
	case <-mes.sem:
		mes.conn[id] = ct
		mes.sem <- 1
	}
	go func() {
		for {
			// fmt.Println("read<", id,">end")
			line, err := ct.reader.ReadString('\n')
			if err != nil {
				mes.kill <- ct.id
				return
			}
			if len(line) == 1 {
				ct.read <- line
			} else {
				ct.read <- (line[0:len(line)-1] + line)
			}
		}
	}()
	go func() {
		for data := range ct.msg_buffer {
			_, err := ct.writer.WriteString(data)
			if err != nil {
				mes.kill <- ct.id
			}
			ct.writer.Flush()
		}

	}()
	return ct
}

func deletect(mes *multiEchoServer, id int) {
	fmt.Println("delete client", id)
	select {
	case <-mes.sem:
		ct := mes.conn[id]
		ct.conn.Close()
		delete(mes.conn, id)
		mes.sem <- 1
	}
}
