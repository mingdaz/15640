package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"math"
	"os"
)

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./miner <hostport>")
		return
	}

	// TODO: implement this!

	hostport := os.Args[1]

	params := lsp.NewParams()
	Client, err := lsp.NewClient(hostport, params)

	if err != nil {
		fmt.Printf("Error create client\n")
		return
	}

	joinreq := bitcoin.NewJoin()
	buf, err := json.Marshal(joinreq)
	if err != nil {
		fmt.Printf("Error Marshal\n")
		Client.Close()
		return
	}

	err = Client.Write(buf)
	if err != nil {
		fmt.Printf("Error join\n")
		Client.Close()
		return
	}

	for {
		readmsg, err := Client.Read()
		if err != nil {
			fmt.Printf("Lost connection\n")
			Client.Close()
			return
		}

		handlemsg := &bitcoin.Message{}
		err = json.Unmarshal(readmsg, handlemsg)
		if err != nil {
			fmt.Printf("Unmarshal error\n")
			Client.Close()
			return
		}

		var min, nonce uint64
		min = math.MaxUint64
		nonce = 0
		data := handlemsg.Data
		lower := handlemsg.Lower
		upper := handlemsg.Upper
		for i := lower; i < upper; i++ {
			hash := bitcoin.Hash(data, i)
			if hash < min {
				min = hash
				nonce = i
			}
		}

		result := bitcoin.NewResult(min, nonce)
		buf, _ := json.Marshal(result)
		Client.Write(buf)
	}
}
