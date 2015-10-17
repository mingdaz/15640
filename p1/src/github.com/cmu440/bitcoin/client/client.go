package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
	"strconv"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./client <hostport> <message> <maxNonce>")
		return
	}

	// TODO: implement this!

	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)

	if err != nil {
		fmt.Printf("Error convert maxNonce\n")
		return
	}

	params := lsp.NewParams()
	Client, err := lsp.NewClient(hostport, params)

	if err != nil {
		fmt.Printf("Error create client\n")
		return
	}

	payload := bitcoin.NewRequest(message, 0, maxNonce)
	buf, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("Error Marshal\n")
		Client.Close()
		return
	}

	Client.Write(buf)

	readmsg, err := Client.Read()
	if err != nil {
		printDisconnected()
		Client.Close()
		return
	}

	result := &bitcoin.Message{}
	err = json.Unmarshal(readmsg, result)
	if err != nil {
		fmt.Printf("Unmarshal error\n")
		Client.Close()
		return
	}
	hash := strconv.FormatUint(result.Hash, 10)
	nonce := strconv.FormatUint(result.Nonce, 10)
	printResult(hash, nonce)

}

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
