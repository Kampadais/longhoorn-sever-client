package main

import (
	"fmt"
	"net"
	"time"
)
import "example/dataconn"

func main() {
	dataAddress := "localhost:9502"
	conn, _ := connect(dataAddress)

	dataConnClient := dataconn.NewZenqClient(conn, 30000000000)

	fmt.Println("Helflo, World!")
	//send 50k messages

	//start a tiimer
	timeStart := time.Now()

	for i := 0; i < 50000; i++ {

		_, err := dataConnClient.WriteAt([]byte("Hffello, World!"), 0)
		if err != nil {
			fmt.Println(err)
			return
		}

	}

	timeEnd := time.Now()

	fmt.Println("Time taken to send 50k messages: ", timeEnd.Sub(timeStart))

}

func connect(address string) (net.Conn, error) {
	return net.Dial("tcp", address)

}
