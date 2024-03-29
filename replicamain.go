package main

import (
	"example/dataconn"
	"fmt"
	"github.com/sirupsen/logrus"
	"net"
)

func main() {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:9502")
	if err != nil {
		return
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return
	}

	fmt.Println("Listening on localhost:9502")

	for {
		conn, err := l.AcceptTCP()
		if err != nil {

			continue
		}

		logrus.Infof("New connection from: %v", conn.RemoteAddr())

		go func(conn net.Conn) {
			server := dataconn.NewServer(conn)
			err := server.Handle()
			if err != nil {
				return
			}
		}(conn)
	}

}
