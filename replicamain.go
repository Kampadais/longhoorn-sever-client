package main

import (
	"example/dataconn"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"net"
	"net/http"
)

func replicamain(c *cli.Context) {

	proffiling := c.Bool("profiling")
	if proffiling {
		fmt.Println("Starting pprof server")
		go func() {
			logrus.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	addr, err := net.ResolveTCPAddr("tcp", "localhost:9502")
	if err != nil {
		fmt.Println(err)
		return
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Listening on ", l.Addr().String())

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
