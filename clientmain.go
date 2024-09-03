package main

import (
	"fmt"
	"github.com/urfave/cli"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)
import "example/dataconn"

var opsToComplete int32 = 500000

func clientmain(c *cli.Context) {
	var wg sync.WaitGroup
	var ops atomic.Int32
	ops.Store(0)
	proffiling := c.Bool("profiling")
	replicaAddr := c.String("replica-addr")
	if proffiling {
		fmt.Println("Starting pprof server")
		go func() {
			fmt.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	streams := c.Int("replica-streams")

	var conns []net.Conn

	for i := 0; i < streams; i++ {
		conn, err := connect(replicaAddr)
		if err != nil {
			fmt.Println(err)
			return
		}

		conns = append(conns, conn)
	}

	fmt.Println("Multiple streams")
	client := dataconn.NewClient(conns, time.Second)
	for i := 0; i < c.Int("threads"); i++ {
		wg.Add(1)
		go sending(client, &ops)
	}

	wg.Wait()
}

func connect(addr string) (net.Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func sending(dataConnClient *dataconn.Client, ops *atomic.Int32) {

	for {
		timeStart := time.Now()
		for {
			//timer1 := time.Now()
			_, err := dataConnClient.WriteAt([]byte("Hello, World!"), 0)
			//timer1End := time.Now()
			//fmt.Println("Time taken to send 1 message : ", timer1End.Sub(timer1).Microseconds(), "μs")
			if err != nil {
				fmt.Println(err)
			}
			ops.Add(1)

			if ops.Load() == opsToComplete {
				timeEnd := time.Now()
				secs := timeEnd.Sub(timeStart).Seconds()
				fmt.Println("Time taken to send ", opsToComplete, " messages V1: ", secs, "s")
				fmt.Println("IOPS : ", float64(opsToComplete)/secs)
				os.Exit(0)
			}

		}

	}
}
