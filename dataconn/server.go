package dataconn

import (
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"sync/atomic"
)

var (
	inflight atomic.Int64
)

type Server struct {
	wire      *Wire
	responses chan *Message
	done      chan struct{}
}

func NewServer(conn net.Conn) *Server {
	inflight.Store(0)
	return &Server{
		wire:      NewWire(conn),
		responses: make(chan *Message, 1024),
		done:      make(chan struct{}, 5),
	}
}

func (s *Server) Handle() error {
	go s.write()
	defer func() {
		s.done <- struct{}{}
	}()
	return s.read()
}

func (s *Server) readFromWire(ret chan<- error) {
	msg, err := s.wire.Read()
	if err == io.EOF {
		ret <- err
		return
	} else if err != nil {
		logrus.WithError(err).Error("Failed to read")
		ret <- err
		return
	}
	switch msg.Type {
	case TypeRead:

		go s.handleRead(msg)
		inflight.Add(1)
	case TypeWrite:
		go s.handleWrite(msg)
		inflight.Add(1)
	case TypeUnmap:
		go s.handleUnmap(msg)
		inflight.Add(1)
	case TypePing:
		go s.handlePing(msg)
		inflight.Add(1)
	}

	//logrus.Infof("Inflight: %d", inflight.Load())
	ret <- nil
}

func (s *Server) read() error {
	ret := make(chan error)
	for {
		go s.readFromWire(ret)

		select {
		case err := <-ret:
			if err != nil {
				return err
			}
			continue
		case <-s.done:
			logrus.Info("RPC server stopped")
			return nil
		}
	}
}

func (s *Server) Stop() {
	s.done <- struct{}{}
}

func (s *Server) handleRead(msg *Message) {
	s.pushResponse(len(msg.Data), msg, nil)
	inflight.Add(-1)
}

func (s *Server) handleWrite(msg *Message) {
	s.pushResponse(len(msg.Data), msg, nil)

	inflight.Add(-1)
}

func (s *Server) handleUnmap(msg *Message) {
	s.pushResponse(len(msg.Data), msg, nil)
}

func (s *Server) handlePing(msg *Message) {
	s.pushResponse(len(msg.Data), msg, nil)
	inflight.Add(-1)
}

func (s *Server) pushResponse(count int, msg *Message, err error) {
	msg.MagicVersion = MagicVersion
	msg.Size = uint32(len(msg.Data))
	if msg.Type == TypeWrite || msg.Type == TypeUnmap {
		msg.Data = nil
		msg.Size = uint32(count)
	}

	msg.Type = TypeResponse
	s.responses <- msg
}

func (s *Server) write() {
	for {
		select {
		case msg := <-s.responses:
			if err := s.wire.Write(msg); err != nil {
				logrus.WithError(err).Error("Failed to write")
			}
		case <-s.done:
			msg := &Message{
				Type: TypeClose,
			}
			//Best effort to notify client to close connection
			s.wire.Write(msg)
		}
	}
}
