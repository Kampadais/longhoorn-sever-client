package dataconn

import (
	"io"
	"net"

	"github.com/sirupsen/logrus"
)

const (
	threadCount = 32
)

type Server struct {
	wire      *Wire
	requests  chan *Message
	responses chan *Message
	done      chan struct{}
}

func NewServer(conn net.Conn) *Server {
	//init theads
	server := &Server{
		wire:      NewWire(conn),
		requests:  make(chan *Message, 1024),
		responses: make(chan *Message, 1024),
		done:      make(chan struct{}, 5),
	}
	for i := 0; i < threadCount; i++ {
		go func(s *Server) {
			for {
				msg := <-s.requests
				switch msg.Type {
				case TypeRead:
					s.handleRead(msg)
				case TypeWrite:
					s.handleWrite(msg)
				case TypeUnmap:
					s.handleUnmap(msg)
				case TypePing:
					s.handlePing(msg)
				}
			}
		}(server)
	}
	return server
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
	s.requests <- msg
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
	msg.Data = make([]byte, msg.Size)
	//c, err := s.data.ReadAt(msg.Data, msg.Offset)
	s.pushResponse(len(msg.Data), msg, nil)
}

func (s *Server) handleWrite(msg *Message) {
	//c, err := s.data.WriteAt(msg.Data, msg.Offset)
	s.pushResponse(len(msg.Data), msg, nil)
}

func (s *Server) handleUnmap(msg *Message) {
	//c, err := s.data.UnmapAt(msg.Size, msg.Offset)
	s.pushResponse(len(msg.Data), msg, nil)
}

func (s *Server) handlePing(msg *Message) {
	//err := s.data.PingResponse()
	s.pushResponse(0, msg, nil)
}

func (s *Server) pushResponse(count int, msg *Message, err error) {
	msg.MagicVersion = MagicVersion
	msg.Size = uint32(len(msg.Data))
	if msg.Type == TypeWrite || msg.Type == TypeUnmap {
		msg.Data = nil
		msg.Size = uint32(count)
	}

	msg.Type = TypeResponse
	if err == io.EOF {
		msg.Type = TypeEOF
		msg.Data = msg.Data[:count]
		msg.Size = uint32(len(msg.Data))
	} else if err != nil {
		msg.Type = TypeError
		msg.Data = []byte(err.Error())
		msg.Size = uint32(len(msg.Data))
	}
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
			if err := s.wire.Write(msg); err != nil {
				logrus.WithError(err).Warn("Failed to write")
			}
		}
	}
}
