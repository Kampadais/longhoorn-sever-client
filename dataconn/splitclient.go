package dataconn

import (
	"errors"
	"fmt"
	"github.com/alphadose/zenq/v2"
	"io"
	"net"
	"time"

	"github.com/sirupsen/logrus"

	journal "github.com/longhorn/sparse-tools/stats"
)

// Client replica client
type zenqClient struct {
	end       zenq.ZenQ[struct{}]
	requests  zenq.ZenQ[*Message]
	send      zenq.ZenQ[*Message]
	responses zenq.ZenQ[*Message]
	seq       uint32
	messages  map[uint32]*Message
	wire      *Wire
	peerAddr  string
	opTimeout time.Duration
}

// NewClient replica client
func NewZenqClient(conn net.Conn, engineToReplicaTimeout time.Duration) *Client {
	c := &Client{
		wire:      NewWire(conn),
		peerAddr:  conn.RemoteAddr().String(),
		end:       make(chan struct{}, 1024),
		requests:  make(chan *Message, 1024),
		send:      make(chan *Message, 1024),
		responses: make(chan *Message, 1024),
		messages:  map[uint32]*Message{},
		opTimeout: engineToReplicaTimeout,
	}
	go c.loop()
	go c.write()
	go c.read()
	return c
}

// TargetID operation target ID
func (c *zenqClient) TargetID() string {
	return c.peerAddr
}

// WriteAt replica client
func (c *zenqClient) WriteAt(buf []byte, offset int64) (int, error) {
	return c.operation(TypeWrite, buf, uint32(len(buf)), offset)
}

// UnmapAt replica client
func (c *zenqClient) UnmapAt(length uint32, offset int64) (int, error) {
	return c.operation(TypeUnmap, nil, length, offset)
}

// SetError replica client transport error
func (c *zenqClient) SetError(err error) {
	c.responses.Write(&Message{
		transportErr: err,
	})
}

// ReadAt replica client
func (c *zenqClient) ReadAt(buf []byte, offset int64) (int, error) {
	return c.operation(TypeRead, buf, uint32(len(buf)), offset)
}

// Ping replica client
func (c *zenqClient) Ping() error {
	_, err := c.operation(TypePing, nil, 0, 0)
	return err
}

func (c *zenqClient) operation(op uint32, buf []byte, length uint32, offset int64) (int, error) {
	msg := Message{
		Complete: make(chan struct{}, 1),
		Type:     op,
		Offset:   offset,
		Size:     length,
		Data:     nil,
	}

	if op == TypeWrite {
		msg.Data = buf
	}

	c.requests.Write(&msg)

	<-msg.Complete
	// Only copy the message if a read is requested
	if op == TypeRead && (msg.Type == TypeResponse || msg.Type == TypeEOF) {
		copy(buf, msg.Data)
	}
	if msg.Type == TypeError {
		return 0, errors.New(string(msg.Data))
	}
	if msg.Type == TypeEOF {
		return int(msg.Size), io.EOF
	}
	fmt.Println("done operation")
	return int(msg.Size), nil
}

// Close replica client
func (c *zenqClient) Close() {
	c.wire.Close()
	c.end.Write(struct{}{})
}

func (c *zenqClient) loop() {
	defer c.send.Close()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var clientError error
	var ioInflight int
	var ioDeadline time.Time

	// handleClientError cleans up all in flight messages
	// also stores the error so that future requests/responses get errored immediately.
	handleClientError := func(err error) {
		clientError = err
		for _, msg := range c.messages {
			fmt.Println("handleClientError")
			c.replyError(msg, err)
		}

		ioInflight = 0
		ioDeadline = time.Time{}
	}

	for {
		if data, queueOpen := c.requests.Read(); queueOpen {
			if clientError != nil {
				fmt.Println("clientError")
				c.replyError(data, clientError)
				continue
			}

			if data.Type == TypeRead || data.Type == TypeWrite || data.Type == TypeUnmap {
				if ioInflight == 0 {
					ioDeadline = time.Now().Add(c.opTimeout)
				}
				ioInflight++
			}

			c.handleRequest(data)
		}

		if data, queueOpen := c.responses.Read(); queueOpen {
			if data.transportErr != nil {
				handleClientError(data.transportErr)
				continue
			}

			req, pending := c.messages[data.Seq]
			if !pending {
				logrus.Warnf("Received response message id %v seq %v type %v for non pending request", data.ID, data.Seq, data.Type)
				continue
			}

			if req.Type == TypeRead || req.Type == TypeWrite || req.Type == TypeUnmap {
				ioInflight--
				if ioInflight > 0 {
					ioDeadline = time.Now().Add(c.opTimeout)
				} else if ioInflight == 0 {
					ioDeadline = time.Time{}
				}
			}

			if clientError != nil {
				fmt.Println("clientError2")
				c.replyError(req, clientError)
				continue
			}

			c.handleResponse(data)
		}
		if _, queueOpen := c.end.Read(); queueOpen {
			return
		}

		select {
		case <-ticker.C:
			if ioDeadline.IsZero() || time.Now().Before(ioDeadline) {
				continue
			}

			logrus.Errorf("R/W Timeout. No response received in %v", c.opTimeout)
			handleClientError(ErrRWTimeout)
			journal.PrintLimited(1000)

		}
	}
}

func (c *zenqClient) nextSeq() uint32 {
	c.seq++
	return c.seq
}

func (c *zenqClient) replyError(req *Message, err error) {
	journal.RemovePendingOp(req.ID, false)
	delete(c.messages, req.Seq)
	req.Type = TypeError
	req.Data = []byte(err.Error())
	req.Complete <- struct{}{}
}

func (c *zenqClient) handleRequest(req *Message) {
	switch req.Type {
	case TypeRead:
		req.ID = journal.InsertPendingOp(time.Now(), c.TargetID(), journal.OpRead, int(req.Size))
	case TypeWrite:
		req.ID = journal.InsertPendingOp(time.Now(), c.TargetID(), journal.OpWrite, int(req.Size))
	case TypeUnmap:
		req.ID = journal.InsertPendingOp(time.Now(), c.TargetID(), journal.OpUnmap, int(req.Size))
	case TypePing:
		req.ID = journal.InsertPendingOp(time.Now(), c.TargetID(), journal.OpPing, 0)
	}

	req.MagicVersion = MagicVersion
	req.Seq = c.nextSeq()
	c.messages[req.Seq] = req
	c.send.Write(req)
}

func (c *zenqClient) handleResponse(resp *Message) {
	if req, ok := c.messages[resp.Seq]; ok {
		journal.RemovePendingOp(req.ID, true)
		delete(c.messages, resp.Seq)
		req.Type = resp.Type
		req.Size = resp.Size
		req.Data = resp.Data
		req.Complete <- struct{}{}
	}
}

func (c *zenqClient) write() {

	for {
		if msg, queueOpen := c.send.Read(); queueOpen {
			if err := c.wire.Write(msg); err != nil {
				c.responses.Write(&Message{
					transportErr: err,
				})
			}
		}
	}
}

func (c *zenqClient) read() {
	for {
		msg, err := c.wire.Read()
		if err != nil {
			logrus.WithError(err).Errorf("Error reading from wire %v", c.peerAddr)
			c.responses.Write(&Message{
				transportErr: err,
			})
			break
		}
		c.responses.Write(msg)
	}
}
