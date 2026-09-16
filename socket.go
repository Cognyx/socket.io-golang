package socketio

import (
	"errors"
	"sync"
	"time"

	"github.com/Cognyx/socket.io-golang/engineio"
	"github.com/Cognyx/socket.io-golang/socket_protocol"
	"github.com/gofiber/websocket/v2"
)

type Socket struct {
	// writeMu serialises the NextWriter → write → Close sequence across
	// every write path on this Socket. fasthttp/websocket only permits one
	// writer per connection and panics ("concurrent write to websocket
	// connection") at conn.go:665 as a deliberate tripwire when a second
	// goroutine writes concurrently. Without this mutex the library's own
	// 1s heartbeat goroutine (server.go's ping loop, calling Socket.Ping)
	// races any application broadcast (Emit → writer, ack → writer,
	// Disconnect → writer) and kills the whole process on the panicking
	// goroutine. Held across the full write — NextWriter, WriteTo/WriteByte,
	// and w.Close — because the underlying writer stays exclusive until
	// Close returns. See TEC-5706 / TEC-5723.
	//
	// writeMu also guards the Conn pointer itself: every write path snapshots
	// Conn under the lock, and disconnect publishes the nil under it, so a
	// writer never re-reads a field a concurrent disconnect is tearing down.
	// Re-reading s.Conn after the lock was the nil dereference of TEC-6264.
	writeMu    sync.Mutex
	Id         string
	Nps        string
	Conn       *websocket.Conn
	rooms      roomNames
	listeners  listeners
	pingTime   time.Duration
	dispose    []func()
	Join       func(room string)
	Leave      func(room string)
	To         func(room string) *Room
	AuthParams map[string]string
}

func (s *Socket) On(event string, fn eventCallback) {
	s.listeners.set(event, fn)
}

func (s *Socket) Emit(event string, agrs ...interface{}) error {
	c := s.Conn
	if c == nil || c.Conn == nil {
		return errors.New("socket has disconnected")
	}
	agrs = append([]interface{}{event}, agrs...)
	return s.writer(socket_protocol.EVENT, agrs)
}

func (s *Socket) ack(ackEvent string, agrs ...interface{}) error {
	c := s.Conn
	if c == nil || c.Conn == nil {
		return errors.New("socket has disconnected")
	}
	agrs = append([]interface{}{ackEvent}, agrs...)
	return s.writer(socket_protocol.ACK, agrs)
}

func (s *Socket) Ping() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	c := s.Conn
	if c == nil || c.Conn == nil {
		return errors.New("socket has disconnected")
	}
	w, err := c.Conn.NextWriter(websocket.TextMessage)
	if err != nil {
		c.Close()
		return err
	}
	engineio.WriteByte(w, engineio.PING, []byte{})
	return w.Close()
}

func (s *Socket) Disconnect() error {
	// writer returns the benign "socket has disconnected" if disconnect wins
	// the race; the read deadline still unblocks the read loop either way.
	s.writer(socket_protocol.DISCONNECT)
	// Re-read Conn under writeMu after the network write instead of reusing a
	// pre-write snapshot: the *websocket.Conn is sync.Pool-backed (gofiber
	// releaseConn), so a snapshot taken before the write can already belong to
	// an unrelated connection once this one's read loop has returned.
	s.writeMu.Lock()
	c := s.Conn
	s.writeMu.Unlock()
	if c == nil || c.Conn == nil {
		return errors.New("socket has disconnected")
	}
	return c.SetReadDeadline(time.Now())
}

func (s *Socket) Rooms() []string {
	return s.rooms.all()
}

func (s *Socket) disconnect() {
	// Publish the nil under writeMu: a writer that already snapshotted Conn
	// finished its frame before we got the lock, and any later one sees the
	// nil and returns "socket has disconnected". Close and the dispose
	// callbacks run outside the lock — an application "disconnect" listener
	// may Emit, which takes writeMu again.
	s.writeMu.Lock()
	c := s.Conn
	s.Conn = nil
	s.writeMu.Unlock()
	if c != nil {
		c.Close()
	}
	// s.rooms = []string{}
	if len(s.dispose) > 0 {
		for _, dispose := range s.dispose {
			dispose()
		}
	}
}

func (s *Socket) engineWrite(t engineio.PacketType, arg ...interface{}) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	c := s.Conn
	if c == nil || c.Conn == nil {
		return errors.New("socket has disconnected")
	}
	w, err := c.Conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	engineio.WriteTo(w, t, arg...)
	return w.Close()
}

func (s *Socket) writer(t socket_protocol.PacketType, arg ...interface{}) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	c := s.Conn
	if c == nil || c.Conn == nil {
		return errors.New("socket has disconnected")
	}
	w, err := c.Conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	nps := ""
	if s.Nps != "/" {
		nps = s.Nps + ","
	}
	var writeErr error
	if t == socket_protocol.ACK {
		agrs := append([]interface{}{}, arg[0].([]interface{})[1:])
		_, writeErr = socket_protocol.WriteToWithAck(w, t, nps, arg[0].([]interface{})[0].(string), agrs...)
	} else {
		_, writeErr = socket_protocol.WriteTo(w, t, nps, arg...)
	}
	return errors.Join(writeErr, w.Close())
}
