package socketio

import (
	"errors"
	"time"

	"github.com/Cognyx/socket.io-golang/engineio"
	"github.com/Cognyx/socket.io-golang/socket_protocol"
	"github.com/gofiber/websocket/v2"
)

type Socket struct {
	Id        string
	Nps       string
	Conn      *websocket.Conn
	rooms     roomNames
	listeners listeners
	pingTime  time.Duration
	dispose   []func()
	Join      func(room string)
	Leave     func(room string)
	To        func(room string) *Room
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
	c := s.Conn
	if c == nil || c.Conn == nil {
		return errors.New("socket has disconnected")
	}
	s.writer(socket_protocol.DISCONNECT)
	return s.Conn.SetReadDeadline(time.Now())
}

func (s *Socket) Rooms() []string {
	return s.rooms.all()
}

func (s *Socket) disconnect() {
	s.Conn.Close()
	s.Conn = nil
	// s.rooms = []string{}
	if len(s.dispose) > 0 {
		for _, dispose := range s.dispose {
			dispose()
		}
	}
}

func (s *Socket) engineWrite(t engineio.PacketType, arg ...interface{}) error {
	w, err := s.Conn.Conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	engineio.WriteTo(w, t, arg...)
	return w.Close()
}

func (s *Socket) writer(t socket_protocol.PacketType, arg ...interface{}) error {
	w, err := s.Conn.Conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	nps := ""
	if s.Nps != "/" {
		nps = s.Nps + ","
	}
	if t == socket_protocol.ACK {
		agrs := append([]interface{}{}, arg[0].([]interface{})[1:])
		socket_protocol.WriteToWithAck(w, t, nps, arg[0].([]interface{})[0].(string), agrs...)
	} else {
		socket_protocol.WriteTo(w, t, nps, arg...)
	}
	return w.Close()
}
