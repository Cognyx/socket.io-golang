package socketio

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	fastws "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v2"
	fiberws "github.com/gofiber/websocket/v2"
	"github.com/valyala/fasthttp/fasthttputil"
)

// TestSocketWritersSerializeUnderConcurrency drives Emit, ack, and Ping on the
// same Socket from many goroutines at once and asserts no goroutine panics
// with "concurrent write to websocket connection". fasthttp/websocket's
// tripwire panic at conn.go:665 fires when a second goroutine calls
// NextWriter while the first hasn't Closed its writer yet — the exact race
// that killed the API on the GoWSClientSync goroutine (TEC-5706). Without the
// per-socket writeMu this test panics reliably under -race; with it, the
// three write paths (writer, engineWrite, Ping) serialize cleanly.
func TestSocketWritersSerializeUnderConcurrency(t *testing.T) {
	t.Parallel()

	socket, cleanup := setupLoopbackSocket(t)
	defer cleanup()

	const (
		emitGoroutines = 8
		pingGoroutines = 2
		writesEach     = 200
	)

	// Panics happen on the offending goroutine; a bare `go` would take the
	// whole test process with it. Recover per-goroutine and report via a
	// channel so the test can t.Fatal cleanly.
	panicCh := make(chan any, emitGoroutines+pingGoroutines)
	var wg sync.WaitGroup

	for i := 0; i < emitGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicCh <- r
				}
			}()
			for j := 0; j < writesEach; j++ {
				if err := socket.Emit("test", "payload", id, j); err != nil {
					return // socket closed by cleanup — expected on teardown
				}
			}
		}(i)
	}
	for i := 0; i < pingGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicCh <- r
				}
			}()
			for j := 0; j < writesEach; j++ {
				if err := socket.Ping(); err != nil {
					return
				}
			}
		}()
	}

	wg.Wait()
	close(panicCh)

	for r := range panicCh {
		t.Fatalf("unexpected panic during concurrent websocket writes (writeMu missing?): %v", r)
	}
}

// setupLoopbackSocket wires a fiber websocket handler to an in-memory
// listener, dials the client side, and returns the Socket wrapping the
// server-side *fiberws.Conn. Cleanup closes both ends and shuts down the
// server so the test doesn't leak goroutines.
func setupLoopbackSocket(t *testing.T) (*Socket, func()) {
	t.Helper()

	ln := fasthttputil.NewInmemoryListener()
	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	app.Use("/", func(c *fiber.Ctx) error {
		if fiberws.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	// The upgrade handler runs in its own goroutine and blocks until the
	// connection closes, so hand the server *Conn out through a channel and
	// keep the handler parked on `done` until cleanup.
	serverConnCh := make(chan *fiberws.Conn, 1)
	done := make(chan struct{})
	app.Get("/", fiberws.New(func(c *fiberws.Conn) {
		serverConnCh <- c
		<-done
	}))

	serveErr := make(chan error, 1)
	go func() { serveErr <- app.Listener(ln) }()

	// Dial via the in-memory listener — a real TCP handshake without a real
	// network. The client Conn is discarded; we only need the server side to
	// exercise Socket.writer / engineWrite / Ping.
	dialer := fastws.Dialer{
		NetDialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return ln.Dial()
		},
		HandshakeTimeout: 2 * time.Second,
	}
	clientConn, _, err := dialer.Dial("ws://loopback/", nil)
	if err != nil {
		close(done)
		_ = app.Shutdown()
		_ = ln.Close()
		t.Fatalf("dial in-memory websocket: %v", err)
	}

	var serverConn *fiberws.Conn
	select {
	case serverConn = <-serverConnCh:
	case <-time.After(2 * time.Second):
		_ = clientConn.Close()
		close(done)
		_ = app.Shutdown()
		_ = ln.Close()
		t.Fatal("upgrade handler did not fire")
	}

	// The client reads and discards everything the server sends so the
	// server-side write buffer never blocks — a wedged writer under the mutex
	// would hide the race we're trying to detect.
	go func() {
		for {
			if _, _, err := clientConn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	socket := &Socket{
		Id:   "test-socket",
		Nps:  "/",
		Conn: serverConn,
	}

	cleanup := func() {
		// Unblock the upgrade handler first; its own deferred releaseConn
		// closes serverConn. Calling serverConn.Close() from here would
		// race that release under -race, and the race would be in the
		// test harness rather than in the code under test.
		close(done)
		_ = clientConn.Close()
		_ = app.Shutdown()
		_ = ln.Close()
		select {
		case <-serveErr:
		case <-time.After(time.Second):
		}
	}

	return socket, cleanup
}

// TestSocketWriteMuBlocksConcurrentWriter is a targeted regression assertion:
// while goroutine A holds writeMu, goroutine B's writer call must block. If
// someone removes the Lock in `writer`, B returns immediately and this test
// fails. This proves the mutex is on the actual code path independently of
// the loopback test above (which relies on -race + timing to trip).
func TestSocketWriteMuBlocksConcurrentWriter(t *testing.T) {
	t.Parallel()

	socket, cleanup := setupLoopbackSocket(t)
	defer cleanup()

	socket.writeMu.Lock()

	// Kick off a writer that will block on the mutex we hold. It also
	// happens to run a real Emit once the lock is released; we don't assert
	// on its outcome, only on whether it blocks first.
	started := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		close(started)
		_ = socket.Emit("event", "arg")
		close(returned)
	}()

	<-started
	// Give the goroutine a fair chance to overtake if the mutex is missing.
	select {
	case <-returned:
		socket.writeMu.Unlock()
		t.Fatal("Emit returned while writeMu was held — the write path is not guarded by writeMu")
	case <-time.After(100 * time.Millisecond):
	}

	socket.writeMu.Unlock()

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("Emit did not complete after writeMu was released")
	}
}

