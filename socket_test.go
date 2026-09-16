package socketio

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
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

// TestSocketEmitDuringDisconnectDoesNotPanic reproduces TEC-6264. Emit passes
// its nil check on s.Conn, then parks in writer on a writeMu that another
// goroutine holds (the 1s heartbeat, or a slow frame, in production). Meanwhile
// the read loop's deferred disconnect nils s.Conn. Before the fix, writer
// re-read the field once the lock was released and dereferenced nil at
// s.Conn.Conn — an unrecovered panic on a broadcast goroutine that took the API
// process down 14 times in seven days on treves-prod. With the fix, writer
// snapshots Conn under writeMu and disconnect publishes the nil under the same
// lock, so Emit either completes its frame or returns the benign
// "socket has disconnected" that the application already tolerates.
func TestSocketEmitDuringDisconnectDoesNotPanic(t *testing.T) {
	t.Parallel()

	socket, cleanup := setupLoopbackSocket(t)
	defer cleanup()

	// A: hold writeMu, as the heartbeat would mid-frame.
	socket.writeMu.Lock()

	// Panics happen on the offending goroutine; recover per-goroutine and
	// report via a channel so the test can t.Fatal cleanly.
	panicCh := make(chan any, 2)
	emitErr := make(chan error, 1)
	var wg sync.WaitGroup

	// B: Emit sees a non-nil Conn, then blocks in writer on the held lock.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
		}()
		emitErr <- socket.Emit("event", "arg")
	}()
	// Give B a fair chance to reach the lock before the disconnect runs, as
	// TestSocketWriteMuBlocksConcurrentWriter does.
	time.Sleep(50 * time.Millisecond)

	// C: the read loop's deferred disconnect, racing the parked writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
		}()
		socket.disconnect()
	}()
	time.Sleep(50 * time.Millisecond)

	// A releases. Pre-fix, B now dereferences the field that C nil'd.
	socket.writeMu.Unlock()

	joined := make(chan struct{})
	go func() {
		wg.Wait()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(2 * time.Second):
		t.Fatal("Emit / disconnect did not complete after writeMu was released")
	}
	close(panicCh)
	for r := range panicCh {
		t.Fatalf("panic while disconnect raced an Emit parked on writeMu (writer re-reads s.Conn?): %v", r)
	}

	if err := <-emitErr; err != nil && !strings.Contains(err.Error(), "socket has disconnected") {
		t.Fatalf("Emit returned %v; want nil or \"socket has disconnected\"", err)
	}
}

// TestRoomEmitSkipsDisconnectedSocket pins the contract the application relies
// on: a socket whose Conn is nil but which is still a room member — permanently
// here, since this synthetic namespace wires no dispose callback; transiently
// in production, between disconnect nil-ing Conn and the dispose room-leave —
// makes Room.Emit return an error containing "socket has disconnected", the
// substring SocketIOAdapter.EmitToRoom matches to classify a benign
// disconnect, rather than panicking.
func TestRoomEmitSkipsDisconnectedSocket(t *testing.T) {
	t.Parallel()

	socket, cleanup := setupLoopbackSocket(t)
	defer cleanup()

	nps := newNamespace("/")
	nps.socketJoinRoom("room", socket)
	socket.disconnect()

	err := nps.To("room").Emit("event", "arg")
	if err == nil || !strings.Contains(err.Error(), "socket has disconnected") {
		t.Fatalf("Room.Emit on a disconnected member returned %v; want \"socket has disconnected\"", err)
	}
}

// TestSocketEmitOverlapsDisconnectRaceFree is the falsifying probe for the
// unsynchronised fast-path read that survived the first TEC-6264 fix: at
// v0.1.15 Emit and ack read s.Conn BEFORE taking writeMu while disconnect
// publishes s.Conn = nil UNDER it. TestSocketEmitDuringDisconnectDoesNotPanic
// sequences that read before disconnect starts, so it cannot see the race.
// Here writers hammer Emit and ack with no ordering at all while disconnect
// runs in the middle of the storm; under -race the pre-fix code reports
// "Read at socket.go:49 … Previous write at socket.go:111". Post-fix every
// read of Conn is under writeMu and the only acceptable outcomes are a nil
// error (frame written before the disconnect) or "socket has disconnected".
func TestSocketEmitOverlapsDisconnectRaceFree(t *testing.T) {
	t.Parallel()

	socket, cleanup := setupLoopbackSocket(t)
	defer cleanup()

	const writers = 8
	var (
		wg        sync.WaitGroup
		stop      atomic.Bool
		panics    = make(chan any, writers+1)
		badErrors = make(chan error, writers)
	)

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panics <- r
				}
			}()
			for !stop.Load() {
				var err error
				if i%2 == 0 {
					err = socket.Emit("event", i)
				} else {
					err = socket.ack("ack", i)
				}
				if err != nil && !strings.Contains(err.Error(), "socket has disconnected") {
					badErrors <- err
					return
				}
			}
		}(i)
	}

	// Let the writers get going, then tear the connection down under them.
	time.Sleep(20 * time.Millisecond)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				panics <- r
			}
		}()
		socket.disconnect()
	}()
	// Keep the writers overlapping the disconnect for a while, then stop.
	time.Sleep(50 * time.Millisecond)
	stop.Store(true)

	joined := make(chan struct{})
	go func() {
		wg.Wait()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(3 * time.Second):
		t.Fatal("writers / disconnect did not finish")
	}
	close(panics)
	for r := range panics {
		t.Fatalf("panic while Emit/ack overlapped disconnect: %v", r)
	}
	close(badErrors)
	for err := range badErrors {
		t.Fatalf("writer returned %v; want nil or \"socket has disconnected\"", err)
	}
	socket.writeMu.Lock()
	c := socket.Conn
	socket.writeMu.Unlock()
	if c != nil {
		t.Fatal("disconnect did not publish a nil Conn")
	}
}
