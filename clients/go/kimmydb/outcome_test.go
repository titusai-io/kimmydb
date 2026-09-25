package kimmydb_test

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/titusai-io/kimmydb/clients/go/kimmydb"
)

// These drive the client against a local server that misbehaves on purpose,
// in the ways a real node cannot be made to on demand: closing the connection
// at a chosen moment, or answering a chosen envelope.

// fakeServer serves each connection with handle, and returns its base URL.
func fakeServer(t *testing.T, handle func(conn net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				handle(conn)
			}()
		}
	}()
	return "http://" + listener.Addr().String()
}

// readRequest reads one whole HTTP request from conn: its head, and a body of
// Content-Length bytes.
func readRequest(conn net.Conn) error {
	reader := bufio.NewReader(conn)
	length := 0
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if name, value, ok := strings.Cut(line, ":"); ok && strings.EqualFold(name, "content-length") {
			length, _ = strconv.Atoi(strings.TrimSpace(value))
		}
	}
	_, err := io.CopyN(io.Discard, reader, int64(length))
	return err
}

// answer writes one HTTP response with a JSON body.
func answer(conn net.Conn, status int, headers string, body string) {
	fmt.Fprintf(conn, "HTTP/1.1 %d X\r\nContent-Type: application/json\r\nContent-Length: %d\r\n%s\r\n%s",
		status, len(body), headers, body)
}

func clientFor(t *testing.T, base string, opts ...kimmydb.Option) *kimmydb.Client {
	t.Helper()
	opts = append([]kimmydb.Option{kimmydb.WithToken("a-token")}, opts...)
	db, err := kimmydb.New(testContext(t), base, opts...)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

func TestAWriteWhoseConnectionClosesAfterItWasSentHasAnUnknownOutcome(t *testing.T) {
	base := fakeServer(t, func(conn net.Conn) { _ = readRequest(conn) })
	db := clientFor(t, base)

	_, err := db.Insert(testContext(t), "shop", "orders", map[string]any{"_id": 1})
	if !kimmydb.IsOutcomeUnknown(err) {
		t.Fatalf("a write sent and never answered may have happened: %v", err)
	}
	var unknown *kimmydb.OutcomeUnknownError
	if !errors.As(err, &unknown) || unknown.Endpoint != base {
		t.Fatalf("the error names the node: %#v", err)
	}
}

func TestAReadWhoseConnectionClosesAfterItWasSentIsATransportFailure(t *testing.T) {
	base := fakeServer(t, func(conn net.Conn) { _ = readRequest(conn) })
	db := clientFor(t, base)

	_, err := db.Version(testContext(t))
	var transport *kimmydb.TransportError
	if kimmydb.IsOutcomeUnknown(err) || !errors.As(err, &transport) {
		t.Fatalf("a read has no outcome to be unknown: %v", err)
	}
}

func TestAWriteToANodeThatRefusesTheConnectionWasNotSent(t *testing.T) {
	db := clientFor(t, dead)

	_, err := db.Insert(testContext(t), "shop", "orders", map[string]any{"_id": 1})
	var transport *kimmydb.TransportError
	if kimmydb.IsOutcomeUnknown(err) || !errors.As(err, &transport) {
		t.Fatalf("a refused connection carried nothing: %v", err)
	}
}

func TestAWriteWhoseBodyCouldNotBeWrittenWasNotSent(t *testing.T) {
	// Larger than any socket buffer, so the write really does fail partway
	// when the server stops reading and closes.
	base := fakeServer(t, func(conn net.Conn) {
		buffer := make([]byte, 1024)
		_, _ = conn.Read(buffer)
	})
	db := clientFor(t, base)

	big := strings.Repeat("x", 32<<20)
	_, err := db.Insert(testContext(t), "shop", "orders", map[string]any{"_id": 1, "big": big})
	var transport *kimmydb.TransportError
	if kimmydb.IsOutcomeUnknown(err) || !errors.As(err, &transport) {
		t.Fatalf("a request that could not be written cannot have been applied: %v", err)
	}
}

func TestAWriteWhoseAnswerIsCutOffHasAnUnknownOutcome(t *testing.T) {
	// The status arrived and the body did not: the node received the write.
	base := fakeServer(t, func(conn net.Conn) {
		if readRequest(conn) == nil {
			fmt.Fprint(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"inser")
		}
	})
	db := clientFor(t, base)

	_, err := db.Insert(testContext(t), "shop", "orders", map[string]any{"_id": 1})
	if !kimmydb.IsOutcomeUnknown(err) {
		t.Fatalf("a write whose answer was cut off may have happened: %v", err)
	}
}

func TestTheServersOutcomeUnknownIsTyped(t *testing.T) {
	base := fakeServer(t, func(conn net.Conn) {
		if readRequest(conn) == nil {
			answer(conn, 500, "", `{"error":"outcome_unknown","message":"m","retry":"verify"}`)
		}
	})
	db := clientFor(t, base)

	_, err := db.Insert(testContext(t), "shop", "orders", map[string]any{"_id": 1})
	var apiErr *kimmydb.APIError
	if !kimmydb.IsOutcomeUnknown(err) || !errors.As(err, &apiErr) || apiErr.Retry != kimmydb.RetryVerify {
		t.Fatalf("the envelope is an unknown outcome, retry verify: %v", err)
	}
}

// failingTransport fails every request after a moment, and reports nothing to
// an httptrace: the kind of transport WithHTTPClient can supply.
type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("the connection was reset")
}

func TestAFailureWithNoEvidenceEitherWayIsUnknownForAWrite(t *testing.T) {
	db := clientFor(t, "http://127.0.0.1:9", kimmydb.WithHTTPClient(&http.Client{Transport: failingTransport{}}))

	_, err := db.Insert(testContext(t), "shop", "orders", map[string]any{"_id": 1})
	if !kimmydb.IsOutcomeUnknown(err) {
		t.Fatalf("no trace is not evidence that nothing was sent: %v", err)
	}
}

func TestAWaitIsRiddenOutOnTheSameNodeForAWrite(t *testing.T) {
	// A single endpoint, refused twice with `wait` and then served: the write
	// goes to the same node again, since `wait` says nothing was done.
	var seen atomic.Int32
	base := fakeServer(t, func(conn net.Conn) {
		for readRequest(conn) == nil {
			if seen.Add(1) <= 2 {
				answer(conn, 503, "Retry-After: 1\r\n",
					`{"error":"collection_purging","message":"m","retry":"wait"}`)
				continue
			}
			answer(conn, 200, "", `{"inserted":1}`)
		}
	})
	db := clientFor(t, base)

	started := time.Now()
	if _, err := db.Insert(testContext(t), "shop", "orders", map[string]any{"_id": 1}); err != nil {
		t.Fatalf("ridden out: %v", err)
	}
	if seen.Load() != 3 {
		t.Fatalf("three attempts, all at the one node: %d", seen.Load())
	}
	if elapsed := time.Since(started); elapsed < 2*time.Second {
		t.Fatalf("each after the Retry-After it was given: %v", elapsed)
	}
}

func TestAWaitThatOutlastsTheBudgetIsReturned(t *testing.T) {
	var seen atomic.Int32
	base := fakeServer(t, func(conn net.Conn) {
		for readRequest(conn) == nil {
			seen.Add(1)
			answer(conn, 503, "Retry-After: 1\r\n",
				`{"error":"collection_purging","message":"m","retry":"wait"}`)
		}
	})
	db := clientFor(t, base, kimmydb.WithWaitBudget(1500*time.Millisecond))

	_, err := db.CreateCollection(testContext(t), "shop", "orders")
	var apiErr *kimmydb.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "collection_purging" {
		t.Fatalf("the wait's own error, once the budget is spent: %v", err)
	}
	if n := seen.Load(); n != 3 {
		t.Fatalf("one attempt, then two within a 1.5 s budget of 1 s waits: %d", n)
	}
}
