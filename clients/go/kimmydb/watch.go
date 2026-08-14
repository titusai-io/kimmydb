package kimmydb

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// ChangeEvent is one event from a collection.
type ChangeEvent struct {
	// Operation is insert, update, replace, delete, uniqueViolation or
	// invalidate.
	Operation string
	// ResumeToken is where to resume. Empty on invalidate, which cannot be
	// resumed past.
	ResumeToken string
	// Raw is the whole event, for everything the fields above do not name.
	Raw map[string]any
}

// DocumentID returns the changed document's _id, when the event has one.
func (e ChangeEvent) DocumentID() any {
	key, _ := e.Raw["documentKey"].(map[string]any)
	return key["_id"]
}

// FullDocument returns the post-image, when it was asked for and the event
// carried one.
//
// An oversized event drops it and still arrives, so absence does not mean the
// document is gone.
func (e ChangeEvent) FullDocument() map[string]any {
	document, _ := e.Raw["fullDocument"].(map[string]any)
	return document
}

// IsInvalidate reports whether the stream cannot continue past this event.
func (e ChangeEvent) IsInvalidate() bool { return e.Operation == "invalidate" }

// InvalidateReason returns why the stream ended: CollectionDropped,
// ConsumerLagged or ResumeTokenExpired. Treat an unrecognized value as "this
// stream is over", which is what every value means.
func (e ChangeEvent) InvalidateReason() string {
	reason, _ := e.Raw["reason"].(string)
	return reason
}

// WatchOptions configures a change stream.
type WatchOptions struct {
	// ResumeAfter continues immediately after a token from a previous stream.
	ResumeAfter string
	// FromStart replays from the beginning of the retained oplog — for this
	// collection's current incarnation. A collection dropped and recreated
	// under the same name reuses its id, and its predecessor's entries are not
	// its history.
	FromStart bool
	// FullDocument includes the whole document on every event that has one.
	FullDocument bool
}

// Watch follows a collection's changes.
//
//	for event, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{}) {
//	    if err != nil { return err }
//	    fmt.Println(event.Operation, event.DocumentID())
//	}
//
// The iterator reconnects on its own, resuming from the last token it saw, and
// ends for exactly two reasons: an invalidate — the stream is over, and the
// event says why — or a failure it could not recover from, which arrives as
// the error in the loop.
//
// Cancel the context to stop early.
func (c *Client) Watch(ctx context.Context, db, collection string, options WatchOptions) iter.Seq2[ChangeEvent, error] {
	return func(yield func(ChangeEvent, error) bool) {
		stream := &changeStream{
			client:     c,
			db:         db,
			collection: collection,
			options:    options,
			resume:     options.ResumeAfter,
		}
		defer stream.close()

		for {
			if stream.socket == nil {
				if err := stream.reconnect(ctx); err != nil {
					yield(ChangeEvent{}, err)
					return
				}
			}

			_, raw, err := stream.socket.Read(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				// A dropped socket, a close frame, a timeout — all three mean
				// the same thing: reconnect and resume.
				stream.close()
				continue
			}

			var body map[string]any
			if err := json.Unmarshal(raw, &body); err != nil {
				yield(ChangeEvent{}, fmt.Errorf("kimmydb: an event was not JSON: %w", err))
				return
			}

			operation, _ := body["operationType"].(string)
			token, _ := body["resumeToken"].(string)
			event := ChangeEvent{Operation: operation, ResumeToken: token, Raw: body}
			// Recorded before the event is handed over, so a caller that stops
			// iterating mid-event resumes at the last one it actually saw.
			if token != "" {
				stream.resume = token
			}

			if !yield(event, nil) {
				return
			}
			if event.IsInvalidate() {
				return
			}
		}
	}
}

type changeStream struct {
	client     *Client
	db         string
	collection string
	options    WatchOptions
	resume     string
	socket     *websocket.Conn
}

func (s *changeStream) close() {
	if s.socket != nil {
		_ = s.socket.Close(websocket.StatusNormalClosure, "")
		s.socket = nil
	}
}

// reconnect dials, backing off, resuming from the last token seen.
func (s *changeStream) reconnect(ctx context.Context) error {
	const attempts = 5
	delay := 100 * time.Millisecond

	var last error
	for attempt := range attempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			if delay *= 2; delay > 5*time.Second {
				delay = 5 * time.Second
			}
		}
		err := s.dial(ctx)
		if err == nil {
			return nil
		}
		// A resume point that cannot be continued from cannot be waited out:
		// retrying the same token loops forever, and the caller has to decide
		// what to do about the gap.
		if apiErr, ok := err.(*APIError); ok && apiErr.Code == "resume_token_expired" {
			return err
		}
		last = err
	}
	return last
}

func (s *changeStream) dial(ctx context.Context) error {
	if err := s.client.authenticate(ctx); err != nil {
		return err
	}
	endpoint := s.client.primary()
	target := strings.Replace(strings.Replace(endpoint, "https://", "wss://", 1), "http://", "ws://", 1)
	url := target + fmt.Sprintf("/v1/db/%s/coll/%s/watch%s", s.db, s.collection, s.query())

	header := http.Header{}
	if token := s.client.Token(); token != "" {
		header.Set("Authorization", "Bearer "+token)
	}

	// Through the client's own *http.Client: the change stream inherits the
	// same TLS configuration, proxy and transport as every other request
	// rather than dialling with a second one that can drift.
	socket, response, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPClient: s.client.http,
		HTTPHeader: header,
	})
	if err != nil {
		// The server refuses before upgrading with the ordinary error
		// envelope, so the reason survives if it can be recovered.
		if response != nil {
			defer response.Body.Close()
			raw := make([]byte, 4096)
			n, _ := response.Body.Read(raw)
			return errorFrom(response.StatusCode, raw[:n], retryAfterOf(response))
		}
		return &TransportError{Endpoint: endpoint, Err: err}
	}
	// A change stream is idle by design — sometimes for hours — so the read
	// limit matters more than a timeout would.
	socket.SetReadLimit(8 << 20)
	s.socket = socket
	return nil
}

func (s *changeStream) query() string {
	parts := make([]string, 0, 2)
	// A resume point learned while streaming wins over the configured one: it
	// is where this stream actually got to. Resuming from the configured point
	// would replay everything since, which for a stream that has been up for a
	// day is a day of events delivered twice.
	if s.resume != "" {
		parts = append(parts, "resume_after="+s.resume)
	} else if s.options.FromStart {
		parts = append(parts, "from_start=true")
	}
	if s.options.FullDocument {
		parts = append(parts, "full_document=true")
	}
	if len(parts) == 0 {
		return ""
	}
	return "?" + strings.Join(parts, "&")
}
