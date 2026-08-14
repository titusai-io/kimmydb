package kimmydb_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/titusai-io/kimmydb/clients/go/kimmydb"
)

func TestAChangeStreamDeliversAndCarriesAResumeToken(t *testing.T) {
	_, db := connected(t)
	seed(t, db, 1)
	ctx := testContext(t)

	events := db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{FullDocument: true})

	go func() {
		time.Sleep(300 * time.Millisecond)
		for id := 100; id < 103; id++ {
			_, _ = db.Insert(ctx, "shop", "orders", map[string]any{"_id": id, "sku": "live"})
		}
	}()

	seen := 0
	var lastToken string
	for event, err := range events {
		if err != nil {
			t.Fatalf("streaming: %v", err)
		}
		if event.Operation != "insert" {
			t.Fatalf("operation = %q", event.Operation)
		}
		if event.FullDocument() == nil {
			t.Fatal("full_document was asked for")
		}
		lastToken = event.ResumeToken
		if seen++; seen == 3 {
			break
		}
	}

	if lastToken == "" {
		t.Fatal("the stream should know where it got to")
	}
}

func TestAChangeStreamResumesFromWhereItStopped(t *testing.T) {
	// What makes reconnection safe: a token carries no server state, so a
	// second stream started from it sees what the first missed rather than
	// everything since the beginning.
	_, db := connected(t)
	seed(t, db, 1)
	ctx := testContext(t)

	var token string
	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = db.Insert(ctx, "shop", "orders", map[string]any{"_id": 200})
	}()
	for event, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{}) {
		if err != nil {
			t.Fatalf("streaming: %v", err)
		}
		token = event.ResumeToken
		break
	}

	// Written while nothing is listening.
	if _, err := db.Insert(ctx, "shop", "orders", map[string]any{"_id": 201}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	for event, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{ResumeAfter: token}) {
		if err != nil {
			t.Fatalf("resuming: %v", err)
		}
		if id, ok := event.DocumentID().(float64); !ok || int(id) != 201 {
			t.Fatalf("the write made while disconnected should be delivered, got %v", event.DocumentID())
		}
		break
	}
}

func TestADroppedCollectionEndsTheStream(t *testing.T) {
	// The server used to leave the stream open and silent — no event, no
	// close, no error. Both other clients assert this too.
	_, db := connected(t)
	seed(t, db, 1)
	ctx := testContext(t)

	events := db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{})

	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = db.Request(ctx, http.MethodDelete, "/v1/db/shop/coll/orders", nil, kimmydb.Idempotent)
	}()

	var final kimmydb.ChangeEvent
	for event, err := range events {
		if err != nil {
			t.Fatalf("streaming: %v", err)
		}
		final = event
	}

	if !final.IsInvalidate() {
		t.Fatalf("expected an invalidate, got %+v", final)
	}
	if final.InvalidateReason() != "CollectionDropped" {
		t.Fatalf("reason = %q", final.InvalidateReason())
	}
}

func TestARecreatedCollectionServesOnlyItsOwnHistory(t *testing.T) {
	// Ids are derived from (database, name), so a recreated collection has the
	// same id and the oplog still holds the dead one's entries. Watching from
	// the start used to replay those and invalidate immediately.
	_, db := connected(t)
	ctx := testContext(t)

	if _, err := db.CreateCollection(ctx, "shop", "orders"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Insert(ctx, "shop", "orders", map[string]any{"_id": 1, "ghost": true}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Request(ctx, http.MethodDelete, "/v1/db/shop/coll/orders", nil, kimmydb.Idempotent); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.CreateCollection(ctx, "shop", "orders"); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	if _, err := db.Insert(ctx, "shop", "orders", map[string]any{"_id": 99, "live": true}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	for event, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{FromStart: true}) {
		if err != nil {
			t.Fatalf("streaming: %v", err)
		}
		if id, ok := event.DocumentID().(float64); !ok || int(id) != 99 {
			t.Fatalf("the dead incarnation's history is not this collection's: %v", event.DocumentID())
		}
		break
	}
}

func TestAResumeTokenFromBeforeADropIsRefused(t *testing.T) {
	// Refused rather than quietly moved forward: between that token and this
	// collection's first event is a gap the client would otherwise never learn
	// about.
	_, db := connected(t)
	ctx := testContext(t)

	if _, err := db.CreateCollection(ctx, "shop", "orders"); err != nil {
		t.Fatalf("create: %v", err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = db.Insert(ctx, "shop", "orders", map[string]any{"_id": 1})
	}()

	var token string
	for event, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{}) {
		if err != nil {
			t.Fatalf("streaming: %v", err)
		}
		token = event.ResumeToken
		break
	}

	if _, err := db.Request(ctx, http.MethodDelete, "/v1/db/shop/coll/orders", nil, kimmydb.Idempotent); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.CreateCollection(ctx, "shop", "orders"); err != nil {
		t.Fatalf("recreate: %v", err)
	}

	refused := false
	for _, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{ResumeAfter: token}) {
		if err != nil {
			var apiErr *kimmydb.APIError
			if errors.As(err, &apiErr) && apiErr.Code == "resume_token_expired" {
				refused = true
			} else {
				t.Fatalf("expected resume_token_expired, got %v", err)
			}
		}
		break
	}
	if !refused {
		t.Fatal("a token from a previous incarnation must be refused")
	}
}
