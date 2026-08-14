package kimmydb_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/titusai-io/kimmydb/clients/go/kimmydb"
)

// Deliberately the same scenario list as the Rust and Python clients. Three
// clients that pass the same scenarios independently are evidence about the
// protocol; three tested differently are three opinions.

// dead is reserved and nothing listens there.
const dead = "http://127.0.0.1:1"

func connected(t *testing.T) (*node, *kimmydb.Client) {
	t.Helper()
	n := startNode(t, 3600)
	db, err := kimmydb.New(testContext(t), n.base,
		kimmydb.WithCredentials("root", rootPassword))
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	t.Cleanup(db.Close)
	return n, db
}

func seed(t *testing.T, db *kimmydb.Client, n int) {
	t.Helper()
	ctx := testContext(t)
	if _, err := db.CreateCollection(ctx, "shop", "orders"); err != nil {
		t.Fatalf("creating the collection: %v", err)
	}
	documents := make([]any, 0, n)
	for i := range n {
		documents = append(documents, map[string]any{"_id": i, "qty": i})
	}
	if _, err := db.InsertMany(ctx, "shop", "orders", documents); err != nil {
		t.Fatalf("seeding: %v", err)
	}
}

func TestAClientBuiltWithCredentialsHoldsAToken(t *testing.T) {
	_, db := connected(t)
	if db.Token() == "" {
		t.Fatal("connecting should log in")
	}
	if _, err := db.Request(testContext(t), http.MethodGet, "/v1/databases", nil, kimmydb.Idempotent); err != nil {
		t.Fatalf("an authenticated request: %v", err)
	}
}

func TestDocumentsRoundTrip(t *testing.T) {
	_, db := connected(t)
	seed(t, db, 5)
	ctx := testContext(t)

	document, err := db.Get(ctx, "shop", "orders", 3)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if document["qty"] != float64(3) {
		t.Fatalf("expected qty 3, got %v", document["qty"])
	}

	// A missing document is nil, not an error: asking whether something exists
	// is an ordinary thing to do.
	missing, err := db.Get(ctx, "shop", "orders", 999)
	if err != nil {
		t.Fatalf("a missing document should not be an error: %v", err)
	}
	if missing != nil {
		t.Fatalf("expected nil, got %v", missing)
	}

	count, err := db.Count(ctx, "shop", "orders", nil)
	if err != nil || count != 5 {
		t.Fatalf("count = %d, %v", count, err)
	}
}

func TestPagingWalksTheWholeCollection(t *testing.T) {
	// The reason the client exists rather than a Find call: an unlimited find
	// returns 100 documents and says nothing about the rest.
	_, db := connected(t)
	seed(t, db, 250)

	seen := 0
	previous := -1
	for document, err := range db.Documents(testContext(t), "shop", "orders", kimmydb.Query{Limit: 50}) {
		if err != nil {
			t.Fatalf("paging: %v", err)
		}
		id := int(document["_id"].(float64))
		if id != previous+1 {
			t.Fatalf("out of order or a gap: %d after %d", id, previous)
		}
		previous = id
		seen++
	}
	if seen != 250 {
		t.Fatalf("the walk saw %d documents", seen)
	}
}

func TestAnUnlimitedFindIsAPageNotTheCollection(t *testing.T) {
	_, db := connected(t)
	seed(t, db, 150)
	ctx := testContext(t)

	page, err := db.Find(ctx, "shop", "orders", kimmydb.Query{})
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if page["count"] != float64(100) {
		t.Fatalf("the default page is 100, got %v", page["count"])
	}
	if _, ok := page["nextCursor"]; !ok {
		t.Fatal("a full page must say how to get the rest")
	}
}

func TestAWalkEndsOnAnEmptyPageNotAMissingToken(t *testing.T) {
	// A collection whose size is an exact multiple of the page size: the last
	// full page still carries a token, so a loop that stopped when the token
	// stopped arriving would read one page too few.
	_, db := connected(t)
	seed(t, db, 100)

	pages := 0
	for page, err := range db.Pages(testContext(t), "shop", "orders", kimmydb.Query{Limit: 100}) {
		if err != nil {
			t.Fatalf("paging: %v", err)
		}
		if len(page) != 100 {
			t.Fatalf("page %d had %d documents", pages, len(page))
		}
		pages++
	}
	if pages != 1 {
		t.Fatalf("expected one page, walked %d", pages)
	}
}

func TestAQueryACursorCannotPageIsRefusedBeforeTheWalk(t *testing.T) {
	_, db := connected(t)
	seed(t, db, 10)

	for _, query := range []kimmydb.Query{
		{Sort: map[string]any{"qty": 1}},
		{Skip: 5},
	} {
		refused := false
		for _, err := range db.Pages(testContext(t), "shop", "orders", query) {
			if err != nil {
				refused = true
			}
		}
		if !refused {
			t.Fatalf("%+v should not be pageable", query)
		}
	}

	// `_id` ascending is the order a cursor already pages in.
	for _, err := range db.Pages(testContext(t), "shop", "orders",
		kimmydb.Query{Sort: map[string]any{"_id": 1}, Limit: 5}) {
		if err != nil {
			t.Fatalf("sorting by _id ascending is allowed: %v", err)
		}
	}
}

func TestARefusalArrivesTyped(t *testing.T) {
	_, db := connected(t)
	seed(t, db, 1)

	_, err := db.Insert(testContext(t), "shop", "orders", map[string]any{"_id": 0})
	var apiErr *kimmydb.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected an APIError, got %v", err)
	}
	if apiErr.Code != "duplicate_key" {
		t.Fatalf("code = %q", apiErr.Code)
	}
	if apiErr.Retry != kimmydb.RetryNo {
		t.Fatalf("a duplicate does not become un-duplicate: %q", apiErr.Retry)
	}
	if apiErr.Status != 409 {
		t.Fatalf("status = %d", apiErr.Status)
	}
}

func TestAClientWithABadTokenAndNoCredentialsSaysSo(t *testing.T) {
	n := startNode(t, 3600)
	db, err := kimmydb.New(testContext(t), n.base, kimmydb.WithToken("not-a-token"))
	if err != nil {
		t.Fatalf("a client with a token does not log in, so it connects: %v", err)
	}
	defer db.Close()

	_, err = db.Request(testContext(t), http.MethodGet, "/v1/databases", nil, kimmydb.Idempotent)
	var apiErr *kimmydb.APIError
	if !errors.As(err, &apiErr) || !apiErr.Unauthorized() {
		t.Fatalf("expected unauthorized, got %v", err)
	}
}

func TestAnExpiredTokenIsReplacedWithoutTheCallerNoticing(t *testing.T) {
	// The point of holding credentials. A one-second lifetime makes the
	// renewal happen on the second request rather than in an hour.
	n := startNode(t, 1)
	db, err := kimmydb.New(testContext(t), n.base, kimmydb.WithCredentials("root", rootPassword))
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer db.Close()

	first := db.Token()
	time.Sleep(1200 * time.Millisecond)

	if _, err := db.Request(testContext(t), http.MethodGet, "/v1/databases", nil, kimmydb.Idempotent); err != nil {
		t.Fatalf("the client should recover from its own token expiring: %v", err)
	}
	if db.Token() == first {
		t.Fatal("the token was not renewed")
	}
}

func TestAnUnreachableNodeIsSkippedForOneThatAnswers(t *testing.T) {
	// Failover, without a cluster: a dead address in front of a live one is
	// the same situation as a node that stopped. The client must survive its
	// own construction failing over — logging in is the first request it
	// makes, and a login that could not move on would make every other
	// endpoint useless.
	n := startNode(t, 3600)
	db, err := kimmydb.New(testContext(t), dead,
		kimmydb.WithEndpoints(n.base),
		kimmydb.WithCredentials("root", rootPassword),
		kimmydb.WithTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("the client gave up on a dead first endpoint: %v", err)
	}
	defer db.Close()

	if _, err := db.Request(testContext(t), http.MethodGet, "/v1/databases", nil, kimmydb.Idempotent); err != nil {
		t.Fatalf("the live node should answer: %v", err)
	}
	if got := db.Endpoints()[0]; got != n.base {
		t.Fatalf("the node that answered should be first, got %q", got)
	}
}

func TestAWriteIsNotRetriedElsewhereAutomatically(t *testing.T) {
	// RetryElsewhere says this node could not answer, not that the work did
	// not happen. A helpful retry of an insert would apply it twice, and no
	// status distinguishes that from one that never landed.
	n, live := connected(t)
	seed(t, live, 1)

	db, err := kimmydb.New(testContext(t), dead,
		kimmydb.WithEndpoints(n.base),
		kimmydb.WithToken(live.Token()),
		kimmydb.WithTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer db.Close()

	_, err = db.Insert(testContext(t), "shop", "orders", map[string]any{"_id": 99})
	var transport *kimmydb.TransportError
	if !errors.As(err, &transport) {
		t.Fatalf("an unsafe request must not move to another node: %v", err)
	}

	// The caller decides — and here it can, because the document carries an
	// _id, so a repeat is a fact rather than a guess.
	created, err := db.Request(testContext(t), http.MethodPost,
		"/v1/db/shop/coll/orders/docs", map[string]any{"_id": 99}, kimmydb.Idempotent)
	if err != nil {
		t.Fatalf("declared idempotent, so it should move to the live node: %v", err)
	}
	if created["insertedId"] != float64(99) {
		t.Fatalf("insertedId = %v", created["insertedId"])
	}
}

func TestEveryEndpointDeadIsReportedAsSuch(t *testing.T) {
	_, err := kimmydb.New(testContext(t), dead,
		kimmydb.WithCredentials("root", "whatever"),
		kimmydb.WithTimeout(3*time.Second))
	if err == nil {
		t.Fatal("expected a failure when nothing is listening")
	}
}

func TestVersionAndTopology(t *testing.T) {
	n, db := connected(t)
	ctx := testContext(t)

	version, err := db.Version(ctx)
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	if version["protocol"] != "v1" {
		t.Fatalf("protocol = %v", version["protocol"])
	}

	has, err := db.HasCapability(ctx, "cursor-paging")
	if err != nil || !has {
		t.Fatalf("cursor-paging should be advertised: %v %v", has, err)
	}
	if has, _ := db.HasCapability(ctx, "a-capability-nobody-has"); has {
		t.Fatal("an unknown capability should not be advertised")
	}

	// A single node with no advertised endpoint still lists itself, so
	// discovery cannot leave a client with nowhere to go.
	topology, err := db.Topology(ctx)
	if err != nil || topology["count"] != float64(1) {
		t.Fatalf("topology = %v, %v", topology, err)
	}
	endpoints, err := db.RefreshTopology(ctx)
	if err != nil || len(endpoints) != 1 || endpoints[0] != n.base {
		t.Fatalf("an unadvertised node should not be dialled: %v %v", endpoints, err)
	}
}
