// Command conformance is the Go client's conformance driver.
//
// One of three programs that answer the same questions in three languages. The
// runner (clients/conformance/run.py) executes every scenario against every
// driver and compares what comes back to expectations declared once, in
// clients/conformance/scenarios.json.
//
// A driver reports observations; it does not decide whether they are right.
// Three clients that each judged themselves would be three opinions, and what
// is wanted is one oracle and three answers.
//
//	conformance list
//	conformance run <scenario> <base-url> [dead-url]
//
// Output is a single JSON object on stdout. Anything else goes to stderr.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/titusai-io/kimmydb/clients/go/kimmydb"
)

var scenarios = []string{
	"capabilities",
	"documents_round_trip",
	"unlimited_find_is_a_page",
	"paging_walks_everything",
	"walk_ends_on_empty_page",
	"cursor_refuses_what_it_cannot_page",
	"creating_a_collection_twice_is_a_conflict",
	"duplicate_key_is_typed",
	"token_is_renewed",
	"failover_past_a_dead_endpoint",
	"write_is_not_retried_elsewhere",
	"change_stream_delivers",
	"change_stream_resumes",
	"dropped_collection_ends_stream",
	"recreated_collection_serves_its_own_history",
	"stale_resume_token_is_refused",
	"stale_write_is_typed",
	"array_filters_address_one_element",
}

func password() string {
	if value := os.Getenv("KIMMY_ROOT_PASSWORD"); value != "" {
		return value
	}
	return "conformance-password"
}

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "list" {
		emit(scenarios)
		return
	}
	if len(os.Args) >= 4 && os.Args[1] == "run" {
		dead := "http://127.0.0.1:1"
		if len(os.Args) > 4 {
			dead = os.Args[4]
		}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		observations, err := run(ctx, os.Args[2], os.Args[3], dead)
		if err != nil {
			emit(map[string]any{"error": err.Error()})
			os.Exit(1)
		}
		emit(observations)
		return
	}
	fmt.Fprintln(os.Stderr, "usage: conformance list | conformance run <scenario> <base-url> [dead-url]")
	os.Exit(2)
}

func emit(value any) {
	encoded, err := json.Marshal(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "encoding the result: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(encoded))
}

func connect(ctx context.Context, base string) (*kimmydb.Client, error) {
	return kimmydb.New(ctx, base, kimmydb.WithCredentials("root", password()))
}

func seed(ctx context.Context, db *kimmydb.Client, n int) error {
	if _, err := db.CreateCollection(ctx, "shop", "orders"); err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	documents := make([]any, 0, n)
	for i := range n {
		documents = append(documents, map[string]any{"_id": i, "qty": i})
	}
	_, err := db.InsertMany(ctx, "shop", "orders", documents)
	return err
}

// recreate creates shop.orders again after it was dropped, waiting out the
// drop's purge: until that is done the name is refused 503 collection_purging,
// retry wait, with a Retry-After (ADR-189). On a tiny collection the purge
// usually wins the race, which is why recreating at once passed until it
// didn't.
func recreate(ctx context.Context, db *kimmydb.Client) error {
	giveUp := time.Now().Add(30 * time.Second)
	for {
		_, err := db.CreateCollection(ctx, "shop", "orders")
		var apiErr *kimmydb.APIError
		if err == nil || !errors.As(err, &apiErr) || apiErr.Code != "collection_purging" ||
			time.Now().After(giveUp) {
			return err
		}
		wait := time.Second
		if apiErr.RetryAfter > 0 {
			wait = time.Duration(apiErr.RetryAfter) * time.Second
		}
		if left := time.Until(giveUp); wait > left {
			wait = left
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func run(ctx context.Context, scenario, base, dead string) (map[string]any, error) {
	switch scenario {
	case "capabilities":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		version, err := db.Version(ctx)
		if err != nil {
			return nil, err
		}
		paging, err := db.HasCapability(ctx, "cursor-paging")
		if err != nil {
			return nil, err
		}
		invented, err := db.HasCapability(ctx, "a-capability-nobody-has")
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"protocol":                version["protocol"],
			"has_cursor_paging":       paging,
			"has_invented_capability": invented,
		}, nil

	case "documents_round_trip":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 5); err != nil {
			return nil, err
		}
		found, err := db.Get(ctx, "shop", "orders", 3)
		if err != nil {
			return nil, err
		}
		missing, err := db.Get(ctx, "shop", "orders", 999)
		if err != nil {
			return nil, err
		}
		count, err := db.Count(ctx, "shop", "orders", nil)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"qty":               found["qty"],
			"missing_is_absent": missing == nil,
			"count":             count,
		}, nil

	case "unlimited_find_is_a_page":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 150); err != nil {
			return nil, err
		}
		page, err := db.Find(ctx, "shop", "orders", kimmydb.Query{})
		if err != nil {
			return nil, err
		}
		_, hasCursor := page["nextCursor"]
		count, err := db.Count(ctx, "shop", "orders", nil)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"page":          page["count"],
			"offers_cursor": hasCursor,
			"total":         count,
		}, nil

	case "paging_walks_everything":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 250); err != nil {
			return nil, err
		}
		var ids []int
		ordered := true
		for document, err := range db.Documents(ctx, "shop", "orders", kimmydb.Query{Limit: 50}) {
			if err != nil {
				return nil, err
			}
			id := int(document["_id"].(float64))
			if len(ids) > 0 && id <= ids[len(ids)-1] {
				ordered = false
			}
			ids = append(ids, id)
		}
		first, last := -1, -1
		if len(ids) > 0 {
			first, last = ids[0], ids[len(ids)-1]
		}
		return map[string]any{
			"documents_seen": len(ids),
			"first_id":       first,
			"last_id":        last,
			"ordered":        ordered,
		}, nil

	case "walk_ends_on_empty_page":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 100); err != nil {
			return nil, err
		}
		pages, seen := 0, 0
		for page, err := range db.Pages(ctx, "shop", "orders", kimmydb.Query{Limit: 100}) {
			if err != nil {
				return nil, err
			}
			pages++
			seen += len(page)
		}
		return map[string]any{"pages": pages, "documents_seen": seen}, nil

	case "cursor_refuses_what_it_cannot_page":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 10); err != nil {
			return nil, err
		}
		refused := false
		for _, err := range db.Pages(ctx, "shop", "orders", kimmydb.Query{Sort: map[string]any{"qty": 1}}) {
			if err != nil {
				refused = true
			}
		}
		allowed := false
		for _, err := range db.Pages(ctx, "shop", "orders",
			kimmydb.Query{Sort: map[string]any{"_id": 1}, Limit: 5}) {
			allowed = err == nil
			break
		}
		return map[string]any{"sorted_walk_refused": refused, "id_sort_allowed": allowed}, nil

	case "creating_a_collection_twice_is_a_conflict":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		first, err := db.CreateCollection(ctx, "shop", "orders")
		if err != nil {
			return nil, err
		}
		_, err = db.CreateCollection(ctx, "shop", "orders")
		var existsErr *kimmydb.APIError
		if !errors.As(err, &existsErr) {
			return nil, fmt.Errorf("creating an existing collection must be a conflict, got %v", err)
		}
		return map[string]any{
			"first_created": first["created"] == "orders",
			"second_code":   existsErr.Code,
			"second_status": existsErr.Status,
		}, nil

	case "duplicate_key_is_typed":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 1); err != nil {
			return nil, err
		}
		_, err = db.Insert(ctx, "shop", "orders", map[string]any{"_id": 0})
		var apiErr *kimmydb.APIError
		if !errors.As(err, &apiErr) {
			return nil, fmt.Errorf("a duplicate _id must be refused, got %v", err)
		}
		return map[string]any{
			"code":   apiErr.Code,
			"retry":  string(apiErr.Retry),
			"status": apiErr.Status,
		}, nil

	case "token_is_renewed":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		first := db.Token()
		// The node this scenario runs against issues one-second tokens.
		time.Sleep(1200 * time.Millisecond)
		_, requestErr := db.Request(ctx, http.MethodGet, "/v1/databases", nil, kimmydb.Idempotent)
		return map[string]any{
			"token_changed":     db.Token() != first,
			"request_succeeded": requestErr == nil,
		}, nil

	case "failover_past_a_dead_endpoint":
		// The dead address is first, so even logging in has to move on.
		db, err := kimmydb.New(ctx, dead,
			kimmydb.WithEndpoints(base),
			kimmydb.WithCredentials("root", password()),
			kimmydb.WithTimeout(5*time.Second))
		if err != nil {
			return nil, err
		}
		_, requestErr := db.Request(ctx, http.MethodGet, "/v1/databases", nil, kimmydb.Idempotent)
		return map[string]any{
			"answered":                  requestErr == nil,
			"live_endpoint_first_after": db.Endpoints()[0] == base,
		}, nil

	case "write_is_not_retried_elsewhere":
		live, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, live, 1); err != nil {
			return nil, err
		}
		db, err := kimmydb.New(ctx, dead,
			kimmydb.WithEndpoints(base),
			kimmydb.WithToken(live.Token()),
			kimmydb.WithTimeout(5*time.Second))
		if err != nil {
			return nil, err
		}
		_, writeErr := db.Insert(ctx, "shop", "orders", map[string]any{"_id": 99})
		var transport *kimmydb.TransportError
		if !errors.As(writeErr, &transport) {
			return nil, fmt.Errorf("an unsafe write must not move to another node, got %v", writeErr)
		}
		_, idempotentErr := db.Request(ctx, http.MethodPost,
			"/v1/db/shop/coll/orders/docs", map[string]any{"_id": 99}, kimmydb.Idempotent)
		return map[string]any{
			"write_failed":               true,
			"retry_class":                string(kimmydb.RetryElsewhere),
			"idempotent_retry_succeeded": idempotentErr == nil,
		}, nil

	case "change_stream_delivers":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 1); err != nil {
			return nil, err
		}
		events := db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{FullDocument: true})

		go func() {
			time.Sleep(300 * time.Millisecond)
			for id := 100; id < 103; id++ {
				_, _ = db.Insert(ctx, "shop", "orders", map[string]any{"_id": id})
			}
		}()

		var ids []int
		allInserts, full := true, true
		for event, err := range events {
			if err != nil {
				return nil, err
			}
			allInserts = allInserts && event.Operation == "insert"
			full = full && event.FullDocument() != nil
			ids = append(ids, int(event.DocumentID().(float64)))
			if len(ids) == 3 {
				break
			}
		}
		return map[string]any{
			"events":            len(ids),
			"ids":               ids,
			"all_inserts":       allInserts,
			"has_full_document": full,
		}, nil

	case "change_stream_resumes":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 1); err != nil {
			return nil, err
		}
		go func() {
			time.Sleep(300 * time.Millisecond)
			_, _ = db.Insert(ctx, "shop", "orders", map[string]any{"_id": 200})
		}()

		var token string
		for event, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{}) {
			if err != nil {
				return nil, err
			}
			token = event.ResumeToken
			break
		}

		// Written while nothing is listening.
		if _, err := db.Insert(ctx, "shop", "orders", map[string]any{"_id": 201}); err != nil {
			return nil, err
		}

		resumed := -1
		for event, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{ResumeAfter: token}) {
			if err != nil {
				return nil, err
			}
			resumed = int(event.DocumentID().(float64))
			break
		}
		return map[string]any{"resumed_id": resumed}, nil

	case "dropped_collection_ends_stream":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 1); err != nil {
			return nil, err
		}
		events := db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{})
		go func() {
			time.Sleep(300 * time.Millisecond)
			_, _ = db.Request(ctx, http.MethodDelete, "/v1/db/shop/coll/orders", nil, kimmydb.Idempotent)
		}()

		var final kimmydb.ChangeEvent
		for event, err := range events {
			if err != nil {
				return nil, err
			}
			final = event
		}
		return map[string]any{
			"operation": final.Operation,
			"reason":    final.InvalidateReason(),
		}, nil

	case "recreated_collection_serves_its_own_history":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 1); err != nil {
			return nil, err
		}
		if _, err := db.Request(ctx, http.MethodDelete, "/v1/db/shop/coll/orders", nil, kimmydb.Idempotent); err != nil {
			return nil, err
		}
		if err := recreate(ctx, db); err != nil {
			return nil, err
		}
		if _, err := db.Insert(ctx, "shop", "orders", map[string]any{"_id": 99}); err != nil {
			return nil, err
		}

		first := -1
		for event, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{FromStart: true}) {
			if err != nil {
				return nil, err
			}
			first = int(event.DocumentID().(float64))
			break
		}
		return map[string]any{"first_id": first}, nil

	case "stale_write_is_typed":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 0); err != nil {
			return nil, err
		}
		inserted, err := db.Insert(ctx, "shop", "orders", map[string]any{"_id": 0, "qty": 0})
		if err != nil {
			return nil, err
		}
		stamp, _ := inserted["stamp"].(string)
		filter := map[string]any{"_id": 0}
		first, err := db.UpdateIf(ctx, "shop", "orders", filter, map[string]any{"$set": map[string]any{"qty": 1}}, stamp)
		if err != nil {
			return nil, err
		}
		_, err = db.UpdateIf(ctx, "shop", "orders", filter, map[string]any{"$set": map[string]any{"qty": 2}}, stamp)
		var apiErr *kimmydb.APIError
		if !errors.As(err, &apiErr) {
			return nil, fmt.Errorf("the same stamp a second time must be refused, got %v", err)
		}
		return map[string]any{
			"first_write_ok": first["modified"] == float64(1),
			"code":           apiErr.Code,
			"retry":          string(apiErr.Retry),
			"status":         apiErr.Status,
		}, nil

	case "array_filters_address_one_element":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 0); err != nil {
			return nil, err
		}
		items := []any{
			map[string]any{"sku": "a", "shipped": false},
			map[string]any{"sku": "b", "shipped": false},
		}
		if _, err := db.Insert(ctx, "shop", "orders", map[string]any{"_id": 0, "items": items}); err != nil {
			return nil, err
		}
		filter := map[string]any{"_id": 0}
		update := map[string]any{"$set": map[string]any{"items.$[line].shipped": true}}
		updated, err := db.UpdateWith(ctx, "shop", "orders", filter, update, kimmydb.UpdateOptions{
			ArrayFilters: []map[string]any{{"line.sku": "b"}},
		})
		if err != nil {
			return nil, err
		}
		document, err := db.Get(ctx, "shop", "orders", 0)
		if err != nil {
			return nil, err
		}
		lines, _ := document["items"].([]any)
		shipped := make([]any, 0, len(lines))
		for _, line := range lines {
			shipped = append(shipped, line.(map[string]any)["shipped"])
		}
		_, err = db.UpdateWith(ctx, "shop", "orders", filter, update, kimmydb.UpdateOptions{})
		var apiErr *kimmydb.APIError
		if !errors.As(err, &apiErr) {
			return nil, fmt.Errorf("an identifier without a filter must be refused, got %v", err)
		}
		return map[string]any{
			"modified":              updated["modified"],
			"shipped":               shipped,
			"missing_filter_code":   apiErr.Code,
			"missing_filter_status": apiErr.Status,
		}, nil

	case "stale_resume_token_is_refused":
		db, err := connect(ctx, base)
		if err != nil {
			return nil, err
		}
		if err := seed(ctx, db, 1); err != nil {
			return nil, err
		}
		go func() {
			time.Sleep(300 * time.Millisecond)
			_, _ = db.Insert(ctx, "shop", "orders", map[string]any{"_id": 5})
		}()

		var token string
		for event, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{}) {
			if err != nil {
				return nil, err
			}
			token = event.ResumeToken
			break
		}

		if _, err := db.Request(ctx, http.MethodDelete, "/v1/db/shop/coll/orders", nil, kimmydb.Idempotent); err != nil {
			return nil, err
		}
		if err := recreate(ctx, db); err != nil {
			return nil, err
		}

		code := ""
		for _, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{ResumeAfter: token}) {
			var apiErr *kimmydb.APIError
			if errors.As(err, &apiErr) {
				code = apiErr.Code
			}
			break
		}
		return map[string]any{"code": code}, nil
	}

	return nil, fmt.Errorf("unknown scenario %q", scenario)
}
