// Command shelf is a small library catalogue, in Go.
//
// One application written three times; see README.md for the other two and for
// why the embedding is deliberately a toy.
//
//	KIMMY_URL=http://localhost:7878 KIMMY_ROOT_PASSWORD=hunter2 \
//	    go run ../../examples/shelf.go
package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/titusai-io/kimmydb/clients/go/kimmydb"
)

// dim is the width of the toy embedding. Small on purpose: it is a hash, not a
// model.
const dim = 16

type book struct {
	ID    int
	Title string
	Year  int
	Blurb string
}

var catalogue = []book{
	{1, "The Long Way to a Small Angry Planet", 2014, "a crew tunnels wormholes between the stars"},
	{2, "A Memory Called Empire", 2019, "an ambassador arrives at a vast interstellar empire"},
	{3, "Ancillary Justice", 2013, "a starship intelligence in a single human body seeks revenge"},
	{4, "The Dispossessed", 1974, "a physicist travels between twin worlds divided by politics"},
	{5, "Piranesi", 2020, "a man lives alone in an infinite house of statues and tides"},
	{6, "The Left Hand of Darkness", 1969, "an envoy on a frozen world learns its people"},
	{7, "Station Eleven", 2014, "a travelling troupe performs after a collapse"},
	{8, "Klara and the Sun", 2021, "an artificial friend watches a family from a shop window"},
	{9, "Project Hail Mary", 2021, "a lone astronaut wakes on a ship between the stars"},
	{10, "The Fifth Season", 2015, "a continent breaks and a mother searches for her child"},
}

// embed is a deterministic bag-of-words hash, normalized.
//
// Not an embedding. It has no semantic understanding: two texts are near each
// other when they share words. It is here so the pipeline is real without
// needing an API key, and it is the same algorithm in all three languages so
// the three applications agree.
func embed(text string) []float64 {
	vector := make([]float64, dim)
	for _, raw := range strings.Fields(text) {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, raw)
		if word == "" {
			continue
		}
		// FNV-1a, like the webhook ownership hash: stable across versions.
		var hash uint64 = 0xcbf29ce484222325
		for _, b := range []byte(word) {
			hash ^= uint64(b)
			hash *= 0x100000001b3
		}
		vector[hash%dim]++
	}
	var length float64
	for _, v := range vector {
		length += v * v
	}
	if length = math.Sqrt(length); length > 0 {
		for i := range vector {
			vector[i] /= length
		}
	}
	return vector
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "shelf: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	url := os.Getenv("KIMMY_URL")
	if url == "" {
		url = "http://localhost:7878"
	}
	password := os.Getenv("KIMMY_ROOT_PASSWORD")
	if password == "" {
		password = "hunter2"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// One address is all a client needs; the rest of the cluster comes from
	// /v1/topology, and the token is kept alive from here on.
	db, err := kimmydb.New(ctx, url,
		kimmydb.WithCredentials("root", password),
		kimmydb.WithDiscovery(true))
	if err != nil {
		return err
	}
	defer db.Close()

	version, err := db.Version(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("connected to %s — protocol %v, build %v\n", url, version["protocol"], version["version"])

	// -- the shelf ---------------------------------------------------------
	var apiErr *kimmydb.APIError
	if _, err := db.CreateCollection(ctx, "library", "books"); err != nil {
		if !errors.As(err, &apiErr) || apiErr.Code != "conflict" {
			return err
		}
	}

	documents := make([]any, 0, len(catalogue))
	for _, b := range catalogue {
		documents = append(documents, map[string]any{
			"_id": b.ID, "title": b.Title, "year": b.Year, "blurb": b.Blurb,
		})
	}

	// One commit for the whole catalogue: the commit is the cost, so batching
	// is worth roughly two orders of magnitude over inserting one at a time.
	//
	// A second run finds them already there. Branching on the code rather than
	// the status is the point of the error taxonomy — and a batch is all or
	// nothing, so one duplicate means none of them landed.
	if _, err := db.InsertMany(ctx, "library", "books", documents); err != nil {
		if !errors.As(err, &apiErr) || apiErr.Code != "duplicate_key" {
			return err
		}
		fmt.Println("the shelf is already stocked; carrying on")
	} else {
		fmt.Printf("shelved %d books in one commit\n", len(documents))
	}

	// -- what is on it -----------------------------------------------------
	byDecade, err := db.Aggregate(ctx, "library", "books", []any{
		map[string]any{"$group": map[string]any{
			"_id":   map[string]any{"$subtract": []any{"$year", map[string]any{"$mod": []any{"$year", 10}}}},
			"books": map[string]any{"$sum": 1},
		}},
		map[string]any{"$sort": map[string]any{"_id": 1}},
	})
	if err != nil {
		return err
	}
	fmt.Print("by decade:")
	for _, item := range byDecade["documents"].([]any) {
		group := item.(map[string]any)
		fmt.Printf(" %.0fs=%.0f", group["_id"], group["books"])
	}
	fmt.Println()

	// Paging, because a find with no limit is a page rather than the shelf.
	pages, seen := 0, 0
	for page, err := range db.Pages(ctx, "library", "books", kimmydb.Query{Limit: 5}) {
		if err != nil {
			return err
		}
		pages++
		seen += len(page)
	}
	fmt.Printf("walked %d books in %d pages\n", seen, pages)

	// -- semantic search ---------------------------------------------------
	//
	// byo is the default provider: the client supplies the vectors, which is
	// what makes this run with no API key and no model.
	if _, err := db.Request(ctx, http.MethodPost, "/v1/db/library/coll/books/vector",
		map[string]any{"fields": []string{"blurb"}, "provider": map[string]any{"kind": "byo"}, "dim": dim},
		kimmydb.Idempotent); err != nil {
		return err
	}
	for _, b := range catalogue {
		text := b.Title + " " + b.Blurb
		if _, err := db.Request(ctx, http.MethodPut,
			fmt.Sprintf("/v1/db/library/coll/books/docs/%d/vectors", b.ID),
			[]any{map[string]any{"chunk": 0, "vector": embed(text), "text": text}},
			kimmydb.Idempotent); err != nil {
			return err
		}
	}

	query := "ships between the stars"
	hits, err := db.Request(ctx, http.MethodPost, "/v1/db/library/coll/books/vector_search",
		map[string]any{"vector": embed(query), "k": 3}, kimmydb.Idempotent)
	if err != nil {
		return err
	}
	titles := map[int]string{}
	for _, b := range catalogue {
		titles[b.ID] = b.Title
	}
	fmt.Printf("\nnearest to %q:\n", query)
	for _, item := range hits["matches"].([]any) {
		hit := item.(map[string]any)
		fmt.Printf("  %.3f  %s\n", hit["score"], titles[int(hit["_id"].(float64))])
	}

	// -- watching it change ------------------------------------------------
	events := db.Watch(ctx, "library", "books", kimmydb.WatchOptions{FullDocument: true})

	go func() {
		time.Sleep(200 * time.Millisecond)
		// Replaced rather than inserted, so a second run still produces an
		// event rather than a duplicate key.
		_, _ = db.Request(ctx, http.MethodPut,
			"/v1/db/library/coll/books/docs/999?upsert=true",
			map[string]any{"title": "A Late Arrival", "year": 2026,
				"blurb": "arrived after the shelf was read"},
			kimmydb.Idempotent)
	}()

	fmt.Println("\nwatching for changes...")
	for event, err := range events {
		if err != nil {
			return err
		}
		title := "(no post-image)"
		if document := event.FullDocument(); document != nil {
			title = fmt.Sprint(document["title"])
		}
		fmt.Printf("  %s %v — %s\n", event.Operation, event.DocumentID(), title)
		break
	}

	fmt.Println("\ndone.")
	return nil
}
