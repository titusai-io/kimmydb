package kimmydb

import (
	"context"
	"fmt"
	"iter"
	"net/http"
)

// Pages walks a collection, one page at a time.
//
//	for page, err := range db.Pages(ctx, "shop", "orders", kimmydb.Query{Limit: 500}) {
//	    if err != nil {
//	        return err
//	    }
//	    ...
//	}
//
// A range-over-function iterator rather than a cursor object with a Next
// method: it is the shape a Go caller expects, and it makes the error
// impossible to skip — the loop variable carries it.
//
// The walk ends on a short or empty page, not on a missing token. A final page
// that is exactly full still carries one — the server cannot know it is the
// last without looking further — so a loop that stopped when the token stopped
// arriving would read one page too few. This handles that; a hand-rolled loop
// is where it gets forgotten.
func (c *Client) Pages(ctx context.Context, db, collection string, query Query) iter.Seq2[[]any, error] {
	return func(yield func([]any, error) bool) {
		if err := pageable(query); err != nil {
			yield(nil, err)
			return
		}

		body := query.body()
		for {
			response, err := c.request(ctx, http.MethodPost, path(db, collection, "find"), body, Idempotent)
			if err != nil {
				yield(nil, err)
				return
			}

			documents, _ := response["documents"].([]any)
			if len(documents) == 0 {
				return
			}
			if !yield(documents, nil) {
				return
			}

			cursor, _ := response["nextCursor"].(string)
			if cursor == "" {
				return
			}
			body["cursor"] = cursor
		}
	}
}

// Documents yields every matching document, paging underneath.
//
// The shape most callers want, and the one that makes forgetting to page
// impossible.
func (c *Client) Documents(ctx context.Context, db, collection string, query Query) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		for page, err := range c.Pages(ctx, db, collection, query) {
			if err != nil {
				yield(nil, err)
				return
			}
			for _, item := range page {
				document, _ := item.(map[string]any)
				if !yield(document, nil) {
					return
				}
			}
		}
	}
}

// pageable applies the server's rule here, so a walk fails on the first page
// with an explanation rather than on the second with a refusal.
func pageable(query Query) error {
	if query.Skip > 0 {
		return fmt.Errorf("kimmydb: `skip` and a cursor both say where to resume; use one")
	}
	if len(query.Sort) == 0 {
		return nil
	}
	if len(query.Sort) == 1 {
		if direction, ok := query.Sort["_id"]; ok && isOne(direction) {
			return nil
		}
	}
	return fmt.Errorf(
		"kimmydb: a cursor pages in _id order, so it takes no other `sort`; " +
			"sorting by another field still uses `skip`")
}

func isOne(value any) bool {
	switch number := value.(type) {
	case int:
		return number == 1
	case int64:
		return number == 1
	case float64:
		return number == 1
	default:
		return false
	}
}
