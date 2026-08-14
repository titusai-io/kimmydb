package kimmydb

import (
	"encoding/json"
	"fmt"
)

// Retry says what a client may do about a failure.
//
// Three-valued rather than a boolean because KimmyDB is leaderless: every node
// accepts writes, so "ask a different node" is a real answer and the right one
// for a failure local to the node that answered.
type Retry string

const (
	// RetryNo means nothing to retry: the request must change, or the
	// condition must.
	RetryNo Retry = "no"
	// RetryWait means the same node, after a delay.
	RetryWait Retry = "wait"
	// RetryElsewhere means a different node.
	RetryElsewhere Retry = "elsewhere"
)

func parseRetry(value string) Retry {
	switch Retry(value) {
	case RetryWait:
		return RetryWait
	case RetryElsewhere:
		return RetryElsewhere
	default:
		// An unknown class reads as `no`, the safe direction: a client that
		// does not understand the advice does not act on it.
		return RetryNo
	}
}

// APIError is a refusal from the server, in the envelope every route uses.
//
// Code is a plain string rather than a set of constants. Codes are additive —
// a server newer than this client will use ones it has never heard of — and
// making an unfamiliar code an error in itself is exactly what Retry exists to
// avoid. Branch on Retry when Code is not one you know.
type APIError struct {
	Status     int
	Code       string
	Message    string
	Retry      Retry
	RetryAfter int // seconds, from Retry-After; 0 when absent
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Message)
}

// Unauthorized reports whether the server rejected the credentials.
func (e *APIError) Unauthorized() bool { return e.Code == "unauthorized" }

// NotFound reports whether the target does not exist.
func (e *APIError) NotFound() bool { return e.Code == "not_found" }

// errorFrom builds an APIError from a refusal.
//
// A body that is not the envelope — a proxy's HTML error page, say — still
// produces an error carrying the status, because the status is the part that
// came from HTTP and is worth keeping.
func errorFrom(status int, body []byte, retryAfter int) *APIError {
	var envelope struct {
		Error   string `json:"error"`
		Message string `json:"message"`
		Retry   string `json:"retry"`
	}
	_ = json.Unmarshal(body, &envelope)

	code := envelope.Error
	if code == "" {
		code = "unknown"
	}
	message := envelope.Message
	if message == "" {
		message = "(no message)"
	}

	retry := envelope.Retry
	if retry == "" {
		// A server older than the retry class. Guessing from the status is
		// worse advice than the server's own, and better than none.
		switch {
		case status >= 500:
			retry = string(RetryElsewhere)
		case status == 429:
			retry = string(RetryWait)
		default:
			retry = string(RetryNo)
		}
	}

	return &APIError{
		Status:     status,
		Code:       code,
		Message:    message,
		Retry:      parseRetry(retry),
		RetryAfter: retryAfter,
	}
}

// TransportError is a request that never got an answer: refused, timed out,
// TLS. It carries the endpoint, because with failover the interesting question
// is *which* node failed rather than that one did.
type TransportError struct {
	Endpoint string
	Err      error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("could not reach %s: %v", e.Endpoint, e.Err)
}

func (e *TransportError) Unwrap() error { return e.Err }

// ErrNoNodeAvailable is returned when every endpoint was tried and none
// answered.
type ErrNoNodeAvailable struct {
	Tried []string
}

func (e *ErrNoNodeAvailable) Error() string {
	return fmt.Sprintf("no node answered; tried %v", e.Tried)
}

// retryOf reports what a client may do about any error this package returns.
//
// A transport failure is Elsewhere because that is what it means: this node
// did not answer, and a peer holds the same data.
func retryOf(err error) Retry {
	switch e := err.(type) {
	case *APIError:
		return e.Retry
	case *TransportError:
		return RetryElsewhere
	default:
		return RetryNo
	}
}
