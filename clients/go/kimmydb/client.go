// Package kimmydb is the Go client for KimmyDB.
//
//	db, err := kimmydb.New(ctx, "http://localhost:7878",
//	    kimmydb.WithCredentials("root", "hunter2"),
//	    kimmydb.WithDiscovery(true),
//	)
//	if err != nil { ... }
//	defer db.Close()
//
//	err = db.Insert(ctx, "shop", "orders", map[string]any{"sku": "widget"})
//
//	for page, err := range db.Pages(ctx, "shop", "orders", Query{Limit: 500}) {
//	    if err != nil { ... }
//	    for _, document := range page { ... }
//	}
//
// # It talks to the protocol, not to the server
//
// This package shares no code with the server, with the Rust client, or with
// the Python one. The three are independent readers of docs/openapi.yaml,
// which is what makes a disagreement between them mean something.
//
// # What it does, and what it deliberately does not
//
//   - Keeps a token alive, refreshing before expiry from expiresIn rather than
//     by decoding a token it is told to treat as opaque.
//   - Fails over between nodes, discovered from /v1/topology.
//   - Pages with cursors, ending a walk on an empty page rather than a missing
//     token.
//   - Returns typed errors carrying the retry class, so an unfamiliar code is
//     still actionable.
//   - Resumes change streams from the last token seen, which is safe only
//     because those tokens are portable between nodes.
//
// And one thing it will not do: retry a write. RetryElsewhere means *this
// node* did not answer, not that the work did not happen.
package kimmydb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// renewBefore is how long before expiry the client renews its token.
//
// Not zero, because a token that expires between the check and the server
// reading it fails for a reason the client could have avoided; and not
// minutes, because it would spend most of a short lifetime refreshing.
const renewBefore = 60 * time.Second

// Client is a connection to a KimmyDB cluster. Safe for concurrent use.
type Client struct {
	http        *http.Client
	credentials *credentials
	timeout     time.Duration

	mu        sync.RWMutex
	endpoints []string
	token     string
	renewAt   time.Time
}

type credentials struct{ user, password string }

// Option configures a Client.
type Option func(*options)

type options struct {
	endpoints   []string
	credentials *credentials
	token       string
	discover    bool
	timeout     time.Duration
	httpClient  *http.Client
}

// WithCredentials logs in and keeps the token renewed.
func WithCredentials(user, password string) Option {
	return func(o *options) { o.credentials = &credentials{user, password} }
}

// WithToken uses a token obtained elsewhere.
//
// Without credentials there is nothing to log in with, so when the token stops
// being accepted the client says so rather than recovering.
func WithToken(token string) Option {
	return func(o *options) { o.token = token }
}

// WithEndpoints adds nodes to try, before any are discovered.
func WithEndpoints(endpoints ...string) Option {
	return func(o *options) { o.endpoints = append(o.endpoints, endpoints...) }
}

// WithDiscovery learns the rest of the cluster from /v1/topology at connect
// time.
func WithDiscovery(discover bool) Option {
	return func(o *options) { o.discover = discover }
}

// WithTimeout bounds a single request.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// WithHTTPClient supplies the HTTP client to use — for a custom TLS
// configuration, a proxy, or a transport with different pool limits.
//
// The change stream uses it too, so there is one configuration rather than two
// that can drift apart.
func WithHTTPClient(client *http.Client) Option {
	return func(o *options) { o.httpClient = client }
}

// New connects to a cluster, logging in if it was given credentials.
func New(ctx context.Context, endpoint string, opts ...Option) (*Client, error) {
	settings := options{timeout: 30 * time.Second}
	for _, apply := range opts {
		apply(&settings)
	}

	httpClient := settings.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: settings.timeout}
	}

	client := &Client{
		http:        httpClient,
		credentials: settings.credentials,
		timeout:     settings.timeout,
		endpoints:   append([]string{normalize(endpoint)}, normalizeAll(settings.endpoints)...),
		token:       settings.token,
	}
	if settings.token != "" {
		// A supplied token has no stated lifetime, so nothing is renewed on a
		// schedule. If it expires the server says so, which is the honest
		// outcome for a credential this client did not obtain and cannot
		// obtain again.
		client.renewAt = time.Now().Add(100 * 365 * 24 * time.Hour)
	}

	if err := client.authenticate(ctx); err != nil {
		return nil, err
	}
	if settings.discover {
		if _, err := client.RefreshTopology(ctx); err != nil {
			return nil, err
		}
	}
	return client, nil
}

// Close releases idle connections.
func (c *Client) Close() {
	c.http.CloseIdleConnections()
}

// Endpoints returns the nodes this client will try, in order.
func (c *Client) Endpoints() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]string(nil), c.endpoints...)
}

// Token returns the token in use, if there is one.
func (c *Client) Token() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.token
}

// -- the node itself --------------------------------------------------------

// Version reports what this node is and what it can do.
//
// Ask before assuming a feature exists: in a cluster mid-upgrade the node
// answering the next request may be older than this one.
func (c *Client) Version(ctx context.Context) (map[string]any, error) {
	return c.request(ctx, http.MethodGet, "/v1/version", nil, idempotent)
}

// HasCapability reports whether the node that answered has a named capability.
func (c *Client) HasCapability(ctx context.Context, capability string) (bool, error) {
	version, err := c.Version(ctx)
	if err != nil {
		return false, err
	}
	list, _ := version["capabilities"].([]any)
	for _, item := range list {
		if name, ok := item.(string); ok && name == capability {
			return true, nil
		}
	}
	return false, nil
}

// Topology reports the nodes this one knows about.
func (c *Client) Topology(ctx context.Context) (map[string]any, error) {
	return c.request(ctx, http.MethodGet, "/v1/topology", nil, idempotent)
}

// RefreshTopology re-reads the cluster's node list and adopts it.
//
// Skips entries with no advertised endpoint — a node that has not been told
// what to advertise cannot be dialled — and entries that are not live, since
// the point of the list is somewhere to go now. The current endpoint stays
// first either way.
func (c *Client) RefreshTopology(ctx context.Context) ([]string, error) {
	body, err := c.Topology(ctx)
	if err != nil {
		return nil, err
	}

	nodes, _ := body["nodes"].([]any)
	discovered := make([]string, 0, len(nodes))
	for _, item := range nodes {
		node, _ := item.(map[string]any)
		if node["status"] != "live" {
			continue
		}
		if endpoint, ok := node["endpoint"].(string); ok && endpoint != "" {
			discovered = append(discovered, normalize(endpoint))
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.endpoints[0]
	ordered := []string{current}
	for _, endpoint := range discovered {
		if endpoint != current {
			ordered = append(ordered, endpoint)
		}
	}
	c.endpoints = ordered
	return append([]string(nil), c.endpoints...), nil
}

// -- documents --------------------------------------------------------------

// CreateCollection creates a collection. Creating one twice is creating it
// once.
func (c *Client) CreateCollection(ctx context.Context, db, collection string) (map[string]any, error) {
	return c.request(ctx, http.MethodPost, "/v1/db/"+db+"/collections",
		map[string]any{"name": collection}, idempotent)
}

// Insert inserts one document.
//
// Not retried automatically. An insert whose answer was lost may have landed,
// and repeating it would insert a second document under a new _id. Give the
// document an _id and use Request with Idempotent if you want that: a repeat
// then fails with duplicate_key, which is a fact rather than a guess.
func (c *Client) Insert(ctx context.Context, db, collection string, document any) (map[string]any, error) {
	return c.request(ctx, http.MethodPost, path(db, collection, "docs"), document, unsafeToRetry)
}

// InsertMany inserts documents in one commit — all of them, or none.
func (c *Client) InsertMany(ctx context.Context, db, collection string, documents []any) (map[string]any, error) {
	return c.request(ctx, http.MethodPost, path(db, collection, "bulk"), documents, unsafeToRetry)
}

// Get returns one document by _id, or nil when there is none.
//
// A missing document is not an error: asking whether something exists is an
// ordinary thing to do, and returning one would make callers inspect an error
// to find out.
func (c *Client) Get(ctx context.Context, db, collection string, id any) (map[string]any, error) {
	document, err := c.request(ctx, http.MethodGet,
		fmt.Sprintf("/v1/db/%s/coll/%s/docs/%v", db, collection, id), nil, idempotent)
	if err != nil {
		if apiErr, ok := err.(*APIError); ok && apiErr.NotFound() {
			return nil, nil
		}
		return nil, err
	}
	return document, nil
}

// Query is what to ask a collection for.
//
// Omitting Limit means 100 documents, not all of them. That is the server's
// behaviour, and hiding it here would only move the surprise.
type Query struct {
	Filter     map[string]any
	Sort       map[string]any
	Projection map[string]any
	Limit      int
	Skip       int
	Explain    bool
}

func (q Query) body() map[string]any {
	body := map[string]any{"explain": q.Explain}
	if q.Filter != nil {
		body["filter"] = q.Filter
	}
	if q.Sort != nil {
		body["sort"] = q.Sort
	}
	if q.Projection != nil {
		body["projection"] = q.Projection
	}
	if q.Limit > 0 {
		body["limit"] = q.Limit
	}
	if q.Skip > 0 {
		body["skip"] = q.Skip
	}
	return body
}

// Find returns one page of a query.
func (c *Client) Find(ctx context.Context, db, collection string, query Query) (map[string]any, error) {
	return c.request(ctx, http.MethodPost, path(db, collection, "find"), query.body(), idempotent)
}

// Count reports how many documents match. No page cap — a count sees
// everything.
func (c *Client) Count(ctx context.Context, db, collection string, filter map[string]any) (int64, error) {
	if filter == nil {
		filter = map[string]any{}
	}
	body, err := c.request(ctx, http.MethodPost, path(db, collection, "count"),
		map[string]any{"filter": filter}, idempotent)
	if err != nil {
		return 0, err
	}
	count, ok := body["count"].(float64)
	if !ok {
		return 0, fmt.Errorf("kimmydb: count did not return a number")
	}
	return int64(count), nil
}

// Update applies update operators to matching documents.
func (c *Client) Update(ctx context.Context, db, collection string, filter, update map[string]any, multi bool) (map[string]any, error) {
	return c.request(ctx, http.MethodPost, path(db, collection, "update"),
		map[string]any{"filter": filter, "update": update, "multi": multi}, unsafeToRetry)
}

// Delete removes matching documents.
func (c *Client) Delete(ctx context.Context, db, collection string, filter map[string]any, multi bool) (map[string]any, error) {
	return c.request(ctx, http.MethodPost, path(db, collection, "delete"),
		map[string]any{"filter": filter, "multi": multi}, unsafeToRetry)
}

// Aggregate runs a pipeline.
func (c *Client) Aggregate(ctx context.Context, db, collection string, pipeline []any) (map[string]any, error) {
	return c.request(ctx, http.MethodPost, path(db, collection, "aggregate"),
		map[string]any{"pipeline": pipeline}, idempotent)
}

// -- the escape hatch -------------------------------------------------------

// Safety says whether a request may be repeated after a failure.
//
// The distinction the protocol cannot make for you: RetryElsewhere says *this
// node* could not answer, not that the work did not happen.
type Safety bool

const (
	// Idempotent: repeating it cannot change the outcome.
	Idempotent Safety = true
	// Unsafe: repeating it might apply the work twice.
	Unsafe Safety = false
)

const (
	idempotent    = Idempotent
	unsafeToRetry = Unsafe
)

// Request reaches any route by path.
//
// Present because a client that covers a subset of an API and cannot reach the
// rest sends people back to curl for one call. Everything above is a
// convenience over this.
func (c *Client) Request(ctx context.Context, method, routePath string, body any, safety Safety) (map[string]any, error) {
	return c.request(ctx, method, routePath, body, safety)
}

// Download fetches raw bytes, for the routes that are not JSON — the backup.
func (c *Client) Download(ctx context.Context, routePath string) ([]byte, error) {
	if err := c.authenticate(ctx); err != nil {
		return nil, err
	}
	endpoint := c.primary()
	response, err := c.send(ctx, endpoint, http.MethodGet, routePath, nil, c.Token())
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	raw, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		return nil, &TransportError{Endpoint: endpoint, Err: readErr}
	}
	if response.StatusCode >= 400 {
		return nil, errorFrom(response.StatusCode, raw, retryAfterOf(response))
	}
	return raw, nil
}

// -- authentication ---------------------------------------------------------

// authenticate ensures there is a usable token, logging in or refreshing.
//
// Refresh is preferred over a fresh login: it costs no password verification —
// the login limiter exists to bound that work — and an application that stored
// credentials should be able to forget them for as long as it stays connected.
func (c *Client) authenticate(ctx context.Context) error {
	c.mu.RLock()
	token, renewAt := c.token, c.renewAt
	c.mu.RUnlock()

	if token != "" && time.Now().Before(renewAt) {
		return nil
	}
	if token == "" && c.credentials == nil {
		return nil
	}

	if token != "" {
		// A refresh that fails is not fatal: the token may still be good, and
		// if it is not, the next request says so with the server's own reason.
		if err := c.refresh(ctx); err == nil {
			return nil
		}
	}
	if c.credentials != nil {
		return c.login(ctx)
	}
	return nil
}

func (c *Client) login(ctx context.Context) error {
	body, err := c.sendAny(ctx, http.MethodPost, "/v1/auth/login", map[string]any{
		"user":     c.credentials.user,
		"password": c.credentials.password,
	}, "")
	if err != nil {
		return err
	}
	return c.adopt(body)
}

func (c *Client) refresh(ctx context.Context) error {
	body, err := c.sendAny(ctx, http.MethodPost, "/v1/auth/refresh", nil, c.Token())
	if err != nil {
		return err
	}
	return c.adopt(body)
}

func (c *Client) adopt(body map[string]any) error {
	token, _ := body["token"].(string)
	if token == "" {
		return fmt.Errorf("kimmydb: no token in the response")
	}
	// expiresIn rather than decoding the token: it is opaque, and a client
	// that parses one depends on a shape nothing promised it.
	lifetime := 3600.0
	if seconds, ok := body["expiresIn"].(float64); ok {
		lifetime = seconds
	}
	renewIn := time.Duration(lifetime)*time.Second - renewBefore
	if renewIn < time.Second {
		renewIn = time.Second
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = token
	c.renewAt = time.Now().Add(renewIn)
	return nil
}

// -- the request path -------------------------------------------------------

func (c *Client) request(ctx context.Context, method, routePath string, body any, safety Safety) (map[string]any, error) {
	if err := c.authenticate(ctx); err != nil {
		return nil, err
	}
	// GET is idempotent by definition, so a caller never has to say so.
	if method == http.MethodGet {
		safety = Idempotent
	}

	endpoints := c.Endpoints()
	tried := make([]string, 0, len(endpoints))
	var last error
	relogged := false

	for _, endpoint := range endpoints {
		tried = append(tried, endpoint)
		for {
			parsed, err := c.roundTrip(ctx, endpoint, method, routePath, body)
			if err == nil {
				c.promote(endpoint)
				return parsed, nil
			}

			// A token the server has stopped accepting: log in again once, in
			// case it merely expired. Once, because a loop here is how a
			// client hammers a login endpoint forever.
			if apiErr, ok := err.(*APIError); ok && apiErr.Unauthorized() && !relogged && c.credentials != nil {
				relogged = true
				if c.login(ctx) == nil {
					continue
				}
			}

			switch retryOf(err) {
			case RetryWait:
				if safety != Idempotent {
					return nil, err
				}
				delay := time.Second
				if apiErr, ok := err.(*APIError); ok && apiErr.RetryAfter > 0 {
					delay = time.Duration(min(apiErr.RetryAfter, 30)) * time.Second
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(delay):
				}
				last = err
			case RetryElsewhere:
				if safety != Idempotent {
					return nil, err
				}
				last = err
			default:
				return nil, err
			}
			break
		}
	}

	if last != nil {
		return nil, last
	}
	return nil, &ErrNoNodeAvailable{Tried: tried}
}

// sendAny is a request that must reach *some* node, tried against each in turn.
//
// Login and refresh use this, and it has to fail over: a client handed a list
// whose first address is dead could otherwise not authenticate at all, which
// is the one failure that makes every other endpoint useless. The Rust client
// shipped without this and a test caught it.
//
// Only transport failures move on. A refusal is the same everywhere: one
// cluster, one signing secret, one user store.
func (c *Client) sendAny(ctx context.Context, method, routePath string, body any, token string) (map[string]any, error) {
	endpoints := c.Endpoints()
	tried := make([]string, 0, len(endpoints))
	var last error

	for _, endpoint := range endpoints {
		tried = append(tried, endpoint)
		parsed, err := c.roundTripWithToken(ctx, endpoint, method, routePath, body, token)
		if err == nil {
			c.promote(endpoint)
			return parsed, nil
		}
		if _, ok := err.(*TransportError); !ok {
			return nil, err
		}
		last = err
	}
	if last != nil {
		return nil, last
	}
	return nil, &ErrNoNodeAvailable{Tried: tried}
}

func (c *Client) roundTrip(ctx context.Context, endpoint, method, routePath string, body any) (map[string]any, error) {
	return c.roundTripWithToken(ctx, endpoint, method, routePath, body, c.Token())
}

func (c *Client) roundTripWithToken(ctx context.Context, endpoint, method, routePath string, body any, token string) (map[string]any, error) {
	response, err := c.send(ctx, endpoint, method, routePath, body, token)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	raw, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		return nil, &TransportError{Endpoint: endpoint, Err: readErr}
	}
	if response.StatusCode >= 400 {
		return nil, errorFrom(response.StatusCode, raw, retryAfterOf(response))
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("kimmydb: the body from %s is not a JSON object: %w", endpoint, err)
	}
	return parsed, nil
}

func (c *Client) send(ctx context.Context, endpoint, method, routePath string, body any, token string) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("kimmydb: encoding the request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}

	request, err := http.NewRequestWithContext(ctx, method, endpoint+routePath, reader)
	if err != nil {
		return nil, fmt.Errorf("kimmydb: building the request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := c.http.Do(request)
	if err != nil {
		return nil, &TransportError{Endpoint: endpoint, Err: err}
	}
	return response, nil
}

// promote moves an endpoint to the front, so the next request starts where the
// last one succeeded rather than re-walking the dead ones.
func (c *Client) promote(endpoint string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.endpoints) > 0 && c.endpoints[0] == endpoint {
		return
	}
	ordered := []string{endpoint}
	for _, existing := range c.endpoints {
		if existing != endpoint {
			ordered = append(ordered, existing)
		}
	}
	c.endpoints = ordered
}

func (c *Client) primary() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.endpoints) == 0 {
		return ""
	}
	return c.endpoints[0]
}

func retryAfterOf(response *http.Response) int {
	seconds, err := strconv.Atoi(response.Header.Get("Retry-After"))
	if err != nil {
		return 0
	}
	return seconds
}

func path(db, collection, route string) string {
	return "/v1/db/" + db + "/coll/" + collection + "/" + route
}

// normalize trims a trailing slash, so joining a path never doubles one.
func normalize(endpoint string) string {
	return strings.TrimRight(endpoint, "/")
}

func normalizeAll(endpoints []string) []string {
	out := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		out = append(out, normalize(endpoint))
	}
	return out
}
