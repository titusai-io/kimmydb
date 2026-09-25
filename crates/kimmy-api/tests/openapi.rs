//! The protocol specification, checked against the server it describes.
//!
//! `docs/openapi.yaml` is hand-written (ADR-056). What keeps it true is this
//! file, and it checks two different things — because a specification can be
//! wrong in two different ways:
//!
//! 1. **Inventory.** Every route the router registers is described, and every
//!    operation described is registered. This catches a route added without a
//!    spec entry, and a spec entry for a route that was renamed or removed.
//! 2. **Behaviour.** Every documented operation is driven against a real
//!    server over a real socket, and the response is validated against the
//!    schema the spec declares for that status. This catches the drift the
//!    inventory cannot see: a route that still exists and still answers, but
//!    no longer answers with what the document claims.
//!
//! The second half is the one that matters. An inventory check has existed
//! since M8 — `every_route_is_in_the_http_reference`, which moved here from
//! `routes.rs` so there is one route scanner rather than two — and inventory
//! alone would have been satisfied by a response whose fields had all been
//! renamed. It was, in fact, satisfied by a route it never looked at: it
//! matched `.route("` at the start of a line and so skipped the three
//! registrations rustfmt breaks across lines.
//!
//! **The coverage assertion is deliberate.** The live test ends by asserting
//! that every documented operation was actually exercised, so a new route
//! cannot be added to both the router and the spec without also being driven
//! here. A spec entry nothing executes is exactly the "prose nothing checks"
//! failure this milestone exists to end.
//!
//! What it does *not* check: that every documented *status* was produced. Some
//! — a provider failing upstream, a resume token past the retention horizon —
//! need conditions this harness has no cheap way to create. A representative
//! set of refusals is exercised below, and the rest are prose until something
//! makes them cheap.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use kimmy_auth::TokenIssuer;
use kimmy_storage::Engine;
use serde_json::{Value, json};

const SPEC_SOURCE: &str = include_str!("../../../docs/openapi.yaml");
const ROUTER_SOURCE: &str = include_str!("../src/routes.rs");

const SECRET: &str = "an-adequately-long-test-secret-for-hs256";
const ROOT_PASSWORD: &str = "root-password";

/// HTTP methods an OpenAPI path item may carry.
const METHODS: [&str; 7] = ["get", "put", "post", "delete", "options", "head", "patch"];

// ---------------------------------------------------------------------------
// The specification
// ---------------------------------------------------------------------------

fn spec() -> &'static Value {
    static SPEC: OnceLock<Value> = OnceLock::new();
    SPEC.get_or_init(|| {
        serde_norway::from_str(SPEC_SOURCE).expect("docs/openapi.yaml is not valid YAML")
    })
}

/// Every `(method, path)` the specification describes.
fn documented_operations() -> BTreeSet<(String, String)> {
    let paths = spec()["paths"].as_object().expect("the spec has a paths object");
    let mut out = BTreeSet::new();
    for (path, item) in paths {
        let item = item.as_object().expect("a path item is an object");
        for method in METHODS {
            if item.contains_key(method) {
                out.insert((method.to_uppercase(), path.clone()));
            }
        }
    }
    out
}

/// Every `(method, path)` the router registers.
///
/// Read out of the source rather than out of the `Router`, because axum
/// exposes no way to enumerate what a router holds. That makes this a text
/// scan, with the limitation text scans have: it sees `.route("…", …)` calls
/// in this file and nothing else. Routes mounted elsewhere are out of scope by
/// the same rule the spec states — `/mcp` is a different protocol, described by
/// `docs/mcp.md`.
fn registered_operations() -> BTreeSet<(String, String)> {
    let mut out = BTreeSet::new();
    // Comments are dropped before the scan. A comment that mentions
    // `.route("` — one in `routes.rs` does, about this very scanner — would
    // otherwise be read as a registration whose path is whatever quoted text
    // follows and whose methods are the next `get(` or `post(` in the file,
    // which was harmless only while nothing followed it.
    let source: String = ROUTER_SOURCE
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut rest = source.as_str();

    while let Some(at) = rest.find(".route(") {
        let open = at + ".route(".len();
        let after = &rest[open..];

        // The path is the first argument, but not necessarily on the same
        // line: three registrations here carry enough methods that rustfmt
        // breaks them. Matching `.route("` — which the older inventory test in
        // `routes.rs` did — silently skipped exactly those three.
        let quoted = after.find('"').expect("a route path literal");
        let path = after[quoted + 1..].split('"').next().expect("a closing quote");
        // axum spells a catch-all segment `{*name}`; OpenAPI path templating
        // has no wildcard form and spells the same parameter `{name}`. The two
        // vocabularies cannot be compared without saying so somewhere, and
        // here is the one place that reads both.
        let path = path.replace("{*", "{");
        let path = path.as_str();

        // The registration runs to the paren matching `.route(`. Route paths
        // hold no parens, so counting them is enough.
        let mut depth = 1usize;
        let mut end = after.len();
        for (i, c) in after.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i;
                        break;
                    }
                }
                _ => {}
            }
        }
        let registration = &after[..end];

        for method in METHODS {
            if contains_method_call(registration, method) {
                out.insert((method.to_uppercase(), path.to_string()));
            }
        }

        rest = &rest[open + end..];
    }

    out
}

/// Whether a registration calls `method(...)` as a routing method rather than
/// merely containing those letters inside a handler's name.
fn contains_method_call(registration: &str, method: &str) -> bool {
    let needle = format!("{method}(");
    let mut from = 0;
    while let Some(at) = registration[from..].find(&needle) {
        let at = from + at;
        let preceding = registration[..at].chars().next_back();
        // `get(find_docs)` and `.get(find_docs)` are routing methods;
        // `budget(x)` is not.
        if matches!(preceding, None | Some('.') | Some('(') | Some(' ') | Some('\n')) {
            return true;
        }
        from = at + needle.len();
    }
    false
}

#[test]
fn the_specification_and_the_router_describe_the_same_operations() {
    let documented = documented_operations();
    let registered = registered_operations();

    // Sanity: a scan that silently matched nothing would make both directions
    // pass vacuously, which is the shape of a check that has stopped checking.
    assert!(
        registered.len() > 20,
        "the route scan found {} routes; it is broken",
        registered.len()
    );

    let undocumented: Vec<_> = registered.difference(&documented).collect();
    let unregistered: Vec<_> = documented.difference(&registered).collect();

    assert!(
        undocumented.is_empty(),
        "these routes are registered but absent from docs/openapi.yaml: {undocumented:#?}"
    );
    assert!(
        unregistered.is_empty(),
        "docs/openapi.yaml describes operations the router does not register: {unregistered:#?}"
    );
}

/// The prose reference stays complete too.
///
/// Moved here from `routes.rs` rather than left beside the router, so there is
/// one scanner instead of two. The one it replaces matched `.route("` at the
/// start of a line, which skipped the three registrations rustfmt breaks
/// across lines — including `/docs/{id}`, the busiest route on the API. It had
/// been passing while never checking them.
///
/// `http-api.md` is not redundant with the specification: it is the page
/// someone reads to *learn* the API, and its endpoint table reads as complete.
/// M8 found it missing six of twenty-eight routes.
#[test]
fn every_route_is_in_the_http_reference() {
    const REFERENCE: &str = include_str!("../../../docs/http-api.md");

    let missing: Vec<_> = registered_operations()
        .into_iter()
        .map(|(_, path)| path)
        .filter(|path| !REFERENCE.contains(path))
        .collect();

    assert!(
        missing.is_empty(),
        "these routes are registered but absent from docs/http-api.md: {missing:#?}"
    );
}

// ---------------------------------------------------------------------------
// The error taxonomy
// ---------------------------------------------------------------------------

/// The code set in the server and the code set in the document are the same
/// set, and they agree on what a client may do about each one.
///
/// This is what makes the taxonomy public surface rather than an accident of
/// where `ApiError::new` happens to be called. `ErrorCode` is an enum, so the
/// compiler already refuses an unlisted code and forces its retry class to be
/// decided; this closes the remaining gap, which is the document falling
/// behind the enum.
///
/// It would have caught `no_vectors`, which existed in `vectors.rs` and in
/// neither document — both were written by reading `error.rs`, and the codes
/// accrete across five modules.
#[test]
fn every_error_code_is_specified_with_the_retry_class_the_server_uses() {
    use kimmy_api::error::ErrorCode;

    let schema = &spec()["components"]["schemas"]["ErrorCode"];
    let documented: BTreeSet<String> = schema["enum"]
        .as_array()
        .expect("the ErrorCode schema is an enum")
        .iter()
        .map(|v| v.as_str().expect("a code is a string").to_string())
        .collect();
    let served: BTreeSet<String> = ErrorCode::ALL.iter().map(|c| c.as_str().to_string()).collect();

    assert_eq!(
        served.len(),
        ErrorCode::ALL.len(),
        "two variants of ErrorCode render to the same wire string"
    );
    assert_eq!(served, documented, "the server's codes and the specification's disagree");

    // The retry class travels in the envelope, so the table in the document is
    // a promise about what the server sends, not a description of it.
    let table = schema["description"].as_str().expect("the ErrorCode schema documents its codes");
    for code in ErrorCode::ALL {
        let row = table
            .lines()
            .find(|line| line.starts_with(&format!("| `{}` |", code.as_str())))
            .unwrap_or_else(|| panic!("{} has no row in the ErrorCode table", code.as_str()));
        let declared = row.split('|').nth(3).expect("a retry column").trim();
        assert_eq!(
            declared,
            code.retry().as_str(),
            "{} is served as `{}` and documented as `{declared}`",
            code.as_str(),
            code.retry().as_str()
        );
    }

    let classes: BTreeSet<&str> = spec()["components"]["schemas"]["Retry"]["enum"]
        .as_array()
        .expect("Retry is an enum")
        .iter()
        .map(|v| v.as_str().expect("a class is a string"))
        .collect();
    // Every class the server sends, and no other: derived from the codes, so
    // a class added to the enum and served is one the specification names.
    let served: BTreeSet<&str> = ErrorCode::ALL.iter().map(|code| code.retry().as_str()).collect();
    assert_eq!(classes, served);
}

/// The error table in `docs/http-api.md`, as `(status, code, retry)` per row.
///
/// A code may hold more than one row — `bad_request` is documented at 400 and
/// at 422, because the two are different mistakes — so this is a list rather
/// than a map, and every row is checked.
///
/// Parsed strictly on purpose. A scan that silently matches nothing passes
/// every assertion built on it, which is the failure this whole family of
/// tests exists to prevent: each step panics with what it expected rather than
/// falling back to an empty result.
fn error_rows_in_the_http_reference() -> Vec<(String, String, String)> {
    const REFERENCE: &str = include_str!("../../../docs/http-api.md");
    const HEADER: &str = "| Status | `error` | `retry` | Cause |";

    let section =
        REFERENCE.split_once("\n## Errors\n").expect("docs/http-api.md has an Errors section").1;
    // Bounded at the next heading, so a table elsewhere in the document cannot
    // stand in for this one.
    let section = section.split("\n## ").next().expect("a section body");
    let table = section
        .split_once(HEADER)
        .unwrap_or_else(|| panic!("the Errors section has no table headed `{HEADER}`"))
        .1;

    // The header ends its own line, and the alignment row is the next one. Both
    // are consumed by position rather than skipped by shape: a scan that
    // tolerates a blank line here would tolerate one anywhere before the first
    // data row, and the count below cannot see that — the rows are all still
    // there and still add up.
    let table = table.strip_prefix('\n').expect("the table header ends its line");
    let mut lines = table.lines();
    let alignment = lines.next().expect("a line under the table header").trim();
    assert!(
        alignment.starts_with('|') && alignment.chars().all(|c| matches!(c, '|' | '-' | ':' | ' ')),
        "the line under the table header is not an alignment row: `{alignment}`"
    );

    let mut rows = Vec::new();
    for line in lines {
        let line = line.trim();
        // The table ends at the first line that is not a row.
        if !line.starts_with('|') {
            break;
        }

        // The first three cells are all before the prose, so a `|` inside a
        // cause — none today — could not shift them.
        let mut cells = line.split('|').skip(1);
        let status = cells.next().expect("a status column").trim().to_string();
        let code = cells.next().expect("a code column").trim();
        let retry = cells.next().expect("a retry column").trim().to_string();
        let code = code
            .strip_prefix('`')
            .and_then(|c| c.strip_suffix('`'))
            .unwrap_or_else(|| panic!("the code column of `{line}` is not a single `code` span"))
            .to_string();
        rows.push((status, code, retry));
    }

    // The scan reached the end of the table. Counting the table lines in the
    // section independently is what turns an early stop into a failure rather
    // than a smaller set that still happens to compare equal — the silent-pass
    // shape this file exists to prevent. The two extra lines are the header
    // and the alignment row; a second table in this section trips it too, and
    // should, because then "the error table" is ambiguous.
    let table_lines = section.lines().filter(|line| line.trim_start().starts_with('|')).count();
    assert_eq!(
        rows.len() + 2,
        table_lines,
        "the Errors section holds {table_lines} table lines and the scan read {} rows",
        rows.len()
    );

    rows
}

/// The prose reference's error table is the server's code set too.
///
/// `every_error_code_is_specified_with_the_retry_class_the_server_uses` holds
/// `docs/openapi.yaml` to the enum, so the machine-readable specification
/// cannot fall behind. Nothing held `docs/http-api.md`, which is the page a
/// person actually reads to learn what a refusal means — so a new code could go
/// missing from it silently, and the drift would be invisible until someone met
/// the code in production and looked it up in vain.
///
/// Both directions, like the specification's: a code with no row is an
/// undocumented refusal, and a row with no code is a refusal the server cannot
/// produce, which sends a reader looking for a condition that does not exist.
#[test]
fn every_error_code_is_in_the_http_reference_with_the_retry_class_the_server_uses() {
    use kimmy_api::error::ErrorCode;

    let rows = error_rows_in_the_http_reference();
    // A parse that found nothing would satisfy nothing below vacuously — the
    // set comparison would fail — but it would fail with a confusing message,
    // and a parse that found *some* rows is the dangerous case.
    assert!(
        rows.len() >= ErrorCode::ALL.len(),
        "the error table scan found {} rows for {} codes; it is broken",
        rows.len(),
        ErrorCode::ALL.len()
    );

    let documented: BTreeSet<&str> = rows.iter().map(|(_, code, _)| code.as_str()).collect();
    let served: BTreeSet<&str> = ErrorCode::ALL.iter().map(|c| c.as_str()).collect();
    assert_eq!(
        served, documented,
        "the server's codes and the error table in docs/http-api.md disagree"
    );

    for (status, code, retry) in &rows {
        let served = ErrorCode::ALL
            .iter()
            .find(|c| c.as_str() == code)
            .expect("the sets are equal, so every row names a served code");
        assert_eq!(
            retry,
            served.retry().as_str(),
            "docs/http-api.md documents `{code}` as `{retry}` and the server sends `{}`",
            served.retry().as_str()
        );
        // Proof that the columns being read are the ones intended: a shifted
        // parse would put prose here.
        assert!(
            status.parse::<u16>().is_ok_and(|s| (100..600).contains(&s)),
            "the status column of the `{code}` row reads `{status}`"
        );
    }
}

// ---------------------------------------------------------------------------
// The compatibility policy
// ---------------------------------------------------------------------------

/// The major version is in the path, and everything agrees about what it is.
///
/// `docs/compatibility.md` promises that `/v1` does not break and that the path
/// carries the major version. This is the part of that promise a test can hold:
/// the route prefixes, the version the server reports, and the specification's
/// own `info.version` cannot drift apart.
#[test]
fn every_versioned_route_carries_the_protocol_major() {
    // The unversioned routes, and why each one is allowed to be: an
    // infrastructure probe is not part of the client protocol and must not
    // move when the protocol majors.
    //
    // The two well-known paths are unversioned for a stronger reason than the
    // probes: RFC 9728 §3 *fixes* where they live. A client looks for them at
    // that exact location, so moving them under `/v1` would put them where no
    // conformant client would look.
    const UNVERSIONED: [&str; 5] = [
        "/healthz",
        "/readyz",
        "/metrics",
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/{resource_path}",
    ];

    let protocol = kimmy_api::version::PROTOCOL;
    let stray: Vec<_> = documented_operations()
        .into_iter()
        .map(|(_, path)| path)
        .filter(|path| !UNVERSIONED.contains(&path.as_str()))
        .filter(|path| !path.starts_with(&format!("/{protocol}/")))
        .collect();
    assert!(stray.is_empty(), "these routes are outside /{protocol}/: {stray:#?}");

    let declared = spec()["info"]["version"].as_str().expect("info.version is a string");
    let major = declared.split('.').next().expect("a major component");
    assert_eq!(
        format!("v{major}"),
        protocol,
        "the specification is version {declared} and the server serves /{protocol}"
    );
}

/// A response schema may not forbid unknown properties.
///
/// This is what makes "a new response field is additive" true rather than
/// merely intended: a client validating against today's document has to keep
/// validating tomorrow's responses. `additionalProperties: false` anywhere in a
/// response would make the next added field a breaking change for every
/// validating client, silently, and only for them.
#[test]
fn no_response_schema_forbids_the_fields_it_has_not_seen() {
    let spec = spec();
    let mut closed = Vec::new();
    find_closed_schemas(&spec["paths"], "/paths".to_string(), &mut closed);
    find_closed_schemas(
        &spec["components"]["responses"],
        "/components/responses".to_string(),
        &mut closed,
    );
    // A component schema is a response schema if any response reaches it,
    // directly or through another schema. One only request bodies refer to —
    // `FindRequest`, `SearchRequest` — is a request shape and is closed on
    // purpose (ADR-121); the test below insists on that.
    for name in response_schemas(spec) {
        find_closed_schemas(
            &spec["components"]["schemas"][&name],
            format!("/components/schemas/{name}"),
            &mut closed,
        );
    }
    assert!(
        closed.is_empty(),
        "these schemas forbid unknown properties, which makes adding a response field \
         breaking for a validating client: {closed:#?}"
    );
}

/// Every request shape is closed: a field the route does not define is
/// refused by the server (ADR-121), and a client validating against this
/// document before sending must be told the same thing. A document body —
/// insert, replace, bulk — is content, and stays open. The one shape that is
/// both a request and a response, `VectorConfig`, keeps the response rule and
/// says so at its request body.
#[test]
fn every_request_shape_is_closed() {
    let spec = spec();
    let responses = response_schemas(spec);
    let mut open = Vec::new();
    for (template, methods) in spec["paths"].as_object().expect("paths") {
        for (method, operation) in methods.as_object().expect("methods") {
            let Some(schema) = operation.pointer("/requestBody/content/application~1json/schema")
            else {
                continue;
            };
            let named = schema.get("$ref").and_then(Value::as_str);
            let resolved = resolve(spec, schema);
            // An array body is a batch of shapes; the element is the shape.
            let (named, resolved) = if resolved["type"] == "array" {
                let items = &resolved["items"];
                (items.get("$ref").and_then(Value::as_str), resolve(spec, items))
            } else {
                (named, resolved)
            };
            let name = named.and_then(|r| r.strip_prefix("#/components/schemas/"));
            let is_document = name == Some("Document");
            let is_response_too = name.is_some_and(|n| responses.contains(n));
            if is_document || is_response_too {
                continue;
            }
            if resolved["additionalProperties"] != Value::Bool(false) {
                open.push(format!("{} {template}", method.to_uppercase()));
            }
        }
    }
    assert!(
        open.is_empty(),
        "these request shapes do not declare `additionalProperties: false`, but the server \
         refuses a field they do not define (ADR-121): {open:#?}"
    );
}

/// A bound the server clamps is not a validation keyword.
///
/// `maximum` and `minimum` tell a generated client what to refuse before
/// sending. A `find` with `limit: 50_000` is answered `200` with 10,000
/// documents (ADR-019), and a search with `k: 2000` with 1,000 hits — so a
/// spec that carried `maximum: 10000` beside prose saying "clamped rather
/// than refused" had a client reject a request the server accepts, and the
/// two halves of one field contradicted each other. The clamp belongs in the
/// description; the keyword is for a bound the server answers `400`.
///
/// The rule reads the prose the way a client author would: on any property
/// or parameter whose description says "clamp", `maximum` is forbidden
/// outright, and `minimum` is forbidden when its value is a number the clamp
/// sentence names — because a bound the prose says is clamped is the clamp
/// restated as a refusal. A `minimum: 0` on an unsigned integer stays, as
/// does the `minimum: 1` on `sample`, whose `0` really is refused: those are
/// what the server does, and the prose beside them says so.
#[test]
fn no_clamped_bound_is_declared_as_a_validation_keyword() {
    let mut offending = Vec::new();
    find_clamped_bounds(spec(), "".to_string(), &mut offending);
    assert!(
        offending.is_empty(),
        "these bounds are clamped by the server but declared as validation keywords, so a \
         generated client refuses a request the server would answer: {offending:#?}"
    );
}

/// The numbers a description names in the same sentence as "clamp".
fn clamp_numbers(description: &str) -> BTreeSet<u64> {
    let lower = description.to_lowercase();
    let mut numbers = BTreeSet::new();
    for (at, _) in lower.match_indices("clamp") {
        let sentence = lower[at..].split('.').next().unwrap_or_default();
        let mut digits = String::new();
        for c in sentence.chars().chain(std::iter::once(' ')) {
            match c {
                '0'..='9' => digits.push(c),
                // `10,000` and `10_000` are one number, not two.
                ',' | '_' if !digits.is_empty() => {}
                _ => {
                    if let Ok(n) = digits.parse::<u64>() {
                        numbers.insert(n);
                    }
                    digits.clear();
                }
            }
        }
    }
    numbers
}

fn find_clamped_bounds(node: &Value, path: String, offending: &mut Vec<String>) {
    match node {
        Value::Object(map) => {
            if let Some(description) = map.get("description").and_then(Value::as_str)
                && description.to_lowercase().contains("clamp")
            {
                let named = clamp_numbers(description);
                // A property carries its bounds itself; a parameter carries
                // them on its `schema`, beside the description.
                let holders = [(map, path.clone()), (map, format!("{path}/schema"))];
                for (holder, at) in holders {
                    let holder = if at.ends_with("/schema") {
                        match holder.get("schema").and_then(Value::as_object) {
                            Some(schema) => schema,
                            None => continue,
                        }
                    } else {
                        holder
                    };
                    if let Some(max) = holder.get("maximum") {
                        offending.push(format!("{at}/maximum = {max}"));
                    }
                    if let Some(min) = holder.get("minimum").and_then(Value::as_u64)
                        && named.contains(&min)
                    {
                        offending.push(format!("{at}/minimum = {min}"));
                    }
                }
            }
            for (key, value) in map {
                find_clamped_bounds(value, format!("{path}/{key}"), offending);
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                find_clamped_bounds(item, format!("{path}/{i}"), offending);
            }
        }
        _ => {}
    }
}

/// The clamps the specification states are the ones the server applies.
///
/// The bounds live in prose now, so this is what holds the prose to the
/// constants: the default and cap of `find`'s `limit` on both routes, and of a
/// search's `k`. A constant changed without the document is a failing test,
/// not a client that learns the new number from a short page.
#[test]
fn the_documented_clamps_are_the_ones_the_server_applies() {
    use kimmy_api::exec::{DEFAULT_LIMIT, MAX_LIMIT};
    use kimmy_api::vectors::{DEFAULT_K, MAX_K};

    let spec = spec();
    let limit = &spec["components"]["schemas"]["FindRequest"]["properties"]["limit"];
    assert_eq!(limit["default"], DEFAULT_LIMIT, "FindRequest.limit default");
    let description = limit["description"].as_str().expect("FindRequest.limit is described");
    assert!(
        description.contains(&format!("clamped to {}", with_thousands(MAX_LIMIT))),
        "FindRequest.limit must name the cap the server clamps to: {description}"
    );

    let listing = spec["paths"]["/v1/db/{db}/coll/{coll}/docs"]["get"]["parameters"]
        .as_array()
        .expect("listDocuments has parameters")
        .iter()
        .find(|p| p["name"] == "limit")
        .expect("listDocuments has a limit parameter");
    assert_eq!(listing["schema"]["default"], DEFAULT_LIMIT, "listDocuments limit default");
    let description = listing["description"].as_str().expect("the limit parameter is described");
    assert!(
        description.contains(&format!("clamped to {}", with_thousands(MAX_LIMIT))),
        "listDocuments?limit must name the cap the server clamps to: {description}"
    );

    let k = &spec["components"]["schemas"]["SearchRequest"]["properties"]["k"];
    assert_eq!(k["default"], DEFAULT_K, "SearchRequest.k default");
    let description = k["description"].as_str().expect("SearchRequest.k is described");
    assert!(
        description.contains(&format!("clamped to `[1, {MAX_K}]`")),
        "SearchRequest.k must name the range the server clamps to: {description}"
    );
}

/// `10000` as a document writes it: `10,000`.
fn with_thousands(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Every `(method, path)` the specification gives a query parameter, at the
/// operation or at the path level.
fn operations_with_a_query_parameter() -> BTreeSet<(String, String)> {
    let spec = spec();
    let is_query = |parameters: &Value| {
        parameters
            .as_array()
            .is_some_and(|list| list.iter().any(|p| resolve(spec, p)["in"] == "query"))
    };
    let mut out = BTreeSet::new();
    for (template, item) in spec["paths"].as_object().expect("paths") {
        let path_level = is_query(&item["parameters"]);
        for (method, operation) in item.as_object().expect("path item") {
            if method == "parameters" {
                continue;
            }
            if path_level || is_query(&operation["parameters"]) {
                out.insert((method.to_uppercase(), template.clone()));
            }
        }
    }
    out
}

/// The routes the server opens to a query string are exactly the operations
/// the specification gives a query parameter (ADR-124).
///
/// `QUERY_STRING_ROUTES` only opens routes: one absent from it refuses every
/// query string, so a handler that gained a `QueryParams<T>` without an entry
/// is a route that refuses what it documents, and an entry without a
/// documented parameter is a route open to a query string nothing reads.
/// Both directions are held here, against the document a client reads
/// rather than against the handlers, because the document is the promise.
/// The entries are also held to be registered routes, so a typo in a
/// template cannot open nothing and pass.
#[test]
fn the_routes_that_read_a_query_string_are_the_ones_the_specification_gives_one() {
    // The catch-all spelling differs between the two vocabularies; see
    // `registered_operations`. No opened route carries one today, and the
    // rewrite is here so that one which does compares correctly.
    let opened: BTreeSet<(String, String)> = kimmy_api::routes::QUERY_STRING_ROUTES
        .iter()
        .map(|(method, template)| (method.to_string(), template.replace("{*", "{")))
        .collect();
    assert_eq!(
        opened.len(),
        kimmy_api::routes::QUERY_STRING_ROUTES.len(),
        "QUERY_STRING_ROUTES lists a route twice"
    );

    let documented = operations_with_a_query_parameter();
    let refused: Vec<_> = documented.difference(&opened).collect();
    let undocumented: Vec<_> = opened.difference(&documented).collect();
    assert!(
        refused.is_empty(),
        "docs/openapi.yaml gives these operations a query parameter, but the server refuses \
         every query string on them because QUERY_STRING_ROUTES does not open them: {refused:#?}"
    );
    assert!(
        undocumented.is_empty(),
        "QUERY_STRING_ROUTES opens these operations to a query string, but docs/openapi.yaml \
         documents no parameter on them: {undocumented:#?}"
    );

    let registered = registered_operations();
    let unregistered: Vec<_> = opened.difference(&registered).collect();
    assert!(
        unregistered.is_empty(),
        "QUERY_STRING_ROUTES names operations the router does not register: {unregistered:#?}"
    );
}

/// Every operation documents the `400` a query string can draw.
///
/// This used to hold only the operations that take a query parameter, for
/// the refusal `QueryParams<T>` makes of one the route does not define
/// (ADR-121). With the guard over the whole table, any query string on any
/// other REST route is the same `400` (ADR-124), so there is no longer an
/// operation on which a client can be promised never to meet one — the
/// health probes included, since a probe with a cache-busting parameter is
/// refused like everything else. The gap the older check closed was found
/// by review: one route's `400` was missed by a text search that matched a
/// longer operation name.
#[test]
fn every_operation_documents_the_400_a_query_string_draws() {
    let spec = spec();
    let mut missing = Vec::new();
    for (template, item) in spec["paths"].as_object().expect("paths") {
        for (method, operation) in item.as_object().expect("path item") {
            if method == "parameters" {
                continue;
            }
            if operation["responses"].get("400").is_none() {
                missing.push(format!("{} {template}", method.to_uppercase()));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "these operations do not document the 400 a query string is refused with, and every \
         REST route refuses one it does not read (ADR-124): {missing:#?}"
    );
}

/// The component schemas some response reaches, transitively.
fn response_schemas(spec: &Value) -> BTreeSet<String> {
    let mut reachable = BTreeSet::new();
    collect_refs(&spec["paths"], false, &mut reachable);
    collect_refs(&spec["components"]["responses"], true, &mut reachable);
    loop {
        let before = reachable.len();
        for name in reachable.clone() {
            collect_refs(&spec["components"]["schemas"][&name], true, &mut reachable);
        }
        if reachable.len() == before {
            return reachable;
        }
    }
}

/// Collect the component schemas referenced under a `responses` key, or
/// anywhere when `under_response` is already set.
fn collect_refs(node: &Value, under_response: bool, refs: &mut BTreeSet<String>) {
    match node {
        Value::Object(map) => {
            if under_response
                && let Some(reference) = map.get("$ref").and_then(Value::as_str)
                && let Some(name) = reference.strip_prefix("#/components/schemas/")
            {
                refs.insert(name.to_string());
            }
            for (key, value) in map {
                collect_refs(value, under_response || key == "responses", refs);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_refs(item, under_response, refs);
            }
        }
        _ => {}
    }
}

fn find_closed_schemas(node: &Value, path: String, closed: &mut Vec<String>) {
    match node {
        Value::Object(map) => {
            if map.get("additionalProperties") == Some(&Value::Bool(false)) {
                closed.push(path.clone());
            }
            for (key, value) in map {
                // Request bodies are strict by rule (ADR-121), so a typo is an
                // error rather than a silent no-op. It is *responses* that
                // must stay open.
                if key == "requestBody" {
                    continue;
                }
                find_closed_schemas(value, format!("{path}/{key}"), closed);
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                find_closed_schemas(item, format!("{path}/{i}"), closed);
            }
        }
        _ => {}
    }
}

/// The capabilities a node advertises are the ones the specification names.
///
/// Same mechanism as the error codes, for the same reason: the set is public
/// surface and a hand-kept list drifts. A node cannot advertise a capability
/// the document does not define, and a feature added without naming it here is
/// a visible omission.
#[test]
fn the_capability_set_is_the_documented_one() {
    use kimmy_api::version::Capability;

    let documented: BTreeSet<String> = spec()["components"]["schemas"]["Capability"]["enum"]
        .as_array()
        .expect("Capability is an enum")
        .iter()
        .map(|v| v.as_str().expect("a capability is a string").to_string())
        .collect();
    let known: BTreeSet<String> = Capability::ALL.iter().map(|c| c.as_str().to_string()).collect();

    assert_eq!(known.len(), Capability::ALL.len(), "two capabilities share a name");
    assert_eq!(known, documented, "the server's capabilities and the specification's disagree");

    // Every capability is also explained, not just listed. A bare name tells a
    // client nothing about what it may then do.
    let table = spec()["components"]["schemas"]["Capability"]["description"]
        .as_str()
        .expect("the Capability schema explains its values");
    for capability in Capability::ALL {
        assert!(
            table.contains(&format!("| `{}` |", capability.as_str())),
            "{} is advertised with no explanation of what it means",
            capability.as_str()
        );
    }
}

/// The example under "Version and capabilities" in `docs/http-api.md` is the
/// list the server emits, in the order it emits it.
///
/// The reference says that example is complete and ordered, and that a test
/// holds it to the enum. This is that test. The list is read out of the
/// section's first JSON block rather than searched for name by name, so a
/// stale entry, a missing one or a reordering all fail — `conditional-writes`,
/// `token-refresh` and `topology` were absent from the example for several
/// releases while the specification's enum, which has its own test, was
/// complete.
#[test]
fn the_capability_example_in_the_http_reference_is_the_list_the_server_emits() {
    use kimmy_api::version::Capability;

    const REFERENCE: &str = include_str!("../../../docs/http-api.md");

    let section = REFERENCE
        .split_once("## Version and capabilities")
        .expect("http-api.md has a Version and capabilities section")
        .1;
    let json_block = section
        .split_once("```json")
        .expect("the section opens with a JSON example")
        .1
        .split_once("```")
        .expect("the JSON example is closed")
        .0;
    let list = json_block
        .split_once("\"capabilities\":")
        .expect("the example carries a capabilities list")
        .1
        .split_once('[')
        .expect("the list opens")
        .1
        .split_once(']')
        .expect("the list closes")
        .0;
    let documented: Vec<&str> = list
        .split(',')
        .map(|entry| entry.trim().trim_matches('"'))
        .filter(|entry| !entry.is_empty())
        .collect();

    let emitted: Vec<&str> = Capability::ALL.iter().map(|c| c.as_str()).collect();
    assert_eq!(
        documented, emitted,
        "the capability example in docs/http-api.md is not the list the server emits, in order"
    );
}

// ---------------------------------------------------------------------------
// Schema validation
// ---------------------------------------------------------------------------

/// Follow a local `$ref`, once. Everything in this document refers to itself.
fn resolve<'a>(root: &'a Value, node: &'a Value) -> &'a Value {
    let Some(reference) = node.get("$ref").and_then(Value::as_str) else {
        return node;
    };
    let pointer = reference.strip_prefix('#').expect("only local references are used");
    root.pointer(pointer).unwrap_or_else(|| panic!("dangling reference {reference}"))
}

/// Validate a response body against the schema the spec declares for it.
///
/// An undocumented status is a failure rather than a skip: the server produced
/// it, so the document is incomplete.
fn validate_response(method: &str, template: &str, status: u16, body: &Value) {
    let spec = spec();
    let operation = &spec["paths"][template][method.to_lowercase()];
    assert!(
        !operation.is_null(),
        "docs/openapi.yaml documents no {method} for {template}, but the router answers it"
    );

    let response = &operation["responses"][status.to_string()];
    assert!(
        !response.is_null(),
        "docs/openapi.yaml documents no {status} for {method} {template}, but the server \
         returned one: {body}"
    );
    let response = resolve(spec, response);

    let schema = &response["content"]["application/json"]["schema"];
    if schema.is_null() {
        // A documented response with no JSON body — a 101 upgrade, or a
        // non-JSON payload checked by its own assertion at the call site.
        return;
    }

    // The validator is handed a document that carries the spec's `components`
    // alongside the schema, so `#/components/schemas/...` resolves.
    let root = json!({ "allOf": [schema], "components": spec["components"] });
    let validator = jsonschema::validator_for(&root)
        .unwrap_or_else(|e| panic!("the schema for {method} {template} {status} is invalid: {e}"));

    let errors: Vec<String> = validator.iter_errors(body).map(|e| format!("  {e}")).collect();
    assert!(
        errors.is_empty(),
        "the {status} response to {method} {template} does not match its schema:\n{}\n\nbody: {}",
        errors.join("\n"),
        serde_json::to_string_pretty(body).unwrap_or_default()
    );
}

// ---------------------------------------------------------------------------
// A server, and a client that can see the raw response
// ---------------------------------------------------------------------------

struct Res {
    status: u16,
    head: String,
    raw: Vec<u8>,
}

impl Res {
    fn json(&self) -> Value {
        let text = String::from_utf8_lossy(&self.raw);
        serde_json::from_str(text.trim()).unwrap_or(Value::Null)
    }

    fn header(&self, name: &str) -> Option<String> {
        let want = name.to_ascii_lowercase();
        self.head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key.trim().to_ascii_lowercase() == want).then(|| value.trim().to_string())
        })
    }
}

/// A tiny HTTP client.
///
/// Hand-rolled for the same reason `tests/api.rs` and `tests/webhooks.rs` each
/// have one: it needs to do something a client library makes awkward — here,
/// keep the raw bytes and the raw head, because `/metrics` is text and
/// `/v1/admin/backup` is binary and both are part of the contract.
struct Server {
    base: String,
    /// The server's own state, for the one setting a request cannot reach:
    /// the local login mode, which is fixed at startup.
    state: kimmy_api::SharedState,
    _dir: tempfile::TempDir,
}

impl Server {
    async fn start() -> Self {
        Self::build(kimmy_api::RateLimits::disabled()).await
    }

    async fn start_rate_limited(burst: u32) -> Self {
        let limits = kimmy_api::RateLimits {
            login_ip: kimmy_api::Limiter::new(
                kimmy_api::RateLimit::new(burst, std::time::Duration::from_secs(60)),
                1024,
            ),
            ..kimmy_api::RateLimits::disabled()
        };
        Self::build(limits).await
    }

    async fn build(limits: kimmy_api::RateLimits) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());

        let users = kimmy_auth::UserStore::open(&engine).unwrap();
        users.bootstrap_root(&engine, "root", ROOT_PASSWORD).unwrap();

        let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
        // Loopback is allowed out, so the documented webhook registration can
        // be exercised against an address that exists.
        let state = kimmy_api::state_with_egress(
            Arc::clone(&engine),
            tokens,
            false,
            limits,
            kimmy_api::egress::EgressPolicy::new(
                kimmy_api::egress::WEBHOOKS,
                vec!["127.0.0.1".into()],
            ),
        )
        .unwrap();
        let app = kimmy_api::router(Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
                .await;
        });

        Self { base: format!("http://{addr}"), state, _dir: dir }
    }

    /// Drive one request through the router as though it arrived from `peer`.
    ///
    /// The socket above can only ever produce a loopback peer, which is the
    /// one case `auth.local.login = "loopback_only"` admits. To produce the
    /// documented 403 the request is handed to the router directly, carrying
    /// the same connect-info extension a listener would have attached.
    async fn request_from(
        &self,
        peer: &str,
        method: &str,
        path: &str,
        token: Option<&str>,
    ) -> (u16, Value) {
        use http_body_util::BodyExt as _;
        use tower::ServiceExt as _;

        let peer: SocketAddr = peer.parse().unwrap();
        let mut request = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json");
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let body = json!({ "user": "root", "password": ROOT_PASSWORD }).to_string();
        let mut request = request.body(axum::body::Body::from(body)).unwrap();
        request.extensions_mut().insert(axum::extract::ConnectInfo(peer));

        let response =
            kimmy_api::router(Arc::clone(&self.state)).oneshot(request).await.expect("router");
        let status = response.status().as_u16();
        let bytes = response.into_body().collect().await.expect("body").to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<&Value>,
    ) -> Res {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let host = self.base.strip_prefix("http://").expect("http url");
        let mut stream = tokio::net::TcpStream::connect(host).await.expect("connect");
        let payload = body.map(|b| b.to_string()).unwrap_or_default();

        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n",
            payload.len()
        );
        if let Some(token) = token {
            request.push_str(&format!("Authorization: Bearer {token}\r\n"));
        }
        request.push_str("\r\n");
        request.push_str(&payload);

        stream.write_all(request.as_bytes()).await.expect("write");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.expect("read");

        split(raw)
    }

    /// A real WebSocket handshake.
    ///
    /// A plain `GET` would be refused by the upgrade extractor with the
    /// framework's own plain-text rejection, which proves nothing about the
    /// documented `101` and would quietly stand in for it.
    async fn handshake(&self, path: &str, token: &str) -> Res {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let host = self.base.strip_prefix("http://").expect("http url");
        let mut stream = tokio::net::TcpStream::connect(host).await.expect("connect");

        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n\
             Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Authorization: Bearer {token}\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.expect("write");

        // An upgraded socket stays open, so read only as far as the head. A
        // read to end would wait for a change that is never coming.
        let mut raw = Vec::new();
        let mut byte = [0u8; 1];
        while !raw.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte).await {
                Ok(0) | Err(_) => break,
                Ok(_) => raw.push(byte[0]),
            }
        }

        split(raw)
    }
}

fn split(raw: Vec<u8>) -> Res {
    let terminator = raw.windows(4).position(|w| w == b"\r\n\r\n");
    let (head, body) = match terminator {
        Some(at) => (raw[..at].to_vec(), raw[at + 4..].to_vec()),
        None => (raw.clone(), Vec::new()),
    };
    let head = String::from_utf8_lossy(&head).into_owned();
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Res { status, head, raw: body }
}

// ---------------------------------------------------------------------------
// Driving every documented operation
// ---------------------------------------------------------------------------

struct Conformance {
    server: Server,
    covered: BTreeSet<(String, String)>,
}

impl Conformance {
    async fn start() -> Self {
        Self { server: Server::start().await, covered: BTreeSet::new() }
    }

    /// Drive one operation, check the status, and validate the body against
    /// the schema the specification declares for it.
    async fn check(
        &mut self,
        method: &str,
        template: &str,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
        want: u16,
    ) -> Value {
        let res = self.server.request(method, path, token, body.as_ref()).await;
        let payload = res.json();
        assert_eq!(res.status, want, "{method} {path} answered {} — {payload}", res.status);

        validate_response(method, template, res.status, &payload);
        self.covered.insert((method.to_string(), template.to_string()));
        payload
    }

    async fn login(&mut self, user: &str, password: &str) -> String {
        let body = self
            .check(
                "POST",
                "/v1/auth/login",
                "/v1/auth/login",
                None,
                Some(json!({ "user": user, "password": password })),
                200,
            )
            .await;
        body["token"].as_str().expect("a token").to_string()
    }
}

#[tokio::test]
async fn every_documented_operation_answers_as_the_specification_says() {
    let mut c = Conformance::start().await;

    // -- health, and the routes that need no token ------------------------
    c.check("GET", "/healthz", "/healthz", None, None, 200).await;
    c.check("GET", "/readyz", "/readyz", None, None, 200).await;

    let metrics = c.server.request("GET", "/metrics", None, None).await;
    assert_eq!(metrics.status, 200);
    assert!(
        metrics.header("content-type").is_some_and(|t| t.starts_with("text/plain")),
        "metrics must be Prometheus text, not JSON: {:?}",
        metrics.header("content-type")
    );
    assert!(String::from_utf8_lossy(&metrics.raw).contains("kimmy_up 1"));
    c.covered.insert(("GET".into(), "/metrics".into()));

    // This node federates with nobody, so both well-known paths answer the
    // documented 404 — which is the response worth driving here, because it is
    // what every non-federated deployment serves. The 200 shape needs a node
    // with an identity provider configured and is covered in `tests/api.rs`.
    c.check(
        "GET",
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource",
        None,
        None,
        404,
    )
    .await;
    c.check(
        "GET",
        "/.well-known/oauth-protected-resource/{resource_path}",
        "/.well-known/oauth-protected-resource/nodes/one",
        None,
        None,
        404,
    )
    .await;

    // Unauthenticated on purpose: a client negotiates before it holds a token.
    let advertised = c.check("GET", "/v1/version", "/v1/version", None, None, 200).await;
    assert_eq!(advertised["protocol"], kimmy_api::version::PROTOCOL);
    assert_eq!(advertised["version"], env!("CARGO_PKG_VERSION"));
    let served: Vec<&str> = advertised["capabilities"]
        .as_array()
        .expect("a list")
        .iter()
        .map(|c| c.as_str().unwrap())
        .collect();
    assert_eq!(
        served,
        kimmy_api::version::capabilities(),
        "the wire and the server's own list disagree"
    );
    // Both assertions around this one are satisfied by an *empty* list: the
    // comparison above comes from the same function that produced the wire
    // value, and "does not contain local-embeddings" is vacuous when nothing
    // is advertised. A mutation pass showed exactly that — `capabilities()`
    // could return `vec![]` and every check here still passed, which would
    // tell every client this node supports nothing at all. Since ADR-058 makes
    // capabilities the thing clients branch on instead of a version number,
    // that is the failure the whole mechanism exists to prevent.
    //
    // So the unconditional ones are named. `Capability::present` answers
    // `true` for everything except `LocalEmbeddings`, so every other
    // capability must appear in any build.
    let always: Vec<&str> = kimmy_api::version::Capability::ALL
        .iter()
        .filter(|c| !matches!(c, kimmy_api::version::Capability::LocalEmbeddings))
        .map(|c| c.as_str())
        .collect();
    assert!(!always.is_empty(), "the fixture itself must not be vacuous");
    for capability in &always {
        assert!(
            served.contains(capability),
            "every build has {capability}, so a node that does not advertise it is broken; \
             served {served:?}"
        );
    }
    assert!(
        !served.contains(&"local-embeddings"),
        "the default build has no in-process embedding, so it must not advertise it — \
         this is the capability that proves the list is answered rather than asserted"
    );

    // -- auth --------------------------------------------------------------
    let root = c.login("root", ROOT_PASSWORD).await;
    let refreshed =
        c.check("POST", "/v1/auth/refresh", "/v1/auth/refresh", Some(&root), None, 200).await;
    assert_eq!(refreshed["user"], "root");
    assert!(refreshed["expiresIn"].as_u64().is_some_and(|s| s > 0));
    // The fresh token works, which is the only claim that matters about it.
    let root = refreshed["token"].as_str().expect("a token").to_string();
    c.check("GET", "/v1/auth/whoami", "/v1/auth/whoami", Some(&root), None, 200).await;

    // Always at least this node, marked `self` — `Members` holds peers only,
    // so a list derived from it alone would omit the node that just answered.
    let topology = c.check("GET", "/v1/topology", "/v1/topology", Some(&root), None, 200).await;
    assert_eq!(topology["count"], 1);
    assert_eq!(topology["nodes"][0]["self"], true);
    assert_eq!(topology["nodes"][0]["status"], "live");

    // -- users -------------------------------------------------------------
    let clerk_grants = json!([{ "db": "shop", "collection": "*", "actions": ["read"] }]);
    c.check(
        "POST",
        "/v1/users",
        "/v1/users",
        Some(&root),
        Some(json!({ "user": "clerk", "password": "clerk-password", "grants": clerk_grants })),
        201,
    )
    .await;
    c.check("GET", "/v1/users", "/v1/users", Some(&root), None, 200).await;
    c.check("GET", "/v1/users/{name}", "/v1/users/clerk", Some(&root), None, 200).await;
    c.check(
        "POST",
        "/v1/users/{name}/password",
        "/v1/users/clerk/password",
        Some(&root),
        Some(json!({ "password": "another-password" })),
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/users/{name}/grants",
        "/v1/users/clerk/grants",
        Some(&root),
        Some(json!({ "grants": clerk_grants })),
        200,
    )
    .await;
    // Disable is driven twice — off, then on again — so the documented
    // operation is exercised without leaving the account disabled for the
    // checks that log `clerk` in further down.
    c.check(
        "POST",
        "/v1/users/{name}/disabled",
        "/v1/users/clerk/disabled",
        Some(&root),
        Some(json!({ "disabled": true })),
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/users/{name}/disabled",
        "/v1/users/clerk/disabled",
        Some(&root),
        Some(json!({ "disabled": false })),
        200,
    )
    .await;

    // -- roles -------------------------------------------------------------
    c.check(
        "POST",
        "/v1/roles",
        "/v1/roles",
        Some(&root),
        Some(json!({
            "name": "reader",
            "grants": [{ "db": "shop", "collection": "orders*", "actions": ["read"] }],
        })),
        201,
    )
    .await;
    c.check("GET", "/v1/roles", "/v1/roles", Some(&root), None, 200).await;
    c.check("GET", "/v1/roles/{name}", "/v1/roles/reader", Some(&root), None, 200).await;
    c.check(
        "POST",
        "/v1/users/{name}/roles",
        "/v1/users/clerk/roles",
        Some(&root),
        Some(json!({ "roles": ["reader"] })),
        200,
    )
    .await;
    // The holder is reported, which is what makes the revocation observable to
    // the caller rather than only to whoever reads the log.
    let narrowed = c
        .check(
            "POST",
            "/v1/roles/{name}/grants",
            "/v1/roles/reader/grants",
            Some(&root),
            Some(json!({ "grants": [] })),
            200,
        )
        .await;
    assert_eq!(narrowed["invalidated"], 1, "the holder's tokens are revoked: {narrowed}");

    // -- databases and collections ----------------------------------------
    c.check(
        "POST",
        "/v1/db/{db}/collections",
        "/v1/db/shop/collections",
        Some(&root),
        Some(json!({ "name": "orders" })),
        200,
    )
    .await;
    c.check("GET", "/v1/databases", "/v1/databases", Some(&root), None, 200).await;
    c.check("GET", "/v1/db/{db}/collections", "/v1/db/shop/collections", Some(&root), None, 200)
        .await;

    // -- documents ---------------------------------------------------------
    let docs = "/v1/db/shop/coll/orders/docs";
    let docs_t = "/v1/db/{db}/coll/{coll}/docs";
    c.check(
        "POST",
        docs_t,
        docs,
        Some(&root),
        Some(json!({ "_id": "a", "sku": "widget", "qty": 2, "note": "a small blue widget" })),
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/bulk",
        "/v1/db/shop/coll/orders/bulk",
        Some(&root),
        Some(json!([
            { "_id": "b", "sku": "sprocket", "qty": 1, "note": "a large red sprocket" },
            { "_id": "c", "sku": "gasket", "qty": 5, "note": "a gasket, sold in packs" },
        ])),
        200,
    )
    .await;
    c.check("GET", docs_t, &format!("{docs}?limit=2"), Some(&root), None, 200).await;

    let page = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/find",
            "/v1/db/shop/coll/orders/find",
            Some(&root),
            Some(json!({ "filter": { "qty": { "$gte": 1 } }, "limit": 2, "explain": true })),
            200,
        )
        .await;
    assert_eq!(page["count"], 2);
    assert!(page["nextCursor"].is_string(), "a full page offers a cursor: {page}");

    // The `idLookup` strategy, driven rather than only declared. M10 task 11's
    // lesson was that a documented *outcome* no test produces is where the
    // next false sentence hides — the specification claimed idempotent
    // collection creation for four tasks because nothing ever created one
    // twice. A strategy name in the enum that no request returns is the same
    // shape of claim.
    // A real `_id` from the page just read, in whatever Extended JSON shape it
    // has, rather than a guessed literal — the filter round-trips the value the
    // server itself just sent.
    let existing_id = page["documents"][0]["_id"].clone();
    let by_id = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/find",
            "/v1/db/shop/coll/orders/find",
            Some(&root),
            Some(json!({ "filter": { "_id": existing_id }, "explain": true })),
            200,
        )
        .await;
    assert_eq!(by_id["explain"]["strategy"], "idLookup", "{by_id}");
    assert_eq!(by_id["explain"]["index"], Value::Null, "the primary key is not an index");
    assert_eq!(by_id["explain"]["documentsExamined"], 1, "one read, not a scan: {by_id}");
    assert_eq!(by_id["count"], 1, "{by_id}");

    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/count",
        "/v1/db/shop/coll/orders/count",
        Some(&root),
        Some(json!({ "filter": {}, "explain": true })),
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/aggregate",
        "/v1/db/shop/coll/orders/aggregate",
        Some(&root),
        Some(json!({ "pipeline": [{ "$group": { "_id": "$sku", "total": { "$sum": "$qty" } } }] })),
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/update",
        "/v1/db/shop/coll/orders/update",
        Some(&root),
        Some(json!({ "filter": {}, "update": { "$set": { "channel": "web" } }, "multi": true,
                     "explain": true })),
        200,
    )
    .await;

    let modified = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/find_and_modify",
            "/v1/db/shop/coll/orders/find_and_modify",
            Some(&root),
            Some(json!({ "filter": { "_id": "a" }, "update": { "$set": { "status": "packed" } },
                         "returnDocument": "after" })),
            200,
        )
        .await;
    assert_eq!(modified["document"]["status"], "packed");

    // `arrayFilters` on both write routes, driven rather than only declared
    // (ADR-104): the update's `$[line]` needs the filter the request carries.
    c.check(
        "POST",
        docs_t,
        docs,
        Some(&root),
        Some(json!({ "_id": "lines", "items": [ { "sku": "widget", "shipped": false },
                                                 { "sku": "gasket", "shipped": false } ] })),
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/update",
        "/v1/db/shop/coll/orders/update",
        Some(&root),
        Some(json!({ "filter": { "_id": "lines" },
                     "update": { "$set": { "items.$[line].shipped": true } },
                     "arrayFilters": [ { "line.sku": "gasket" } ] })),
        200,
    )
    .await;
    let modified = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/find_and_modify",
            "/v1/db/shop/coll/orders/find_and_modify",
            Some(&root),
            Some(json!({ "filter": { "_id": "lines" },
                         "update": { "$set": { "items.$[line].shipped": true } },
                         "arrayFilters": [ { "line.sku": "widget" } ],
                         "returnDocument": "after" })),
            200,
        )
        .await;
    assert_eq!(modified["document"]["items"][0]["shipped"], true, "{modified}");
    assert_eq!(modified["document"]["items"][1]["shipped"], true, "{modified}");

    let by_id = "/v1/db/shop/coll/orders/docs/a";
    let by_id_t = "/v1/db/{db}/coll/{coll}/docs/{id}";
    c.check("GET", by_id_t, by_id, Some(&root), None, 200).await;
    c.check(
        "PUT",
        by_id_t,
        &format!("{by_id}?upsert=true"),
        Some(&root),
        Some(json!({ "sku": "widget", "qty": 3, "note": "a small blue widget" })),
        200,
    )
    .await;

    c.check(
        "GET",
        "/v1/db/{db}/coll/{coll}/violations",
        "/v1/db/shop/coll/orders/violations",
        Some(&root),
        None,
        200,
    )
    .await;
    c.check(
        "GET",
        "/v1/db/{db}/coll/{coll}/violations",
        "/v1/db/shop/coll/orders/violations?index=sku_1",
        Some(&root),
        None,
        200,
    )
    .await;

    // -- indexes -----------------------------------------------------------
    let index = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/indexes",
            "/v1/db/shop/coll/orders/indexes",
            Some(&root),
            Some(json!({ "fields": [{ "path": "sku" }], "unique": true })),
            200,
        )
        .await;
    let index_name = index["name"].as_str().expect("an index name").to_string();
    c.check(
        "GET",
        "/v1/db/{db}/coll/{coll}/indexes",
        "/v1/db/shop/coll/orders/indexes",
        Some(&root),
        None,
        200,
    )
    .await;

    // -- schema ------------------------------------------------------------
    c.check(
        "GET",
        "/v1/db/{db}/coll/{coll}/describe",
        "/v1/db/shop/coll/orders/describe?examples=true",
        Some(&root),
        None,
        200,
    )
    .await;

    // -- vectors -----------------------------------------------------------
    let vector = "/v1/db/shop/coll/orders/vector";
    let vector_t = "/v1/db/{db}/coll/{coll}/vector";
    c.check(
        "POST",
        vector_t,
        vector,
        Some(&root),
        Some(json!({ "fields": ["note"], "provider": { "kind": "byo" }, "dim": 3 })),
        200,
    )
    .await;
    c.check("GET", vector_t, vector, Some(&root), None, 200).await;

    let doc_vectors = "/v1/db/shop/coll/orders/docs/a/vectors";
    let doc_vectors_t = "/v1/db/{db}/coll/{coll}/docs/{id}/vectors";
    c.check(
        "PUT",
        doc_vectors_t,
        doc_vectors,
        Some(&root),
        Some(json!([{ "chunk": 0, "vector": [1.0, 0.0, 0.0], "text": "a small blue widget" }])),
        200,
    )
    .await;
    c.check("GET", doc_vectors_t, doc_vectors, Some(&root), None, 200).await;

    let hits = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/vector_search",
            "/v1/db/shop/coll/orders/vector_search",
            Some(&root),
            Some(json!({ "vector": [1.0, 0.0, 0.0], "k": 3 })),
            200,
        )
        .await;
    assert_eq!(hits["count"], 1, "the one stored chunk should be found: {hits}");
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/hybrid_search",
        "/v1/db/shop/coll/orders/hybrid_search",
        Some(&root),
        Some(json!({ "query": "blue widget", "vector": [1.0, 0.0, 0.0], "k": 3 })),
        200,
    )
    .await;
    // The fusion controls (ADR-094) are part of the documented request, and a
    // meaningless one is the documented 400.
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/hybrid_search",
        "/v1/db/shop/coll/orders/hybrid_search",
        Some(&root),
        Some(json!({
            "query": "blue widget", "vector": [1.0, 0.0, 0.0], "k": 3,
            "weights": { "dense": 0.7, "lexical": 0.3 }, "min_overlap": 2,
        })),
        200,
    )
    .await;
    let refused = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/hybrid_search",
            "/v1/db/shop/coll/orders/hybrid_search",
            Some(&root),
            Some(json!({ "query": "blue widget", "vector": [1.0, 0.0, 0.0], "min_overlap": 0 })),
            400,
        )
        .await;
    assert_eq!(refused["error"], "bad_request");

    // -- webhooks ----------------------------------------------------------
    let hooks = "/v1/db/shop/coll/orders/webhooks";
    let hooks_t = "/v1/db/{db}/coll/{coll}/webhooks";
    let registered = c
        .check(
            "POST",
            hooks_t,
            hooks,
            Some(&root),
            Some(json!({ "url": "http://127.0.0.1:9/hook", "operations": ["insert", "delete"] })),
            200,
        )
        .await;
    let hook_id = registered["id"].as_str().expect("a subscription id").to_string();
    c.check("GET", hooks_t, hooks, Some(&root), None, 200).await;

    // -- change streams ----------------------------------------------------
    let upgraded = c.server.handshake("/v1/db/shop/coll/orders/watch", &root).await;
    assert_eq!(upgraded.status, 101, "the watch route must upgrade: {}", upgraded.head);
    validate_response("GET", "/v1/db/{db}/coll/{coll}/watch", 101, &Value::Null);
    c.covered.insert(("GET".into(), "/v1/db/{db}/coll/{coll}/watch".into()));

    // -- backup ------------------------------------------------------------
    let backup = c.server.request("GET", "/v1/admin/backup", Some(&root), None).await;
    assert_eq!(backup.status, 200);
    assert_eq!(backup.header("content-type").as_deref(), Some("application/octet-stream"));
    assert!(
        backup.header("content-disposition").is_some_and(|d| d.starts_with("attachment;")),
        "a backup is served as an attachment"
    );
    assert!(!backup.raw.is_empty(), "a backup of a seeded node is not empty");
    c.covered.insert(("GET".into(), "/v1/admin/backup".into()));

    // -- teardown, which is also coverage of every remaining verb -----------
    c.check("DELETE", doc_vectors_t, doc_vectors, Some(&root), None, 200).await;
    c.check("DELETE", vector_t, &format!("{vector}?drop_vectors=true"), Some(&root), None, 200)
        .await;
    c.check(
        "DELETE",
        "/v1/db/{db}/coll/{coll}/webhooks/{id}",
        &format!("{hooks}/{hook_id}"),
        Some(&root),
        None,
        200,
    )
    .await;
    c.check(
        "DELETE",
        "/v1/db/{db}/coll/{coll}/indexes/{name}",
        &format!("/v1/db/shop/coll/orders/indexes/{index_name}"),
        Some(&root),
        None,
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/delete",
        "/v1/db/shop/coll/orders/delete",
        Some(&root),
        Some(json!({ "filter": { "_id": "c" } })),
        200,
    )
    .await;
    c.check("DELETE", by_id_t, "/v1/db/shop/coll/orders/docs/b", Some(&root), None, 200).await;
    // Deleted while `clerk` still holds it, so the revocation is exercised
    // rather than trivially zero — and `clerk` keeps the dangling name.
    let dropped =
        c.check("DELETE", "/v1/roles/{name}", "/v1/roles/reader", Some(&root), None, 200).await;
    assert_eq!(dropped["invalidated"], 1, "the holder's tokens are revoked: {dropped}");
    c.check("DELETE", "/v1/users/{name}", "/v1/users/clerk", Some(&root), None, 200).await;
    c.check("DELETE", "/v1/db/{db}/coll/{coll}", "/v1/db/shop/coll/orders", Some(&root), None, 200)
        .await;
    c.check("DELETE", "/v1/db/{db}", "/v1/db/shop", Some(&root), None, 200).await;

    // -- the gate ----------------------------------------------------------
    let documented = documented_operations();
    let missed: Vec<_> = documented.difference(&c.covered).collect();
    assert!(
        missed.is_empty(),
        "docs/openapi.yaml documents these operations and nothing here drives them: {missed:#?}"
    );
}

/// The refusals, which are as much a contract as the successes.
///
/// A client branches on these, so a status or an envelope that changes here is
/// a breaking change whether or not anyone meant it to be.
#[tokio::test]
async fn documented_refusals_use_the_documented_envelope() {
    let mut c = Conformance::start().await;
    let root = c.login("root", ROOT_PASSWORD).await;

    // No token at all.
    c.check("GET", "/v1/databases", "/v1/databases", None, None, 401).await;

    // Wrong password: indistinguishable from an unknown user, by design.
    c.check(
        "POST",
        "/v1/auth/login",
        "/v1/auth/login",
        None,
        Some(json!({ "user": "root", "password": "wrong" })),
        401,
    )
    .await;

    // A password policy failure is a 400, not a 500.
    c.check(
        "POST",
        "/v1/users",
        "/v1/users",
        Some(&root),
        Some(json!({ "user": "short", "password": "tiny" })),
        400,
    )
    .await;

    // Held by a principal who may read one collection and administer nothing.
    c.check(
        "POST",
        "/v1/users",
        "/v1/users",
        Some(&root),
        Some(json!({ "user": "clerk", "password": "clerk-password",
                     "grants": [{ "db": "shop", "collection": "*", "actions": ["read"] }] })),
        201,
    )
    .await;
    let clerk = c.login("clerk", "clerk-password").await;
    c.check("GET", "/v1/users", "/v1/users", Some(&clerk), None, 403).await;

    // Creating a collection that exists is a conflict, not a second success.
    // The specification claimed creation was idempotent for four tasks, and
    // nothing exercised it: every test created a collection exactly once.
    c.check(
        "POST",
        "/v1/db/{db}/collections",
        "/v1/db/shop/collections",
        Some(&root),
        Some(json!({ "name": "twice" })),
        200,
    )
    .await;
    let conflict = c
        .check(
            "POST",
            "/v1/db/{db}/collections",
            "/v1/db/shop/collections",
            Some(&root),
            Some(json!({ "name": "twice" })),
            409,
        )
        .await;
    assert_eq!(conflict["error"], "conflict");

    // A collection that does not exist.
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/find",
        "/v1/db/shop/coll/ghost/find",
        Some(&root),
        Some(json!({ "filter": {} })),
        404,
    )
    .await;

    // A duplicate `_id`.
    c.check(
        "POST",
        "/v1/db/{db}/collections",
        "/v1/db/shop/collections",
        Some(&root),
        Some(json!({ "name": "orders" })),
        200,
    )
    .await;
    let docs = "/v1/db/shop/coll/orders/docs";
    let docs_t = "/v1/db/{db}/coll/{coll}/docs";
    c.check("POST", docs_t, docs, Some(&root), Some(json!({ "_id": "a" })), 200).await;
    let conflict =
        c.check("POST", docs_t, docs, Some(&root), Some(json!({ "_id": "a" })), 409).await;
    assert_eq!(conflict["error"], "duplicate_key", "the code is what a client branches on");
    assert_eq!(conflict["retry"], "no", "a duplicate key does not become un-duplicate");

    // Valid JSON of the wrong shape. 422 rather than 400, and it was in the
    // HTTP reference but in no specification until the taxonomy was written
    // down: `/bulk` takes an array, and an object is not one.
    let shape = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/bulk",
            "/v1/db/shop/coll/orders/bulk",
            Some(&root),
            Some(json!({ "_id": "b" })),
            422,
        )
        .await;
    assert_eq!(shape["error"], "bad_request");

    // A cursor combined with something that contradicts it. Refused rather
    // than honoured in part: a page that quietly dropped the sort it was given
    // would be wrong in a way a caller reads as data.
    for body in [
        json!({ "cursor": "AA", "skip": 5 }),
        json!({ "cursor": "AA", "sort": { "qty": 1 } }),
        json!({ "cursor": "!!! not a token" }),
    ] {
        let refused = c
            .check(
                "POST",
                "/v1/db/{db}/coll/{coll}/find",
                "/v1/db/shop/coll/orders/find",
                Some(&root),
                Some(body),
                400,
            )
            .await;
        assert_eq!(refused["error"], "bad_request");
    }

    // A query string on a route that reads none, and on one that needs no
    // token at all: the guard over the table answers before anything else
    // does (ADR-124), and what it answers with is validated here against the
    // shared `BadRequest` response every operation now documents.
    let guarded = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/find",
            "/v1/db/shop/coll/orders/find?bogus=1",
            Some(&root),
            Some(json!({ "filter": {} })),
            400,
        )
        .await;
    assert_eq!(guarded["error"], "bad_request");
    assert!(guarded["message"].as_str().is_some_and(|m| m.contains("`bogus`")), "{guarded}");
    let probe = c.check("GET", "/healthz", "/healthz?zz=1", None, None, 400).await;
    assert_eq!(probe["error"], "bad_request");

    // A wrong-shaped body on a route that is *not* `/bulk`. Until the
    // extractor carried the mapping, `/bulk` was the only one of nineteen
    // handlers that reached it, and every other route answered 422 as bare
    // text with no code — outside the taxonomy entirely. Found by driving a
    // real node, because this scenario had only ever been run against `/bulk`.
    let typed = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/vector",
            "/v1/db/shop/coll/orders/vector",
            Some(&root),
            Some(json!({ "fields": "title", "provider": { "kind": "byo" }, "dim": 3 })),
            422,
        )
        .await;
    assert_eq!(typed["error"], "bad_request", "a wrong-shaped body is still in the envelope");
    assert_eq!(typed["retry"], "no");

    // `no_vectors`: the code that existed in the server and in neither
    // document. A search against a collection nobody ingested vectors for is a
    // refusal, not an empty result — an empty result is indistinguishable from
    // "nothing matched", which is how a `byo` collection silently returns
    // nothing forever.
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/vector",
        "/v1/db/shop/coll/orders/vector",
        Some(&root),
        Some(json!({ "fields": ["title"], "provider": { "kind": "byo" }, "dim": 3 })),
        200,
    )
    .await;
    let empty = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/vector_search",
            "/v1/db/shop/coll/orders/vector_search",
            Some(&root),
            Some(json!({ "vector": [1.0, 0.0, 0.0] })),
            409,
        )
        .await;
    assert_eq!(empty["error"], "no_vectors");
    assert_eq!(empty["retry"], "no", "ingesting vectors is the fix, not repeating the search");

    // A plain GET to the change-stream route. Found by driving a real node:
    // the framework's own rejection is bare text with no code, so this was the
    // one refusal on the API a client could not branch on. Same fix, and the
    // same reason, as the JSON body rejection.
    let not_upgraded = c
        .check(
            "GET",
            "/v1/db/{db}/coll/{coll}/watch",
            "/v1/db/shop/coll/orders/watch",
            Some(&root),
            None,
            400,
        )
        .await;
    assert_eq!(not_upgraded["error"], "bad_request");
    assert_eq!(not_upgraded["retry"], "no");
}

/// A 429 carries `Retry-After`, and the spec says so.
///
/// Its own server: the limiter has to be on, and every other test would trip
/// over it for reasons unrelated to what they assert.
#[tokio::test]
async fn a_rate_limited_login_matches_its_documented_response() {
    let server = Server::start_rate_limited(1).await;
    let attempt = json!({ "user": "root", "password": "wrong" });

    let first = server.request("POST", "/v1/auth/login", None, Some(&attempt)).await;
    assert_eq!(first.status, 401);

    let limited = server.request("POST", "/v1/auth/login", None, Some(&attempt)).await;
    assert_eq!(limited.status, 429, "the second attempt is over the limit");
    let body = limited.json();
    validate_response("POST", "/v1/auth/login", 429, &body);
    assert!(
        limited.header("retry-after").is_some(),
        "the spec declares Retry-After on a 429, and a refusal without one leaves a client \
         to guess"
    );

    // A code whose answer is "the same node, later", and the only one that can
    // say how much later: `Retry-After` is a number this node already knows,
    // where `provider_error` and `timeout` are waiting on something that never
    // told them. The class is what makes a client wait at all rather than
    // moving on to a peer that shares nothing about this limit.
    assert_eq!(body["error"], "rate_limited");
    assert_eq!(body["retry"], "wait");
}

/// The two refusals `auth.local.login` adds to the token-minting routes
/// (ADR-100), validated against the envelope the specification documents for
/// them. Two servers, because the mode is fixed for the life of a process.
#[tokio::test]
async fn local_login_refusals_use_the_documented_envelope() {
    // `loopback_only`: the socket peer is loopback and is admitted, so the
    // 403 has to come from a peer off the host, which `request_from` supplies.
    let server = Server::start().await;
    let root = server
        .request(
            "POST",
            "/v1/auth/login",
            None,
            Some(&json!({"user":"root","password":ROOT_PASSWORD})),
        )
        .await
        .json()["token"]
        .as_str()
        .unwrap()
        .to_string();
    server.state.set_local_login(kimmy_api::LocalLogin::LoopbackOnly);

    let (status, body) =
        server.request_from("203.0.113.9:4000", "POST", "/v1/auth/login", None).await;
    assert_eq!(status, 403, "{body}");
    validate_response("POST", "/v1/auth/login", 403, &body);
    assert_eq!(body["error"], "forbidden");

    let (status, body) =
        server.request_from("203.0.113.9:4000", "POST", "/v1/auth/refresh", Some(&root)).await;
    assert_eq!(status, 403, "{body}");
    validate_response("POST", "/v1/auth/refresh", 403, &body);

    // `disabled`: 404 from anywhere, and — for refresh — before the token is
    // examined, so no token at all still gets the documented answer.
    let server = Server::start().await;
    server.state.set_local_login(kimmy_api::LocalLogin::Disabled);

    let res = server
        .request(
            "POST",
            "/v1/auth/login",
            None,
            Some(&json!({"user":"root","password":ROOT_PASSWORD})),
        )
        .await;
    assert_eq!(res.status, 404, "{}", res.json());
    validate_response("POST", "/v1/auth/login", 404, &res.json());
    assert_eq!(res.json()["error"], "not_found");

    let res = server.request("POST", "/v1/auth/refresh", None, None).await;
    assert_eq!(res.status, 404, "{}", res.json());
    validate_response("POST", "/v1/auth/refresh", 404, &res.json());
}

/// The document is a specification, so it has to be one.
#[test]
fn the_specification_is_well_formed() {
    let spec = spec();
    assert_eq!(spec["openapi"], "3.1.0");
    assert!(spec["info"]["version"].is_string());

    // Every `$ref` in the document resolves. A dangling one makes a generated
    // client fail at generation time, in a message about a pointer.
    let mut dangling = Vec::new();
    walk_refs(spec, spec, &mut dangling);
    assert!(dangling.is_empty(), "these references do not resolve: {dangling:#?}");
}

fn walk_refs(root: &Value, node: &Value, dangling: &mut Vec<String>) {
    match node {
        Value::Object(map) => {
            for (key, value) in map {
                if key == "$ref"
                    && let Some(reference) = value.as_str()
                {
                    match reference.strip_prefix('#') {
                        Some(pointer) if root.pointer(pointer).is_some() => {}
                        _ => dangling.push(reference.to_string()),
                    }
                }
                walk_refs(root, value, dangling);
            }
        }
        Value::Array(items) => {
            for item in items {
                walk_refs(root, item, dangling);
            }
        }
        _ => {}
    }
}
