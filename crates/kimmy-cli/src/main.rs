//! `kimmy` — a terminal client for KimmyDB.
//!
//! # One-shot commands, not a shell
//!
//! Each invocation does one thing and exits, so the tool composes with pipes,
//! `jq`, shell loops and CI. An interactive shell is nicer for exploring, but it
//! is the same command surface plus a terminal UI — so the commands come first
//! and a REPL, if it is ever wanted, sits on top of them rather than beside.
//!
//! # It is a consumer of `kimmy-client`
//!
//! Every request here goes through the Rust client crate. That is deliberate
//! and it is the point: a client library nobody uses is a library whose rough
//! edges nobody finds. Converting this tool from 200 lines of hand-rolled
//! `reqwest` is what proved the crate pleasant rather than merely present —
//! and it is why this file no longer builds a URL, reads a status code, or
//! decides what an error means.
//!
//! # It speaks HTTP, like every other client
//!
//! Nothing here opens the database file. redb allows one process to hold a
//! database, so a file-opening CLI could not be used while a node was running —
//! which is most of the time anyone wants one — and it would bypass
//! authentication and RBAC entirely. Going over the API means this exercises the
//! same surface as any other client and works against a remote node.
//!
//! # Output is JSON on stdout, diagnostics on stderr
//!
//! So `kimmy find ... | jq` works without flags, and a non-zero exit means the
//! command failed rather than "the query matched nothing".

use std::io::Read;
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use kimmy_client::{Client, ErrorCode, Method, Query, Safety};
use serde_json::{Value, json};

#[derive(Parser)]
#[command(
    name = "kimmy",
    version = kimmy_core::build::ident(),
    about = "Terminal client for KimmyDB",
    after_help = "Authentication:\n  \
        kimmy login root                  # reads the password from stdin or KIMMY_PASSWORD\n  \
        export KIMMY_TOKEN=$(echo hunter2 | kimmy login root)\n\n\
    Against a node federated with an OIDC provider:\n  \
        export KIMMY_OIDC_ISSUER=https://auth.example.com\n  \
        export KIMMY_OIDC_CLIENT_ID=kimmy-cli\n  \
        export KIMMY_TOKEN=$(kimmy login --oidc)              # opens a browser code flow\n  \
        export KIMMY_TOKEN=$(kimmy login --client-credentials) # for a service, secret from env\n\n\
    There is deliberately no --password flag and no --client-secret flag: either\n\
    would land in shell history and in `ps` output for every user on the machine.\n\
    No token is written to disk either -- an environment variable answers for its\n\
    permissions, its lifetime and its cleanup by not existing afterwards."
)]
struct Cli {
    /// Base URL of the node.
    #[arg(long, env = "KIMMY_URL", default_value = "http://localhost:7878", global = true)]
    url: String,

    /// Bearer token. `kimmy login` prints one.
    #[arg(long, env = "KIMMY_TOKEN", global = true)]
    token: Option<String>,

    /// Pretty-print the JSON rather than emitting one line.
    #[arg(long, global = true)]
    pretty: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Exchange credentials for a token, printed to stdout.
    ///
    /// Three ways in, and the token comes out the same on all three: bare, on
    /// stdout, so `$(kimmy login ...)` is directly usable. Nothing is written
    /// to disk.
    Login {
        /// Local user name. Omit it when using --oidc or --client-credentials.
        user: Option<String>,

        /// Log in through the node's OIDC provider, in a browser.
        ///
        /// RFC 8628 device authorization: this prints a code and a URL to
        /// stderr, you approve it in a browser, and the token lands on stdout.
        /// The same flow `gh auth login` uses, and it is the one that works
        /// over SSH and in a container, where a redirect back to localhost
        /// does not.
        #[arg(long, conflicts_with_all = ["user", "client_credentials"])]
        oidc: bool,

        /// Log in as a service, with OAuth2 client credentials.
        ///
        /// The client secret is read from `KIMMY_OIDC_CLIENT_SECRET` and from
        /// nowhere else: a flag would put it in shell history and in `ps`
        /// output, the same reason there is no --password.
        #[arg(long, conflicts_with = "user")]
        client_credentials: bool,

        /// Issuer URL of the OIDC provider. Must match the node's.
        #[arg(long, env = "KIMMY_OIDC_ISSUER")]
        issuer: Option<String>,

        /// OAuth2 client id registered with the provider for this CLI.
        #[arg(long, env = "KIMMY_OIDC_CLIENT_ID")]
        client_id: Option<String>,

        /// The resource the token should be *for* (RFC 8707).
        ///
        /// Without one, a provider mints its default audience — usually its own
        /// issuer URL — and every resource that trusts it shares a single
        /// audience, which is the thing an audience restriction exists to
        /// prevent. Left unset this is read off the node named by --url, so it
        /// normally needs no setting at all.
        #[arg(long, env = "KIMMY_OIDC_RESOURCE")]
        resource: Option<String>,

        /// Scopes to request. The node reads roles from the token, so the
        /// provider has to be configured to put them there.
        #[arg(long, env = "KIMMY_OIDC_SCOPE", default_value = "openid profile")]
        scope: String,
    },
    /// Health and readiness of the node.
    Ping,
    /// List databases you can read.
    Databases,
    /// List collections in a database.
    Collections { database: String },
    /// Create a collection. `target` is `db.collection`.
    ///
    /// Added after driving the converted CLI: without it, a fresh database
    /// could not be used from this tool at all — the first `insert` fails with
    /// "collection not found" and offers nowhere to go but `curl`.
    CreateCollection { target: String },
    /// Query a collection. `target` is `db.collection`.
    Find {
        target: String,
        /// Filter as JSON. Omit to match everything.
        filter: Option<String>,
        #[arg(long)]
        sort: Option<String>,
        #[arg(long)]
        projection: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        skip: Option<usize>,
        /// Report how the query was answered instead of guessing.
        #[arg(long)]
        explain: bool,
    },
    /// Count matching documents without returning them.
    Count { target: String, filter: Option<String> },
    /// Insert one document. Reads from stdin when `document` is omitted.
    Insert { target: String, document: Option<String> },
    /// Insert an array of documents in one commit, all or nothing. Reads from
    /// stdin when `documents` is omitted.
    BulkInsert { target: String, documents: Option<String> },
    /// Apply update operators to matching documents.
    Update {
        target: String,
        filter: String,
        update: String,
        #[arg(long)]
        multi: bool,
    },
    /// Delete matching documents.
    Delete {
        target: String,
        filter: String,
        #[arg(long)]
        multi: bool,
    },
    /// Run an aggregation pipeline. Reads from stdin when `pipeline` is omitted.
    Aggregate { target: String, pipeline: Option<String> },
    /// Inferred schema of a collection: field names, types, how often present.
    Describe {
        target: String,
        #[arg(long)]
        sample: Option<usize>,
    },
    /// List a collection's indexes.
    Indexes { target: String },
    /// Define an index.
    ///
    /// `fields` is a comma-separated list of paths, each optionally prefixed
    /// with `-` for descending: `item,-qty`.
    CreateIndex {
        target: String,
        /// Index name. Unique within the collection.
        name: String,
        /// Comma-separated paths, `-` prefix for descending.
        fields: String,
        /// Reject documents that repeat a key. Enforced per node, not
        /// cluster-wide — see docs/indexes.md before relying on it.
        #[arg(long)]
        unique: bool,
        /// Make it a TTL index: documents expire this long after the field's
        /// timestamp. `0` is the absolute-deadline pattern.
        #[arg(long)]
        expire_after_seconds: Option<u64>,
        /// Index only the documents matching this filter, as JSON.
        #[arg(long)]
        partial: Option<String>,
    },
    /// Remove an index by name.
    DropIndex { target: String, name: String },
    /// Follow a collection's changes until interrupted.
    Watch {
        target: String,
        /// Include the whole document on every event.
        #[arg(long)]
        full: bool,
        /// Resume after a token from an earlier run.
        #[arg(long)]
        resume_after: Option<String>,
    },
    /// Search a collection by meaning.
    ///
    /// The query is text by default and the server embeds it. Pass `--vector`
    /// when the collection is `byo`, or when the embedding was computed
    /// elsewhere — a `byo` collection has no provider to embed text with.
    VectorSearch {
        target: String,
        /// Query text for the server to embed. Omit when using --vector.
        query: Option<String>,
        /// A pre-computed embedding, as a JSON array of numbers.
        #[arg(long, conflicts_with = "query")]
        vector: Option<String>,
        /// How many results to return.
        #[arg(long, default_value_t = 10)]
        k: usize,
        /// Query-language filter, applied before the search.
        #[arg(long)]
        filter: Option<String>,
        /// Cap how many chunks of one document may fill result slots.
        #[arg(long)]
        per_document: Option<usize>,
    },
    /// Search by meaning and by keyword at once, fused by rank.
    ///
    /// Scores are fusion scores and are not comparable with the similarity
    /// scores `vector-search` returns.
    HybridSearch {
        target: String,
        query: Option<String>,
        #[arg(long, conflicts_with = "query")]
        vector: Option<String>,
        #[arg(long, default_value_t = 10)]
        k: usize,
        #[arg(long)]
        filter: Option<String>,
        #[arg(long)]
        per_document: Option<usize>,
    },
    /// The nodes this cluster is made of, and which are live.
    Topology,
    /// Download a backup of the whole node. Needs admin over everything.
    Backup {
        /// Where to write it. `-` for stdout.
        #[arg(long, default_value = "kimmy.backup")]
        out: String,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Diagnostics on stderr so a failed command does not put anything
            // on stdout that a pipeline might mistake for a result.
            eprintln!("kimmy: {e:#}");
            // The one error worth a hint, because the fix is a flag rather
            // than a change to the request. Recovered from the typed error
            // rather than by matching on the message — which is what the
            // client crate's `ErrorCode` is for, and what the string-matching
            // version of this would have got wrong the first time a message
            // was reworded.
            if e.downcast_ref::<kimmy_client::Error>().is_some_and(|e| e.is_unauthorized()) {
                eprintln!("  {}", unauthorized_hint(std::env::var("KIMMY_OIDC_ISSUER").ok()));
            }
            ExitCode::FAILURE
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn run() -> Result<()> {
    let cli = Cli::parse();

    // Login is the one command that runs without a token, because producing
    // one is what it is for.
    if let Command::Login { user, oidc, client_credentials, issuer, client_id, scope, resource } =
        &cli.command
    {
        // The token alone, with no decoration, so `$(kimmy login ...)` is
        // usable directly. Deliberately not written to a file: a CLI that
        // stores a bearer token on disk has to answer for its permissions, its
        // lifetime and its cleanup, and an environment variable answers all
        // three by not existing afterwards. The same goes for the refresh
        // token the flows below deliberately never ask for and never keep.
        let token = if *oidc || *client_credentials {
            // Ask the node itself where to authenticate and what to ask the
            // token to be for, so the usual invocation is `kimmy login --oidc`
            // with nothing else set. A flag still wins, and a node that
            // publishes nothing leaves both as they were.
            let (issuer, resource) =
                oidc::defaults_from_node(&cli.url, issuer.clone(), resource.clone()).await;
            let issuer = issuer.as_deref();
            let resource = resource.as_deref();
            let client_id = client_id.as_deref();
            if *oidc {
                oidc::device_login(issuer, client_id, scope, resource).await?
            } else {
                oidc::client_credentials_login(issuer, client_id, scope, resource).await?
            }
        } else {
            let user = user.as_deref().context(
                "name the user to log in as, or pass --oidc / --client-credentials to log in \
                 through the node's identity provider",
            )?;
            let password = read_password()?;
            let client = Client::builder(&cli.url).credentials(user, password).connect().await?;
            client.token().await.context("the server did not return a token")?
        };
        println!("{token}");
        return Ok(());
    }

    let mut builder = Client::builder(&cli.url);
    if let Some(token) = &cli.token {
        builder = builder.token(token);
    }
    let client = builder.connect().await?;

    match &cli.command {
        // Handled above, before a client was built.
        Command::Login { .. } => unreachable!("login returns early"),
        Command::Ping => {
            let health = client.request(Method::Get, "/healthz", None, Safety::Idempotent).await?;
            let ready = client.request(Method::Get, "/readyz", None, Safety::Idempotent).await?;
            let version = client.version().await?;
            emit(&cli, &json!({ "healthz": health, "readyz": ready, "version": version }));
        }
        Command::VectorSearch { target, query, vector, k, filter, per_document } => {
            let (db, coll) = split_target(target)?;
            let body = search_body(
                query.as_deref(),
                vector.as_deref(),
                *k,
                filter.as_deref(),
                *per_document,
            )?;
            emit(&cli, &client.vector_search(db, coll, &body).await?);
        }
        Command::HybridSearch { target, query, vector, k, filter, per_document } => {
            let (db, coll) = split_target(target)?;
            let body = search_body(
                query.as_deref(),
                vector.as_deref(),
                *k,
                filter.as_deref(),
                *per_document,
            )?;
            emit(&cli, &client.hybrid_search(db, coll, &body).await?);
        }
        Command::Topology => emit(&cli, &client.topology().await?),
        Command::CreateCollection { target } => {
            let (db, coll) = split_target(target)?;
            let created = client
                .request(
                    Method::Post,
                    &format!("/v1/db/{db}/collections"),
                    Some(json!({ "name": coll })),
                    // Safe to retry on another node, which is all `Idempotent`
                    // claims — the *server* answers a second create with a
                    // conflict. Reconciling those two is what the match below
                    // is for.
                    Safety::Idempotent,
                )
                .await;

            emit(&cli, &collection_created(created, coll)?);
        }
        Command::Databases => {
            emit(
                &cli,
                &client.request(Method::Get, "/v1/databases", None, Safety::Idempotent).await?,
            );
        }
        Command::Collections { database } => {
            let path = format!("/v1/db/{database}/collections");
            emit(&cli, &client.request(Method::Get, &path, None, Safety::Idempotent).await?);
        }
        Command::Find { target, filter, sort, projection, limit, skip, explain } => {
            let (db, coll) = split_target(target)?;
            let mut query = Query::new().explain(*explain);
            if let Some(filter) = filter {
                query = query.filter(parse_json("filter", filter)?);
            }
            if let Some(sort) = sort {
                query = query.sort(parse_json("sort", sort)?);
            }
            if let Some(projection) = projection {
                query = query.projection(parse_json("projection", projection)?);
            }
            if let Some(limit) = limit {
                query = query.limit(*limit);
            }
            if let Some(skip) = skip {
                query = query.skip(*skip);
            }
            emit(&cli, &client.find(db, coll, &query).await?);
        }
        Command::Count { target, filter } => {
            let (db, coll) = split_target(target)?;
            let filter = match filter {
                Some(text) => parse_json("filter", text)?,
                None => json!({}),
            };
            emit(&cli, &json!({ "count": client.count(db, coll, &filter).await? }));
        }
        Command::Insert { target, document } => {
            let (db, coll) = split_target(target)?;
            let document = parse_json_arg("document", document.as_deref())?;
            emit(&cli, &client.insert(db, coll, &document).await?);
        }
        Command::BulkInsert { target, documents } => {
            let (db, coll) = split_target(target)?;
            let documents = parse_json_arg("documents", documents.as_deref())?;
            let Some(documents) = documents.as_array() else {
                bail!("documents must be a JSON array");
            };
            emit(&cli, &client.insert_many(db, coll, documents).await?);
        }
        Command::Update { target, filter, update, multi } => {
            let (db, coll) = split_target(target)?;
            let filter = parse_json("filter", filter)?;
            let update = parse_json("update", update)?;
            emit(&cli, &client.update(db, coll, &filter, &update, *multi).await?);
        }
        Command::Delete { target, filter, multi } => {
            let (db, coll) = split_target(target)?;
            let filter = parse_json("filter", filter)?;
            emit(&cli, &client.delete(db, coll, &filter, *multi).await?);
        }
        Command::Aggregate { target, pipeline } => {
            let (db, coll) = split_target(target)?;
            let pipeline = parse_json_arg("pipeline", pipeline.as_deref())?;
            emit(&cli, &client.aggregate(db, coll, &pipeline).await?);
        }
        Command::Describe { target, sample } => {
            let (db, coll) = split_target(target)?;
            let mut path = format!("/v1/db/{db}/coll/{coll}/describe");
            if let Some(sample) = sample {
                path.push_str(&format!("?sample={sample}"));
            }
            emit(&cli, &client.request(Method::Get, &path, None, Safety::Idempotent).await?);
        }
        Command::CreateIndex { target, name, fields, unique, expire_after_seconds, partial } => {
            let (db, coll) = split_target(target)?;
            let spec =
                index_spec(name, fields, *unique, *expire_after_seconds, partial.as_deref())?;
            emit(&cli, &client.create_index(db, coll, &spec).await?);
        }
        Command::DropIndex { target, name } => {
            let (db, coll) = split_target(target)?;
            emit(&cli, &client.drop_index(db, coll, name).await?);
        }
        Command::Indexes { target } => {
            let (db, coll) = split_target(target)?;
            let path = format!("/v1/db/{db}/coll/{coll}/indexes");
            emit(&cli, &client.request(Method::Get, &path, None, Safety::Idempotent).await?);
        }
        Command::Watch { target, full, resume_after } => {
            let (db, coll) = split_target(target)?;
            let mut options = kimmy_client::WatchOptions::new().full_document(*full);
            if let Some(token) = resume_after {
                options = options.resume_after(token);
            }
            let mut stream = client.watch(db, coll, options).await?;
            // One event per line, so `kimmy watch shop.orders | jq` works and
            // a pipeline sees each event as it happens rather than at the end.
            while let Some(event) = stream.next().await? {
                emit(&cli, &event.raw);
                if event.is_invalidate() {
                    break;
                }
            }
        }
        Command::Backup { out } => {
            let bytes = client.download("/v1/admin/backup").await?;
            if out == "-" {
                use std::io::Write;
                std::io::stdout().write_all(&bytes)?;
            } else {
                std::fs::write(out, &bytes)
                    .with_context(|| format!("writing the backup to {out}"))?;
                eprintln!("wrote {} bytes to {out}", bytes.len());
            }
        }
    }
    Ok(())
}

/// Build the request body both search commands send.
///
/// Shared because the two routes take the same document — only the ranking
/// behind them differs, and duplicating this would let the commands drift into
/// accepting different flags for the same thing.
///
/// Exactly one of `query` and `vector` is required. The "neither" case is
/// caught here rather than by clap because "one of these is required" is not
/// something `conflicts_with` expresses, and the message should name both.
fn search_body(
    query: Option<&str>,
    vector: Option<&str>,
    k: usize,
    filter: Option<&str>,
    per_document: Option<usize>,
) -> Result<Value> {
    let mut body = json!({ "k": k });

    match (query, vector) {
        (Some(query), None) => body["query"] = json!(query),
        (None, Some(vector)) => {
            let parsed = parse_json("vector", vector)?;
            if !parsed.as_array().is_some_and(|v| v.iter().all(Value::is_number)) {
                bail!("--vector must be a JSON array of numbers");
            }
            body["vector"] = parsed;
        }
        // Clap rejects both at once, so this is the neither case.
        _ => bail!("give query text, or --vector with a pre-computed embedding"),
    }

    if let Some(filter) = filter {
        body["filter"] = parse_json("filter", filter)?;
    }
    if let Some(per_document) = per_document {
        body["per_document"] = json!(per_document);
    }
    Ok(body)
}

/// `db.collection` → `("db", "collection")`.
///
/// Split at the **first** dot: a collection name may contain one, a database
/// name may not, so anything after the first belongs to the collection.
fn split_target(target: &str) -> Result<(&str, &str)> {
    match target.split_once('.') {
        Some((db, coll)) if !db.is_empty() && !coll.is_empty() => Ok((db, coll)),
        _ => bail!("expected a target of the form db.collection, got {target:?}"),
    }
}

fn parse_json(label: &str, text: &str) -> Result<Value> {
    serde_json::from_str(text).with_context(|| format!("{label} is not valid JSON"))
}

/// A JSON argument, or stdin when it is omitted.
///
/// Reading stdin is what makes `kimmy insert shop.orders < doc.json` and
/// `... | kimmy aggregate shop.orders` work, which is most of the point of a
/// one-shot tool.
fn parse_json_arg(label: &str, text: Option<&str>) -> Result<Value> {
    match text {
        Some(text) => parse_json(label, text),
        None => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .with_context(|| format!("reading {label} from stdin"))?;
            if buf.trim().is_empty() {
                bail!("no {label} given, and stdin was empty");
            }
            parse_json(label, &buf)
        }
    }
}

/// Build an index definition from the command line's shorthand.
///
/// `item,-qty` rather than
/// `[{"path":"item"},{"path":"qty","descending":true}]`, because index fields
/// are the most tedious JSON this tool would otherwise ask anyone to type, and
/// a CLI that makes you hand-write the wire format is not saving you from
/// `curl`.
///
/// A `-` prefix means descending. Paths are dotted, so neither a comma nor a
/// leading `-` appears in a real one and the shorthand stays unambiguous.
///
/// Everything the route accepts is reachable — `unique`, `expireAfterSeconds`
/// and `partialFilterExpression` — so nothing about indexes is left needing
/// HTTP.
fn index_spec(
    name: &str,
    fields: &str,
    unique: bool,
    expire_after_seconds: Option<u64>,
    partial: Option<&str>,
) -> Result<Value> {
    let parsed: Vec<Value> = fields
        .split(',')
        .map(str::trim)
        .filter(|f| !f.is_empty())
        .map(|field| match field.strip_prefix('-') {
            Some(path) if !path.is_empty() => Ok(json!({ "path": path, "descending": true })),
            Some(_) => bail!("a field is just \"-\" with no path: {fields:?}"),
            None => Ok(json!({ "path": field })),
        })
        .collect::<Result<_>>()?;

    if parsed.is_empty() {
        bail!("an index needs at least one field, for example \"item\" or \"item,-qty\"");
    }

    let mut spec = json!({ "name": name, "fields": parsed, "unique": unique });
    if let Some(seconds) = expire_after_seconds {
        spec["expireAfterSeconds"] = json!(seconds);
    }
    if let Some(partial) = partial {
        spec["partialFilterExpression"] = parse_json("partial", partial)?;
    }
    Ok(spec)
}

/// The password, from `KIMMY_PASSWORD` or stdin.
///
/// There is deliberately no `--password` flag. It would be recorded in shell
/// history and visible in `ps` to every user on the machine — a credential that
/// leaks by being typed.
fn read_password() -> Result<String> {
    if let Ok(password) = std::env::var("KIMMY_PASSWORD") {
        return Ok(password);
    }
    let mut buf = String::new();
    std::io::stdin().read_to_string(&mut buf).context("reading the password from stdin")?;
    let password = buf.trim_end_matches(['\n', '\r']).to_string();
    if password.is_empty() {
        bail!(
            "no password: set KIMMY_PASSWORD or pipe it in, e.g. `echo hunter2 | kimmy login root`"
        );
    }
    Ok(password)
}

/// What to suggest after a 401.
///
/// Issuer-aware, because the two deployments need opposite advice and the wrong
/// one sends someone to a login endpoint that does not exist for them: a
/// federated caller has no local password, and `kimmy login <user>` would ask
/// them for one all the same.
fn unauthorized_hint(issuer: Option<String>) -> String {
    match issuer {
        Some(issuer) => format!(
            "set --token, or KIMMY_TOKEN from `kimmy login --oidc` (issuer {issuer}); \
             a local account still works with `kimmy login <user>`"
        ),
        None => "set --token, or KIMMY_TOKEN from `kimmy login`".to_string(),
    }
}

/// Getting a token out of an OpenID Connect provider.
///
/// # Why this talks to the provider directly rather than through `kimmy-client`
///
/// Everything else in this file goes through the client crate, deliberately
/// (see the module docs). This does not, because it is not a KimmyDB request:
/// the provider is a different service with a different protocol, and teaching
/// the database client OAuth2 would put an OAuth2 implementation in every
/// application that links it — which is exactly what `Builder::token_provider`
/// exists to avoid.
///
/// # Why the device flow rather than a redirect
///
/// A redirect flow needs a browser and a loopback listener on the same machine.
/// A database CLI is run over SSH and inside containers, where neither holds.
/// The device flow works everywhere a person can read a code off one screen and
/// type it on another — the same reason `gh auth login` uses it.
mod oidc {
    use anyhow::{Context, Result, bail};
    use serde_json::Value;

    /// How long to keep polling before giving up on a person approving.
    ///
    /// A backstop only: the provider states its own expiry and that is what is
    /// honoured. This bounds the case where it does not.
    const MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(600);

    /// The interval to poll at when the provider does not name one (RFC 8628 §3.5).
    const DEFAULT_POLL_SECS: u64 = 5;

    /// Where a node publishes what it is, as an OAuth 2.0 protected resource.
    ///
    /// Written out here rather than shared with the server's constant in
    /// `kimmy-auth`. That crate carries password hashing and a storage engine,
    /// and `kimmy-client` — the one dependency that could have re-exported it —
    /// deliberately links no kimmy crate at all, which is a property worth more
    /// than deduplicating a string. It is fixed by RFC 9728 §3 in any case, so
    /// the two cannot drift without the specification changing under both.
    const PROTECTED_RESOURCE_METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

    fn http() -> Result<reqwest::Client> {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("building the HTTP client")
    }

    fn issuer_or_bail(issuer: Option<&str>) -> Result<&str> {
        issuer.context(
            "no OIDC issuer: set KIMMY_OIDC_ISSUER or pass --issuer, using the same value the \
             node is configured with",
        )
    }

    /// Fill in the issuer and the resource from the node about to be used.
    ///
    /// This is the whole ergonomic point of publishing RFC 9728 metadata: the
    /// node already knows which authorization server it trusts and what name it
    /// was registered under, and making an operator restate both in environment
    /// variables is asking them to keep three places in agreement by hand.
    ///
    /// **Never fatal.** A node built before this existed, one whose audience is
    /// an opaque string, or one that is simply unreachable all publish nothing —
    /// and in every one of those cases the flags are still the answer, so a
    /// lookup failure returns what it was given rather than stopping a login.
    /// Nothing is fetched at all when both values are already known.
    pub async fn defaults_from_node(
        url: &str,
        issuer: Option<String>,
        resource: Option<String>,
    ) -> (Option<String>, Option<String>) {
        if issuer.is_some() && resource.is_some() {
            return (issuer, resource);
        }
        let Some(document) = protected_resource_metadata(url).await else {
            return (issuer, resource);
        };
        // `authorization_servers` is a list because a resource may trust
        // several; this node trusts exactly one (ADR-064), so the first entry
        // is the only entry.
        let discovered_issuer = document
            .get("authorization_servers")
            .and_then(Value::as_array)
            .and_then(|servers| servers.first())
            .and_then(Value::as_str)
            .map(str::to_string);
        let discovered_resource =
            document.get("resource").and_then(Value::as_str).map(str::to_string);
        (issuer.or(discovered_issuer), resource.or(discovered_resource))
    }

    /// The node's own RFC 9728 document, if it publishes one.
    async fn protected_resource_metadata(url: &str) -> Option<Value> {
        let url = format!("{}{PROTECTED_RESOURCE_METADATA_PATH}", url.trim_end_matches('/'));
        let response = http().ok()?.get(&url).send().await.ok()?;
        if !response.status().is_success() {
            return None;
        }
        response.json().await.ok()
    }

    /// Name the resource a token is being asked for, when one is known.
    ///
    /// Omitted rather than sent empty when it is not: a `resource` parameter
    /// with no value is a malformed request, whereas its absence is the
    /// well-defined "give me your default audience" that every provider
    /// predating RFC 8707 already implements.
    pub(super) fn with_resource<'a>(
        mut form: Vec<(&'a str, &'a str)>,
        resource: Option<&'a str>,
    ) -> Vec<(&'a str, &'a str)> {
        if let Some(resource) = resource {
            form.push(("resource", resource));
        }
        form
    }

    fn client_id_or_bail(client_id: Option<&str>) -> Result<&str> {
        client_id.context(
            "no OIDC client id: set KIMMY_OIDC_CLIENT_ID or pass --client-id, using an \
             application registered with your provider",
        )
    }

    /// Read the provider's discovery document.
    ///
    /// Appended to the issuer rather than URL-joined, which is what RFC 8414
    /// specifies — and for an issuer with a path, as Keycloak realms and Entra
    /// tenants have, joining would look at the host root instead.
    async fn discover(http: &reqwest::Client, issuer: &str) -> Result<Value> {
        let url = format!("{}/.well-known/openid-configuration", issuer.trim_end_matches('/'));
        let response = http.get(&url).send().await.with_context(|| format!("fetching {url}"))?;
        if !response.status().is_success() {
            bail!("the provider answered {} for {url}", response.status());
        }
        response.json().await.with_context(|| format!("parsing the discovery document at {url}"))
    }

    fn endpoint(document: &Value, name: &str) -> Result<String> {
        document
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_string)
            .with_context(|| format!("the provider's discovery document names no {name}"))
    }

    /// The OAuth2 error code in a failed token response, if there is one.
    fn oauth_error(body: &Value) -> &str {
        body.get("error").and_then(Value::as_str).unwrap_or("")
    }

    /// Pull the access token out of a successful token response.
    ///
    /// The **access** token, never the id token: the node verifies an audience,
    /// and an id token's audience is the client, not the API. Handing over an
    /// id token is the mistake that produces a 401 nobody can explain.
    fn access_token(body: &Value) -> Result<String> {
        body.get("access_token")
            .and_then(Value::as_str)
            .map(str::to_string)
            .context("the provider returned no access_token")
    }

    /// RFC 8628 device authorization grant.
    ///
    /// Everything a person reads goes to **stderr** and only the token goes to
    /// stdout, so `$(kimmy login --oidc)` captures the token and the
    /// instructions still reach the terminal.
    pub async fn device_login(
        issuer: Option<&str>,
        client_id: Option<&str>,
        scope: &str,
        resource: Option<&str>,
    ) -> Result<String> {
        let issuer = issuer_or_bail(issuer)?;
        let client_id = client_id_or_bail(client_id)?;
        let http = http()?;
        let document = discover(&http, issuer).await?;
        let device_endpoint = endpoint(&document, "device_authorization_endpoint").context(
            "this provider does not advertise the device authorization endpoint; \
             --client-credentials works for a service account",
        )?;
        let token_endpoint = endpoint(&document, "token_endpoint")?;

        let start: Value = http
            .post(&device_endpoint)
            // The resource goes on the *authorization* request as well as the
            // token request below (RFC 8707 §2.1 and §2.2). Sending it only at
            // the token endpoint would ask a provider to widen a grant that was
            // recorded without it, which is the one direction it may not go.
            .form(&with_resource(vec![("client_id", client_id), ("scope", scope)], resource))
            .send()
            .await
            .with_context(|| format!("starting the device flow at {device_endpoint}"))?
            .json()
            .await
            .context("parsing the device authorization response")?;

        if !oauth_error(&start).is_empty() {
            bail!("the provider refused the device request: {}", describe(&start));
        }

        let device_code = start
            .get("device_code")
            .and_then(Value::as_str)
            .context("the provider returned no device_code")?;
        let user_code = start
            .get("user_code")
            .and_then(Value::as_str)
            .context("the provider returned no user_code")?;
        let verification = start
            .get("verification_uri_complete")
            .or_else(|| start.get("verification_uri"))
            .and_then(Value::as_str)
            .context("the provider returned no verification_uri")?;

        eprintln!("Open {verification} and enter the code: {user_code}");
        eprintln!("Waiting for approval...");

        // The provider's own pacing, honoured: polling faster than it asked for
        // is what `slow_down` exists to punish, and being told to slow down
        // twice is how a client gets throttled out of the flow entirely.
        let mut interval = std::time::Duration::from_secs(
            start.get("interval").and_then(Value::as_u64).unwrap_or(DEFAULT_POLL_SECS),
        );
        let deadline = std::time::Instant::now()
            + start
                .get("expires_in")
                .and_then(Value::as_u64)
                .map(std::time::Duration::from_secs)
                .unwrap_or(MAX_WAIT);

        loop {
            tokio::time::sleep(interval).await;
            if std::time::Instant::now() > deadline {
                bail!("the device code expired before it was approved; run the command again");
            }

            let body: Value = http
                .post(&token_endpoint)
                .form(&with_resource(
                    vec![
                        ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                        ("device_code", device_code),
                        ("client_id", client_id),
                    ],
                    resource,
                ))
                .send()
                .await
                .with_context(|| format!("polling {token_endpoint}"))?
                .json()
                .await
                .context("parsing the token response")?;

            match oauth_error(&body) {
                // Nobody has approved it yet. Not an error — it is the normal
                // answer for as long as the person is still typing.
                "authorization_pending" => continue,
                // Told to back off. Five seconds is what RFC 8628 §3.5 says to
                // add, and it is cumulative across repeats.
                "slow_down" => {
                    interval += std::time::Duration::from_secs(5);
                    continue;
                }
                "expired_token" => {
                    bail!("the device code expired before it was approved; run it again")
                }
                "access_denied" => bail!("the request was denied at the provider"),
                "" => return access_token(&body),
                _ => bail!("the provider refused the token request: {}", describe(&body)),
            }
        }
    }

    /// OAuth2 client credentials, for a service rather than a person.
    ///
    /// The secret comes from the environment and from nowhere else, for the
    /// same reason there is no `--password`: a flag lands in shell history and
    /// in `ps` output for every user on the machine.
    pub async fn client_credentials_login(
        issuer: Option<&str>,
        client_id: Option<&str>,
        scope: &str,
        resource: Option<&str>,
    ) -> Result<String> {
        let issuer = issuer_or_bail(issuer)?;
        let client_id = client_id_or_bail(client_id)?;
        let secret =
            std::env::var("KIMMY_OIDC_CLIENT_SECRET").ok().filter(|s| !s.is_empty()).context(
                "no client secret: set KIMMY_OIDC_CLIENT_SECRET. There is deliberately no flag \
                 for it — it would land in shell history and in `ps` output.",
            )?;

        let http = http()?;
        let document = discover(&http, issuer).await?;
        let token_endpoint = endpoint(&document, "token_endpoint")?;

        let body: Value = http
            .post(&token_endpoint)
            .form(&with_resource(
                vec![
                    ("grant_type", "client_credentials"),
                    ("client_id", client_id),
                    ("client_secret", secret.as_str()),
                    ("scope", scope),
                ],
                resource,
            ))
            .send()
            .await
            .with_context(|| format!("requesting a token from {token_endpoint}"))?
            .json()
            .await
            .context("parsing the token response")?;

        if !oauth_error(&body).is_empty() {
            bail!("the provider refused the token request: {}", describe(&body));
        }
        access_token(&body)
    }

    /// An OAuth2 error, as something a person can act on.
    ///
    /// `error_description` is the half that says what to fix; the code alone is
    /// usually `invalid_client`, which is true of several different mistakes.
    pub(super) fn describe(body: &Value) -> String {
        let code = oauth_error(body);
        match body.get("error_description").and_then(Value::as_str) {
            Some(detail) => format!("{code}: {detail}"),
            None => code.to_string(),
        }
    }
}

fn emit(cli: &Cli, value: &Value) {
    let rendered =
        if cli.pretty { serde_json::to_string_pretty(value) } else { serde_json::to_string(value) };
    match rendered {
        Ok(text) => println!("{text}"),
        Err(e) => eprintln!("kimmy: could not render the response: {e}"),
    }
}

/// What `create-collection` reports, given what the server answered.
///
/// A collection that is already there is the state this command exists to
/// reach, so reporting failure is wrong twice over.
///
/// It is wrong for a person, who asked for a collection and has one, and who
/// otherwise has to wrap a command that succeeded in `|| true`. And it is wrong
/// after a failover: [`Safety::Idempotent`] lets the request be retried on
/// another node, so a create that landed and lost its answer comes back as a
/// conflict raised by this client's *own* first attempt. Exiting non-zero there
/// reports a failure that did not happen.
///
/// The two cases stay distinguishable in the output rather than being flattened
/// into one, because "I made this" and "this was already here" are different
/// answers to the same question — and a script that cares can tell them apart.
///
/// Only `conflict` is absorbed. A reserved name, a missing database or a denied
/// grant are all still failures, because none of them leave the caller with the
/// collection they asked for.
fn collection_created(answer: kimmy_client::Result<Value>, collection: &str) -> Result<Value> {
    match answer {
        Ok(created) => Ok(created),
        Err(e) if e.code() == Some(ErrorCode::Conflict) => Ok(json!({ "exists": collection })),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    /// One workspace version, one binary story (ADR-062): what
    /// `kimmy --version` prints is the workspace version the server also
    /// reports, and the identity line leads with it.
    #[test]
    fn the_cli_version_is_the_workspace_version() {
        assert_eq!(env!("CARGO_PKG_VERSION"), kimmy_core::build::VERSION);
        assert!(kimmy_core::build::ident().starts_with(kimmy_core::build::VERSION));
    }

    // -----------------------------------------------------------------------
    // The RFC 8707 resource parameter (ADR-071)
    // -----------------------------------------------------------------------

    #[test]
    fn the_resource_rides_on_both_oauth_flows() {
        // The gap this workstream exists to close: without it the only audience
        // `kimmy login` could ever obtain was the provider's default, so the
        // correct configuration was unreachable from this tool.
        let device = oidc::with_resource(
            vec![("client_id", "kimmy-cli"), ("scope", "openid profile")],
            Some("https://kimmydb.example.com"),
        );
        assert!(device.contains(&("resource", "https://kimmydb.example.com")));

        let service = oidc::with_resource(
            vec![("grant_type", "client_credentials")],
            Some("https://kimmydb.example.com"),
        );
        assert!(service.contains(&("resource", "https://kimmydb.example.com")));
    }

    #[test]
    fn no_resource_means_the_parameter_is_absent_rather_than_empty() {
        // An empty `resource` is a malformed request; its absence is the
        // well-defined "your default audience" that every provider predating
        // RFC 8707 already implements.
        let form = oidc::with_resource(vec![("client_id", "kimmy-cli")], None);
        assert!(form.iter().all(|(key, _)| *key != "resource"), "{form:?}");
    }

    #[tokio::test]
    async fn an_unreachable_node_does_not_stop_a_login() {
        // Discovery off the node is an ergonomic shortcut, never a dependency:
        // a node built before this existed, one with an opaque audience, and
        // one that is simply down all publish nothing, and in every case the
        // flags are still the answer.
        let (issuer, resource) = oidc::defaults_from_node(
            // Refused rather than blackholed, so the test costs a syscall
            // instead of the client's 30-second timeout.
            "http://127.0.0.1:1",
            Some("https://auth.example.com".into()),
            None,
        )
        .await;
        assert_eq!(issuer.as_deref(), Some("https://auth.example.com"));
        assert_eq!(resource, None);
    }

    #[tokio::test]
    async fn nothing_is_fetched_when_both_values_are_already_known() {
        // 192.0.2.0/24 is reserved for documentation (RFC 5737) and blackholes,
        // so a request to it hangs until the client's own timeout. Returning
        // inside the deadline below is therefore the assertion: it says the
        // network was never touched, and a regression fails in two seconds
        // rather than thirty.
        let resolved = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            oidc::defaults_from_node(
                "http://192.0.2.1:7878",
                Some("https://auth.example.com".into()),
                Some("https://kimmydb.example.com".into()),
            ),
        )
        .await
        .expect("nothing should be fetched when both values are already known");

        assert_eq!(resolved.0.as_deref(), Some("https://auth.example.com"));
        assert_eq!(resolved.1.as_deref(), Some("https://kimmydb.example.com"));
    }

    #[test]
    fn a_single_field_index_is_ascending() {
        let spec = index_spec("item_idx", "item", false, None, None).unwrap();
        assert_eq!(spec["name"], "item_idx");
        assert_eq!(spec["fields"], json!([{ "path": "item" }]));
        assert_eq!(spec["unique"], false);
    }

    #[test]
    fn a_dash_prefix_means_descending() {
        // The whole reason the shorthand exists: this is the alternative to
        // typing the wire format by hand.
        let spec = index_spec("item_qty", "item,-qty", false, None, None).unwrap();
        assert_eq!(
            spec["fields"],
            json!([{ "path": "item" }, { "path": "qty", "descending": true }])
        );
    }

    #[test]
    fn whitespace_around_fields_is_tolerated() {
        let spec = index_spec("i", " item , -qty ", false, None, None).unwrap();
        assert_eq!(
            spec["fields"],
            json!([{ "path": "item" }, { "path": "qty", "descending": true }])
        );
    }

    #[test]
    fn dotted_paths_survive_the_shorthand() {
        // Paths are dotted, which is exactly why the separator is a comma.
        let spec = index_spec("i", "customer.address.city", false, None, None).unwrap();
        assert_eq!(spec["fields"], json!([{ "path": "customer.address.city" }]));
    }

    #[test]
    fn an_index_with_no_fields_is_refused() {
        for empty in ["", "   ", ",", " , "] {
            let e = index_spec("i", empty, false, None, None).unwrap_err().to_string();
            assert!(e.contains("at least one field"), "unhelpful message for {empty:?}: {e}");
        }
    }

    #[test]
    fn a_dash_with_no_path_is_refused() {
        assert!(index_spec("i", "-", false, None, None).is_err());
        assert!(index_spec("i", "item,-", false, None, None).is_err());
    }

    #[test]
    fn ttl_and_partial_are_only_sent_when_asked_for() {
        let plain = index_spec("i", "item", false, None, None).unwrap();
        assert!(plain.get("expireAfterSeconds").is_none(), "absent must not be sent as null");
        assert!(plain.get("partialFilterExpression").is_none());

        // Zero is the absolute-deadline pattern, so it must survive rather than
        // being treated as "unset".
        let ttl = index_spec("i", "seen", false, Some(0), None).unwrap();
        assert_eq!(ttl["expireAfterSeconds"], 0);

        let partial =
            index_spec("i", "email", true, None, Some("{\"email\":{\"$exists\":true}}")).unwrap();
        assert_eq!(partial["partialFilterExpression"]["email"]["$exists"], true);
        assert_eq!(partial["unique"], true);
    }

    #[test]
    fn a_target_splits_at_the_first_dot() {
        // A collection name may contain a dot — the vector shadow collections
        // are literally `orders.__vectors` — while a database name may not. So
        // everything after the first dot belongs to the collection.
        assert_eq!(split_target("shop.orders").unwrap(), ("shop", "orders"));
        assert_eq!(split_target("shop.orders.__vectors").unwrap(), ("shop", "orders.__vectors"));
    }

    #[test]
    fn a_malformed_target_says_what_was_expected() {
        for bad in ["orders", "", ".orders", "shop."] {
            let err = split_target(bad).unwrap_err().to_string();
            assert!(err.contains("db.collection"), "unhelpful error for {bad:?}: {err}");
        }
    }

    #[test]
    fn bad_json_is_reported_against_the_argument_that_held_it() {
        // "invalid JSON" without saying which argument sends someone hunting
        // through a command line with three JSON documents on it.
        let err = parse_json("filter", "{not json").unwrap_err().to_string();
        assert!(err.contains("filter"), "{err}");
    }

    #[test]
    fn there_is_no_password_flag() {
        // Asserted rather than assumed: a --password flag lands in shell
        // history and in `ps` for every user on the machine, so its absence is
        // a security property and not a gap someone should helpfully fill.
        // Inspecting the arguments, not the help text: the help *explains*
        // why there is no such flag, so a substring search finds the
        // explanation and passes for the wrong reason.
        fn has_password(cmd: &clap::Command) -> bool {
            cmd.get_arguments().any(|a| a.get_long() == Some("password"))
                || cmd.get_subcommands().any(has_password)
        }

        // `build()` first. Without it clap has not propagated subcommand
        // arguments yet, so the walk finds nothing and the test passes however
        // many password flags exist — checked by adding one and watching this
        // fail.
        let mut command = Cli::command();
        command.build();
        assert!(!has_password(&command), "a --password flag has been added");
    }

    #[test]
    fn there_is_no_client_secret_flag_either() {
        // Same property as `--password`, same reason: a secret on a command
        // line is in shell history and in `ps` for every user on the machine.
        // It is read from KIMMY_OIDC_CLIENT_SECRET and from nowhere else.
        fn has(cmd: &clap::Command, long: &str) -> bool {
            cmd.get_arguments().any(|a| a.get_long() == Some(long))
                || cmd.get_subcommands().any(|c| has(c, long))
        }
        let mut command = Cli::command();
        command.build();
        assert!(!has(&command, "client-secret"), "a --client-secret flag has been added");
    }

    #[test]
    fn login_takes_a_user_or_a_federated_flag_but_not_both() {
        // The user argument became optional so that `--oidc` can stand alone.
        // Clap has to keep the combinations honest, or the wrong pair reaches
        // the handler and fails somewhere less obvious.
        assert!(Cli::try_parse_from(["kimmy", "login", "root"]).is_ok());
        assert!(Cli::try_parse_from(["kimmy", "login", "--oidc"]).is_ok());
        assert!(Cli::try_parse_from(["kimmy", "login", "--client-credentials"]).is_ok());
        assert!(Cli::try_parse_from(["kimmy", "login", "--oidc", "root"]).is_err());
        assert!(Cli::try_parse_from(["kimmy", "login", "--oidc", "--client-credentials"]).is_err());
    }

    #[test]
    fn the_unauthorized_hint_matches_the_deployment() {
        // The wrong hint sends a federated user to a local login that will ask
        // them for a password they do not have.
        let local = unauthorized_hint(None);
        assert!(local.contains("kimmy login"), "{local}");
        assert!(!local.contains("--oidc"), "{local}");

        let federated = unauthorized_hint(Some("https://auth.example.com".into()));
        assert!(federated.contains("kimmy login --oidc"), "{federated}");
        assert!(federated.contains("auth.example.com"), "the issuer names itself: {federated}");
        // ...and it still says a local account works, because both do at once.
        assert!(federated.contains("kimmy login <user>"), "{federated}");
    }

    #[test]
    fn an_oauth_error_carries_the_half_that_says_what_to_fix() {
        // `invalid_client` alone is true of several different mistakes; the
        // description is what distinguishes them.
        let described = oidc::describe(&json!({
            "error": "invalid_client",
            "error_description": "client kimmy-cli is not configured for the device flow",
        }));
        assert!(described.contains("invalid_client"), "{described}");
        assert!(described.contains("device flow"), "{described}");

        assert_eq!(oidc::describe(&json!({ "error": "access_denied" })), "access_denied");
    }

    fn api_error(status: u16, code: ErrorCode) -> kimmy_client::Error {
        kimmy_client::Error::Api {
            status,
            code,
            message: "from the server".into(),
            retry: kimmy_client::Retry::No,
            retry_after: None,
        }
    }

    #[test]
    fn creating_a_collection_that_exists_succeeds() {
        // The command exists to leave the caller with a collection. It did.
        let out = collection_created(Err(api_error(409, ErrorCode::Conflict)), "orders")
            .expect("an existing collection is not a failure");
        assert_eq!(out, json!({ "exists": "orders" }));
    }

    #[test]
    fn a_created_collection_reports_what_the_server_said() {
        // And the two outcomes stay distinguishable: a script that cares which
        // happened can still tell.
        let answer = json!({ "created": "orders", "id": 7 });
        let out = collection_created(Ok(answer.clone()), "orders").unwrap();
        assert_eq!(out, answer);
        assert!(out.get("exists").is_none(), "a fresh create must not look like an existing one");
    }

    #[test]
    fn only_a_conflict_is_absorbed() {
        // A reserved name, a missing database and a denied grant all leave the
        // caller without the collection they asked for, so all of them fail.
        for code in [ErrorCode::BadRequest, ErrorCode::NotFound, ErrorCode::Forbidden] {
            assert!(
                collection_created(Err(api_error(400, code)), "orders").is_err(),
                "{code:?} must not be swallowed"
            );
        }
    }

    #[test]
    fn a_search_defaults_to_embedding_the_query_text() {
        let body = search_body(Some("wet sticky dough"), None, 10, None, None).unwrap();
        assert_eq!(body["query"], "wet sticky dough");
        assert_eq!(body["k"], 10);
        assert!(body.get("vector").is_none(), "text and a vector must not both be sent");
    }

    #[test]
    fn a_precomputed_vector_is_sent_instead_of_text() {
        // What a `byo` collection needs: it has no provider to embed text with.
        let body = search_body(None, Some("[0.1, 0.2]"), 5, None, None).unwrap();
        assert_eq!(body["vector"], json!([0.1, 0.2]));
        assert!(body.get("query").is_none());
    }

    #[test]
    fn a_search_with_neither_text_nor_vector_is_refused() {
        // Clap catches both-at-once; nothing but this catches neither.
        let e = search_body(None, None, 10, None, None).unwrap_err().to_string();
        assert!(e.contains("query text"), "the message must name both options: {e}");
        assert!(e.contains("--vector"), "the message must name both options: {e}");
    }

    #[test]
    fn a_vector_that_is_not_numbers_is_refused_before_the_request() {
        // Caught here rather than as a 400, because the server's complaint
        // would be about dimensions and the mistake is a type.
        for bad in ["[\"a\"]", "{}", "\"nope\"", "[1, \"two\"]"] {
            assert!(search_body(None, Some(bad), 10, None, None).is_err(), "accepted {bad}");
        }
    }

    #[test]
    fn filter_and_per_document_are_passed_through_when_given() {
        let body =
            search_body(Some("q"), None, 3, Some("{\"status\":\"published\"}"), Some(1)).unwrap();
        assert_eq!(body["filter"]["status"], "published");
        assert_eq!(body["per_document"], 1);

        let bare = search_body(Some("q"), None, 3, None, None).unwrap();
        assert!(bare.get("filter").is_none(), "an absent filter must not be sent as null");
        assert!(bare.get("per_document").is_none());
    }
}
