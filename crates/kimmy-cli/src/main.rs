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
use clap::{CommandFactory, Parser, Subcommand};
use kimmy_client::{Client, ErrorCode, Method, Query, Safety, UpdateOptions};
use serde_json::{Value, json};

#[derive(Parser)]
#[command(
    name = "kimmy",
    version = kimmy_core::build::ident(),
    about = "Terminal client for KimmyDB",
    after_help = "Authentication:\n  \
        kimmy login                        # federates by default: a browser code flow\n  \
        export KIMMY_TOKEN=$(kimmy login)\n  \
        kimmy token                        # prints the token again while the cached one is fresh\n\n\
    A local account on the node itself instead:\n  \
        kimmy login root                   # reads the password from stdin or KIMMY_PASSWORD\n  \
        export KIMMY_TOKEN=$(echo hunter2 | kimmy login root)\n\n\
    The provider and resource are discovered from the node; to name them:\n  \
        export KIMMY_OIDC_ISSUER=https://auth.example.com\n  \
        export KIMMY_OIDC_CLIENT_ID=kimmy-cli\n\n\
    A script or a service is not a person, so it does not log in: it sets\n  \
        export KIMMY_TOKEN=<a token minted elsewhere, e.g. a personal access token>\n\n\
    Settings file: ~/.config/kimmydb/.kimmy — url, token, issuer, client_id and\n\
    friends; a flag or environment variable always wins over the file.\n\
    There is deliberately no --password flag: it would land in shell history\n\
    and in `ps` output for every user on the machine.\n\
    The access token is cached in a 0600 file under the user's cache directory\n\
    so every other command works afterwards; a refresh token is never requested\n\
    or stored at all -- an environment variable answers for a token's\n\
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
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Exchange credentials for a token, printed to stdout.
    ///
    /// Bare `kimmy login` federates: it runs the device flow against the
    /// node's identity provider, because that is what nearly every deployment
    /// of this database is configured with. Naming a local account —
    /// `kimmy login ada` — takes the password path instead, so a node running
    /// without any provider loses nothing.
    ///
    /// The token comes out bare on stdout, so `$(kimmy login ...)` is directly
    /// usable. The access token is kept in a `0600` file under the user's
    /// cache directory, keyed by issuer, client and resource — that cache is
    /// what makes every other command work afterwards without `--token`, and
    /// what lets `kimmy token` answer instantly while it stays fresh. A
    /// refresh token is never requested and never stored.
    Login {
        /// Local user name. Given, this is a password login against the node's
        /// own user store; omitted, the device flow answers.
        user: Option<String>,

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
        ///
        /// Left unset the device flow asks for `openid profile`: there is a
        /// person behind it, so `openid` is meaningful.
        #[arg(long, env = "KIMMY_OIDC_SCOPE")]
        scope: Option<String>,
    },
    /// Print the access token again — the cached one while it stays fresh.
    ///
    /// Where `kimmy login` authenticates, `kimmy token` answers "what is my
    /// token right now": if a cached access token for this issuer, client and
    /// resource is still good it prints immediately, and otherwise it runs the
    /// federated flow once, keeps the result, and prints that. Every call
    /// after the first costs nothing until the token nears expiry.
    ///
    /// Caching is not an option here because it is the point of the command:
    /// invoking it *is* the asking. What
    /// gets stored does not change — the access token alone, in a `0600`
    /// file, never a refresh token.
    ///
    /// Federated flows only. A local account has no provider to key a cache
    /// entry by, so `kimmy login <user>` stays how those print a token.
    Token {
        /// Issuer URL of the OIDC provider. Must match the node's.
        #[arg(long, env = "KIMMY_OIDC_ISSUER")]
        issuer: Option<String>,

        /// OAuth2 client id registered with the provider for this CLI.
        #[arg(long, env = "KIMMY_OIDC_CLIENT_ID")]
        client_id: Option<String>,

        /// The resource the token should be *for* (RFC 8707). Read off the
        /// node named by --url when omitted.
        #[arg(long, env = "KIMMY_OIDC_RESOURCE")]
        resource: Option<String>,

        /// Scopes to request. Defaults per flow exactly as `login` does.
        #[arg(long, env = "KIMMY_OIDC_SCOPE")]
        scope: Option<String>,
    },
    /// Health and readiness of the node.
    Ping,
    /// Interactively write ~/.config/kimmydb/.kimmy.
    ///
    /// Prompts once per known setting, showing any current value (from the
    /// existing file or the environment) as the default — Enter keeps it,
    /// typing replaces it, and a setting left empty with nothing to keep is
    /// simply not written. Running init again re-prompts and overwrites the
    /// file wholesale. The file is written 0600: it can carry a token, a
    /// password and a client secret.
    Init,
    /// How this node sees the caller's identity.
    ///
    /// The principal name, whether it came from this node's own user store or
    /// from its OIDC provider, and — the part worth reading after an empty
    /// listing or a bare 403 — the grants that identity actually holds. A
    /// federated login that succeeds while everything else refuses is almost
    /// always a token whose roles no mapping turned into grants.
    Whoami,
    /// Manage stored roles — the named grant sets that OIDC role mappings
    /// point at and local users can hold.
    Roles {
        #[command(subcommand)]
        command: RolesSub,
    },
    /// Manage this node's local user accounts.
    Users {
        #[command(subcommand)]
        command: UsersSub,
    },
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
        /// A JSON array of filters for the `$[<identifier>]` segments in the
        /// update's paths, one document per identifier, for example
        /// `'[{"line.sku": "b"}]'` with `'{"$set": {"items.$[line].shipped": true}}'`.
        #[arg(long, value_name = "JSON")]
        array_filters: Option<String>,
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
        /// Weight of the dense (vector) half in fusion. At least 0; the
        /// server's default is 1. Only the ratio to --lexical-weight matters.
        #[arg(long)]
        dense_weight: Option<f64>,
        /// Weight of the lexical (keyword) half in fusion. At least 0; the
        /// server's default is 1. Zero switches the keyword half off.
        #[arg(long)]
        lexical_weight: Option<f64>,
        /// Distinct query terms a chunk must share with the query to count as
        /// keyword evidence. At least 1; the server's default is 1. Use 2 on a
        /// collection of short documents.
        #[arg(long)]
        min_overlap: Option<usize>,
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

/// Subcommands of `kimmy roles`.
#[derive(Subcommand, Debug)]
enum RolesSub {
    /// List every stored role by name.
    List,
    /// Print one role: its name and its grants.
    Show { name: String },
    /// Create a role. Repeat --grant for each grant; see `--grant` for the shape.
    Create {
        name: String,
        /// `[collection]:action[,action…]`, or `db/collection:actions`. The
        /// collection part defaults to `*`. Examples:
        /// `--grant 'sales/orders*:read,search'`, `--grant '*:*:read'`.
        #[arg(long = "grant")]
        grants: Vec<String>,
    },
    /// Add actions to one of a role's grants, creating the grant if absent.
    Grant {
        name: String,
        /// Same shape as roles create's --grant. One grant per invocation.
        grant: String,
    },
    /// Remove actions from one of a role's grants, dropping it when empty.
    Revoke {
        name: String,
        /// Same shape as roles create's --grant (actions required).
        grant: String,
    },
    /// Delete a role. Principals mapping to or holding it lose its grants
    /// on their next request — nothing else is affected.
    Delete { name: String },
}

/// Subcommands of `kimmy users`.
#[derive(Subcommand, Debug)]
enum UsersSub {
    /// List local accounts with their state.
    List,
    /// Print one account: grants, disabled flag.
    Show { user: String },
    /// Create an account. The password is read from stdin, like login's.
    ///
    /// Repeat --grant for initial grants (same shape as roles create), and
    /// --role to hold stored roles from birth.
    Create {
        user: String,
        #[arg(long = "grant")]
        grants: Vec<String>,
        #[arg(long = "role")]
        roles: Vec<String>,
    },
    /// Set a new password. Read from stdin, like login's.
    ResetPassword { user: String },
    /// Replace an account's direct grants. Repeat --grant; none means empty.
    SetGrants {
        user: String,
        #[arg(long = "grant")]
        grants: Vec<String>,
    },
    /// Replace the stored roles an account holds. Names only; none clears.
    SetRoles { user: String, roles: Vec<String> },
    /// Disable an account: refused at authentication, existing sessions end,
    /// record kept. The reversible form of delete.
    Disable { user: String },
    /// Re-enable a disabled account. Sessions ended by the disable stay gone.
    Enable { user: String },
    /// Delete an account outright.
    Delete { user: String },
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
    let mut cli = Cli::parse();
    let dot_password = apply_kimmy_file(&mut cli)?;

    // Bare `kimmy` shows byte-for-byte what `--help` shows — same screen,
    // stdout, success. The clap default for a missing subcommand is a two-
    // line usage error telling the user to run it again with a flag, which
    // is a wall the tool puts in front of its own front door.
    let command = match &cli.command {
        Some(command) => command,
        None => {
            let _ = Cli::command().print_help();
            return Ok(());
        }
    };

    // init writes the settings file and touches no node beyond a metadata
    // probe, so it runs before any client is built.
    if matches!(command, Command::Init) {
        return run_init().await;
    }

    // `login` and `token` are the two commands that run without a token,
    // because producing one is what they are for. They share everything after
    // the arguments: Token is Login with the answers already decided —
    // federated always, cache always.
    let token_request = match &command {
        Command::Login { user, issuer, client_id, resource, scope } => {
            Some((user.as_deref(), true, issuer, client_id, resource, scope))
        }
        Command::Token { issuer, client_id, resource, scope } => {
            Some((None, true, issuer, client_id, resource, scope))
        }
        _ => None,
    };
    if let Some((login_user, cache_token, issuer, client_id, resource, scope)) = token_request {
        // Which flow answers was decided by the arguments alone: a named
        // local account is the password login — `kimmy login ada` must never
        // grow a browser step — and anything else is the device flow.
        let flow = login_flow(login_user);
        if !matches!(flow, LoginFlow::Local) {
            // The token alone, with no decoration, so `$(kimmy ...)` is usable
            // directly. The access token lands in the cache — that is what
            // makes every other command work without `--token` afterwards —
            // and only the access token: a CLI that stores a bearer token has
            // to answer for its permissions, its lifetime and its cleanup. A
            // refresh token is never requested and never kept, cache or no
            // cache — it is the credential that outlives the session, and the
            // one worth stealing.
            //
            // Ask the node itself where to authenticate and what to ask the token
            // to be for, so the usual invocation needs nothing else set. A flag
            // still wins, and a node that publishes nothing leaves both as they
            // were.
            let (issuer, resource) =
                oidc::defaults_from_node(&cli.url, issuer.clone(), resource.clone()).await;
            let issuer = issuer.as_deref();
            let resource = resource.as_deref();
            let client_id = client_id.as_deref();
            let scope = scope.as_deref().unwrap_or(oidc::DEVICE_SCOPE);

            let cache_key = cache::Key::new(issuer, client_id, resource);
            if let Some(cached) = cache_key.as_ref().filter(|_| cache_token).and_then(cache::get) {
                println!("{cached}");
                return Ok(());
            }

            let (token, expires_in) =
                oidc::device_login(issuer, client_id, scope, resource).await?;
            if cache_token && let Some(key) = cache_key {
                // A cache that cannot be written is a slower login, not a
                // failed one, so this reports and carries on.
                if let Err(e) = cache::put(&key, &token, expires_in) {
                    eprintln!("warning: could not cache the token: {e:#}");
                }
            }
            println!("{token}");
            return Ok(());
        }

        // The local path: a named user, a password from stdin or the
        // environment.
        let user = login_user.expect("a local flow has a user by construction");
        let password = read_password(dot_password.as_deref())?;
        let client =
            Client::builder(&cli.url).credentials(user, password).connect().await.map_err(|e| {
                match local_login_hint(&e) {
                    Some(hint) => anyhow::Error::new(e).context(hint),
                    None => anyhow::Error::new(e),
                }
            })?;
        let token = client.token().await.context("the server did not return a token")?;
        println!("{token}");
        return Ok(());
    }

    let mut builder = Client::builder(&cli.url);
    if let Some(token) = &cli.token {
        builder = builder.token(token);
    } else if let Some(token) = cached_bearer(&cli.url).await {
        // No token was said anywhere, but a federated flow this identity ran
        // earlier left a still-fresh access token in the cache. Using it is
        // what makes "log in once, then just use the database" true; the
        // lookup keys on exactly what the flow keyed on.
        builder = builder.token(&token);
    }
    let client = builder.connect().await?;

    match &command {
        // Handled above, before a client was built.
        Command::Init => unreachable!("init returns early"),
        Command::Login { .. } | Command::Token { .. } => {
            unreachable!("token commands return early")
        }
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
        Command::HybridSearch {
            target,
            query,
            vector,
            k,
            filter,
            per_document,
            dense_weight,
            lexical_weight,
            min_overlap,
        } => {
            let (db, coll) = split_target(target)?;
            let body = search_body(
                query.as_deref(),
                vector.as_deref(),
                *k,
                filter.as_deref(),
                *per_document,
            )?;
            let body = with_fusion_controls(body, *dense_weight, *lexical_weight, *min_overlap);
            emit(&cli, &client.hybrid_search(db, coll, &body).await?);
        }
        Command::Topology => emit(&cli, &client.topology().await?),
        Command::Roles { command } => roles_command(&cli, &client, command).await?,
        Command::Users { command } => {
            users_command(&cli, &client, command, dot_password.as_deref()).await?
        }
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
        Command::Whoami => emit(
            &cli,
            &client.request(Method::Get, "/v1/auth/whoami", None, Safety::Idempotent).await?,
        ),
        Command::Databases => {
            let listing =
                client.request(Method::Get, "/v1/databases", None, Safety::Idempotent).await?;
            let empty = listing_is_empty("databases", &listing);
            emit(&cli, &listing);
            if empty {
                zero_grant_note(&client).await;
            }
        }
        Command::Collections { database } => {
            let path = format!("/v1/db/{database}/collections");
            let listing = client.request(Method::Get, &path, None, Safety::Idempotent).await?;
            let empty = listing_is_empty("collections", &listing);
            emit(&cli, &listing);
            if empty {
                zero_grant_note(&client).await;
            }
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
        Command::Update { target, filter, update, multi, array_filters } => {
            let (db, coll) = split_target(target)?;
            let filter = parse_json("filter", filter)?;
            let update = parse_json("update", update)?;
            let mut options = UpdateOptions::new().multi(*multi);
            if let Some(raw) = array_filters {
                let Value::Array(filters) = parse_json("array-filters", raw)? else {
                    bail!("--array-filters must be a JSON array of filter documents");
                };
                options = options.array_filters(filters);
            }
            emit(&cli, &client.update_with(db, coll, &filter, &update, &options).await?);
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
            // Streamed to its destination as it arrives, never held whole: a
            // backup is the size of the store. The node sends nothing until it
            // has walked the store, so the wait for it is not timed; the body
            // is, as a read-idle timeout (ADR-170).
            if out == "-" {
                client.download_to("/v1/admin/backup", &mut tokio::io::stdout()).await?;
            } else {
                let bytes = download_into_place(
                    std::path::Path::new(&out),
                    async |file: &mut tokio::fs::File| {
                        client.download_to("/v1/admin/backup", file).await
                    },
                )
                .await?;
                eprintln!("wrote {bytes} bytes to {out}");
            }
        }
    }
    Ok(())
}

/// Run `download` into a file beside `out`, and put it at `out` only once it is
/// complete and on disk.
///
/// `out` is often yesterday's good backup, so it is never opened: a download
/// that fails leaves it byte for byte as it was, and one killed by a signal
/// leaves at most the `.<name>.<pid>.partial` file beside it. The rename is
/// within one directory, so `out` is either the old file or the whole new one.
async fn download_into_place(
    out: &std::path::Path,
    download: impl AsyncFnOnce(&mut tokio::fs::File) -> kimmy_client::Result<u64>,
) -> Result<u64> {
    let dir = match out.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => std::path::Path::new("."),
    };
    let name = out.file_name().context("the backup's destination names no file")?;
    let partial = dir.join(format!(".{}.{}.partial", name.to_string_lossy(), std::process::id()));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial)
        .await
        .with_context(|| format!("creating {}", partial.display()))?;

    let written: Result<u64> = async {
        let bytes = download(&mut file).await?;
        file.sync_all().await.context("syncing the backup to disk")?;
        Ok(bytes)
    }
    .await;
    drop(file);

    let placed = match written {
        Ok(bytes) => tokio::fs::rename(&partial, out)
            .await
            .map(|()| bytes)
            .with_context(|| format!("moving the backup into place at {}", out.display())),
        Err(e) => Err(e),
    };
    if placed.is_err() {
        let _ = tokio::fs::remove_file(&partial).await;
    }
    placed.with_context(|| format!("downloading the backup to {}", out.display()))
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

/// Add the hybrid-only fusion controls to a search body.
///
/// Each is sent only when given, and a weight that was not given is left out
/// of `weights` rather than sent as 1: the server owns the defaults, and a
/// body that spells them out would pin them here the day they change there.
/// Range checks are the server's too — its message names the field.
fn with_fusion_controls(
    mut body: Value,
    dense_weight: Option<f64>,
    lexical_weight: Option<f64>,
    min_overlap: Option<usize>,
) -> Value {
    if dense_weight.is_some() || lexical_weight.is_some() {
        let mut weights = json!({});
        if let Some(dense) = dense_weight {
            weights["dense"] = json!(dense);
        }
        if let Some(lexical) = lexical_weight {
            weights["lexical"] = json!(lexical);
        }
        body["weights"] = weights;
    }
    if let Some(min_overlap) = min_overlap {
        body["min_overlap"] = json!(min_overlap);
    }
    body
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
fn read_password(from_dotfile: Option<&str>) -> Result<String> {
    if let Ok(password) = std::env::var("KIMMY_PASSWORD") {
        return Ok(password);
    }
    if let Some(password) = from_dotfile.filter(|p| !p.is_empty()) {
        return Ok(password.to_string());
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

/// What a 403 or 404 from the login route means, when it means something.
///
/// `POST /v1/auth/login` answers 401 for a bad password and never 403 or 404
/// of its own accord; both come from the node's `auth.local.login` mode
/// (ADR-100) — `loopback_only` refuses a peer off the host with 403, and
/// `disabled` answers 404 to everyone. Recovered from the typed error rather
/// than from the message, for the reason `unauthorized_hint` is: a message is
/// prose, and prose gets reworded. Anything else — a transport failure, a 401 —
/// is left to speak for itself.
fn local_login_hint(error: &kimmy_client::Error) -> Option<&'static str> {
    match error {
        kimmy_client::Error::Api { status: 403 | 404, .. } => Some(
            "this node does not accept a local login from here (auth.local.login is \
             loopback_only or disabled on it); log in from the node's own host, or use the \
             federated `kimmy login`. A token already issued keeps working",
        ),
        _ => None,
    }
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
            "run `kimmy login` (issuer {issuer}); its token is cached and reused \
             automatically — or set --token / KIMMY_TOKEN. A local account still \
             works with `kimmy login <user>`"
        ),
        None => "run `kimmy login`; its token is cached and reused automatically — \
                 or set --token / KIMMY_TOKEN"
            .to_string(),
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

    /// Scopes the device flow asks for when nothing was specified.
    ///
    /// There is a person behind this flow, so `openid` is meaningful: it is
    /// what makes the exchange an OpenID Connect one rather than bare OAuth 2.
    pub(super) const DEVICE_SCOPE: &str = "openid profile";

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
             node is configured with — or log in to a local account with `kimmy login <user>`",
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

    /// Pull `(resource, issuer)` out of a protected-resource document.
    ///
    /// `authorization_servers` is a list because a resource may trust several;
    /// this node trusts exactly one (ADR-064), so the first entry is the only
    /// entry. Both halves are required: a document naming one without the
    /// other is not usable for setup and is reported as no metadata at all.
    pub(super) fn extract_discovery(document: &Value) -> Option<(String, String)> {
        let resource = document.get("resource").and_then(Value::as_str)?;
        let issuer = document
            .get("authorization_servers")
            .and_then(Value::as_array)
            .and_then(|servers| servers.first())
            .and_then(Value::as_str)?;
        Some((resource.to_string(), issuer.to_string()))
    }

    /// Ask a node what it is and who it trusts — the two answers `kimmy init`
    /// used to make an operator hand-type.
    ///
    /// `None` when the node publishes nothing usable: too old to have the
    /// endpoint, unreachable, or an opaque-audience deployment. The caller
    /// decides what that means; for init it means falling back to prompts.
    pub(super) async fn discover_from_node(url: &str) -> Option<(String, String)> {
        extract_discovery(&protected_resource_metadata(url).await?)
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
        let document: Value = response
            .json()
            .await
            .with_context(|| format!("parsing the discovery document at {url}"))?;
        check_issuer(&document, issuer)?;
        Ok(document)
    }

    /// Refuse a discovery document that does not name the issuer it was
    /// fetched for.
    ///
    /// OpenID Connect Discovery §4.3 and RFC 8414 §3.3 make this a MUST, and
    /// on this side it protects something specific: the endpoints in this
    /// document are where the CLI sends a **client secret** and where it polls
    /// for an **access token**. A document that is not bound to the issuer the
    /// user named is a document that can nominate somewhere else to send both.
    /// The node performs the same check independently before trusting a
    /// `jwks_uri`; neither substitutes for the other, because they read the
    /// document for different reasons.
    pub(super) fn check_issuer(document: &Value, issuer: &str) -> Result<()> {
        let named = document
            .get("issuer")
            .and_then(Value::as_str)
            .context("the provider's discovery document names no issuer")?;
        if named != issuer {
            bail!(
                "the discovery document says its issuer is {named:?}, not {issuer:?}. A \
                 provider's metadata must name the issuer it was fetched for (OpenID Connect \
                 Discovery §4.3, RFC 8414 §3.3). Check --issuer, or KIMMY_OIDC_ISSUER, against \
                 what your provider publishes."
            );
        }
        Ok(())
    }

    /// An endpoint named by the discovery document, required to be https.
    ///
    /// Every caller of this sends a device code
    /// to the URL it returns, and receives an access token back. RFC 8414 §2
    /// requires these to be https for exactly that reason, and the node
    /// refuses a non-https issuer outright (`OidcConfig::validate`), so a
    /// plaintext endpoint here could only ever come from a document that had
    /// been tampered with or a provider that is misconfigured.
    pub(super) fn endpoint(document: &Value, name: &str) -> Result<String> {
        let url = document
            .get(name)
            .and_then(Value::as_str)
            .with_context(|| format!("the provider's discovery document names no {name}"))?;
        if !is_secure_url(url) {
            bail!(
                "the provider's discovery document gives a {name} of {url:?}, which is not \
                 https. Credentials and access tokens travel over it, so it is not usable. \
                 Only a loopback address is exempt."
            );
        }
        Ok(url.to_string())
    }

    /// Whether a URL is one credentials may safely travel over.
    ///
    /// https, or plain http to **loopback** — the exemption RFC 8252 §7.3
    /// makes for native applications, on the ground that there is no network
    /// path to be on between a process and itself. It is what lets someone
    /// develop against a provider running on their own machine, and a check
    /// that made that impossible would be a check somebody eventually deletes.
    ///
    /// The host is parsed, not prefix-matched: `http://127.0.0.1.example.com`
    /// merely begins with a loopback address, and in
    /// `http://127.0.0.1@example.com` the loopback part is userinfo and the
    /// host is what follows the `@`.
    ///
    /// The node applies the same rule to the `jwks_uri` it fetches. Kept
    /// separate rather than shared for the reason given on
    /// `PROTECTED_RESOURCE_METADATA_PATH`: this binary links no kimmy crate
    /// that could carry it.
    pub(super) fn is_secure_url(url: &str) -> bool {
        if url.starts_with("https://") {
            return true;
        }
        let Some(rest) = url.strip_prefix("http://") else { return false };
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        let authority = authority.rsplit('@').next().unwrap_or("");
        let host = match authority.strip_prefix('[') {
            Some(bracketed) => bracketed.split(']').next().unwrap_or(""),
            None => authority.split(':').next().unwrap_or(""),
        };
        match host.parse::<std::net::IpAddr>() {
            Ok(ip) => ip.is_loopback(),
            Err(_) => host.eq_ignore_ascii_case("localhost"),
        }
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

    /// How many seconds the provider says the token is good for.
    ///
    /// `expires_in` is only RECOMMENDED by RFC 6749 §5.1, so its absence is
    /// ordinary rather than an error. A token with no stated lifetime is not
    /// cached: the cache exists to reuse a token that is known to still be
    /// valid, and without a lifetime there is nothing to know.
    pub(super) fn expires_in(body: &Value) -> Option<u64> {
        body.get("expires_in").and_then(Value::as_u64)
    }

    /// RFC 8628 device authorization grant.
    ///
    /// Everything a person reads goes to **stderr** and only the token goes to
    /// stdout, so `$(kimmy login)` captures the token and the
    /// instructions still reach the terminal.
    pub async fn device_login(
        issuer: Option<&str>,
        client_id: Option<&str>,
        scope: &str,
        resource: Option<&str>,
    ) -> Result<(String, Option<u64>)> {
        let issuer = issuer_or_bail(issuer)?;
        let client_id = client_id_or_bail(client_id)?;
        let http = http()?;
        let document = discover(&http, issuer).await?;
        let device_endpoint = endpoint(&document, "device_authorization_endpoint").context(
            "this provider does not advertise the device authorization endpoint; \
             `kimmy login <user>` is the local-account path, and KIMMY_TOKEN takes a \
             token minted elsewhere",
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
        super::offer_browser(verification).await;
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
                "" => return Ok((access_token(&body)?, expires_in(&body))),
                _ => bail!("the provider refused the token request: {}", describe(&body)),
            }
        }
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

/// The cache for access tokens that federated flows mint.
///
/// # On by default, and why that changed
///
/// `gh`, `aws`, `az` and `kubectl` all cache credentials at `0600`, so doing
/// so is unremarkable. It began opt-in (ADR-075) and stayed there only until
/// the cache had a consumer: with data commands reading it, "log in once, then
/// use the database" is the whole product, and an off-by-default cache made
/// every command after login fail with a misleading 401 — observed live,
/// not hypothesised. What is stored did not grow to buy this: still the
/// access token alone, still `0600`, never a refresh token. A token in
/// terminal scrollback — which printing one already implies — was always the
/// less guarded copy.
///
/// # A refresh token is never stored, and never even requested
///
/// The access token is short-lived and audience-restricted; a refresh token is
/// the credential that outlives the session and can mint more. Caching one
/// would be a different feature with a different risk, and neither flow asks
/// for one.
mod cache {
    use std::io::Write;
    use std::path::PathBuf;

    use anyhow::{Context, Result};
    use serde_json::{Value, json};

    /// Treat a token as spent this long before it actually expires.
    ///
    /// A token that passes the freshness test has to survive the command that
    /// is about to use it, not merely exist at the moment it is read.
    const EXPIRY_MARGIN_SECS: u64 = 60;

    /// What a cached token is filed under.
    ///
    /// All three parts, because all three change what the token *is*: a
    /// different issuer is a different trust root, a different client is a
    /// different identity, and a different resource is a different audience —
    /// and a token for the wrong audience is refused by the node, which would
    /// look like a broken cache rather than a wrong key.
    ///
    /// Constructing one requires an issuer and a client id. Without them there
    /// was no OAuth flow to cache the result of.
    pub struct Key(String);

    impl Key {
        pub fn new(
            issuer: Option<&str>,
            client_id: Option<&str>,
            resource: Option<&str>,
        ) -> Option<Self> {
            // A tab cannot appear in a URL or a client id, so it separates
            // without any escaping and without two different triples ever
            // colliding on one key.
            Some(Self(format!("{}\t{}\t{}", issuer?, client_id?, resource.unwrap_or(""))))
        }
    }

    /// `$XDG_CACHE_HOME/kimmy/tokens.json`, or `~/.cache/kimmy/tokens.json`.
    ///
    /// One file rather than one per key: it is what makes `0600` a single
    /// thing to get right, and it keeps the issuer out of a filename.
    fn path() -> Result<PathBuf> {
        let base = match std::env::var_os("XDG_CACHE_HOME").filter(|v| !v.is_empty()) {
            Some(dir) => PathBuf::from(dir),
            None => PathBuf::from(
                std::env::var_os("HOME").context("neither XDG_CACHE_HOME nor HOME is set")?,
            )
            .join(".cache"),
        };
        Ok(base.join("kimmy").join("tokens.json"))
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// A still-valid token for this key, if one was stored.
    ///
    /// **Every failure is a cache miss, not an error.** A corrupt file, a
    /// missing one, or one written by a future version all mean the same thing
    /// to the caller: authenticate again. Reporting them would turn a cache
    /// into something that can break a login.
    pub fn get(key: &Key) -> Option<String> {
        get_from(&path().ok()?, key)
    }

    /// [`get`], against a named file.
    ///
    /// Split out so the tests never touch `XDG_CACHE_HOME`. Environment
    /// variables are process-global and cargo runs tests on parallel threads,
    /// so a test that set one would be racing every other test that reads it —
    /// the same shape as the `include_names` race that took a round to
    /// diagnose.
    pub fn get_from(path: &std::path::Path, key: &Key) -> Option<String> {
        let document: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
        let entry = document.get("entries")?.get(&key.0)?;
        let expires_at = entry.get("expires_at")?.as_u64()?;
        if expires_at.saturating_sub(EXPIRY_MARGIN_SECS) <= now() {
            return None;
        }
        entry.get("access_token")?.as_str().map(str::to_string)
    }

    /// Store a token against this key.
    ///
    /// A token whose lifetime the provider did not state is **not** stored:
    /// the cache exists to reuse a token known to still be valid, and without
    /// `expires_in` there is nothing to know. Guessing a lifetime would mean
    /// serving a dead token, which fails as a 401 somewhere unrelated.
    pub fn put(key: &Key, token: &str, expires_in: Option<u64>) -> Result<()> {
        put_into(&path()?, key, token, expires_in)
    }

    /// [`put`], into a named file. Split out for the reason [`get_from`] is.
    pub fn put_into(
        path: &std::path::Path,
        key: &Key,
        token: &str,
        expires_in: Option<u64>,
    ) -> Result<()> {
        let Some(expires_in) = expires_in else { return Ok(()) };

        let dir = path.parent().context("the cache path has no parent")?;
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        restrict(dir, 0o700)?;

        // Read-modify-write, so caching a token for one node does not discard
        // the token cached for another.
        let mut document = std::fs::read(path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<Value>(&raw).ok())
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
        document["version"] = json!(1);
        // Drop entries that have already expired while we are here, so an
        // abandoned issuer does not leave a token on disk indefinitely.
        let entries = document["entries"].as_object().cloned().unwrap_or_default();
        let mut kept = serde_json::Map::new();
        for (name, entry) in entries {
            if entry.get("expires_at").and_then(Value::as_u64).is_some_and(|at| at > now()) {
                kept.insert(name, entry);
            }
        }
        kept.insert(
            key.0.clone(),
            json!({ "access_token": token, "expires_at": now().saturating_add(expires_in) }),
        );
        document["entries"] = Value::Object(kept);

        // Written to a temporary file in the same directory and renamed, so a
        // reader never sees a half-written cache and an interrupted write
        // cannot destroy the tokens already stored.
        let temporary = path.with_extension("tmp");
        let mut file = std::fs::File::create(&temporary)
            .with_context(|| format!("creating {}", temporary.display()))?;
        restrict(&temporary, 0o600)?;
        file.write_all(serde_json::to_string(&document)?.as_bytes())
            .with_context(|| format!("writing {}", temporary.display()))?;
        file.sync_all().ok();
        std::fs::rename(&temporary, path)
            .with_context(|| format!("replacing {}", path.display()))?;
        Ok(())
    }

    /// Narrow a path's permissions to its owner.
    ///
    /// Applied after creation rather than at it, because `create_dir_all` and
    /// `File::create` both go through the process umask and a permissive one
    /// would otherwise leave a token group- or world-readable.
    #[cfg(unix)]
    fn restrict(path: &std::path::Path, mode: u32) -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .with_context(|| format!("restricting permissions on {}", path.display()))
    }

    /// No equivalent on other platforms, and nothing is shipped for one — the
    /// release targets are macOS and Linux. A stub rather than a silent
    /// success, so this is found rather than assumed if that changes.
    #[cfg(not(unix))]
    fn restrict(_path: &std::path::Path, _mode: u32) -> Result<()> {
        anyhow::bail!(
            "the token cache is only implemented for platforms with POSIX file permissions"
        )
    }
}

/// Which credential flow a token-producing command should run.
#[derive(Debug, PartialEq, Eq)]
enum LoginFlow {
    /// Password login for a local account on the node's own user store.
    Local,
    /// RFC 8628 device flow through the node's identity provider.
    Device,
}

/// Decide a token-producing command's flow from its arguments alone.
///
/// A named user is the local flow — `kimmy login ada` must never grow a
/// browser step, however the defaults evolve. Anything else is the device
/// flow: the default because it needs nothing but a browser and works
/// everywhere one exists, including SSH sessions and containers. There is no
/// machine flow, by design (ADR-089): a script sets `KIMMY_TOKEN` to a token
/// minted elsewhere.
fn login_flow(user: Option<&str>) -> LoginFlow {
    match user {
        Some(_) => LoginFlow::Local,
        None => LoginFlow::Device,
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

/// Whether a listing response carries no entries.
///
/// The two listing shapes are fixed by the server; anything else (a missing
/// key, an unexpected shape) is treated as "not empty" so a surprise payload
/// can never manufacture a hint that does not apply.
fn listing_is_empty(key: &str, listing: &Value) -> bool {
    listing[key].as_array().is_some_and(|entries| entries.is_empty())
}

/// Whether a `/v1/auth/whoami` answer describes an identity with no grants.
///
/// Gated on the field being present *and* empty. A principal holding grants
/// that sees an empty result may simply be looking at an empty namespace —
/// nagging them would train people to ignore the note, which is worse than
/// never printing it.
fn is_zero_grant(whoami: &Value) -> bool {
    whoami["grants"].as_array().is_some_and(|grants| grants.is_empty())
}

/// After an empty listing, tell a zero-grant identity why theirs is empty.
///
/// `list_databases` and `list_collections` filter through authorization
/// server-side, so "you may see nothing" and "there is nothing" arrive
/// byte-identical on stdout — and stdout must stay byte-identical, because it
/// is a machine-readable contract. The note goes to stderr only when the
/// caller's grants are actually empty, turning "this database does not exist"
/// into "your token carries no grants" in one line. A whoami that fails for
/// any reason is swallowed: this is a hint, never a second error.
async fn zero_grant_note(client: &Client) {
    let Ok(who) = client.request(Method::Get, "/v1/auth/whoami", None, Safety::Idempotent).await
    else {
        return;
    };
    if is_zero_grant(&who) {
        eprintln!(
            "note: this identity carries no grants, so listings only show what you are allowed \
             to read.\n       run `kimmy whoami` to see how the node sees you."
        );
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

/// Parse a grant shorthand: `db:actions` or `db:collection:actions`.
///
/// Actions are comma-separated; the collection defaults to `*`. Colons split,
/// actions last — so `*:*:read` is every database, every collection, read;
/// `sales:orders*:read,search` is one database's order collections. Action
/// spelling is the server's to judge: anything unrecognized is refused there
/// with the list of valid ones rather than guessed at here.
fn parse_grant_spec(spec: &str) -> Result<Value> {
    let parts: Vec<&str> = spec.split(':').collect();
    let (db, collection, actions) = match parts.as_slice() {
        [db, actions] => (*db, "*", *actions),
        [db, collection, actions] => (*db, *collection, *actions),
        _ => anyhow::bail!(
            "grant {spec:?} must be db:actions or db:collection:actions, \
             e.g. '*:*:read' or 'sales:orders*:read,search'"
        ),
    };
    let actions: Vec<&str> = actions.split(',').map(str::trim).filter(|s| !s.is_empty()).collect();
    if actions.is_empty() {
        anyhow::bail!("grant {spec:?} names no actions");
    }
    if db.is_empty() {
        anyhow::bail!("grant {spec:?} has an empty database part");
    }
    Ok(json!({ "db": db, "collection": collection, "actions": actions }))
}

/// Parse every `--grant` occurrence, refusing duplicates of the same
/// db+collection pair — the server replaces wholesale, so two specs naming one
/// grant would silently keep only the later.
fn parse_grant_specs(specs: &[String]) -> Result<Vec<Value>> {
    let mut grants: Vec<Value> = Vec::new();
    for spec in specs {
        let grant = parse_grant_spec(spec)?;
        if grants.iter().any(|g| g["db"] == grant["db"] && g["collection"] == grant["collection"]) {
            anyhow::bail!("grant {spec:?} repeats a db+collection already given");
        }
        grants.push(grant);
    }
    Ok(grants)
}

/// Apply `add` (union) or `revoke` (subtract) to one stored role's grants.
///
/// The API replaces a role's grants wholesale, so an incremental edit is
/// fetch-modify-post here. Two operators editing one role concurrently race in
/// the usual last-write-wins way; that is documented rather than hidden behind
/// invented server semantics.
async fn merge_role_grants(
    cli: &Cli,
    client: &Client,
    name: &str,
    spec: &str,
    add: bool,
) -> Result<()> {
    let path = format!("/v1/roles/{name}");
    let role = client.request(Method::Get, &path, None, Safety::Idempotent).await?;
    let mut grants: Vec<Value> = role["grants"].as_array().cloned().ok_or_else(|| {
        anyhow::anyhow!(
            "role {name:?} answered without a grants array; is the node older than 0.4.0?"
        )
    })?;

    let parsed = parse_grant_spec(spec)?;
    let entry = grants
        .iter_mut()
        .find(|g| g["db"] == parsed["db"] && g["collection"] == parsed["collection"]);
    match (entry, add) {
        (Some(entry), true) => {
            let held = entry["actions"].as_array().cloned().unwrap_or_default();
            let fresh: Vec<&Value> = parsed["actions"]
                .as_array()
                .expect("parse_grant_spec builds an actions array")
                .iter()
                .filter(|a| !held.contains(a))
                .collect();
            entry["actions"] =
                json!(held.iter().cloned().chain(fresh.into_iter().cloned()).collect::<Vec<_>>());
        }
        (Some(entry), false) => {
            let kept: Vec<&Value> = entry["actions"]
                .as_array()
                .expect("parse_grant_spec builds an actions array")
                .iter()
                .filter(|a| !parsed["actions"].as_array().unwrap().contains(a))
                .collect();
            if kept.is_empty() {
                let db = parsed["db"].clone();
                let collection = parsed["collection"].clone();
                grants.retain(|g| !(g["db"] == db && g["collection"] == collection));
            } else {
                entry["actions"] = json!(kept);
            }
        }
        // Adding actions to a grant the role does not have: push it whole.
        // Revoking from one it does not have changes nothing, so nothing is
        // written — the caller sees today's definition either way.
        (None, true) => grants.push(parsed),
        (None, false) => {}
    }

    let updated = client
        .request(
            Method::Post,
            &format!("/v1/roles/{name}/grants"),
            Some(json!({ "grants": grants })),
            Safety::Idempotent,
        )
        .await?;
    emit(cli, &updated);
    Ok(())
}

async fn roles_command(cli: &Cli, client: &Client, command: &RolesSub) -> Result<()> {
    match command {
        RolesSub::List => {
            emit(cli, &client.request(Method::Get, "/v1/roles", None, Safety::Idempotent).await?)
        }
        RolesSub::Show { name } => emit(
            cli,
            &client
                .request(Method::Get, &format!("/v1/roles/{name}"), None, Safety::Idempotent)
                .await?,
        ),
        RolesSub::Create { name, grants } => {
            let created = client
                .request(
                    Method::Post,
                    "/v1/roles",
                    Some(json!({ "name": name, "grants": parse_grant_specs(grants)? })),
                    Safety::Idempotent,
                )
                .await;
            emit(cli, &collection_created(created, name)?);
        }
        RolesSub::Grant { name, grant } => {
            merge_role_grants(cli, client, name, grant, true).await?;
        }
        RolesSub::Revoke { name, grant } => {
            merge_role_grants(cli, client, name, grant, false).await?;
        }
        RolesSub::Delete { name } => {
            let deleted = client
                .request(Method::Delete, &format!("/v1/roles/{name}"), None, Safety::Idempotent)
                .await?;
            emit(cli, &deleted);
        }
    }
    Ok(())
}

/// Read a password for a user-management command — stdin or KIMMY_PASSWORD,
/// exactly as `login` reads its own, for the same reasons.
fn read_password_for(_user: &str, dotfile: Option<&str>) -> Result<String> {
    read_password(dotfile)
}

async fn users_command(
    cli: &Cli,
    client: &Client,
    command: &UsersSub,
    password_override: Option<&str>,
) -> Result<()> {
    match command {
        UsersSub::List => {
            // The listing endpoint answers names; each record carries the
            // state worth seeing. Accounts are few; round trips are cheap.
            let listed = client.request(Method::Get, "/v1/users", None, Safety::Idempotent).await?;
            let names: Vec<String> = serde_json::from_value(
                listed.get("users").cloned().context("listing answered without a users array")?,
            )?;
            let mut users = Vec::new();
            for name in &names {
                let record = client
                    .request(Method::Get, &format!("/v1/users/{name}"), None, Safety::Idempotent)
                    .await?;
                users.push(json!({
                    "user": record.get("user").cloned().unwrap_or(json!(name)),
                    "disabled": record.get("disabled").cloned().unwrap_or(json!(false)),
                }));
            }
            emit(cli, &json!({ "users": users }));
        }
        UsersSub::Show { user } => emit(
            cli,
            &client
                .request(Method::Get, &format!("/v1/users/{user}"), None, Safety::Idempotent)
                .await?,
        ),
        UsersSub::Create { user, grants, roles } => {
            let password = read_password_for(user, password_override)?;
            let mut payload = json!({ "user": user, "password": password });
            let parsed = parse_grant_specs(grants)?;
            if !parsed.is_empty() {
                payload["grants"] = json!(parsed);
            }
            let created = client
                .request(Method::Post, "/v1/users", Some(payload), Safety::Idempotent)
                .await?;
            if !roles.is_empty() {
                client
                    .request(
                        Method::Post,
                        &format!("/v1/users/{user}/roles"),
                        Some(json!({ "roles": roles })),
                        Safety::Idempotent,
                    )
                    .await?;
            }
            emit(cli, &created);
        }
        UsersSub::ResetPassword { user } => {
            let password = read_password_for(user, password_override)?;
            let updated = client
                .request(
                    Method::Post,
                    &format!("/v1/users/{user}/password"),
                    Some(json!({ "password": password })),
                    Safety::Idempotent,
                )
                .await?;
            emit(cli, &updated);
        }
        UsersSub::SetGrants { user, grants } => {
            let updated = client
                .request(
                    Method::Post,
                    &format!("/v1/users/{user}/grants"),
                    Some(json!({ "grants": parse_grant_specs(grants)? })),
                    Safety::Idempotent,
                )
                .await?;
            emit(cli, &updated);
        }
        UsersSub::SetRoles { user, roles } => {
            let updated = client
                .request(
                    Method::Post,
                    &format!("/v1/users/{user}/roles"),
                    Some(json!({ "roles": roles })),
                    Safety::Idempotent,
                )
                .await?;
            emit(cli, &updated);
        }
        UsersSub::Disable { user } => {
            let updated = client
                .request(
                    Method::Post,
                    &format!("/v1/users/{user}/disabled"),
                    Some(json!({ "disabled": true })),
                    Safety::Idempotent,
                )
                .await?;
            emit(cli, &updated);
        }
        UsersSub::Enable { user } => {
            let updated = client
                .request(
                    Method::Post,
                    &format!("/v1/users/{user}/disabled"),
                    Some(json!({ "disabled": false })),
                    Safety::Idempotent,
                )
                .await?;
            emit(cli, &updated);
        }
        UsersSub::Delete { user } => {
            let deleted = client
                .request(Method::Delete, &format!("/v1/users/{user}"), None, Safety::Idempotent)
                .await?;
            emit(cli, &deleted);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The settings file: ~/.config/kimmydb/.kimmy
//
// One dotenv-style file for the settings a person keeps setting: url, token,
// password, issuer, client_id, resource, scope, cache_token.
// Precedence per setting is explicit flag > environment variable > this file
// > built-in default — the file is what fills the gaps, never what overrides
// something the caller or the environment already said.
// ---------------------------------------------------------------------------

/// Everything `~/.config/kimmydb/.kimmy` can supply.
#[derive(Default, Debug)]
struct DotfileSettings {
    url: Option<String>,
    token: Option<String>,
    password: Option<String>,
    issuer: Option<String>,
    client_id: Option<String>,
    resource: Option<String>,
    scope: Option<String>,
    cache_token: Option<bool>,
}

const DOTFILE_KEYS: &str = "url, token, password, issuer, client_id, resource, scope, cache_token";

fn parse_kimmy_file(text: &str) -> Result<DotfileSettings> {
    let mut out = DotfileSettings::default();
    for (idx, raw) in text.lines().enumerate() {
        let line = raw.trim();
        let n = idx + 1;
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("line {n}: expected `key = value`, found {raw:?}"))?;
        let key = key.trim();
        let mut value = value.trim();
        // One level of matching quotes is stripped, so values may contain
        // spaces and `#` without ceremony.
        if value.len() >= 2
            && ((value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\'')))
        {
            value = &value[1..value.len() - 1];
        }
        match key {
            "url" => out.url = Some(value.into()),
            "token" => out.token = Some(value.into()),
            "password" => out.password = Some(value.into()),
            "issuer" => out.issuer = Some(value.into()),
            "client_id" => out.client_id = Some(value.into()),
            // Retired in 0.13.0 with the client_credentials grant (ADR-089).
            // A key that used to be valid is not a typo, and refusing every
            // command over it would be the worse failure — so it is named,
            // warned about, and skipped. Every other unknown key still fails.
            // The file is read more than once per invocation, so the warning
            // is gated to print once rather than once per read.
            "client_secret" => {
                static WARNED: std::sync::Once = std::sync::Once::new();
                WARNED.call_once(|| {
                    eprintln!(
                        "warning: line {n}: client_secret is no longer used by kimmy — the \
                         client_credentials grant was removed in 0.13.0; remove the line"
                    )
                });
            }
            "resource" => out.resource = Some(value.into()),
            "scope" => out.scope = Some(value.into()),
            "cache_token" => {
                out.cache_token = Some(match value {
                    "true" | "1" | "yes" | "on" => true,
                    "false" | "0" | "no" | "off" => false,
                    other => anyhow::bail!(
                        "line {n}: cache_token {other:?} is not one of \
                         true/false/1/0/yes/no/on/off"
                    ),
                });
            }
            other => anyhow::bail!("line {n}: unknown key {other:?}. Known keys: {DOTFILE_KEYS}"),
        }
    }
    Ok(out)
}

/// Where the settings file lives. Missing is normal; unreadable is an error.
fn kimmy_file_path() -> Result<std::path::PathBuf> {
    let base = match std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        Some(dir) => std::path::PathBuf::from(dir),
        None => std::path::PathBuf::from(
            std::env::var_os("HOME").context("neither XDG_CONFIG_HOME nor HOME is set")?,
        )
        .join(".config"),
    };
    Ok(base.join("kimmydb").join(".kimmy"))
}

fn load_kimmy_file() -> Result<Option<(std::path::PathBuf, DotfileSettings)>> {
    let path = kimmy_file_path()?;
    match std::fs::read_to_string(&path) {
        Ok(text) => parse_kimmy_file(&text)
            .map(|settings| Some((path.clone(), settings)))
            .map_err(|e| anyhow::anyhow!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow::anyhow!("reading {}: {e}", path.display())),
    }
}

/// Fill unset settings from the dotfile, returning the two values no flag
/// carries (password, client secret) for their call sites to pick up.
///
/// Precedence per setting: **explicit flag > environment variable > this
/// file**. A flag is detected by its clap source rather than by comparing
/// values, so `--url localhost:7878` still beats a dotfile that names another
/// node; an environment variable wins because clap has already folded it into
/// the parsed field before this runs.
fn apply_kimmy_file(cli: &mut Cli) -> Result<Option<String>> {
    use clap::parser::ValueSource;

    let matches = Cli::command().get_matches();
    let from_cli = |id: &str| matches.value_source(id) == Some(ValueSource::CommandLine);
    let env_absent = |name: &str| std::env::var_os(name).is_none();
    let Some((_, dot)) = load_kimmy_file()? else {
        return Ok(None);
    };

    // Globals first.
    if !from_cli("url")
        && env_absent("KIMMY_URL")
        && let Some(v) = dot.url
    {
        cli.url = v;
    }
    if !from_cli("token")
        && env_absent("KIMMY_TOKEN")
        && let Some(v) = dot.token
    {
        cli.token = Some(v);
    }

    // Provider settings ride on the login/token subcommands. No other
    // subcommand carries those arguments, and asking clap for a value source
    // by name panics on a command that lacks the id — so anything else skips
    // this block entirely rather than falling through it.
    type ProviderFields<'a> = (
        &'a mut Option<String>,
        &'a mut Option<String>,
        &'a mut Option<String>,
        &'a mut Option<String>,
    );
    if let Some(command) = &mut cli.command {
        let (issuer, client_id, resource, scope): ProviderFields<'_> = match command {
            Command::Login { issuer, client_id, resource, scope, .. } => {
                (issuer, client_id, resource, scope)
            }
            Command::Token { issuer, client_id, resource, scope, .. } => {
                (issuer, client_id, resource, scope)
            }
            _ => {
                return Ok(dot.password.filter(|p| !p.is_empty()));
            }
        };
        if !from_cli("issuer")
            && env_absent("KIMMY_OIDC_ISSUER")
            && let Some(v) = dot.issuer
        {
            *issuer = Some(v);
        }
        if !from_cli("client_id")
            && env_absent("KIMMY_OIDC_CLIENT_ID")
            && let Some(v) = dot.client_id
        {
            *client_id = Some(v);
        }
        if !from_cli("resource")
            && env_absent("KIMMY_OIDC_RESOURCE")
            && let Some(v) = dot.resource
        {
            *resource = Some(v);
        }
        if !from_cli("scope")
            && env_absent("KIMMY_OIDC_SCOPE")
            && let Some(v) = dot.scope
        {
            *scope = Some(v);
        }
    }

    // The one setting no flag carries, consumed by its call site.
    Ok(dot.password.filter(|p| !p.is_empty()))
}

/// The client id a cache key is built from: environment first, then the
/// settings file.
///
/// The same precedence `apply_kimmy_file` gives the login/token flags, so a
/// fallback lookup lands on the exact key a flow wrote under — a lookup that
/// resolved differently would miss silently and look like "no token", which is
/// the one outcome worse than asking for one. The environment value arrives as
/// a parameter rather than being read here so the precedence stays testable
/// without process-global mutation.
fn cached_key_client_id(
    env_client_id: Option<String>,
    dotfile_client_id: Option<String>,
) -> Option<String> {
    env_client_id.filter(|v| !v.is_empty()).or(dotfile_client_id)
}

/// The bearer an earlier federated flow left behind, when there is one.
///
/// Data commands never run flows themselves — a browser round-trip out of
/// `kimmy databases` would be a surprise — but `kimmy login` and `kimmy token`
/// cache what they mint, and this is what makes that cache count: same node,
/// same discovery, same key. Explicit tokens (`--token`, `KIMMY_TOKEN`, the
/// settings file) win by arriving through `cli.token` instead of here.
async fn cached_bearer(url: &str) -> Option<String> {
    let (issuer, resource) = oidc::defaults_from_node(url, None, None).await;
    let env_client_id = std::env::var("KIMMY_OIDC_CLIENT_ID").ok();
    let dotfile_client_id =
        load_kimmy_file().ok().flatten().and_then(|(_, settings)| settings.client_id);
    let client_id = cached_key_client_id(env_client_id, dotfile_client_id);
    let key = cache::Key::new(issuer.as_deref(), client_id.as_deref(), resource.as_deref())?;
    cache::get(&key)
}

// ---------------------------------------------------------------------------
// kimmy init: prompt for the settings file
// ---------------------------------------------------------------------------

/// Render the settings file body from collected `key = value` pairs.
fn render_kimmy_file(pairs: &[(&str, String)]) -> String {
    let mut out = String::from(
        "# Written by `kimmy init`. Flags and environment variables win over\n\
         # anything in this file. Keys: url, token, password, issuer,\n\
         # client_id, resource, scope, cache_token.\n",
    );
    for (key, value) in pairs {
        out.push_str(&format!("{key} = {value}\n"));
    }
    out
}

/// One interactive prompt: shows the current value as the default; Enter keeps
/// it (or skips the setting when there is nothing to keep).
fn prompt_setting(prompt: &str, current: Option<&str>) -> Result<Option<String>> {
    use std::io::Write;
    match current {
        Some(current) => print!("{prompt} [{current}]: "),
        None => print!("{prompt}: "),
    }
    std::io::stdout().flush()?;
    let mut buf = String::new();
    std::io::stdin().read_line(&mut buf).context("reading input")?;
    let answer = buf.trim();
    Ok(match (answer.is_empty(), current) {
        (true, kept) => kept.map(str::to_string),
        (false, _) => Some(answer.to_string()),
    })
}

/// Wrap text in an SGR sequence when decoration applies: stdout is a terminal
/// and `NO_COLOR` is unset. Decoration only — data lines stay plain.
fn ansi(code: &str, text: &str) -> String {
    use std::io::IsTerminal;
    let enabled = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    if enabled { format!("\x1b[{code}m{text}\x1b[0m") } else { text.to_string() }
}

/// Accept http(s) URLs only, trimmed of a trailing slash — the shape every
/// other command assumes when joining paths onto the base.
fn normalize_node_url(input: &str) -> Option<String> {
    let parsed = input.trim().parse::<reqwest::Url>().ok()?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return None;
    }
    parsed.host_str()?;
    let mut s = parsed.as_str().to_string();
    while s.ends_with('/') {
        s.pop();
    }
    Some(s)
}

/// Offer to open the verification URL in the default browser, the way `gh`
/// does: the URL is already on screen, Enter is one keystroke, and a failure
/// to spawn changes nothing at all — the URL remains the instruction.
///
/// Only when stdin and stdout are both terminals. A piped or scripted run
/// keeps today's behavior exactly, which is also what keeps `$(kimmy login)`
/// from hanging on a human who is not there.
async fn offer_browser(url: &str) {
    use std::io::{IsTerminal, Write};
    use tokio::io::AsyncBufReadExt;

    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return;
    }

    eprint!("{}", ansi("2", "Press Enter to open it in your browser"));
    std::io::stdout().flush().ok();
    let mut line = String::new();
    // The prompt has been shown and the URL printed above it; a read that
    // fails or ends just means no browser, not no login.
    let _ = tokio::io::BufReader::new(tokio::io::stdin()).read_line(&mut line).await;

    if open_in_browser(url) {
        // Silence is confirmation: the browser is in front of them.
    } else {
        eprintln!("{}", ansi("33", "Could not open a browser — use the URL above."));
    }
}

/// Hand the URL to the platform's default browser. Best effort by design.
fn open_in_browser(url: &str) -> bool {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open").arg(url).spawn().is_ok()
    }
    #[cfg(target_os = "linux")]
    {
        std::process::Command::new("xdg-open").arg(url).spawn().is_ok()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = url;
        false
    }
}

/// The whole of `kimmy init`: one required answer — the node URL — then the
/// node describes itself.
///
/// The RFC 9728 document every node publishes names both the resource
/// identifier and the issuer (`oidc::discover_from_node`), which is what makes
/// the old nine-prompt walk unnecessary. Asking an operator to hand-type a
/// value the node already publishes is how `kimmy.x` gets typed where
/// `kimmydb.x` belongs — and a mistyped resource fails later, silently, as a
/// token audience mismatch. Only when discovery comes up empty (a node too old
/// to publish metadata) does init fall back to asking, with Enter skipping.
///
/// Secrets are never read interactively here: a typed secret lives in terminal
/// scrollback forever. Secret keys already in an existing file are carried
/// into the rewritten one untouched, so re-running init cannot silently drop
/// them; setting them fresh happens through the environment or flags.
async fn run_init() -> Result<()> {
    let existing = load_kimmy_file()?.map(|(_, settings)| settings);
    let env = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let path = kimmy_file_path()?;

    // The one thing nothing else can supply: which node to talk to.
    let url_prefill = existing.as_ref().and_then(|s| s.url.clone()).or_else(|| env("KIMMY_URL"));
    let url = loop {
        match prompt_setting("Node URL (any cluster member)", url_prefill.as_deref())? {
            Some(u) => match normalize_node_url(&u) {
                Some(v) => break v,
                None => eprintln!(
                    "{}",
                    ansi(
                        "33",
                        "  that is not an http(s) URL — try again, e.g. https://kimmy1.example.com"
                    )
                ),
            },
            None => {
                anyhow::bail!("a node URL is required: there is nothing to discover without one")
            }
        }
    };
    let mut pairs: Vec<(&'static str, String)> = vec![("url", url.clone())];

    // Everything else the node can say for itself.
    match oidc::discover_from_node(&url).await {
        Some((resource, issuer)) => {
            eprintln!(
                "{}",
                ansi("32", &format!("  \u{2713} resource  {resource}  (discovered from the node)"))
            );
            eprintln!(
                "{}",
                ansi("32", &format!("  \u{2713} issuer    {issuer}  (discovered from the node)"))
            );
            pairs.push(("resource", resource));
            pairs.push(("issuer", issuer));
        }
        None => {
            eprintln!(
                "{}",
                ansi(
                    "33",
                    "  no metadata from this node (an older build?) — enter its identity by hand; Enter skips each"
                )
            );
            let prev_resource = existing
                .as_ref()
                .and_then(|s| s.resource.clone())
                .or_else(|| env("KIMMY_OIDC_RESOURCE"));
            if let Some(v) = prompt_setting(
                "Resource identifier (what tokens name this node)",
                prev_resource.as_deref(),
            )? {
                pairs.push(("resource", v));
            }
            let prev_issuer = existing
                .as_ref()
                .and_then(|s| s.issuer.clone())
                .or_else(|| env("KIMMY_OIDC_ISSUER"));
            if let Some(v) = prompt_setting("OIDC issuer URL", prev_issuer.as_deref())? {
                pairs.push(("issuer", v));
            }
        }
    }

    // The client id has no discoverable source — it names THIS program's
    // registration with the provider. Its registered name is the answer
    // almost everywhere, so it is shown and kept rather than asked cold.
    const DEFAULT_CLIENT_ID: &str = "kimmy-cli";
    let client_id_prefill = existing
        .as_ref()
        .and_then(|s| s.client_id.clone())
        .or_else(|| env("KIMMY_OIDC_CLIENT_ID"))
        .unwrap_or_else(|| DEFAULT_CLIENT_ID.to_string());
    if let Some(v) = prompt_setting("OAuth client id", Some(&client_id_prefill))? {
        pairs.push(("client_id", v));
    }

    // Carry secrets forward untouched rather than re-asking or dropping them.
    if let Some(prev) = &existing {
        for (key, value) in [
            ("token", prev.token.clone()),
            ("password", prev.password.clone()),
            ("scope", prev.scope.clone()),
        ] {
            if let Some(value) = value {
                pairs.push((key, value));
            }
        }
        if let Some(cache_token) = prev.cache_token {
            pairs.push(("cache_token", cache_token.to_string()));
        }
    }

    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let body = render_kimmy_file(&pairs);
    std::fs::write(&path, body).with_context(|| format!("writing {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("restricting {}", path.display()))?;
    }

    eprintln!("{}", ansi("32", &format!("\u{2713} wrote {} (0600)", path.display())));
    let keys: Vec<String> = pairs.iter().map(|(k, _)| k.to_string()).collect();
    println!("{}", json!({ "written": path.display().to_string(), "keys": keys }));
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    /// `kimmy backup --out last.backup` over yesterday's good backup: a download
    /// that fails part-way leaves it byte for byte as it was and nothing beside
    /// it, and one that completes replaces it whole.
    #[tokio::test]
    async fn a_failed_backup_download_leaves_the_existing_file_untouched() {
        use tokio::io::AsyncWriteExt;

        let dir =
            std::env::temp_dir().join(format!("kimmy-cli-backup-in-place-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("last.backup");
        std::fs::write(&target, b"yesterday's good backup").unwrap();

        let failed = download_into_place(&target, async |file: &mut tokio::fs::File| {
            file.write_all(b"half of today's").await.unwrap();
            Err(kimmy_client::Error::Stalled {
                endpoint: "http://node:7878".into(),
                idle: std::time::Duration::from_secs(30),
            })
        })
        .await;
        assert!(failed.is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"yesterday's good backup");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1, "nothing is left beside it");

        let placed = download_into_place(&target, async |file: &mut tokio::fs::File| {
            file.write_all(b"today's").await.unwrap();
            Ok(7)
        })
        .await
        .unwrap();
        assert_eq!(placed, 7);
        assert_eq!(std::fs::read(&target).unwrap(), b"today's");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1, "the partial file became it");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    // -----------------------------------------------------------------------
    // The zero-grant note: which listings and identities trigger it
    // -----------------------------------------------------------------------

    #[test]
    fn an_empty_listing_is_detected_per_shape() {
        assert!(listing_is_empty("databases", &json!({ "databases": [] })));
        assert!(listing_is_empty("collections", &json!({ "collections": [] })));
        assert!(!listing_is_empty("databases", &json!({ "databases": ["notes"] })));
    }

    #[test]
    fn a_surprise_listing_shape_never_triggers_the_note() {
        // The gate must fail closed: a payload this CLI does not recognise is
        // treated as "not empty", so the hint can only ever appear when the
        // server really said the list was empty.
        assert!(!listing_is_empty("databases", &json!({})));
        assert!(!listing_is_empty("databases", &json!({ "databases": null })));
        assert!(!listing_is_empty("databases", &json!({ "databases": "[]" })));
    }

    #[test]
    fn only_an_actually_empty_grants_array_is_zero_grant() {
        // The exact shape a federated token with no mappings resolves to —
        // proven live against a production cluster on 2026-08-24, where the
        // cluster owner's own account saw it.
        assert!(is_zero_grant(&json!({ "federated": true, "grants": [] })));
        assert!(!is_zero_grant(&json!({ "grants": [{ "db": "*", "actions": ["read"] }] })));

        // A principal with grants seeing an empty result may be looking at an
        // empty namespace; the field being absent or odd means no hint at all.
        assert!(!is_zero_grant(&json!({ "federated": false })));
        assert!(!is_zero_grant(&json!({})));
        assert!(!is_zero_grant(&json!({ "grants": null })));
    }

    #[test]
    fn init_renders_only_what_was_collected() {
        let body = render_kimmy_file(&[
            ("url", "https://kimmy1.example.com".into()),
            ("client_id", "kimmy-cli".into()),
        ]);
        assert!(body.starts_with("# Written by `kimmy init`"));
        assert!(body.contains("url = https://kimmy1.example.com\n"));
        assert!(body.contains("client_id = kimmy-cli\n"));
        assert!(!body.lines().any(|l| l.starts_with("password = ")));
    }

    // ---------------------------------------------------------------------
    // init: discovery and URL handling
    // ---------------------------------------------------------------------

    #[test]
    fn discovery_extracts_resource_and_first_authorization_server() {
        let doc = json!({
            "resource": "https://kimmydb.example.com",
            "authorization_servers": ["https://auth.example.com"],
        });
        assert_eq!(
            oidc::extract_discovery(&doc),
            Some((
                "https://kimmydb.example.com".to_string(),
                "https://auth.example.com".to_string()
            ))
        );
    }

    #[test]
    fn discovery_needs_both_halves_of_the_document() {
        // Either half alone cannot set up a client; reporting "no metadata"
        // sends init down the explicit-prompt path for the whole pair.
        assert!(oidc::extract_discovery(&json!({ "resource": "https://k" })).is_none());
        assert!(oidc::extract_discovery(&json!({ "authorization_servers": [] })).is_none());
        assert!(oidc::extract_discovery(&json!({})).is_none());
    }

    #[test]
    fn node_urls_normalize_and_refuse_nonsense() {
        assert_eq!(
            normalize_node_url("https://kimmy1.example.com").as_deref(),
            Some("https://kimmy1.example.com")
        );
        assert_eq!(
            normalize_node_url(" https://kimmy1.example.com/ ").as_deref(),
            Some("https://kimmy1.example.com")
        );
        // No scheme, wrong scheme, or scheme-only: all refused, because every
        // other command joins paths onto this base sight unseen.
        assert!(normalize_node_url("kimmy1.example.com").is_none());
        assert!(normalize_node_url("ftp://kimmy1.example.com").is_none());
        assert!(normalize_node_url("https://").is_none());
    }

    #[test]
    fn init_is_a_known_subcommand() {
        assert!(Cli::try_parse_from(["kimmy", "init"]).is_ok());
    }

    // -----------------------------------------------------------------------
    // The settings file: ~/.config/kimmydb/.kimmy
    // -----------------------------------------------------------------------

    #[test]
    fn a_settings_file_parses_comments_quotes_and_every_key() {
        let dot = parse_kimmy_file(
            "# my node\n\
             url = https://kimmy1.example.com\n\
             \n\
             issuer = 'https://auth.example.com'\n\
             client_id = kimmy-cli\n\
             cache_token = on\n",
        )
        .unwrap();
        assert_eq!(dot.url.as_deref(), Some("https://kimmy1.example.com"));
        assert_eq!(dot.issuer.as_deref(), Some("https://auth.example.com"));
        assert_eq!(dot.client_id.as_deref(), Some("kimmy-cli"));
        assert_eq!(dot.cache_token, Some(true));
        assert!(dot.password.is_none());
    }

    #[test]
    fn a_settings_file_refuses_unknown_keys_and_broken_lines_by_number() {
        let err = parse_kimmy_file("url = x\nusrer = typo\n").unwrap_err().to_string();
        assert!(err.contains("line 2") && err.contains("usrer"), "{err}");
        // ADR-089: `client_secret` used to be valid. A retired key is not a
        // typo, so it is skipped with a warning rather than refusing every
        // command; the loud contract holds for everything else.
        let dot = parse_kimmy_file("client_secret = hunter2\nurl = x\n").expect("a retired key");
        assert_eq!(dot.url.as_deref(), Some("x"));
        assert!(parse_kimmy_file("clientsecret = hunter2\n").is_err());
        assert!(!render_kimmy_file(&[]).contains("client_secret"), "init still lists the key");

        let err = parse_kimmy_file("no equals sign here\n").unwrap_err().to_string();
        assert!(err.contains("line 1"), "{err}");
    }

    #[test]
    fn cache_token_is_boolish_or_refused() {
        assert_eq!(parse_kimmy_file("cache_token = off").unwrap().cache_token, Some(false));
        assert!(parse_kimmy_file("cache_token = maybe").is_err());
    }

    #[test]
    fn bare_invocation_is_valid_and_means_help() {
        // The clap default for a missing subcommand is a usage error; the
        // flip makes bare `kimmy` parse cleanly and print the long help
        // instead. run() owns the printing — this pins the parse half.
        let cli = Cli::try_parse_from(["kimmy"]).unwrap();
        assert!(cli.command.is_none(), "no args means no subcommand: {:?}", cli.command);
        assert!(
            Cli::command().render_help().to_string().contains("Commands:"),
            "the rendered help is what a bare invocation shows"
        );
    }

    #[test]
    fn whoami_is_a_known_subcommand() {
        let cli = Cli::try_parse_from(["kimmy", "whoami"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Whoami)));
    }

    // -----------------------------------------------------------------------
    // The login default, and `kimmy token`
    // -----------------------------------------------------------------------

    #[test]
    fn bare_login_defaults_to_the_device_flow() {
        assert_eq!(login_flow(None), LoginFlow::Device);
    }

    #[test]
    fn a_named_user_is_always_the_local_flow() {
        assert_eq!(login_flow(Some("ada")), LoginFlow::Local);
    }

    #[test]
    fn no_user_is_the_device_flow_and_there_is_no_machine_flow() {
        // ADR-089: the CLI is for people. A script sets KIMMY_TOKEN.
        assert_eq!(login_flow(None), LoginFlow::Device);
        assert!(Cli::try_parse_from(["kimmy", "login", "--client-credentials"]).is_err());
        assert!(Cli::try_parse_from(["kimmy", "token", "--client-credentials"]).is_err());
    }

    #[test]
    fn the_flipped_login_forms_parse() {
        assert!(Cli::try_parse_from(["kimmy", "login"]).is_ok(), "bare login is now valid");
        assert!(Cli::try_parse_from(["kimmy", "login", "ada"]).is_ok());
        // The flag is gone outright: it only ever spelled the default out,
        // and a spelling nobody needs is one more thing to misread.
        assert!(Cli::try_parse_from(["kimmy", "login", "--oidc"]).is_err());
        assert!(Cli::try_parse_from(["kimmy", "login", "ada", "--oidc"]).is_err());
    }

    #[test]
    fn the_removed_cache_token_flag_is_gone_outright() {
        // Login caches by design now (ADR-080): a flag that spelled the
        // default out is one more thing to misread, and an opt-in nobody
        // could see broke every command after login with a 401.
        assert!(Cli::try_parse_from(["kimmy", "login", "--cache-token"]).is_err());
        assert!(Cli::try_parse_from(["kimmy", "login", "--cache-token=false"]).is_err());
    }

    #[test]
    fn the_cache_key_client_id_prefers_the_environment_over_the_file() {
        assert_eq!(
            cached_key_client_id(None, Some("from-file".into())).as_deref(),
            Some("from-file")
        );
        assert_eq!(
            cached_key_client_id(Some("from-env".into()), Some("from-file".into())).as_deref(),
            Some("from-env")
        );
        assert_eq!(
            cached_key_client_id(Some(String::new()), Some("from-file".into())).as_deref(),
            Some("from-file"),
            "an empty variable is unset, not a client id"
        );
        assert_eq!(cached_key_client_id(None, None), None);
    }

    #[test]
    fn token_parses_and_takes_no_positional_user() {
        assert!(Cli::try_parse_from(["kimmy", "token"]).is_ok());
        assert!(
            Cli::try_parse_from(["kimmy", "token", "ada"]).is_err(),
            "a local account has no provider to key a cache by; kimmy login <user> is that path"
        );
    }

    // -----------------------------------------------------------------------
    // The admin surface: grant shorthand and command grouping
    // -----------------------------------------------------------------------

    #[test]
    fn grant_shorthand_parses_all_three_shapes() {
        let two = parse_grant_spec("notes:read").unwrap();
        assert_eq!(two["db"], json!("notes"));
        assert_eq!(two["collection"], json!("*"));
        assert_eq!(two["actions"], json!(["read"]));

        let three = parse_grant_spec("sales:orders*:read, search").unwrap();
        assert_eq!(three["db"], json!("sales"));
        assert_eq!(three["collection"], json!("orders*"));
        assert_eq!(three["actions"], json!(["read", "search"]), "spaces around commas are trimmed");

        let star = parse_grant_spec("*:*:read").unwrap();
        assert_eq!(star["db"], json!("*"));
        assert_eq!(star["collection"], json!("*"));
    }

    #[test]
    fn a_malformed_grant_shorthand_is_refused_with_the_shape_in_the_error() {
        for bad in ["no-colon", "db:coll:extra:read", "db:", ":read", "db:  "] {
            let err = parse_grant_spec(bad).unwrap_err().to_string();
            assert!(
                err.contains("db:actions")
                    || err.contains("names no actions")
                    || err.contains("empty database"),
                "{bad:?}: {err}"
            );
        }
    }

    #[test]
    fn duplicate_grant_specs_are_refused_before_anything_is_sent() {
        let specs = ["sales:read".to_string(), "sales:*:write".to_string()];
        let err = parse_grant_specs(&specs).unwrap_err().to_string();
        assert!(err.contains("repeats"), "{err}");
    }

    #[test]
    fn roles_and_users_are_grouped_subcommands() {
        for args in [
            vec!["roles", "list"],
            vec!["roles", "show", "analyst"],
            vec!["roles", "create", "analyst", "--grant", "*:*:read"],
            vec!["roles", "grant", "analyst", "shop:orders*:write"],
            vec!["roles", "delete", "analyst"],
            vec!["users", "list"],
            vec!["users", "create", "ada"],
            vec!["users", "disable", "ada"],
            vec!["users", "enable", "ada"],
            vec!["users", "set-roles", "ada", "analyst", "auditor"],
            vec!["users", "delete", "ada"],
        ] {
            let mut full = vec!["kimmy"];
            full.extend(args.iter());
            assert!(Cli::try_parse_from(full).is_ok(), "expected {args:?} to parse");
        }
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
    fn the_resource_rides_on_the_device_flow() {
        // The gap this workstream exists to close: without it the only audience
        // `kimmy login` could ever obtain was the provider's default, so the
        // correct configuration was unreachable from this tool.
        let device = oidc::with_resource(
            vec![("client_id", "kimmy-cli"), ("scope", "openid profile")],
            Some("https://kimmydb.example.com"),
        );
        assert!(device.contains(&("resource", "https://kimmydb.example.com")));
    }

    #[test]
    fn no_resource_means_the_parameter_is_absent_rather_than_empty() {
        // An empty `resource` is a malformed request; its absence is the
        // well-defined "your default audience" that every provider predating
        // RFC 8707 already implements.
        let form = oidc::with_resource(vec![("client_id", "kimmy-cli")], None);
        assert!(form.iter().all(|(key, _)| *key != "resource"), "{form:?}");
    }

    #[test]
    fn a_discovery_document_must_name_the_issuer_it_was_fetched_for() {
        // OpenID Connect Discovery §4.3 / RFC 8414 §3.3. On this side the
        // stake is where a client secret gets sent and where an access token
        // is collected from: a document not bound to the issuer the user named
        // can nominate somewhere else for both.
        let good = json!({ "issuer": "https://auth.example.com" });
        assert!(oidc::check_issuer(&good, "https://auth.example.com").is_ok());

        for named in ["https://auth.example.com/", "https://evil.example.com"] {
            let err = oidc::check_issuer(&json!({ "issuer": named }), "https://auth.example.com")
                .expect_err(&format!("{named:?} is not the issuer"))
                .to_string();
            assert!(err.contains(named), "the error must name what it found: {err}");
        }

        let err = oidc::check_issuer(&json!({}), "https://auth.example.com").unwrap_err();
        assert!(err.to_string().contains("no issuer"), "unhelpful error: {err}");
    }

    #[test]
    fn a_plaintext_endpoint_is_refused_unless_it_is_loopback() {
        // The CLI POSTs a client secret to the token endpoint. Over plaintext
        // that is the secret handed to anyone on the path.
        let document = |url: &str| json!({ "token_endpoint": url });

        assert_eq!(
            oidc::endpoint(&document("https://auth.example.com/token"), "token_endpoint").unwrap(),
            "https://auth.example.com/token"
        );

        let err = oidc::endpoint(&document("http://auth.example.com/token"), "token_endpoint")
            .unwrap_err()
            .to_string();
        assert!(err.contains("not https"), "unhelpful error: {err}");

        // A provider running on the developer's own machine stays usable.
        assert!(oidc::endpoint(&document("http://localhost:8080/token"), "token_endpoint").is_ok());
    }

    #[test]
    fn only_a_real_loopback_host_is_exempt_from_https() {
        // Prefix-matching the host would wave through a subdomain that merely
        // begins with a loopback address, and userinfo hiding the real host
        // after an `@`.
        for url in [
            "http://127.0.0.1.attacker.example/token",
            "http://127.0.0.1@attacker.example/token",
            "http://auth.internal/token",
            "ftp://127.0.0.1/token",
        ] {
            assert!(!oidc::is_secure_url(url), "{url} must not count as secure");
        }
        for url in [
            "https://auth.example.com/token",
            "http://127.0.0.1/token",
            "http://127.0.0.53:8080/token",
            "http://localhost:9000/token",
            "http://LOCALHOST/token",
            "http://[::1]:8080/token",
        ] {
            assert!(oidc::is_secure_url(url), "{url} must count as secure");
        }
    }

    #[test]
    fn a_token_with_no_stated_lifetime_is_not_cached() {
        // `expires_in` is only RECOMMENDED by RFC 6749 §5.1. Without it there
        // is nothing to base freshness on, and guessing would mean serving a
        // dead token as a 401 somewhere unrelated.
        assert_eq!(
            oidc::expires_in(&json!({ "access_token": "t", "expires_in": 3600 })),
            Some(3600)
        );
        assert_eq!(oidc::expires_in(&json!({ "access_token": "t" })), None);
        // Some providers send it as a string. Not accepted rather than
        // guessed at: not caching is the safe reading.
        assert_eq!(oidc::expires_in(&json!({ "expires_in": "3600" })), None);
    }

    /// A scratch path under the OS temp directory, unique per test.
    ///
    /// No `XDG_CACHE_HOME`, no environment variable of any kind: the tests
    /// name the file directly, so nothing here is process-global and nothing
    /// races the rest of the suite.
    fn scratch(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kimmy-cache-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("tokens.json")
    }

    fn key(resource: Option<&str>) -> cache::Key {
        cache::Key::new(Some("https://auth.example.com"), Some("kimmy-cli"), resource)
            .expect("issuer and client id are both present")
    }

    #[test]
    fn a_cached_token_comes_back_and_the_file_is_owner_only() {
        let path = scratch("roundtrip");
        let key = key(Some("https://kimmydb.example.com"));

        assert_eq!(cache::get_from(&path, &key), None, "nothing is cached before anything is put");

        cache::put_into(&path, &key, "the-token", Some(3600)).unwrap();
        assert_eq!(cache::get_from(&path, &key).as_deref(), Some("the-token"));

        // The whole reason a token on disk is a decision rather than a
        // convenience: it has to be unreadable by anyone else on the machine.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "the cache file must be owner-only, got {mode:o}");
            let dir_mode =
                std::fs::metadata(path.parent().unwrap()).unwrap().permissions().mode() & 0o777;
            assert_eq!(dir_mode, 0o700, "the cache directory must be owner-only, got {dir_mode:o}");
        }

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn the_key_separates_issuer_client_and_resource() {
        // A token for the wrong audience is refused by the node, which would
        // read as a broken cache rather than as the wrong key being used.
        let path = scratch("keys");
        cache::put_into(&path, &key(Some("https://a.example.com")), "token-a", Some(3600)).unwrap();
        cache::put_into(&path, &key(Some("https://b.example.com")), "token-b", Some(3600)).unwrap();

        assert_eq!(
            cache::get_from(&path, &key(Some("https://a.example.com"))).as_deref(),
            Some("token-a")
        );
        assert_eq!(
            cache::get_from(&path, &key(Some("https://b.example.com"))).as_deref(),
            Some("token-b"),
            "caching the second must not have discarded the first"
        );
        assert_eq!(cache::get_from(&path, &key(Some("https://c.example.com"))), None);
        assert_eq!(cache::get_from(&path, &key(None)), None, "no resource is its own key");

        let other_client = cache::Key::new(
            Some("https://auth.example.com"),
            Some("some-other-client"),
            Some("https://a.example.com"),
        )
        .unwrap();
        assert_eq!(cache::get_from(&path, &other_client), None);

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn a_token_close_to_expiry_is_not_served() {
        // It has to survive the command about to use it, not merely exist at
        // the moment it is read.
        let path = scratch("expiry");
        let key = key(None);

        cache::put_into(&path, &key, "nearly-dead", Some(30)).unwrap();
        assert_eq!(
            cache::get_from(&path, &key),
            None,
            "inside the margin, so it must be treated as spent"
        );

        cache::put_into(&path, &key, "alive", Some(3600)).unwrap();
        assert_eq!(cache::get_from(&path, &key).as_deref(), Some("alive"));

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn a_corrupt_or_missing_cache_is_a_miss_rather_than_a_failure() {
        // Anything that goes wrong reading it means the same thing to the
        // caller — authenticate again — and a cache that can break a login is
        // worse than no cache.
        let path = scratch("corrupt");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"this is not json").unwrap();
        assert_eq!(cache::get_from(&path, &key(None)), None);

        std::fs::write(&path, br#"{"entries":{"x":{"access_token":"t"}}}"#).unwrap();
        assert_eq!(cache::get_from(&path, &key(None)), None, "an entry with no expiry is unusable");

        assert_eq!(
            cache::get_from(std::path::Path::new("/nonexistent/kimmy/x.json"), &key(None)),
            None
        );

        // ...and a corrupt file must not stop a new token being stored.
        std::fs::write(&path, b"this is not json").unwrap();
        cache::put_into(&path, &key(None), "fresh", Some(3600)).unwrap();
        assert_eq!(cache::get_from(&path, &key(None)).as_deref(), Some("fresh"));

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn an_expired_entry_is_dropped_when_the_cache_is_next_written() {
        // Otherwise an issuer nobody uses any more leaves a token on disk
        // indefinitely.
        let path = scratch("sweep");
        let stale = key(Some("https://stale.example.com"));
        cache::put_into(&path, &stale, "stale-token", Some(1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));

        cache::put_into(&path, &key(Some("https://fresh.example.com")), "fresh", Some(3600))
            .unwrap();

        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("stale-token"), "the expired entry must be gone from disk: {raw}");
        assert!(raw.contains("fresh"));

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn a_cache_key_needs_an_issuer_and_a_client() {
        // Without both there was no OAuth flow whose result could be cached.
        assert!(cache::Key::new(None, Some("kimmy-cli"), None).is_none());
        assert!(cache::Key::new(Some("https://auth.example.com"), None, None).is_none());
        assert!(
            cache::Key::new(Some("https://auth.example.com"), Some("kimmy-cli"), None).is_some()
        );
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
    fn login_takes_a_user_or_nothing() {
        assert!(Cli::try_parse_from(["kimmy", "login", "root"]).is_ok());
        assert!(Cli::try_parse_from(["kimmy", "login"]).is_ok());
        assert!(Cli::try_parse_from(["kimmy", "login", "root", "ada"]).is_err());
    }

    #[test]
    fn the_unauthorized_hint_matches_the_deployment() {
        // The wrong hint sends a federated user to a local login that will ask
        // them for a password they do not have.
        let local = unauthorized_hint(None);
        assert!(local.contains("kimmy login"), "{local}");
        assert!(!local.contains("--oidc"), "{local}");

        let federated = unauthorized_hint(Some("https://auth.example.com".into()));
        assert!(federated.contains("kimmy login"), "{federated}");
        assert!(
            !federated.contains("--oidc"),
            "the flag is now only a compatibility spelling: {federated}"
        );
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
    fn a_login_refused_by_the_node_s_mode_gets_a_hint_and_nothing_else_does() {
        // 403 and 404 from the login route can only be `auth.local.login`
        // (ADR-100): the route answers 401 for a bad password, so those two
        // are the ones worth explaining, and the explanation names the setting.
        for status in [403, 404] {
            let code = if status == 403 { ErrorCode::Forbidden } else { ErrorCode::NotFound };
            let hint = local_login_hint(&api_error(status, code)).expect("a hint");
            assert!(hint.contains("auth.local.login"), "{hint}");
            assert!(hint.contains("kimmy login"), "say what to do instead: {hint}");
        }

        // A wrong password is a 401 and already has its own hint; a node that
        // could not be reached is not a policy. Neither gets this one.
        assert!(local_login_hint(&api_error(401, ErrorCode::Unauthorized)).is_none());
        assert!(local_login_hint(&kimmy_client::Error::NotAuthenticated).is_none());
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
                collection_created(Err(api_error(400, code.clone())), "orders").is_err(),
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

    #[test]
    fn fusion_controls_are_sent_only_when_given() {
        let base = search_body(Some("q"), None, 3, None, None).unwrap();

        let bare = with_fusion_controls(base.clone(), None, None, None);
        assert_eq!(bare, base, "a bare hybrid-search must send exactly what it always has");

        let tilted = with_fusion_controls(base.clone(), Some(0.7), None, Some(2));
        assert_eq!(tilted["weights"], json!({ "dense": 0.7 }), "the other weight is the server's");
        assert_eq!(tilted["min_overlap"], 2);

        let lexical_off = with_fusion_controls(base, None, Some(0.0), None);
        assert_eq!(lexical_off["weights"], json!({ "lexical": 0.0 }));
        assert!(lexical_off.get("min_overlap").is_none());
    }
}
