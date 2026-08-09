//! `kimmy` — a terminal client for KimmyDB.
//!
//! # One-shot commands, not a shell
//!
//! Each invocation does one thing and exits, so the tool composes with pipes,
//! `jq`, shell loops and CI. An interactive shell is nicer for exploring, but it
//! is the same command surface plus a terminal UI — so the commands come first
//! and a REPL, if it is ever wanted, sits on top of them rather than beside.
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
use serde_json::{Value, json};

#[derive(Parser)]
#[command(
    name = "kimmy",
    version,
    about = "Terminal client for KimmyDB",
    after_help = "Authentication:\n  \
        kimmy login root                  # reads the password from stdin or KIMMY_PASSWORD\n  \
        export KIMMY_TOKEN=$(echo hunter2 | kimmy login root)\n\n\
    There is deliberately no --password flag: it would land in shell history\n\
    and in `ps` output for every user on the machine."
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
    Login { user: String },
    /// Health and readiness of the node.
    Ping,
    /// List databases you can read.
    Databases,
    /// List collections in a database.
    Collections { database: String },
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
            ExitCode::FAILURE
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn run() -> Result<()> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();

    match &cli.command {
        Command::Login { user } => {
            let password = read_password()?;
            let body = json!({ "user": user, "password": password });
            let response =
                send(&client, &cli, reqwest::Method::POST, "/v1/auth/login", Some(body)).await?;
            let token = response["token"]
                .as_str()
                .context("the server did not return a token")?
                .to_string();
            // The token alone, with no decoration, so `$(kimmy login ...)` is
            // usable directly. Deliberately not written to a file: a CLI that
            // stores a bearer token on disk has to answer for its permissions,
            // its lifetime and its cleanup, and an environment variable answers
            // all three by not existing afterwards.
            println!("{token}");
            return Ok(());
        }
        Command::Ping => {
            let health = send(&client, &cli, reqwest::Method::GET, "/healthz", None).await?;
            let ready = send(&client, &cli, reqwest::Method::GET, "/readyz", None).await?;
            emit(&cli, &json!({ "healthz": health, "readyz": ready }));
        }
        Command::Databases => {
            let out = send(&client, &cli, reqwest::Method::GET, "/v1/databases", None).await?;
            emit(&cli, &out);
        }
        Command::Collections { database } => {
            let path = format!("/v1/db/{database}/collections");
            let out = send(&client, &cli, reqwest::Method::GET, &path, None).await?;
            emit(&cli, &out);
        }
        Command::Find { target, filter, sort, projection, limit, skip, explain } => {
            let (db, coll) = split_target(target)?;
            let mut body = json!({ "explain": explain });
            insert_json(&mut body, "filter", filter.as_deref())?;
            insert_json(&mut body, "sort", sort.as_deref())?;
            insert_json(&mut body, "projection", projection.as_deref())?;
            if let Some(limit) = limit {
                body["limit"] = json!(limit);
            }
            if let Some(skip) = skip {
                body["skip"] = json!(skip);
            }
            let path = format!("/v1/db/{db}/coll/{coll}/find");
            let out = send(&client, &cli, reqwest::Method::POST, &path, Some(body)).await?;
            emit(&cli, &out);
        }
        Command::Count { target, filter } => {
            let (db, coll) = split_target(target)?;
            let mut body = json!({});
            insert_json(&mut body, "filter", filter.as_deref())?;
            let path = format!("/v1/db/{db}/coll/{coll}/count");
            let out = send(&client, &cli, reqwest::Method::POST, &path, Some(body)).await?;
            emit(&cli, &out);
        }
        Command::Insert { target, document } => {
            let (db, coll) = split_target(target)?;
            let body = parse_json_arg("document", document.as_deref())?;
            let path = format!("/v1/db/{db}/coll/{coll}/docs");
            let out = send(&client, &cli, reqwest::Method::POST, &path, Some(body)).await?;
            emit(&cli, &out);
        }
        Command::Update { target, filter, update, multi } => {
            let (db, coll) = split_target(target)?;
            let body = json!({
                "filter": parse_json("filter", filter)?,
                "update": parse_json("update", update)?,
                "multi": multi,
            });
            let path = format!("/v1/db/{db}/coll/{coll}/update");
            let out = send(&client, &cli, reqwest::Method::POST, &path, Some(body)).await?;
            emit(&cli, &out);
        }
        Command::Delete { target, filter, multi } => {
            let (db, coll) = split_target(target)?;
            let body = json!({ "filter": parse_json("filter", filter)?, "multi": multi });
            let path = format!("/v1/db/{db}/coll/{coll}/delete");
            let out = send(&client, &cli, reqwest::Method::POST, &path, Some(body)).await?;
            emit(&cli, &out);
        }
        Command::Aggregate { target, pipeline } => {
            let (db, coll) = split_target(target)?;
            let stages = parse_json_arg("pipeline", pipeline.as_deref())?;
            let path = format!("/v1/db/{db}/coll/{coll}/aggregate");
            let body = json!({ "pipeline": stages });
            let out = send(&client, &cli, reqwest::Method::POST, &path, Some(body)).await?;
            emit(&cli, &out);
        }
        Command::Describe { target, sample } => {
            let (db, coll) = split_target(target)?;
            let mut path = format!("/v1/db/{db}/coll/{coll}/describe");
            if let Some(sample) = sample {
                path.push_str(&format!("?sample={sample}"));
            }
            let out = send(&client, &cli, reqwest::Method::GET, &path, None).await?;
            emit(&cli, &out);
        }
        Command::Indexes { target } => {
            let (db, coll) = split_target(target)?;
            let path = format!("/v1/db/{db}/coll/{coll}/indexes");
            let out = send(&client, &cli, reqwest::Method::GET, &path, None).await?;
            emit(&cli, &out);
        }
        Command::Backup { out } => {
            let bytes = download(&client, &cli, "/v1/admin/backup").await?;
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

fn insert_json(body: &mut Value, key: &str, text: Option<&str>) -> Result<()> {
    if let Some(text) = text {
        body[key] = parse_json(key, text)?;
    }
    Ok(())
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

fn emit(cli: &Cli, value: &Value) {
    let rendered =
        if cli.pretty { serde_json::to_string_pretty(value) } else { serde_json::to_string(value) };
    match rendered {
        Ok(text) => println!("{text}"),
        Err(e) => eprintln!("kimmy: could not render the response: {e}"),
    }
}

fn request(
    client: &reqwest::Client,
    cli: &Cli,
    method: reqwest::Method,
    path: &str,
) -> reqwest::RequestBuilder {
    let url = format!("{}{path}", cli.url.trim_end_matches('/'));
    let mut builder = client.request(method, url);
    if let Some(token) = &cli.token {
        builder = builder.bearer_auth(token);
    }
    builder
}

/// Send a request and parse the JSON response, turning an error status into an
/// error rather than printing a body that looks like a result.
async fn send(
    client: &reqwest::Client,
    cli: &Cli,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> Result<Value> {
    let mut builder = request(client, cli, method, path);
    if let Some(body) = body {
        builder = builder.json(&body);
    }
    let response = builder.send().await.with_context(|| format!("requesting {}{path}", cli.url))?;
    let status = response.status();
    let text = response.text().await.context("reading the response")?;

    if !status.is_success() {
        // The server's own message is the useful part; the status alone would
        // send someone to the documentation for something already explained.
        let detail = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|v| v["message"].as_str().map(str::to_string))
            .unwrap_or_else(|| text.clone());
        if status == reqwest::StatusCode::UNAUTHORIZED {
            bail!("{status}: {detail} (set --token, or KIMMY_TOKEN from `kimmy login`)");
        }
        bail!("{status}: {detail}");
    }
    if text.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&text).context("the server returned a body that is not JSON")
}

/// Fetch raw bytes, for the backup endpoint.
async fn download(client: &reqwest::Client, cli: &Cli, path: &str) -> Result<Vec<u8>> {
    let response = request(client, cli, reqwest::Method::GET, path)
        .send()
        .await
        .with_context(|| format!("requesting {}{path}", cli.url))?;
    let status = response.status();
    let bytes = response.bytes().await.context("reading the response")?;
    if !status.is_success() {
        let detail = String::from_utf8_lossy(&bytes).to_string();
        bail!("{status}: {detail}");
    }
    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
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
}
