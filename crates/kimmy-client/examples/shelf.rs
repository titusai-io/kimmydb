//! `shelf` — a small library catalogue, in Rust.
//!
//! One application written three times; see `examples/README.md` for the other
//! two and for why the embedding is deliberately a toy.
//!
//! ```bash
//! KIMMY_URL=http://localhost:7878 KIMMY_ROOT_PASSWORD=hunter2 \
//!     cargo run --example shelf -p kimmy-client
//! ```

use std::time::Duration;

use kimmy_client::{Client, Method, Query, Safety, WatchOptions};
use serde_json::{Value, json};

/// Width of the toy embedding. Small on purpose: it is a hash, not a model.
const DIM: usize = 16;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("KIMMY_URL").unwrap_or_else(|_| "http://localhost:7878".into());
    let password = std::env::var("KIMMY_ROOT_PASSWORD").unwrap_or_else(|_| "hunter2".into());

    // One address is all a client needs; the rest of the cluster comes from
    // `/v1/topology`, and the token is kept alive from here on.
    let db =
        Client::builder(&url).credentials("root", password).discover_nodes(true).connect().await?;

    let version = db.version().await?;
    println!("connected to {url} — protocol {}, build {}", version["protocol"], version["version"]);

    // -- the shelf ---------------------------------------------------------
    let _ = db
        .request(
            Method::Post,
            "/v1/db/library/collections",
            Some(json!({ "name": "books" })),
            Safety::Idempotent,
        )
        .await;

    // One commit for the whole catalogue: the commit is the cost, so batching
    // is worth roughly two orders of magnitude over inserting one at a time.
    //
    // A second run finds them already there. Branching on the *code* rather
    // than the status is the point of the error taxonomy — and a batch is all
    // or nothing, so one duplicate means none of them landed.
    let books = catalogue();
    match db.insert_many("library", "books", &books).await {
        Ok(_) => println!("shelved {} books in one commit", books.len()),
        Err(e) if e.code() == Some(kimmy_client::ErrorCode::DuplicateKey) => {
            println!("the shelf is already stocked; carrying on")
        }
        Err(e) => return Err(e.into()),
    }

    // -- what is on it -----------------------------------------------------
    let by_decade = db
        .aggregate(
            "library",
            "books",
            &json!([
                { "$group": { "_id": { "$subtract": ["$year", { "$mod": ["$year", 10] }] },
                              "books": { "$sum": 1 } } },
                { "$sort": { "_id": 1 } }
            ]),
        )
        .await?;
    print!("by decade:");
    for group in by_decade["documents"].as_array().into_iter().flatten() {
        print!(" {}s={}", group["_id"], group["books"]);
    }
    println!();

    // Paging, because a `find` with no limit is a page rather than the shelf.
    let mut pages = db.pages("library", "books", Query::new().limit(5));
    let (mut titles, mut page_count) = (0, 0);
    while let Some(page) = pages.next().await? {
        page_count += 1;
        titles += page.len();
    }
    println!("walked {titles} books in {page_count} pages");

    // -- semantic search ---------------------------------------------------
    //
    // `byo` is the default provider: the client supplies the vectors, which is
    // what makes this run with no API key and no model.
    db.request(
        Method::Post,
        "/v1/db/library/coll/books/vector",
        Some(json!({ "fields": ["blurb"], "provider": { "kind": "byo" }, "dim": DIM })),
        Safety::Idempotent,
    )
    .await?;

    for book in &books {
        let text = format!(
            "{} {}",
            book["title"].as_str().unwrap_or(""),
            book["blurb"].as_str().unwrap_or("")
        );
        db.request(
            Method::Put,
            &format!("/v1/db/library/coll/books/docs/{}/vectors", book["_id"]),
            Some(json!([{ "chunk": 0, "vector": embed(&text), "text": text }])),
            Safety::Idempotent,
        )
        .await?;
    }

    let query = "ships between the stars";
    let hits = db
        .request(
            Method::Post,
            "/v1/db/library/coll/books/vector_search",
            Some(json!({ "vector": embed(query), "k": 3 })),
            Safety::Idempotent,
        )
        .await?;
    println!("\nnearest to {query:?}:");
    for hit in hits["matches"].as_array().into_iter().flatten() {
        let id = hit["_id"].as_i64().unwrap_or(-1);
        let title = books
            .iter()
            .find(|b| b["_id"].as_i64() == Some(id))
            .and_then(|b| b["title"].as_str())
            .unwrap_or("?");
        println!("  {:.3}  {title}", hit["score"].as_f64().unwrap_or(0.0));
    }

    // -- watching it change ------------------------------------------------
    let mut stream = db.watch("library", "books", WatchOptions::new().full_document(true)).await?;

    let writer = db.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        // Replaced rather than inserted, so a second run still produces an
        // event rather than a duplicate key.
        let _ = writer
            .replace_document(
                "library",
                "books",
                "999",
                &json!({ "title": "A Late Arrival", "year": 2026, "blurb": "arrived after the shelf was read" }),
                true,
            )
            .await;
    });

    println!("\nwatching for changes...");
    if let Some(event) = tokio::time::timeout(Duration::from_secs(10), stream.next()).await?? {
        println!(
            "  {} {} — {}",
            event.operation,
            event.document_id().cloned().unwrap_or(Value::Null),
            event.full_document().and_then(|d| d["title"].as_str()).unwrap_or("(no post-image)")
        );
    }
    stream.close().await;

    println!("\ndone.");
    Ok(())
}

/// A deterministic bag-of-words hash, normalized.
///
/// **Not an embedding.** It has no semantic understanding: two texts are near
/// each other when they share words. It is here so the *pipeline* is real
/// without needing an API key, and it is the same algorithm in all three
/// languages so the three applications agree.
fn embed(text: &str) -> Vec<f64> {
    let mut vector = vec![0.0f64; DIM];
    for word in text.split_whitespace() {
        let word = word.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase();
        if word.is_empty() {
            continue;
        }
        // FNV-1a, like the webhook ownership hash: stable across versions,
        // which `DefaultHasher` is not.
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in word.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        vector[(hash % DIM as u64) as usize] += 1.0;
    }
    let length: f64 = vector.iter().map(|v| v * v).sum::<f64>().sqrt();
    if length > 0.0 {
        for value in &mut vector {
            *value /= length;
        }
    }
    vector
}

fn catalogue() -> Vec<Value> {
    [
        (1, "The Long Way to a Small Angry Planet", 2014, "a crew tunnels wormholes between the stars"),
        (2, "A Memory Called Empire", 2019, "an ambassador arrives at a vast interstellar empire"),
        (3, "Ancillary Justice", 2013, "a starship intelligence in a single human body seeks revenge"),
        (4, "The Dispossessed", 1974, "a physicist travels between twin worlds divided by politics"),
        (5, "Piranesi", 2020, "a man lives alone in an infinite house of statues and tides"),
        (6, "The Left Hand of Darkness", 1969, "an envoy on a frozen world learns its people"),
        (7, "Station Eleven", 2014, "a travelling troupe performs after a collapse"),
        (8, "Klara and the Sun", 2021, "an artificial friend watches a family from a shop window"),
        (9, "Project Hail Mary", 2021, "a lone astronaut wakes on a ship between the stars"),
        (10, "The Fifth Season", 2015, "a continent breaks and a mother searches for her child"),
    ]
    .into_iter()
    .map(|(id, title, year, blurb)| json!({ "_id": id, "title": title, "year": year, "blurb": blurb }))
    .collect()
}
