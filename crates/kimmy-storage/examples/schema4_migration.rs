//! Build a schema 4 store the size of a real one, fast, and time its schema 5
//! migration (ADR-238, design 6.3). A test tool: an example, so no release
//! builds or ships it.
//!
//! ```text
//! cargo run --release -p kimmy-storage --example schema4_migration -- build <dir> <documents>
//! cargo run --release -p kimmy-storage --example schema4_migration -- migrate <dir> [--keep]
//! ```
//!
//! **`build`** writes `<dir>/kimmy.redb` as a 0.50 node leaves it: one
//! collection, `shop.items`, of `<documents>` documents of about 350 bytes with
//! `Int64` ids `0..n`, and three secondary indexes (`by_n` over an integer,
//! `by_score` over a double, `by_name` over a string). The documents, the index
//! entries and one insert oplog entry per document are written with raw redb
//! writes in large transactions, every key by the frozen schema 4 encoder in
//! this file (never by the live encoder), so the bytes are what 0.50 wrote. One
//! open of this build then derives what an open derives from those tables (the
//! arrival index, the version vector and its verified record, the live counts
//! and the cut digests, folded over the schema 4 keys as 0.50 folds them) while
//! META says schema 5, so that it migrates nothing; META, the verified record
//! and the sidecar are then put back to schema 4, written by 0.50.0. The store
//! is copied to `<dir>/pristine.redb`, so `migrate` can be run again.
//!
//! **`migrate`** copies `pristine.redb` over `kimmy.redb` (unless `--keep`),
//! drops nothing from the page cache (run it twice, or drop the cache by hand,
//! to compare warm and cold), and times `Engine::open` end to end: the
//! preflight, the re-key's two phases, the three indexes' drains and refills,
//! and the first schema 5 start's oplog walk. The migration's own lines (its
//! plan, a progress line every 100,000 rows, one line per index and the closing
//! line with the time of each phase) go to stderr; the last line is a summary.
//!
//! On mars, against a real 0.50 store instead, copy its data directory and run
//! only the timing: `migrate` with `--keep` on a directory whose `kimmy.redb`
//! is that copy.

use std::path::{Path, PathBuf};
use std::time::Instant;

use bson::doc;
use kimmy_core::{CollectionId, DocId, DocRecord, Hlc, NodeId, OpKind, OplogEntry, Stamp};
use kimmy_storage::{CollectionMeta, Engine, IndexField, IndexMeta, codec, tables};
use redb::ReadableTable;

fn main() {
    if let Err(e) = run() {
        eprintln!("schema4_migration: {e}");
        std::process::exit(1);
    }
}

const USAGE: &str = "usage: schema4_migration build <dir> <documents> | migrate <dir> [--keep]";

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or(USAGE)?;
    let dir = PathBuf::from(args.next().ok_or(USAGE)?);
    match command.as_str() {
        "build" => {
            let n: i64 = args.next().ok_or(USAGE)?.parse().map_err(|e| format!("{e}"))?;
            build(&dir, n)
        }
        "migrate" => migrate(&dir, args.next().as_deref() == Some("--keep")),
        other => Err(format!("{other:?}: {USAGE}")),
    }
}

/// The frozen schema 4 key encoding of the three value types this store
/// indexes: `keyenc` as v0.50.0 has it.
mod v1 {
    fn finite(negative: bool, exp: i32, mantissa: u64, out: &mut Vec<u8>) {
        out.push(if negative { 0x02 } else { 0x04 });
        let mut body = [0u8; 10];
        body[..2].copy_from_slice(&((exp + 32_768) as u16).to_be_bytes());
        body[2..].copy_from_slice(&mantissa.to_be_bytes());
        if negative {
            for b in &mut body {
                *b = !*b;
            }
        }
        out.extend_from_slice(&body);
    }

    pub fn int64(v: i64) -> Vec<u8> {
        let mut out = vec![0x20];
        if v == 0 {
            out.push(0x03);
            return out;
        }
        let magnitude = v.unsigned_abs();
        let shift = magnitude.leading_zeros();
        finite(v < 0, 63 - shift as i32, magnitude << shift, &mut out);
        out
    }

    pub fn double(v: f64) -> Vec<u8> {
        let mut out = vec![0x20];
        assert!(v.is_finite() && v != 0.0, "the store's scores are finite and nonzero");
        let bits = v.to_bits();
        let exponent_field = ((bits >> 52) & 0x7FF) as i32;
        let fraction = bits & ((1u64 << 52) - 1);
        let (significand, power) = if exponent_field == 0 {
            (fraction, -1074i32)
        } else {
            ((1u64 << 52) | fraction, exponent_field - 1075)
        };
        let shift = significand.leading_zeros();
        finite(bits >> 63 == 1, power - shift as i32 + 63, significand << shift, &mut out);
        out
    }

    pub fn string(s: &str) -> Vec<u8> {
        let mut out = vec![0x30];
        for &b in s.as_bytes() {
            if b == 0 {
                out.extend_from_slice(&[0x00, 0xFF]);
            } else {
                out.push(b);
            }
        }
        out.extend_from_slice(&[0x00, 0x00]);
        out
    }
}

/// One document: about 350 bytes of BSON.
fn document(i: i64, pad: &str) -> bson::Document {
    doc! {
        "_id": i,
        "n": i * 7,
        "score": 1.0 + (i % 1_000) as f64 / 8.0,
        "name": format!("user-{i:09}"),
        "pad": pad,
    }
}

const BATCH: i64 = 100_000;

#[allow(clippy::disallowed_methods)]
fn build(dir: &Path, n: i64) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let path = dir.join("kimmy.redb");
    for stale in [path.clone(), kimmy_storage::format::sidecar_path(&path)] {
        let _ = std::fs::remove_file(stale);
    }
    // The frozen encoder agrees with the product's frozen legacy form: a check
    // of both, not a source of either's bytes.
    for id in [0, 1, -1, 5, 255, 256, i64::MAX, i64::MIN, n - 1] {
        assert_eq!(
            v1::int64(id),
            kimmy_core::legacy_key::legacy_key(&DocId::Int64(id)).into_bytes()
        );
    }
    let started = Instant::now();
    let coll = CollectionId::derive("shop", "items");
    let node = NodeId::generate();
    let wall = kimmy_storage::physical_now_ms().saturating_sub(n as u64 + 60_000);
    let mut meta = CollectionMeta::new(coll, "shop", "items", Hlc::new(wall, 0), None);
    for (name, field) in [("by_n", "n"), ("by_score", "score"), ("by_name", "name")] {
        let fields = vec![IndexField::ascending(field)];
        meta.indexes.push(IndexMeta {
            id: IndexMeta::derive_id(name),
            name: name.into(),
            fields,
            unique: false,
            enforcement: Default::default(),
            multikey: false,
            expire_after_secs: None,
            partial_filter: None,
            created: Some(Stamp::new(Hlc::new(wall, 1), node)),
        });
    }
    let ids: Vec<u32> = meta.indexes.iter().map(|i| i.id).collect();
    let pad = "p".repeat(280);
    let db = redb::Database::create(&path).map_err(|e| e.to_string())?;
    {
        let txn = db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut meta_table = txn.open_table(tables::META).map_err(|e| e.to_string())?;
            meta_table
                .insert(tables::META_FORMAT_VERSION, [5u8].as_slice())
                .map_err(|e| e.to_string())?;
            let mut collections = txn.open_table(tables::COLLECTIONS).map_err(|e| e.to_string())?;
            let json = serde_json::to_vec(&meta).map_err(|e| e.to_string())?;
            collections.insert(("shop", "items"), json.as_slice()).map_err(|e| e.to_string())?;
        }
        txn.commit().map_err(|e| e.to_string())?;
    }
    for start in (0..n).step_by(BATCH as usize) {
        let txn = db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut docs = txn.open_table(tables::DOCS).map_err(|e| e.to_string())?;
            let mut entries = txn.open_table(tables::INDEX_ENTRIES).map_err(|e| e.to_string())?;
            let mut oplog = txn.open_table(tables::OPLOG).map_err(|e| e.to_string())?;
            for i in start..(start + BATCH).min(n) {
                let stamp = Stamp::new(Hlc::new(wall + 10 + i as u64, 0), node);
                let doc = document(i, &pad);
                let body = bson::serialize_to_vec(&doc).map_err(|e| e.to_string())?;
                let key = v1::int64(i);
                let record = codec::encode_doc_record(&DocRecord {
                    stamp,
                    deleted: false,
                    body: body.clone(),
                });
                docs.insert((coll.0, key.as_slice()), record.as_slice())
                    .map_err(|e| e.to_string())?;
                for (id, value) in ids.iter().zip([
                    v1::int64(i * 7),
                    v1::double(1.0 + (i % 1_000) as f64 / 8.0),
                    v1::string(&format!("user-{i:09}")),
                ]) {
                    entries
                        .insert((coll.0, *id, value.as_slice(), key.as_slice()), ())
                        .map_err(|e| e.to_string())?;
                }
                let entry = OplogEntry {
                    stamp,
                    kind: OpKind::Insert,
                    collection: coll,
                    doc_id: Some(DocId::Int64(i)),
                    body: Some(body),
                };
                oplog
                    .insert(
                        codec::oplog_key(&stamp).as_slice(),
                        codec::encode_oplog_entry(&entry).as_slice(),
                    )
                    .map_err(|e| e.to_string())?;
            }
        }
        txn.commit().map_err(|e| e.to_string())?;
        eprintln!("built {} of {n} documents, {:?}", (start + BATCH).min(n), started.elapsed());
    }
    drop(db);
    // The tables an open derives: one open while META says schema 5, so it
    // migrates nothing and folds the schema 4 keys as 0.50 did.
    let derived = Instant::now();
    drop(Engine::open(&path).map_err(|e| format!("the deriving open: {e}"))?);
    eprintln!("derived the open's tables in {:?}", derived.elapsed());
    // Back to what 0.50 leaves: schema 4 in META, its verified record, and its
    // sidecar.
    let db = redb::Database::create(&path).map_err(|e| e.to_string())?;
    let txn = db.begin_write().map_err(|e| e.to_string())?;
    {
        let mut meta = txn.open_table(tables::META).map_err(|e| e.to_string())?;
        meta.insert(tables::META_FORMAT_VERSION, [4u8].as_slice()).map_err(|e| e.to_string())?;
        let mut verified = txn.open_table(tables::VECTOR_VERIFIED).map_err(|e| e.to_string())?;
        let records: Vec<(String, Vec<u8>)> = verified
            .iter()
            .map_err(|e| e.to_string())?
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.value().to_string(), v.value().to_vec())
            })
            .collect();
        for (k, mut v) in records {
            v[1] = 4;
            verified.insert(k.as_str(), v.as_slice()).map_err(|e| e.to_string())?;
        }
    }
    txn.commit().map_err(|e| e.to_string())?;
    drop(db);
    let sidecar = format!(
        "schema = 4\nredb_version = \"{}\"\nredb_file_format = {}\nwritten_by = \"0.50.0+release\"\n",
        read_sidecar_field(&path, "redb_version")?,
        read_sidecar_field(&path, "redb_file_format")?
    );
    std::fs::write(kimmy_storage::format::sidecar_path(&path), sidecar)
        .map_err(|e| e.to_string())?;
    let pristine = dir.join("pristine.redb");
    std::fs::copy(&path, &pristine).map_err(|e| e.to_string())?;
    std::fs::copy(
        kimmy_storage::format::sidecar_path(&path),
        kimmy_storage::format::sidecar_path(&pristine),
    )
    .map_err(|e| e.to_string())?;
    eprintln!(
        "built a schema 4 store of {n} documents in {:?}: {} MiB",
        started.elapsed(),
        std::fs::metadata(&path).map_err(|e| e.to_string())?.len() >> 20
    );
    Ok(())
}

/// One field of the sidecar this build wrote, as it wrote it.
fn read_sidecar_field(path: &Path, field: &str) -> Result<String, String> {
    let text = std::fs::read_to_string(kimmy_storage::format::sidecar_path(path))
        .map_err(|e| e.to_string())?;
    text.lines()
        .find_map(|l| l.strip_prefix(&format!("{field} = ")))
        .map(|v| v.trim_matches('"').to_string())
        .ok_or_else(|| format!("no {field} in the sidecar"))
}

fn migrate(dir: &Path, keep: bool) -> Result<(), String> {
    let path = dir.join("kimmy.redb");
    if !keep {
        let pristine = dir.join("pristine.redb");
        std::fs::copy(&pristine, &path).map_err(|e| format!("{}: {e}", pristine.display()))?;
        std::fs::copy(
            kimmy_storage::format::sidecar_path(&pristine),
            kimmy_storage::format::sidecar_path(&path),
        )
        .map_err(|e| e.to_string())?;
        // The copy's pages to disk before the clock starts: left dirty, the
        // migration's first sync would pay for writing the whole copy.
        std::fs::File::open(&path).and_then(|f| f.sync_all()).map_err(|e| e.to_string())?;
    }
    tracing_subscriber::fmt().with_writer(std::io::stderr).with_ansi(false).init();
    let before = std::fs::metadata(&path).map_err(|e| e.to_string())?.len();
    let started = Instant::now();
    let engine = Engine::open(&path).map_err(|e| e.to_string())?;
    let opened = started.elapsed();
    let during = std::fs::metadata(&path).map_err(|e| e.to_string())?.len();
    drop(engine);
    let (reads, writes) = block_io();
    eprintln!(
        "SUMMARY open with the migration {opened:?}; file {} MiB before, {} MiB after, {} MiB \
         closed; peak rss {} MiB; block reads {reads}, block writes {writes} (getrusage; 512-byte \
         blocks on Linux, operations on macOS)",
        before >> 20,
        during >> 20,
        std::fs::metadata(&path).map_err(|e| e.to_string())?.len() >> 20,
        peak_rss_mib()
    );
    Ok(())
}

/// The process's block input and output, as `getrusage` counts them.
fn block_io() -> (i64, i64) {
    // SAFETY: as in `peak_rss_mib`.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    (usage.ru_inblock as i64, usage.ru_oublock as i64)
}

/// The process's peak resident set, in MiB.
fn peak_rss_mib() -> u64 {
    // SAFETY: `getrusage` fills the struct it is handed and reads nothing else.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let max = usage.ru_maxrss as u64;
    // Bytes on macOS, KiB on Linux.
    if cfg!(target_os = "macos") { max >> 20 } else { max >> 10 }
}
