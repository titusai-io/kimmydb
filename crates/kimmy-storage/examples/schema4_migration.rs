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
//!
//! **The preflight on a volume of exactly the free space it asks for**:
//!
//! ```text
//! cargo run --release -p kimmy-storage --features test-hooks --example schema4_migration -- \
//!     build-strings <dir>
//! cargo run --release -p kimmy-storage --features test-hooks --example schema4_migration -- \
//!     build-purge <dir> <documents>
//! cargo run --release -p kimmy-storage --example schema4_migration -- \
//!     prove <dir> <volume> <MiB short>
//! ```
//!
//! **`build-strings`** writes a store shaped like the lab's kimmy1 to kimmy3:
//! four documents with string `_id`s of 16 KiB, each rewritten 2,500 times, so
//! the file is mostly oplog, and one index over a string. **`build-purge`**
//! writes a collection of `<documents>` integer ids that is dropped and left
//! for the purger, beside a small live one. Both are taken back to schema 4 by
//! the product's test helper and kept as `pristine.redb`.
//!
//! **`compact`** compacts `<dir>/pristine.redb` with redb until it has no free
//! page left, so a migration of it must grow the file: the case the growth
//! step is for.
//!
//! **`prove`** copies `<dir>/pristine.redb` onto `<volume>` (a mounted disk
//! image of a fixed size), learns the requirement from a start refused for
//! want of space, then fills the volume with a filler file until the free space
//! the preflight reads is the requirement, less `<MiB short>` MiB, rounded up
//! to the volume's block, and starts again. At 0 MiB short the start must
//! migrate to its end (the volume's lowest free space during it is sampled
//! every 10 ms); at 1 MiB short it must be refused with nothing of the
//! migration written, the store still opening on 0.50. Any other error,
//! `ENOSPC` included, is reported and exits 2.

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

const USAGE: &str = "usage: schema4_migration build <dir> <documents> | migrate <dir> [--keep] \
     | build-strings <dir> | build-purge <dir> <documents> | compact <dir> \
     | prove <dir> <volume> <MiB short>";

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
        #[cfg(feature = "test-hooks")]
        "build-strings" => build_strings(&dir),
        #[cfg(feature = "test-hooks")]
        "build-purge" => {
            let n: i64 = args.next().ok_or(USAGE)?.parse().map_err(|e| format!("{e}"))?;
            build_purge(&dir, n)
        }
        "compact" => compact(&dir),
        "prove" => {
            let volume = PathBuf::from(args.next().ok_or(USAGE)?);
            let short: u64 = args.next().ok_or(USAGE)?.parse().map_err(|e| format!("{e}"))?;
            prove(&dir, &volume, short << 20)
        }
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
    // The file's length, sampled every 20 ms while the open runs: its largest,
    // and every length it took, which is what the preflight's figure is about.
    let sampling = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let sampler = {
        let (path, sampling) = (path.clone(), std::sync::Arc::clone(&sampling));
        std::thread::spawn(move || {
            let mut lengths: Vec<u64> = Vec::new();
            while sampling.load(std::sync::atomic::Ordering::Relaxed) {
                let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                if lengths.last() != Some(&len) {
                    lengths.push(len);
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            lengths
        })
    };
    let started = Instant::now();
    let engine = Engine::open(&path).map_err(|e| e.to_string())?;
    let opened = started.elapsed();
    sampling.store(false, std::sync::atomic::Ordering::Relaxed);
    let lengths = sampler.join().map_err(|_| "the sampler panicked".to_string())?;
    let during = std::fs::metadata(&path).map_err(|e| e.to_string())?.len();
    drop(engine);
    let (reads, writes) = block_io();
    let largest = lengths.iter().copied().max().unwrap_or(before);
    let path_mib: Vec<u64> = lengths.iter().map(|l| l >> 20).collect();
    eprintln!(
        "SUMMARY open with the migration {opened:?}; file {} MiB before, {} MiB at its largest, \
         {} MiB after, {} MiB closed (lengths seen, MiB: {path_mib:?}); peak rss {} MiB; block \
         reads {reads}, block writes {writes} (getrusage; 512-byte blocks on Linux, operations on \
         macOS)",
        before >> 20,
        largest >> 20,
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

/// Keep `<dir>/kimmy.redb` and its sidecar as `pristine.redb`.
#[cfg(feature = "test-hooks")]
fn keep_pristine(dir: &Path) -> Result<(), String> {
    let path = dir.join("kimmy.redb");
    let pristine = dir.join("pristine.redb");
    std::fs::copy(&path, &pristine).map_err(|e| e.to_string())?;
    std::fs::copy(
        kimmy_storage::format::sidecar_path(&path),
        kimmy_storage::format::sidecar_path(&pristine),
    )
    .map_err(|e| e.to_string())?;
    eprintln!(
        "built {}: {} MiB",
        pristine.display(),
        std::fs::metadata(&pristine).map_err(|e| e.to_string())?.len() >> 20
    );
    Ok(())
}

/// A fresh `<dir>/kimmy.redb`, written by `build` with this build's engine and
/// taken back to schema 4.
#[cfg(feature = "test-hooks")]
fn engine_store(
    dir: &Path,
    build: impl FnOnce(&Engine) -> Result<(), String>,
) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let path = dir.join("kimmy.redb");
    for stale in [path.clone(), kimmy_storage::format::sidecar_path(&path)] {
        let _ = std::fs::remove_file(stale);
    }
    {
        let engine = Engine::open(&path).map_err(|e| e.to_string())?;
        build(&engine)?;
    }
    kimmy_storage::make_schema_4_for_test(&path).map_err(|e| e.to_string())?;
    keep_pristine(dir)
}

#[cfg(feature = "test-hooks")]
fn build_strings(dir: &Path) -> Result<(), String> {
    engine_store(dir, |engine| {
        let c = engine.create_collection("app", "c").map_err(|e| e.to_string())?;
        engine
            .create_index_with(
                "app",
                "c",
                vec![IndexField::ascending("tag")],
                false,
                Default::default(),
                Some("by_tag".into()),
                None,
                None,
            )
            .map_err(|e| e.to_string())?;
        let pad = "q".repeat(16_000);
        let body = |id: i64, n: i64| doc! {"_id": format!("doc-{id}"), "n": n, "tag": format!("t{id}"), "pad": &pad};
        engine
            .insert_many(&c, (1..=4).map(|id| body(id, 0)).collect())
            .map_err(|e| e.to_string())?;
        for n in 0..2_500_i64 {
            for id in 1..=4 {
                engine
                    .replace(&c, &DocId::String(format!("doc-{id}")), body(id, n), false)
                    .map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    })
}

#[cfg(feature = "test-hooks")]
fn build_purge(dir: &Path, n: i64) -> Result<(), String> {
    engine_store(dir, |engine| {
        let doomed = engine.create_collection("app", "doomed").map_err(|e| e.to_string())?;
        let pad = "p".repeat(280);
        for start in (0..n).step_by(20_000) {
            let docs =
                (start..(start + 20_000).min(n)).map(|i| doc! {"_id": i, "n": i, "pad": &pad});
            engine.insert_many(&doomed, docs.collect()).map_err(|e| e.to_string())?;
        }
        if !engine.drop_collection("app", "doomed").map_err(|e| e.to_string())? {
            return Err("the drop found nothing".into());
        }
        let live = engine.create_collection("app", "live").map_err(|e| e.to_string())?;
        engine
            .insert_many(&live, (0..1_000_i64).map(|i| doc! {"_id": i, "n": i}).collect())
            .map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// The free space on the volume holding `dir`, as the preflight reads it, and
/// the volume's block.
fn volume_free(dir: &Path) -> Result<(u64, u64), String> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(dir.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
    // SAFETY: `c_path` is a valid NUL-terminated string and `stat` a properly
    // sized, writable `statvfs` the call fills in.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) } != 0 {
        return Err(format!("statvfs {}", dir.display()));
    }
    #[allow(clippy::unnecessary_cast)]
    Ok(((stat.f_bavail as u64) * (stat.f_frsize as u64), stat.f_frsize as u64))
}

/// The volume's free space once it has settled: synced, then read until two
/// readings 50 ms apart agree (a filesystem can hand back freed blocks late).
fn settled_free(volume: &Path) -> Result<u64, String> {
    // SAFETY: `sync` takes no arguments and only flushes.
    unsafe { libc::sync() };
    let mut last = volume_free(volume)?.0;
    for _ in 0..40 {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let now = volume_free(volume)?.0;
        if now == last {
            return Ok(now);
        }
        last = now;
    }
    Ok(last)
}

/// Grow or shrink `<volume>/filler.bin` until the volume's free space is
/// `target`, as near as its block allows; the free space reached. A large gap
/// is closed in two steps, and a write the volume refuses is backed off, since
/// a filesystem's own figure can run ahead of what it will hand out.
fn set_free(volume: &Path, target: u64) -> Result<u64, String> {
    use std::io::Write;
    let filler = volume.join("filler.bin");
    let mut free = 0;
    for _ in 0..80 {
        free = settled_free(volume)?;
        if free == target {
            break;
        }
        let len = std::fs::metadata(&filler).map(|m| m.len()).unwrap_or(0);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&filler)
            .map_err(|e| e.to_string())?;
        if free > target {
            let gap = free - target;
            let step = if gap > 32 << 20 { gap - (16 << 20) } else { gap };
            let zeros = vec![0u8; 1 << 20];
            let mut left = step;
            let mut f = &file;
            let mut wrote = Ok(());
            while left > 0 && wrote.is_ok() {
                let n = left.min(zeros.len() as u64) as usize;
                wrote = f.write_all(&zeros[..n]);
                left -= n as u64;
            }
            if wrote.and_then(|()| file.sync_all()).is_err() {
                let now = std::fs::metadata(&filler).map(|m| m.len()).unwrap_or(0);
                file.set_len(now.saturating_sub(16 << 20)).map_err(|e| e.to_string())?;
            }
        } else {
            file.set_len(len.saturating_sub(target - free)).map_err(|e| e.to_string())?;
        }
        file.sync_all().map_err(|e| format!("the filler: {e}"))?;
    }
    Ok(free)
}

/// What a refused start must leave: every row of the tables the migration
/// rewrites, and the sidecar, as bytes. Read raw, on purpose: the point is to
/// look at the store without an open's own commit.
#[allow(clippy::disallowed_methods)]
fn store_rows(path: &Path) -> Result<(u64, Option<u8>, Vec<String>), String> {
    use redb::ReadableDatabase;
    use std::hash::{Hash, Hasher};
    let db = redb::Database::create(path).map_err(|e| e.to_string())?;
    let txn = db.begin_read().map_err(|e| e.to_string())?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for row in txn
        .open_table(tables::DOCS)
        .map_err(|e| e.to_string())?
        .iter()
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        (k.value().0, k.value().1, v.value()).hash(&mut h);
    }
    for row in txn
        .open_table(tables::INDEX_ENTRIES)
        .map_err(|e| e.to_string())?
        .iter()
        .map_err(|e| e.to_string())?
    {
        let (k, _) = row.map_err(|e| e.to_string())?;
        k.value().hash(&mut h);
    }
    for row in txn
        .open_table(tables::COLLECTIONS)
        .map_err(|e| e.to_string())?
        .iter()
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        (k.value(), v.value()).hash(&mut h);
    }
    std::fs::read(kimmy_storage::format::sidecar_path(path))
        .map_err(|e| e.to_string())?
        .hash(&mut h);
    let version = txn
        .open_table(tables::META)
        .map_err(|e| e.to_string())?
        .get(tables::META_FORMAT_VERSION)
        .map_err(|e| e.to_string())?
        .and_then(|v| v.value().first().copied());
    let tables: Vec<String> = {
        use redb::TableHandle;
        txn.list_tables()
            .map_err(|e| e.to_string())?
            .map(|t| t.name().to_string())
            .filter(|t| t.starts_with("schema5_"))
            .collect()
    };
    Ok((h.finish(), version, tables))
}

/// The bytes of a file, hashed.
fn file_hash(path: &Path) -> Result<u64, String> {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    std::fs::read(path).map_err(|e| e.to_string())?.hash(&mut h);
    Ok(h.finish())
}

fn prove(dir: &Path, volume: &Path, short: u64) -> Result<(), String> {
    tracing_subscriber::fmt().with_writer(std::io::stderr).with_ansi(false).init();
    let path = volume.join("kimmy.redb");
    let pristine = dir.join("pristine.redb");
    for stale in [volume.join("filler.bin"), path.clone()] {
        let _ = std::fs::remove_file(stale);
    }
    settled_free(volume)?;
    std::fs::copy(&pristine, &path).map_err(|e| format!("{}: {e}", pristine.display()))?;
    std::fs::copy(
        kimmy_storage::format::sidecar_path(&pristine),
        kimmy_storage::format::sidecar_path(&path),
    )
    .map_err(|e| format!("the sidecar: {e}"))?;
    std::fs::File::open(&path).and_then(|f| f.sync_all()).map_err(|e| e.to_string())?;
    let (_, block) = volume_free(volume)?;
    let up = |n: u64| n.div_ceil(block) * block;
    // The requirement, from starts refused for want of space: with the file's
    // length and 32 MiB free, less than any store's requirement (its growth
    // step and its 64 MiB margin), and room for the commit every start makes
    // first, which grows a file with no free page by a step of its own on a
    // filesystem that allocates what the file's length covers. Probed until two
    // starts in a row agree, since that first commit can grow the file once.
    // What a start writes before its preflight reads the free space; it can
    // free some too (a filesystem handing back blocks late), so it is signed.
    let (mut needed, mut written) = (0u64, 0i64);
    for probe in 1..=4 {
        let len = std::fs::metadata(&path).map_err(|e| e.to_string())?.len();
        let before = set_free(volume, up(len) + (32 << 20))?;
        match Engine::open(&path).map(drop) {
            Err(kimmy_storage::StorageError::NotEnoughSpace { needed: n, free, .. }) => {
                let settled = n == needed;
                (needed, written) = (n, before as i64 - free as i64);
                eprintln!(
                    "PROBE {probe}: file {} MiB, requirement {n} bytes, the start wrote {written} \
                     bytes before its preflight",
                    len >> 20
                );
                if settled {
                    break;
                }
            }
            other => return Err(format!("the probing start was not refused for space: {other:?}")),
        }
    }
    eprintln!(
        "PROVE {}: block {block}; requirement {needed} bytes ({} MiB); the start writes {written} \
         bytes before its preflight",
        dir.display(),
        needed >> 20
    );
    for attempt in 1..=8 {
        let want = up(needed).saturating_sub(short);
        let before = set_free(volume, (want as i64 + written).max(0) as u64)?;
        let rows_before = store_rows(&path)?;
        let file_before = file_hash(&path)?;
        let lowest = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
        let sampling = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let sampler = {
            let (volume, lowest, sampling) =
                (volume.to_path_buf(), lowest.clone(), sampling.clone());
            std::thread::spawn(move || {
                while sampling.load(std::sync::atomic::Ordering::Relaxed) {
                    if let Ok((free, _)) = volume_free(&volume) {
                        lowest.fetch_min(free, std::sync::atomic::Ordering::Relaxed);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            })
        };
        let started = Instant::now();
        let opened = Engine::open(&path).map(drop);
        sampling.store(false, std::sync::atomic::Ordering::Relaxed);
        sampler.join().map_err(|_| "the sampler panicked".to_string())?;
        let lowest = lowest.load(std::sync::atomic::Ordering::Relaxed);
        match opened {
            Ok(()) if short == 0 => {
                println!(
                    "RESULT migrated: requirement {needed} bytes, free at the preflight {} bytes \
                     (the requirement rounded up to the {block}-byte block), lowest free during the \
                     start {lowest} bytes ({} MiB), file {} MiB after, in {:?}, attempt {attempt}",
                    (before as i64 - written) as u64,
                    lowest >> 20,
                    std::fs::metadata(&path).map_err(|e| e.to_string())?.len() >> 20,
                    started.elapsed()
                );
                return Ok(());
            }
            Ok(()) => {
                return Err(format!("{} bytes short and not refused", short));
            }
            Err(kimmy_storage::StorageError::NotEnoughSpace { needed: n, free, .. })
                if short == 0 =>
            {
                // The free space the preflight read was not the one aimed at:
                // the start wrote more or less before it than the probe did.
                eprintln!(
                    "attempt {attempt}: aimed at {want}, the preflight read {free} and asked {n}"
                );
                written = before as i64 - free as i64;
                needed = n;
                continue;
            }
            Err(kimmy_storage::StorageError::NotEnoughSpace { needed: n, free, .. }) => {
                let rows_after = store_rows(&path)?;
                let v050 = kimmy_storage::format::BuildVersions {
                    schema: 4,
                    written_by: "0.50.0".into(),
                    ..kimmy_storage::format::BuildVersions::ours()
                };
                let opens_on_050 = kimmy_storage::format::check_before_open_with(&path, &v050);
                println!(
                    "RESULT refused: requirement {n} bytes, free at the preflight {free} bytes ({} \
                     bytes short); rows and sidecar unchanged {}; file bytes unchanged {}; schema \
                     {:?}; migration tables {:?}; 0.50 opens it {}",
                    n - free,
                    rows_after == rows_before,
                    file_hash(&path)? == file_before,
                    rows_after.1,
                    rows_after.2,
                    opens_on_050.is_ok()
                );
                return Ok(());
            }
            Err(other) => {
                println!("RESULT FAILED: {other} (lowest free {lowest} bytes)");
                std::process::exit(2);
            }
        }
    }
    Err("the free space the preflight reads never settled on the requirement".into())
}

/// Compact `<dir>/pristine.redb` until redb finds nothing more to move: a test
/// tool rewriting a closed store on purpose, outside the open's check.
#[allow(clippy::disallowed_methods)]
fn compact(dir: &Path) -> Result<(), String> {
    let pristine = dir.join("pristine.redb");
    let before = std::fs::metadata(&pristine).map_err(|e| e.to_string())?.len();
    let mut db = redb::Database::open(&pristine).map_err(|e| e.to_string())?;
    let mut passes = 0;
    while db.compact().map_err(|e| e.to_string())? && passes < 32 {
        passes += 1;
    }
    drop(db);
    eprintln!(
        "compacted {} in {passes} passes: {} MiB to {} MiB",
        pristine.display(),
        before >> 20,
        std::fs::metadata(&pristine).map_err(|e| e.to_string())?.len() >> 20
    );
    Ok(())
}
