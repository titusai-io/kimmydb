//! The record that the version vector covers the oplog, so an open need not
//! walk the oplog to check it (ADR-173's addendum of 2026-09-26).
//!
//! `Engine::open` used to walk every oplog entry, keys and values, on every
//! start, to raise `OPLOG_VERSIONS` over whatever it did not cover. On a store
//! where invariant I holds -- every entry at or below the vector for its
//! origin, or held (`OPLOG_HELD`) -- that walk raises nothing, and on a cold
//! page cache it cost about 30 s per GB of oplog read (round 0420, on the lab
//! host). The record says a walk, or a rewind's reset, has left I holding;
//! every writer of every build that can open the store keeps it; so an open
//! that finds the record skips the walk.
//!
//! The record is its own table, [`tables::VECTOR_VERIFIED`], and not a key in
//! `META`, because a backup copies `META` and lists its tables by hand: no
//! build's backup carries a table it does not list, so a restore -- which
//! omits `OPLOG_HELD` and so can leave I broken -- never carries the record,
//! whichever build restores it.

use redb::{ReadTransaction, ReadableDatabase, WriteTransaction};

use crate::error::Result;
use crate::tables;

/// What the record claims, versioned. The record counts only under this
/// exact epoch.
///
/// **The rule for changing it** (ADR-173's addendum): a change that only
/// *weakens* what the vector must cover may keep it. A change that
/// *strengthens* invariant I must bump it **and** ship with a rollback
/// boundary, a schema bump under ADR-190, that excludes every build that does
/// not keep the new I. Otherwise an older build could write under the old I
/// while a record written by the new one claims the new.
pub(crate) const I_EPOCH: u8 = 1;

/// The one key the table holds.
const KEY: &str = "verified";

/// Epoch, schema, rows, logical bytes, elapsed milliseconds.
const LEN: usize = 1 + 1 + 8 + 8 + 8;

/// What the walk that verified the vector found. Kept in the record so that
/// the open which skips the walk, and `/metrics`, can say what it would have
/// cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VerifiedWalk {
    /// Oplog entries the walk read.
    pub rows: u64,
    /// Their keys' and values' lengths, summed: logical bytes, not the bytes
    /// of the file, which redb's pages and free space make larger.
    pub logical_bytes: u64,
    /// How long the walk took.
    pub elapsed_ms: u64,
}

fn encode(schema: u8, walk: &VerifiedWalk) -> [u8; LEN] {
    let mut out = [0u8; LEN];
    out[0] = I_EPOCH;
    out[1] = schema;
    out[2..10].copy_from_slice(&walk.rows.to_be_bytes());
    out[10..18].copy_from_slice(&walk.logical_bytes.to_be_bytes());
    out[18..26].copy_from_slice(&walk.elapsed_ms.to_be_bytes());
    out
}

/// The record's walk, when it is one this build and this store's schema
/// count: exactly the right length, this build's epoch, the store's schema.
/// Anything else is no record, and the open walks.
fn decode(raw: &[u8], schema: u8) -> Option<VerifiedWalk> {
    if raw.len() != LEN || raw[0] != I_EPOCH || raw[1] != schema {
        return None;
    }
    let u64_at = |at: usize| u64::from_be_bytes(raw[at..at + 8].try_into().expect("8 bytes"));
    Some(VerifiedWalk { rows: u64_at(2), logical_bytes: u64_at(10), elapsed_ms: u64_at(18) })
}

/// The record, read inside `txn`, if it counts for `schema`.
///
/// Never fails for a missing table: the table is created with the others at
/// open, and a store an older build wrote has none until then, which is no
/// record. A storage error is still an error, as it is on every other read.
pub(crate) fn read(txn: &ReadTransaction, schema: u8) -> Result<Option<VerifiedWalk>> {
    let table = match txn.open_table(tables::VECTOR_VERIFIED) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    Ok(table.get(KEY)?.and_then(|raw| decode(raw.value(), schema)))
}

/// [`read`] on a database, in a transaction of its own.
pub(crate) fn read_db(db: &redb::Database, schema: u8) -> Result<Option<VerifiedWalk>> {
    read(&db.begin_read()?, schema)
}

/// Record that the vector covers the oplog, as `walk` found it. Only ever
/// called in the transaction that leaves invariant I holding: the open's
/// walk, and a rewind's reset.
pub(crate) fn write(txn: &WriteTransaction, schema: u8, walk: &VerifiedWalk) -> Result<()> {
    txn.open_table(tables::VECTOR_VERIFIED)?.insert(KEY, encode(schema, walk).as_slice())?;
    Ok(())
}

/// Whether an open should walk even with a record: `KIMMY_VERIFY_OPLOG_AT_OPEN`
/// set to `1`. The walk then rewrites the record. Read once per open.
pub(crate) fn forced() -> bool {
    std::env::var_os("KIMMY_VERIFY_OPLOG_AT_OPEN").is_some_and(|v| v == "1")
}

#[cfg(test)]
pub(crate) mod test_support {
    /// Write raw bytes as the record, for the tests of what does not count.
    pub(crate) fn write_raw(db: &redb::Database, raw: &[u8]) {
        let txn = db.begin_write().unwrap();
        txn.open_table(crate::tables::VECTOR_VERIFIED).unwrap().insert(super::KEY, raw).unwrap();
        txn.commit().unwrap();
    }

    pub(crate) fn encode(schema: u8, walk: &super::VerifiedWalk) -> Vec<u8> {
        super::encode(schema, walk).to_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use bson::doc;
    use kimmy_core::{Hlc, NodeId, OpKind, OplogEntry, Stamp, VersionVector};
    use proptest::prelude::*;
    use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

    use super::*;
    use crate::codec;
    use crate::engine::{Engine, Position, WriterHolder};
    use crate::meta::CollectionMeta;

    // --- helpers ---

    /// What `body` logs on this thread.
    fn logs_of(body: impl FnOnce()) -> String {
        #[derive(Clone)]
        struct Captured(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Captured {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let out = Captured(Arc::new(Mutex::new(Vec::new())));
        let writer = out.clone();
        let subscriber =
            tracing_subscriber::fmt().with_writer(move || writer.clone()).with_ansi(false).finish();
        tracing::subscriber::with_default(subscriber, body);
        String::from_utf8(out.0.lock().unwrap().clone()).unwrap()
    }

    /// Run `f` with the walk forced, as `KIMMY_VERIFY_OPLOG_AT_OPEN=1` does.
    /// Each test runs in its own process under nextest, so the variable
    /// reaches no other test.
    fn forcing<T>(f: impl FnOnce() -> T) -> T {
        // SAFETY: no other thread of this test reads the environment.
        unsafe { std::env::set_var("KIMMY_VERIFY_OPLOG_AT_OPEN", "1") };
        let out = f();
        // SAFETY: as above.
        unsafe { std::env::remove_var("KIMMY_VERIFY_OPLOG_AT_OPEN") };
        out
    }

    /// Change the raw store under a closed engine.
    fn raw(path: &Path, f: impl FnOnce(&redb::WriteTransaction)) {
        let db = redb::Database::create(path).unwrap();
        let txn = db.begin_write().unwrap();
        f(&txn);
        txn.commit().unwrap();
    }

    fn record_of(path: &Path) -> Option<Vec<u8>> {
        let db = redb::Database::create(path).unwrap();
        let txn = db.begin_read().unwrap();
        let table = match txn.open_table(tables::VECTOR_VERIFIED) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return None,
            Err(e) => panic!("{e}"),
        };
        table.get(KEY).unwrap().map(|v| v.value().to_vec())
    }

    fn wipe_vector(txn: &redb::WriteTransaction) {
        txn.open_table(tables::OPLOG_VERSIONS).unwrap().retain(|_, _| false).unwrap();
    }

    /// A store with some local history, closed.
    fn store(docs: i64) -> (tempfile::TempDir, PathBuf, VersionVector) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let vector = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            for i in 0..docs {
                engine.insert(&coll, doc! { "_id": i, "pad": "x".repeat(1_000) }).unwrap();
            }
            engine.version_vector().unwrap()
        };
        (dir, path, vector)
    }

    /// Invariant I on the tables, and witnessed at or above servable: what a
    /// forced walk would find nothing to raise over.
    fn a_walk_would_raise_nothing(engine: &Engine) -> std::result::Result<(), String> {
        let txn = engine.db().begin_read().unwrap();
        let oplog = txn.open_table(tables::OPLOG).unwrap();
        let held = txn.open_table(tables::OPLOG_HELD).unwrap();
        let servable = Engine::read_versions_in(&txn, tables::OPLOG_VERSIONS).unwrap();
        let witnessed = Engine::read_versions_in(&txn, tables::OPLOG_WITNESSED).unwrap();
        for row in oplog.iter().unwrap() {
            let (key, _) = row.unwrap();
            let stamp = codec::decode_oplog_key(key.value()).unwrap();
            if stamp.hlc > servable.get(stamp.node) && held.get(key.value()).unwrap().is_none() {
                return Err(format!("{stamp:?} is above the vector and not held"));
            }
        }
        for (node, hlc) in servable.iter() {
            if witnessed.get(node) < hlc {
                return Err(format!("witnessed is below servable for {node:?}"));
            }
        }
        Ok(())
    }

    // --- (a) the record: what counts, and what does not ---

    /// Whatever is wrong with the record, the open walks, and leaves a record
    /// that counts. The vector is wiped under each, so only a walk puts it back.
    #[test]
    fn anything_but_an_exact_record_walks_and_is_rewritten() {
        let schema = crate::migrate::SCHEMA_VERSION;
        let walk = VerifiedWalk { rows: 1, logical_bytes: 2, elapsed_ms: 3 };
        let good = test_support::encode(schema, &walk);
        let mut other_epoch = good.clone();
        other_epoch[0] = I_EPOCH + 1;
        let mut other_schema = good.clone();
        other_schema[1] = schema - 1;
        type Spoil = Box<dyn Fn(&redb::WriteTransaction)>;
        let cases: [(&str, Spoil); 5] = [
            (
                "no table",
                Box::new(|txn| {
                    txn.delete_table(tables::VECTOR_VERIFIED).unwrap();
                }),
            ),
            (
                "no key",
                Box::new(|txn| {
                    txn.open_table(tables::VECTOR_VERIFIED).unwrap().remove(KEY).unwrap();
                }),
            ),
            (
                "a short value",
                Box::new(move |txn| {
                    txn.open_table(tables::VECTOR_VERIFIED)
                        .unwrap()
                        .insert(KEY, &good[..25])
                        .unwrap();
                }),
            ),
            (
                "another epoch",
                Box::new(move |txn| {
                    txn.open_table(tables::VECTOR_VERIFIED)
                        .unwrap()
                        .insert(KEY, other_epoch.as_slice())
                        .unwrap();
                }),
            ),
            (
                "another schema",
                Box::new(move |txn| {
                    txn.open_table(tables::VECTOR_VERIFIED)
                        .unwrap()
                        .insert(KEY, other_schema.as_slice())
                        .unwrap();
                }),
            ),
        ];
        for (what, spoil) in cases {
            let (_dir, path, vector) = store(3);
            raw(&path, |txn| {
                wipe_vector(txn);
                spoil(txn);
            });
            let engine = Engine::open(&path).unwrap();
            assert_eq!(engine.version_vector().unwrap(), vector, "{what}: the open walked");
            let walk = engine.version_vector_verified().unwrap().expect("a record that counts");
            assert!(walk.rows > 0, "{what}: the record carries the walk's rows");
        }
    }

    /// A record that counts is trusted: with it, the open does not walk. The
    /// vector wiped under it stays wiped, which is the proof the walk was
    /// skipped -- and why only a writer that keeps I may leave the record.
    #[test]
    fn an_exact_record_is_trusted_and_the_escape_hatch_walks_anyway() {
        let (_dir, path, vector) = store(3);
        drop(Engine::open(&path).unwrap());
        raw(&path, wipe_vector);
        {
            let engine = Engine::open(&path).unwrap();
            assert!(engine.version_vector().unwrap().is_empty(), "skipped: nothing raised");
        }
        let engine = forcing(|| Engine::open(&path).unwrap());
        assert_eq!(engine.version_vector().unwrap(), vector, "forced: the walk raised it back");
    }

    // --- (b) what a skipping open reads ---

    /// An open that skips reads a fixed amount, whatever the oplog's size; an
    /// open that walks reads the oplog. Metered at the storage backend, so no
    /// page cache can hide a read.
    ///
    /// Release builds only, and run in CI's release job: in a debug build
    /// redb's own open reads every allocated page (its
    /// `mark_allocated_page_for_debug`, under `debug_assertions`), so every
    /// open there reads the whole file whatever this code does. Measured in
    /// release: the skipping open read 69,961 bytes at 500, 3,000 and 6,000
    /// documents, and the walking one 0.76, 4.2 and 8.4 MB.
    #[test]
    #[cfg_attr(
        debug_assertions,
        ignore = "redb reads every allocated page at open in a debug build; run with --release"
    )]
    fn a_skipping_open_reads_a_fixed_amount_and_a_walking_one_reads_the_oplog() {
        let read = |docs: i64| {
            let (_dir, path, _) = store(docs);
            drop(Engine::open(&path).unwrap());
            let metered = |force: bool| {
                let scope = crate::hold_meter::Scope::walk();
                let engine = if force {
                    forcing(|| Engine::open(&path).unwrap())
                } else {
                    Engine::open(&path).unwrap()
                };
                let (meter, _) = scope.finish();
                // The forced walk rewrote the record with the oplog's size.
                (meter.read_bytes, engine.version_vector_verified().unwrap().unwrap().logical_bytes)
            };
            let (skipped, _) = metered(false);
            let (walked, logical) = metered(true);
            (skipped, walked, logical)
        };
        let (small_skip, small_walk, _) = read(500);
        let (large_skip, large_walk, large_logical) = read(6_000);
        assert!(large_logical > 6_000_000, "the large oplog holds some 6 MB: {large_logical}");
        assert!(large_walk >= large_logical, "the walk read the oplog: {large_walk}");
        assert!(large_walk > small_walk * 5, "the walk grows with the oplog");
        assert_eq!(large_skip, small_skip, "the skipping open reads the same at either size");
        assert!(large_skip < 256 * 1024, "the skipping open read {large_skip} bytes");
    }

    // --- (d) restore, and restore --until ---

    /// A restore carries no record, whichever build restores it; the first
    /// open walks. A rewind after it (`restore --until`) leaves a record that
    /// is true, and the open after that skips with the vector a walk gives.
    #[test]
    fn restore_carries_no_record_and_restore_until_leaves_a_true_one() {
        let (_dir, path, _) = store(10);
        let mut backup = Vec::new();
        let until = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.get_collection("app", "docs").unwrap();
            let until = engine.read_oplog_from(Hlc::ZERO, usize::MAX).unwrap()[5].stamp.hlc;
            engine.insert(&coll, doc! { "_id": "late" }).unwrap();
            engine.backup_to(&mut backup).unwrap();
            until
        };
        let restored_dir = tempfile::tempdir().unwrap();
        let restored = restored_dir.path().join("kimmy.redb");
        crate::backup::restore(&restored, &mut backup.as_slice()).unwrap();
        assert_eq!(record_of(&restored), None, "a restored store carries no record");

        // As `kimmyd restore --until` does: open, rewind, close.
        let (before, after, recorded) = {
            let engine = Engine::open(&restored).unwrap();
            let before = engine.oplog_entries().unwrap();
            engine.rewind_to(until).unwrap();
            let recorded = engine.version_vector_verified().unwrap().expect("a record");
            (before, engine.oplog_entries().unwrap(), recorded)
        };
        assert!(after < before, "the rewind removed entries: {before} -> {after}");
        assert_eq!(recorded.rows, after, "the reset rewrote the record with what it walked");
        let logged = logs_of(|| drop(Engine::open(&restored).unwrap()));
        assert!(logged.contains("skipped the oplog walk"), "{logged}");
        let skipped = Engine::open(&restored).unwrap().version_vector().unwrap();
        let walked = forcing(|| Engine::open(&restored).unwrap()).version_vector().unwrap();
        assert_eq!(skipped, walked);
    }

    // --- (f) the two lines ---

    #[test]
    fn an_open_says_whether_it_walked_and_what_the_walk_found() {
        let (_dir, path, _) = store(4);
        raw(&path, |txn| {
            txn.delete_table(tables::VECTOR_VERIFIED).unwrap();
        });
        let walked = logs_of(|| drop(Engine::open(&path).unwrap()));
        assert!(walked.contains("checked the version vector against the oplog"), "{walked}");
        for field in ["elapsed_ms=", "rows=", "logical_bytes=", "raised=false"] {
            assert!(walked.contains(field), "{field}: {walked}");
        }
        let skipped = logs_of(|| drop(Engine::open(&path).unwrap()));
        assert!(
            skipped.contains("skipped the oplog walk: the version vector is verified"),
            "{skipped}"
        );
        for field in ["rows=", "logical_bytes=", "walk_ms="] {
            assert!(skipped.contains(field), "{field}: {skipped}");
        }
        assert!(!skipped.contains("checked the version vector"), "{skipped}");
    }

    // --- the break: a writer that does not keep I, with the record present ---

    /// An entry appended without raising the vector or marking it held, as a
    /// build that did not keep I would. With the record present the open
    /// trusts the vector and does not raise over it: what the check below
    /// catches, and what the guard and the epoch rule exist to prevent.
    #[test]
    fn a_writer_that_breaks_i_under_the_record_is_what_the_check_catches() {
        let (_dir, path, _) = store(2);
        let engine = Engine::open(&path).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        let stranger = remote(&coll, NodeId::generate(), 5, "stray");
        append_without_raising(&engine, &coll, &stranger);
        assert!(engine.version_vector_verified().unwrap().is_some());
        assert!(a_walk_would_raise_nothing(&engine).is_err(), "the check sees I broken");
    }

    fn remote(coll: &CollectionMeta, origin: NodeId, wall: u64, id: &str) -> OplogEntry {
        OplogEntry {
            stamp: Stamp::new(Hlc::new(wall, 0), origin),
            kind: OpKind::Insert,
            collection: coll.id,
            doc_id: Some(kimmy_core::DocId::String(id.into())),
            body: Some(bson::serialize_to_vec(&doc! { "_id": id }).unwrap()),
        }
    }

    fn apply(engine: &Engine, coll: &CollectionMeta, entry: &OplogEntry, position: Position) {
        let txn = engine.begin_write(WriterHolder::Replication).unwrap();
        engine.apply_remote_in_txn(&txn, coll, entry, position).unwrap();
        txn.commit().unwrap();
    }

    /// A held append with its mark taken away: in the oplog and the index,
    /// above the vector, and not held.
    fn append_without_raising(engine: &Engine, coll: &CollectionMeta, entry: &OplogEntry) {
        apply(engine, coll, entry, Position::Hold);
        let db = engine.db();
        let txn = db.begin_write().unwrap();
        txn.open_table(tables::OPLOG_HELD)
            .unwrap()
            .remove(codec::oplog_key(&entry.stamp).as_slice())
            .unwrap();
        txn.commit().unwrap();
    }

    // --- the property: skip against walk ---

    #[derive(Clone, Debug)]
    enum Op {
        Local(u8),
        Remote {
            origin: usize,
            wall: u64,
            position: u8,
        },
        Release(usize),
        Absorb {
            mask: u8,
            wall: u64,
        },
        Orphan {
            origin: usize,
            wall: u64,
        },
        Collect(u64),
        Rewind(usize),
        Restore,
        /// A record from before a schema raise: the next open must walk.
        StaleSchema,
        /// A store from before the vector was kept: no record, and an entry
        /// above the vector, unmarked.
        PreVector {
            origin: usize,
            wall: u64,
        },
        Reopen,
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => (1u8..4).prop_map(Op::Local),
            8 => (0usize..3, 1u64..60, 0u8..3)
                .prop_map(|(origin, wall, position)| Op::Remote { origin, wall, position }),
            2 => any::<usize>().prop_map(Op::Release),
            2 => (1u8..8, 1u64..60).prop_map(|(mask, wall)| Op::Absorb { mask, wall }),
            1 => (0usize..3, 1u64..60).prop_map(|(origin, wall)| Op::Orphan { origin, wall }),
            1 => (1u64..60).prop_map(Op::Collect),
            1 => any::<usize>().prop_map(Op::Rewind),
            1 => Just(Op::Restore),
            1 => Just(Op::StaleSchema),
            1 => (0usize..3, 1u64..60).prop_map(|(origin, wall)| Op::PreVector { origin, wall }),
            2 => Just(Op::Reopen),
        ]
    }

    struct Store {
        dirs: Vec<tempfile::TempDir>,
        path: PathBuf,
        engine: Option<Engine>,
        coll: CollectionMeta,
        origins: [NodeId; 3],
        applied: Vec<OplogEntry>,
        next: u64,
    }

    impl Store {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("kimmy.redb");
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            let origins = [NodeId::generate(), NodeId::generate(), NodeId::generate()];
            Self {
                dirs: vec![dir],
                path,
                engine: Some(engine),
                coll,
                origins,
                applied: Vec::new(),
                next: 0,
            }
        }

        fn engine(&self) -> &Engine {
            self.engine.as_ref().unwrap()
        }

        fn id(&mut self) -> String {
            self.next += 1;
            format!("d{}", self.next)
        }

        fn close(&mut self) {
            drop(self.engine.take());
        }

        fn open(&mut self) {
            self.engine = Some(Engine::open(&self.path).unwrap());
            self.coll = self.engine().get_collection("app", "docs").unwrap();
        }

        fn run(&mut self, op: &Op) {
            match *op {
                Op::Local(n) => {
                    for _ in 0..n {
                        let id = self.id();
                        self.engine().insert(&self.coll, doc! { "_id": id }).unwrap();
                    }
                }
                Op::Remote { origin, wall, position } => {
                    let id = self.id();
                    let entry = remote(&self.coll, self.origins[origin], wall, &id);
                    let position = match position {
                        0 => Position::Raise,
                        1 => Position::InWindow,
                        _ => Position::Hold,
                    };
                    apply(self.engine(), &self.coll, &entry, position);
                    self.applied.push(entry);
                }
                Op::Release(pick) => {
                    if !self.applied.is_empty() {
                        let entry = self.applied[pick % self.applied.len()].clone();
                        apply(self.engine(), &self.coll, &entry, Position::InWindow);
                    }
                }
                Op::Absorb { mask, wall } => {
                    let mut granted = VersionVector::new();
                    for (i, origin) in self.origins.iter().enumerate() {
                        if mask & (1 << i) != 0 {
                            granted.insert(*origin, Hlc::new(wall, 0));
                        }
                    }
                    self.engine().absorb_version_vector(&granted).unwrap();
                }
                Op::Orphan { origin, wall } => {
                    let stamp = Stamp::new(Hlc::new(wall, 7), self.origins[origin]);
                    let db = self.engine().db();
                    let txn = db.begin_write().unwrap();
                    txn.open_table(tables::OPLOG_HELD)
                        .unwrap()
                        .insert(codec::oplog_key(&stamp).as_slice(), ())
                        .unwrap();
                    txn.commit().unwrap();
                }
                Op::Collect(wall) => {
                    self.engine()
                        .collect_garbage_at(wall, crate::gc::RetentionPolicy::new(0, 0))
                        .unwrap();
                }
                Op::Rewind(pick) => {
                    let local = self.engine().node_id();
                    let stamps: Vec<Hlc> = self
                        .engine()
                        .read_oplog_from(Hlc::ZERO, usize::MAX)
                        .unwrap()
                        .into_iter()
                        .filter(|e| e.stamp.node == local)
                        .map(|e| e.stamp.hlc)
                        .collect();
                    if self.engine().rewind_to(stamps[pick % stamps.len()]).is_ok() {
                        let verified = self.engine().version_vector_verified().unwrap();
                        assert!(verified.is_some(), "a rewind leaves a record");
                    }
                }
                Op::Restore => {
                    let mut backup = Vec::new();
                    self.engine().backup_to(&mut backup).unwrap();
                    self.close();
                    let dir = tempfile::tempdir().unwrap();
                    self.path = dir.path().join("kimmy.redb");
                    crate::backup::restore(&self.path, &mut backup.as_slice()).unwrap();
                    self.dirs.push(dir);
                    assert_eq!(record_of(&self.path), None, "a restore carries no record");
                    self.open();
                }
                Op::StaleSchema => {
                    self.close();
                    let schema = crate::migrate::SCHEMA_VERSION;
                    let stale = test_support::encode(schema - 1, &VerifiedWalk::default());
                    let db = redb::Database::create(&self.path).unwrap();
                    test_support::write_raw(&db, &stale);
                    drop(db);
                    self.open();
                }
                Op::PreVector { origin, wall } => {
                    // A store from before the vector was kept has no record.
                    self.close();
                    raw(&self.path, |txn| {
                        txn.delete_table(tables::VECTOR_VERIFIED).unwrap();
                    });
                    self.open();
                    let id = self.id();
                    let entry = remote(&self.coll, self.origins[origin], wall, &id);
                    let coll = self.coll.clone();
                    append_without_raising(self.engine(), &coll, &entry);
                    self.close();
                    raw(&self.path, |txn| {
                        txn.delete_table(tables::VECTOR_VERIFIED).unwrap();
                    });
                    self.open();
                }
                Op::Reopen => {
                    self.close();
                    self.open();
                }
            }
        }

        /// After every step, closed and reopened: the record is present only
        /// where a walk would raise nothing, and the vector a skipping open
        /// ends with is the one a walking open ends with.
        fn check(&mut self, after: &Op) {
            self.close();
            self.open();
            if self.engine().version_vector_verified().unwrap().is_some() {
                a_walk_would_raise_nothing(self.engine())
                    .unwrap_or_else(|e| panic!("a record stands over {e}, after {after:?}"));
            }
            let skipped = self.engine().version_vector().unwrap();
            let skipped_seen = self.engine().witnessed_vector().unwrap();
            self.close();
            self.engine = Some(forcing(|| Engine::open(&self.path).unwrap()));
            assert_eq!(self.engine().version_vector().unwrap(), skipped, "after {after:?}");
            assert_eq!(self.engine().witnessed_vector().unwrap(), skipped_seen, "after {after:?}");
            self.coll = self.engine().get_collection("app", "docs").unwrap();
        }
    }

    proptest! {
        // Each case builds a real store through up to 20 operations, reopening
        // it twice after every one.
        #![proptest_config(ProptestConfig::with_cases(32))]

        /// Whatever wrote the store, an open that skips the walk ends with the
        /// vector an open that walks does, and the record never stands over a
        /// vector a walk would raise.
        #[test]
        fn skipping_the_walk_gives_the_vector_walking_gives(
            ops in prop::collection::vec(op(), 1..20),
        ) {
            let mut store = Store::new();
            for op in &ops {
                store.run(op);
                store.check(op);
            }
        }
    }

    // --- (e) the guard ---

    /// Tables whose writers invariant I depends on.
    const WATCHED: [&str; 5] = [
        "tables::OPLOG)",
        "tables::OPLOG_VERSIONS)",
        "tables::OPLOG_HELD)",
        "tables::OPLOG_WITNESSED)",
        // `raise_version`'s parameter.
        "open_table(table)",
    ];

    /// Files every one of whose writers is audited for I, and the functions
    /// in the others that are.
    const AUDITED_FILES: [&str; 4] = ["gc.rs", "rewind.rs", "backup.rs", "faults.rs"];
    const AUDITED_FNS: [(&str, &str); 7] = [
        ("engine.rs", "append_oplog_at"),
        ("engine.rs", "raise_version"),
        ("engine.rs", "release_held_in_position"),
        ("engine.rs", "release_held_under"),
        ("engine.rs", "rebuild_version_vector_if_stale"),
        ("engine.rs", "reset_version_vector_to_oplog"),
        ("migrate.rs", "rewrite_oplog"),
    ];

    /// `body` with its `#[cfg(test)]` modules removed.
    fn without_test_modules(body: &str) -> String {
        let lines: Vec<&str> = body.lines().collect();
        let mut out = String::new();
        let mut i = 0;
        while i < lines.len() {
            let next = lines.get(i + 1).map(|l| l.trim()).unwrap_or("");
            let opens_a_module = (next.starts_with("mod ") || next.starts_with("pub(crate) mod "))
                && next.ends_with('{');
            if lines[i].trim() == "#[cfg(test)]" && opens_a_module {
                let mut depth = 0i64;
                i += 1;
                loop {
                    depth += lines[i].matches('{').count() as i64;
                    depth -= lines[i].matches('}').count() as i64;
                    i += 1;
                    if depth <= 0 || i >= lines.len() {
                        break;
                    }
                }
                continue;
            }
            out.push_str(lines[i]);
            out.push('\n');
            i += 1;
        }
        out
    }

    /// The function a line is in, from the last `fn` declared above it.
    fn fn_name(line: &str) -> Option<String> {
        let t = line.trim_start();
        let rest = ["pub(crate) fn ", "pub fn ", "fn ", "pub(crate) async fn ", "async fn "]
            .iter()
            .find_map(|p| t.strip_prefix(p))?;
        Some(rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect())
    }

    /// Lines outside the audited writers that can change a watched table.
    fn offenders_in(dir: &Path) -> Vec<String> {
        let mut found = Vec::new();
        let mut paths: Vec<_> =
            std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).collect();
        paths.sort();
        for path in paths {
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if path.extension().is_none_or(|e| e != "rs") || AUDITED_FILES.contains(&name.as_str())
            {
                continue;
            }
            let body = without_test_modules(&std::fs::read_to_string(&path).unwrap());
            let mut current = String::new();
            for (n, line) in body.lines().enumerate() {
                if let Some(f) = fn_name(line) {
                    current = f;
                }
                let watched = WATCHED.iter().any(|w| line.contains(w));
                if !watched {
                    continue;
                }
                let changes = line.contains("delete_table(")
                    || (line.contains("open_table(") && line.contains("let mut "))
                    || [".insert(", ".remove(", ".retain", ".pop_", ".drain", ".extract"]
                        .iter()
                        .any(|m| line.contains(m));
                let audited = AUDITED_FNS.iter().any(|(f, func)| *f == name && *func == current);
                if changes && !audited {
                    found.push(format!("{name}:{} in {current}: {}", n + 1, line.trim()));
                }
            }
        }
        found
    }

    /// Every writer of the oplog, the vectors or the marks is one audited for
    /// invariant I. A new one is a finding until it is audited here.
    #[test]
    fn every_writer_of_the_invariants_tables_is_an_audited_one() {
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let found = offenders_in(&src);
        assert!(
            found.is_empty(),
            "these change the oplog, the version vectors or the held marks outside the writers \
             audited for invariant I. Keep invariant I (ADR-173) or bump I_EPOCH with a rollback \
             boundary, then add the writer here:\n  {}",
            found.join("\n  ")
        );
    }

    /// The guard's own mutant: a new writer of the vector, and an audited
    /// function's name in a file it is not audited in, are both found.
    #[test]
    fn the_guard_finds_a_new_writer() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("mutant.rs"),
            "fn sneak(txn: &WriteTxn) -> Result<()> {\n    \
             let mut versions = txn.open_table(tables::OPLOG_VERSIONS)?;\n    \
             versions.remove(key)?;\n    Ok(())\n}\n\n\
             fn append_oplog_at(txn: &WriteTxn) {\n    \
             txn.open_table(tables::OPLOG_HELD).unwrap().insert(k, ()).unwrap();\n}\n\n\
             #[cfg(test)]\nmod tests {\n    fn t() {\n        \
             let mut oplog = txn.open_table(tables::OPLOG).unwrap();\n    }\n}\n",
        )
        .unwrap();
        let found = offenders_in(dir.path());
        assert_eq!(found.len(), 2, "{found:?}");
        assert!(found[0].contains("in sneak"), "{found:?}");
        assert!(found[1].contains("in append_oplog_at"), "not audited outside engine.rs");
    }

    /// No backup carries the record: the tables a backup writes are the ones
    /// it lists, and this is not one of them.
    #[test]
    fn no_backup_lists_the_record() {
        let source = include_str!("backup.rs");
        assert!(!source.contains("VECTOR_VERIFIED"), "backup.rs names the record's table");
        let (_dir, path, _) = store(2);
        let mut backup = Vec::new();
        Engine::open(&path).unwrap().backup_to(&mut backup).unwrap();
        let restored_dir = tempfile::tempdir().unwrap();
        let restored = restored_dir.path().join("kimmy.redb");
        crate::backup::restore(&restored, &mut backup.as_slice()).unwrap();
        assert_eq!(record_of(&restored), None);
        let db = redb::Database::create(&restored).unwrap();
        let txn = db.begin_read().unwrap();
        assert!(
            txn.open_table(tables::VECTOR_VERIFIED).map(|t| t.len().unwrap() == 0).unwrap_or(true)
        );
    }
}
