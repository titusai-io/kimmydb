//! `KIMMY_TEST_DROP_APPLY`, the lab's loss of one replicated entry (repair design section
//! 15.1). Built only with the `lab-hooks` cargo feature; the shipped binary has none of it
//! (the release check is `strings kimmyd | grep -c DROP_APPLY` reading 0).
//!
//! ```text
//! KIMMY_TEST_DROP_APPLY=<db>.<coll>:<n>[:<kind>][,<db>.<coll>:<n>[:<kind>]...]
//! ```
//!
//! The n-th (1-based) *counted* entry applied to that collection by the replicated path since
//! this process started is **witnessed but not written**: the vector covers it and nothing
//! re-serves it, the same state a refused entry leaves. What counts is the entries of the kind
//! named, and only those; with no kind, any document entry (`insert`, `update`, `replace`,
//! `delete`). A `CreateCollection`, `DropCollection` or `ConfigureVectors` entry never counts.
//! One loss per list entry per process start.
//!
//! The grammar is made unambiguous or refused: `.`, `:` and `,` are legal in names, so
//! entries are separated at `,`; within an entry the trailing `:<kind>` is recognised only
//! when it is one of the six kind words, then `:<n>` is the trailing digits; what is left is
//! `<db>.<coll>`, split at the first `.`. A value that does not parse unambiguously, or names
//! a database containing `.`, `:` or `,`, arms nothing.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use kimmy_core::{CollectionId, OpKind, OplogEntry, Stamp};
use tracing::warn;

/// The six kind words.
const KINDS: [(&str, OpKind); 6] = [
    ("insert", OpKind::Insert),
    ("update", OpKind::Update),
    ("replace", OpKind::Replace),
    ("delete", OpKind::Delete),
    ("create_index", OpKind::CreateIndex),
    ("drop_index", OpKind::DropIndex),
];

#[derive(Debug)]
struct Armed {
    db: String,
    coll: String,
    id: CollectionId,
    n: u64,
    kind: Option<OpKind>,
    seen: AtomicU64,
    fired: AtomicBool,
    /// The stamp of the entry this hook lost. A batch that fails after the loss is applied
    /// again, and the retry must lose the same entry again: one loss per process, not one per
    /// attempt (review revA m1).
    lost: Mutex<Option<Stamp>>,
}

static ARMED: OnceLock<Vec<Armed>> = OnceLock::new();

/// One list entry, parsed. `Err` says why it is refused.
fn parse_entry(entry: &str) -> Result<Armed, String> {
    let mut rest = entry;
    let mut kind = None;
    if let Some((head, tail)) = rest.rsplit_once(':')
        && let Some((_, k)) = KINDS.iter().find(|(word, _)| *word == tail)
    {
        kind = Some(*k);
        rest = head;
    }
    let (name, digits) = rest.rsplit_once(':').ok_or("no `:<n>`")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("`{digits}` is not a count"));
    }
    let n: u64 = digits.parse().map_err(|_| format!("`{digits}` is not a count"))?;
    let (db, coll) = name.split_once('.').ok_or("no `<db>.<coll>`")?;
    if db.is_empty() || coll.is_empty() || db.contains([':', ',']) {
        return Err("a database name with `.`, `:` or `,`, or an empty name".into());
    }
    Ok(Armed {
        db: db.to_string(),
        coll: coll.to_string(),
        id: CollectionId::derive(db, coll),
        n,
        kind,
        seen: AtomicU64::new(0),
        fired: AtomicBool::new(false),
        lost: Mutex::new(None),
    })
}

/// Parse a whole value. A value with any entry that does not parse arms nothing.
fn parse(value: &str) -> Result<Vec<Armed>, String> {
    let mut out = Vec::new();
    for entry in value.split(',') {
        out.push(parse_entry(entry).map_err(|why| format!("`{entry}`: {why}"))?);
    }
    Ok(out)
}

/// Arm from `KIMMY_TEST_DROP_APPLY`, once per process. Returns how many losses are armed.
pub fn arm_from_env() -> usize {
    let Ok(value) = std::env::var("KIMMY_TEST_DROP_APPLY") else {
        return 0;
    };
    arm(&value)
}

/// Arm from `value`. Used by [`arm_from_env`] and by tests.
pub fn arm(value: &str) -> usize {
    match parse(value) {
        Ok(armed) => {
            let count = armed.len();
            if ARMED.set(armed).is_err() {
                warn!("KIMMY_TEST_DROP_APPLY is already armed in this process; ignored");
                return 0;
            }
            warn!(
                losses = count,
                "KIMMY_TEST_DROP_APPLY is armed: this member will lose one replicated entry"
            );
            count
        }
        Err(why) => {
            warn!(
                reason = %why,
                "KIMMY_TEST_DROP_APPLY does not parse unambiguously and arms nothing"
            );
            0
        }
    }
}

/// Whether the replicated entry `entry` is the one an armed list entry loses. Called once
/// per entry the replicated path takes. `would_write` says whether the entry would change this
/// member's state if it were applied: a re-delivery of an entry already applied, or one a
/// newer row supersedes, is not counted, so the loss the log announces is a loss.
pub(crate) fn drops(entry: &OplogEntry, would_write: impl Fn() -> bool) -> bool {
    let Some(armed) = ARMED.get() else {
        return false;
    };
    for hook in armed {
        if hook.id != entry.collection {
            continue;
        }
        if hook.lost.lock().unwrap().as_ref() == Some(&entry.stamp) {
            // The batch that lost it failed and is applied again.
            return true;
        }
        if hook.fired.load(Ordering::Relaxed) {
            continue;
        }
        let counts = match hook.kind {
            Some(kind) => entry.kind == kind,
            None => matches!(
                entry.kind,
                OpKind::Insert | OpKind::Update | OpKind::Replace | OpKind::Delete
            ),
        };
        if !counts || !would_write() {
            continue;
        }
        if hook.n != 0 && hook.seen.fetch_add(1, Ordering::Relaxed) + 1 == hook.n {
            hook.fired.store(true, Ordering::Relaxed);
            *hook.lost.lock().unwrap() = Some(entry.stamp);
            warn!(
                db = %hook.db,
                collection = %hook.coll,
                kind = ?entry.kind,
                stamp = ?entry.stamp,
                "KIMMY_TEST_DROP_APPLY: lost a replicated entry (witnessed, not applied)"
            );
            return true;
        }
    }
    false
}

impl crate::Engine {
    /// Whether applying the replicated entry would change this member: a document entry that a
    /// row at the same or a newer stamp already supersedes does not. Anything this cannot read
    /// counts as a write. Only for the lab's hook.
    pub(crate) fn lab_hook_entry_would_write(&self, entry: &OplogEntry) -> bool {
        use redb::ReadableDatabase;
        let Some(id) = &entry.doc_id else {
            return true;
        };
        let Ok(key) = crate::docs::doc_key(id) else {
            return true;
        };
        let Ok(txn) = self.db().begin_read() else {
            return true;
        };
        let Ok(docs) = txn.open_table(crate::tables::DOCS) else {
            return true;
        };
        let Ok(Some(raw)) = docs.get((entry.collection.0, key.as_slice())) else {
            return true;
        };
        match crate::codec::decode_doc_record(raw.value()) {
            Ok(held) => entry.stamp.wins_over(&held.stamp),
            Err(_) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(value: &str) -> Vec<Armed> {
        parse(value).unwrap_or_else(|why| panic!("{value}: {why}"))
    }

    #[test]
    fn a_plain_entry_and_a_kind_parse() {
        let a = ok("shop.orders:1");
        assert_eq!(
            (a[0].db.as_str(), a[0].coll.as_str(), a[0].n, a[0].kind),
            ("shop", "orders", 1, None)
        );
        let a = ok("shop.orders:3:create_index");
        assert_eq!((a[0].n, a[0].kind), (3, Some(OpKind::CreateIndex)));
    }

    #[test]
    fn a_list_parses_and_the_collection_may_contain_dots_and_colons() {
        let a = ok("shop.c1:1:create_index,shop.c2:2,shop.v1.2:5:delete,shop.a:b:4");
        assert_eq!(a.len(), 4);
        assert_eq!(a[2].coll, "v1.2");
        assert_eq!(a[3].coll, "a:b", "a colon in a collection name stays in the name");
        assert_eq!(a[3].n, 4);
    }

    #[test]
    fn a_value_that_does_not_parse_unambiguously_arms_nothing() {
        for bad in [
            "",
            "shop.orders",
            "shop.orders:",
            "shop.orders:x",
            "shop:1",
            ".orders:1",
            "shop.:1",
            "sh:op.orders:1",
            "shop.orders:1,",
            "shop.orders:1:nonsense",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn n_zero_parses_and_loses_nothing() {
        let a = ok("shop.orders:0");
        assert_eq!(a[0].n, 0);
    }

    /// The one test that arms the process-wide hook. It loses the second *insert* of
    /// `hook.lost`, counts nothing of `CreateCollection`, and loses exactly one.
    #[test]
    fn the_nth_counted_entry_is_witnessed_and_not_written_and_only_once() {
        use bson::doc;
        assert_eq!(arm("hook.lost:2:insert,hook.other:1,hook.dup:2:insert"), 3);
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let a = crate::Engine::open(&dir_a.path().join("kimmy.redb")).unwrap();
        let b = crate::Engine::open(&dir_b.path().join("kimmy.redb")).unwrap();
        let ca = a.create_collection("hook", "lost").unwrap();
        for i in 1..=3i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }
        // An update of document 1 is not an insert, so it does not count.
        a.replace(&ca, &kimmy_core::DocId::Int64(1), doc! { "_id": 1, "v": 1 }, false).unwrap();
        let entries = a
            .entries_for_peer(kimmy_core::Hlc::ZERO, 1_000, crate::WalkScope::Background)
            .unwrap()
            .entries;
        b.apply_batch(&entries).unwrap();
        let cb = b.get_collection("hook", "lost").unwrap();
        let held = |id: i64| b.get(&cb, &kimmy_core::DocId::Int64(id)).unwrap().is_some();
        assert!(held(1) && !held(2) && held(3), "document 2 (the second insert) is the one lost");
        // Witnessed: the vector covers it, so a pull from the position does not bring it back.
        assert!(b.witnessed_vector().unwrap().covers(&a.witnessed_vector().unwrap()));
        // One loss for good: the batch that lost it, applied again (it failed after the loss,
        // or a window overlapped), loses the same entry again and no other.
        b.apply_batch(&entries).unwrap();
        assert!(!held(2), "the retry of the batch applied the entry the hook lost");
        assert!(held(1) && held(3));

        // A re-delivery of an entry already applied is not a count: the second *applied*
        // insert of `hook.dup` is the one lost, not the second delivery.
        let dup = a.create_collection("hook", "dup").unwrap();
        a.insert(&dup, doc! { "_id": 1 }).unwrap();
        let first =
            a.entries_for_peer(kimmy_core::Hlc::ZERO, 1_000, crate::WalkScope::Background).unwrap();
        b.apply_batch(&first.entries).unwrap();
        b.apply_batch(&first.entries).unwrap();
        a.insert(&dup, doc! { "_id": 2 }).unwrap();
        let all =
            a.entries_for_peer(kimmy_core::Hlc::ZERO, 1_000, crate::WalkScope::Background).unwrap();
        b.apply_batch(&all.entries).unwrap();
        let cd = b.get_collection("hook", "dup").unwrap();
        let has = |id: i64| b.get(&cd, &kimmy_core::DocId::Int64(id)).unwrap().is_some();
        assert!(has(1) && !has(2), "a re-delivery was counted as an applied entry");
    }
}
