//! This member's own block for the replication contact (ADR-201): what the
//! store and the configuration say about who may hand it work.

use std::sync::{Arc, Weak};

use kimmy_cluster::catchup::CatchUp;
use kimmy_cluster::{Facts, FactsSource, TtlHeld};
use kimmy_storage::{CollectionMeta, Engine, ttl_indexes};

/// The block, read from the store's schema and the configuration. `boot` is
/// this process's random id; `ttl_disabled` and `embeddings_disabled` are the
/// operator's choices (`storage.ttl_interval_secs = 0`,
/// `vector.worker_enabled = false`).
pub fn source(
    engine: Weak<Engine>,
    boot: Vec<u8>,
    ttl_disabled: bool,
    embeddings_disabled: bool,
    catch_up: Option<Arc<CatchUp>>,
) -> FactsSource {
    Arc::new(move || {
        let (catching_up, catching_up_reason) =
            catch_up.as_ref().map_or((false, None), |c| (c.is_set(), c.marker().map(|m| m.reason)));
        let base = Facts {
            boot: boot.clone(),
            ttl_disabled,
            embeddings_disabled,
            catching_up,
            catching_up_reason,
            ..Facts::default()
        };
        let Some(engine) = engine.upgrade() else { return base };
        // A schema that cannot be read is sent as holding nothing: the peers
        // then do not count this member a holder, which fails toward another
        // holder taking the work, and the next read may succeed.
        let collections = engine.all_collections().unwrap_or_default();
        base.with_ttl(held(&collections))
    })
}

/// The collections with at least one TTL index, each with a digest of those
/// indexes' definitions.
pub(crate) fn held(collections: &[CollectionMeta]) -> Vec<TtlHeld> {
    collections
        .iter()
        .filter_map(|coll| {
            let mut defs: Vec<String> = ttl_indexes(coll)
                .map(|index| {
                    let fields: Vec<String> = index
                        .fields
                        .iter()
                        .map(|f| format!("{}{}", f.path, if f.descending { "-" } else { "+" }))
                        .collect();
                    format!("{}\0{}\0{:?}", index.name, fields.join(","), index.expire_after_secs)
                })
                .collect();
            if defs.is_empty() {
                return None;
            }
            defs.sort();
            Some(TtlHeld { collection: coll.id, digest: digest(&defs.join("\n")).to_vec() })
        })
        .collect()
}

/// FNV-1a over the canonical text: eight bytes, stable across processes and
/// releases, which is what a digest compared between members needs.
fn digest(text: &str) -> [u8; 8] {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.as_bytes() {
        h ^= u64::from(*byte);
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h.to_be_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> (Arc<Engine>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap()), dir)
    }

    fn ttl(engine: &Engine, db: &str, coll: &str, secs: i64) {
        engine.create_collection(db, coll).unwrap();
        engine
            .create_index_with(
                db,
                coll,
                vec![kimmy_storage::IndexField { path: "at".into(), descending: false }],
                false,
                Default::default(),
                Some("ttl".into()),
                Some(secs),
                None,
            )
            .unwrap();
    }

    /// A collection with a TTL index is listed, one without is not, and the
    /// digest follows the definition.
    #[test]
    fn the_block_lists_the_collections_that_hold_a_ttl_index() {
        let (engine, _dir) = engine();
        ttl(&engine, "app", "sessions", 60);
        engine.create_collection("app", "plain").unwrap();
        let source = source(Arc::downgrade(&engine), vec![1; 16], false, true, None);
        let block = source();
        assert_eq!(block.boot, vec![1; 16]);
        assert!(block.embeddings_disabled && !block.ttl_disabled);
        let sessions = engine.get_collection("app", "sessions").unwrap();
        let plain = engine.get_collection("app", "plain").unwrap();
        assert!(block.holds_ttl(sessions.id));
        assert!(!block.holds_ttl(plain.id));
        assert_eq!(block.ttl.len(), 1);
        assert_eq!(block.ttl[0].digest.len(), 8);

        let (other, _dir2) = engine_pair();
        ttl(&other, "app", "sessions", 120);
        let other_block = super::source(Arc::downgrade(&other), vec![2; 16], false, false, None)();
        assert_ne!(block.ttl[0].digest, other_block.ttl[0].digest, "a different definition");
    }

    fn engine_pair() -> (Arc<Engine>, tempfile::TempDir) {
        engine()
    }

    /// With the engine gone the block still says who it is, and lists nothing.
    #[test]
    fn a_block_without_an_engine_lists_nothing() {
        let (engine, _dir) = engine();
        let weak = Arc::downgrade(&engine);
        drop(engine);
        let block = source(weak, vec![3; 16], true, false, None)();
        assert_eq!(block.boot, vec![3; 16]);
        assert!(block.ttl.is_empty() && block.ttl_disabled);
    }

    /// The block carries the marker's state live: the bit and the reason while it
    /// is set (which is what makes peers leave this member out of the candidates),
    /// none once it clears, and none for a member with no marker at all.
    #[test]
    fn the_block_says_when_the_catching_up_marker_is_set() {
        let (engine, _dir) = engine();
        let marker_dir = tempfile::tempdir().unwrap();
        let catch_up = CatchUp::open(marker_dir.path(), std::time::Duration::from_secs(120));
        let with =
            source(Arc::downgrade(&engine), vec![1; 16], false, false, Some(Arc::clone(&catch_up)));
        let without = source(Arc::downgrade(&engine), vec![1; 16], false, false, None);

        assert!(!with().catching_up && with().catching_up_reason.is_none());
        catch_up.mark(kimmy_cluster::CatchUpReason::Restored).unwrap();
        let block = with();
        assert!(block.catching_up, "{block:?}");
        assert_eq!(block.catching_up_reason, Some(kimmy_cluster::CatchUpReason::Restored));
        assert!(!without().catching_up, "a member with no marker is never catching up");
        catch_up.clear("the test");
        assert!(!with().catching_up && with().catching_up_reason.is_none());
    }
}
