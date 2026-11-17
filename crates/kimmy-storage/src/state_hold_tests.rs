//! Step 3, item (3.3): the hole a state-only apply leaves (`state_held`). The recorder is raise-only, keyed by
//! the origin's node and epoch, and the one the grant uses still declines to record over what the servable
//! vector already covers.

use kimmy_core::{Epoch, Hlc, NodeId, Origin, Stamp};

use crate::Engine;
use crate::engine::WriterHolder;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn origin(node: u8, epoch: u64) -> Origin {
    Origin { node: NodeId::from_bytes([node; 16]), epoch: Epoch::from_u64(epoch) }
}

fn record_at(e: &Engine, origin: Origin, wall_ms: u64) -> bool {
    let txn = e.begin_write(WriterHolder::Replication).unwrap();
    let raised = Engine::record_state_held_at_in_txn(&txn, origin, Hlc::new(wall_ms, 0)).unwrap();
    txn.commit().unwrap();
    raised
}

#[test]
fn a_hole_only_rises_and_is_keyed_by_node_and_epoch() {
    let (e, _d) = engine();
    let (first, second) = (origin(7, 1), origin(7, 2));
    assert!(record_at(&e, first, 100), "a new row is a raise");
    assert!(!record_at(&e, first, 50), "a lower stamp raises nothing");
    assert_eq!(e.state_held().unwrap().get(first), Hlc::new(100, 0), "and does not lower the row");
    assert!(record_at(&e, first, 200));
    assert!(record_at(&e, second, 10), "another epoch of the same node is its own row");
    let held = e.state_held().unwrap();
    assert_eq!((held.get(first), held.get(second)), (Hlc::new(200, 0), Hlc::new(10, 0)));
}

#[test]
fn the_grants_recorder_still_declines_over_what_the_servable_vector_covers() {
    let (e, _d) = engine();
    let o = origin(9, 0);
    let stamp = Stamp::new(Hlc::new(100, 0), o.node, o.epoch);
    {
        let txn = e.begin_write(WriterHolder::Replication).unwrap();
        crate::engine::raise_version(&txn, crate::tables::OPLOG_VERSIONS, &stamp).unwrap();
        txn.commit().unwrap();
    }
    let txn = e.begin_write(WriterHolder::Replication).unwrap();
    assert!(!Engine::record_state_held_in_txn(&txn, &stamp).unwrap(), "covered: no hole");
    let above = Stamp::new(Hlc::new(200, 0), o.node, o.epoch);
    assert!(Engine::record_state_held_in_txn(&txn, &above).unwrap(), "above servable: a hole");
    txn.commit().unwrap();
    assert_eq!(e.state_held().unwrap().get(o), Hlc::new(200, 0));
}
