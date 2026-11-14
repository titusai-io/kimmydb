//! The depth limit counts the path and the value together: a cell at a path of N segments holds a
//! value at most 100 - N levels deep. Past it the write is refused before anything is written
//! (`BadValue(TooDeep)` where the entry names a refusal, `None` where it does not), at every entry
//! that takes a path and a value and in the effects composition.

use super::tests::{i, opts, wid};
use super::*;
use crate::effects::UpsertEffects;

/// `levels` documents nested one in the next, the innermost holding a scalar (0 levels: the scalar).
fn nested(levels: usize) -> Value {
    let mut v = i(1);
    for _ in 0..levels {
        v = Value::Document(vec![("k".to_string(), v)]);
    }
    v
}

fn path_of(segments: usize) -> Path {
    Path::new((0..segments).map(|n| format!("p{n}"))).unwrap()
}

fn seeded() -> Writer {
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&[(Path::flat("x").unwrap(), i(1))], 0).unwrap();
    w
}

const WIDTHS: [usize; 5] = [1, 2, 3, 50, 100];

#[test]
fn check_input_under_counts_the_path_with_the_value() {
    for n in WIDTHS {
        assert_eq!(nested(100 - n).check_input_under(n), Ok(()), "path {n}, value {}", 100 - n);
        assert_eq!(
            nested(101 - n).check_input_under(n),
            Err(InvalidValue::TooDeep),
            "path {n}, value {}",
            101 - n
        );
    }
    // an array nest counts the same way
    let mut arr = i(1);
    for _ in 0..(100 - 3) {
        arr = Value::Array(vec![arr]);
    }
    assert_eq!(arr.check_input_under(3), Ok(()));
    assert_eq!(Value::Array(vec![arr]).check_input_under(3), Err(InvalidValue::TooDeep));
}

#[test]
fn the_writer_entries_refuse_a_value_too_deep_for_its_path() {
    for n in WIDTHS {
        let (p, ok, bad) = (path_of(n), nested(100 - n), nested(101 - n));
        assert!(seeded().write_set(&p, ok.clone(), 0).is_some(), "write_set ok, path {n}");
        assert!(seeded().write_set(&p, bad.clone(), 0).is_none(), "write_set bad, path {n}");
        assert!(
            seeded().write_update(&[UpdateOp::Set(p.clone(), ok.clone())], 0).unwrap().is_some(),
            "write_update ok, path {n}"
        );
        match seeded().write_update(&[UpdateOp::Set(p.clone(), bad.clone())], 0) {
            Err(UpdateRefused::BadValue(q, InvalidValue::TooDeep)) => assert_eq!(q, p),
            other => panic!("write_update bad, path {n}: {other:?}"),
        }
        let z = Path::flat("z").unwrap();
        let mut fresh = Writer::new(wid('A', 0), opts());
        assert!(
            fresh.upsert_soi_create(&[], &z, &[(p.clone(), ok.clone())], 0).is_some(),
            "upsert ok, path {n}"
        );
        let mut fresh = Writer::new(wid('A', 0), opts());
        assert!(
            fresh.upsert_soi_create(&[], &z, &[(p.clone(), bad.clone())], 0).is_none(),
            "upsert bad, path {n}"
        );
        assert!(fresh.state.lives.is_empty(), "a refusal changes nothing");
    }
}

#[test]
fn the_composition_refuses_a_value_too_deep_for_its_path() {
    for n in WIDTHS {
        let (p, ok, bad) = (path_of(n), nested(100 - n), nested(101 - n));
        for layer in 0..3 {
            let mk = |v: Value| match layer {
                0 => UpsertEffects { from_filter: vec![(p.clone(), v)], ..Default::default() },
                1 => UpsertEffects { set_on_insert: vec![(p.clone(), v)], ..Default::default() },
                _ => UpsertEffects { set: vec![(p.clone(), v)], ..Default::default() },
            };
            assert!(mk(ok.clone()).creation_cells().is_ok(), "layer {layer}, path {n}");
            match mk(bad.clone()).creation_cells() {
                Err(UpdateRefused::BadValue(_, InvalidValue::TooDeep)) => {}
                other => panic!("layer {layer}, path {n}: {other:?}"),
            }
        }
    }
}
