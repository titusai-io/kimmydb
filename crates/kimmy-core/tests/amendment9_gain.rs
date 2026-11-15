//! Amendment 9 through the public API only (so it runs on the code before the change and after it).
//!
//! `{n: Int32 0}`, `$inc` by a Decimal128, `$set` `Int32 5`, `$inc` by an `Int32`: what the counter reads is the
//! base plus the operand, an `Int32`, as the reference reads it. Before the change the gain after the absorb was
//! the 34-digit difference of the totals: with `1E-34` or `1E+34` as the decimal operand the integer is lost in the
//! total and the counter read a Decimal128, or a wrong integer. A Decimal128 counter is exact or refused now, so
//! those two histories end in a refusal of the last `$inc` (its slot total would round); the histories at the
//! edge of 34 digits (`1E-33`, `1E+33`) are exact and read the reference's `Int32`.

use kimmy_core::ids::NodeId;
use kimmy_core::merge_state::{
    Epoch, JoinOptions, Path, UpdateOp, UpdateRefused, Value, Writer, WriterId,
};

fn writer() -> Writer {
    let id = WriterId { node: NodeId::from_bytes([b'A'; 16]), epoch: Epoch::from_u64(0) };
    Writer::new(id, JoinOptions::default())
}

fn p(s: &str) -> Path {
    Path::flat(s).expect("a valid name")
}

fn dec(t: &str) -> Value {
    Value::Decimal128(t.parse::<bson::Decimal128>().unwrap().bytes())
}

/// The history, and what the last `$inc` and the read gave.
fn history(decimal_operand: &str, int_operand: i32) -> (Result<(), UpdateRefused>, Option<Value>) {
    let o = JoinOptions::default();
    let n = p("n");
    let mut w = writer();
    w.insert(&[(n.clone(), Value::Int32(0))], 0).unwrap();
    w.write_inc_value(&n, &dec(decimal_operand), 0).unwrap();
    w.write_set(&n, Value::Int32(5), 0).unwrap();
    let last = w.write_update(&[UpdateOp::Inc(n.clone(), Value::Int32(int_operand))], 0);
    (last.map(|_| ()), w.state.value(&n, &o))
}

/// The two histories of the review of the typed counters: the reference reads `Int32 6` and `Int32 12`. The
/// counter either refuses the last `$inc` (its slot total `1E-34 + 1` or `1E+34 + 7` has 35 digits) or reads
/// that integer; it never reads a Decimal128 or another integer.
#[test]
fn the_reviewed_histories_are_refused_or_read_the_reference_integer() {
    for (decimal_operand, int_operand, want) in [("1E-34", 1, 6), ("1E+34", 7, 12)] {
        let (last, value) = history(decimal_operand, int_operand);
        match last {
            Err(UpdateRefused::IncOutOfRange(_)) => {}
            Ok(()) => assert_eq!(value, Some(Value::Int32(want)), "{decimal_operand}"),
            Err(e) => panic!("{decimal_operand}: {e:?}"),
        }
    }
}

/// The same histories at the edge of what 34 digits hold: the slot totals are exact, the last `$inc` is accepted
/// and the counter reads the reference's `Int32`.
#[test]
fn the_histories_at_the_edge_of_34_digits_read_the_reference_integer() {
    for (decimal_operand, int_operand, want) in [("1E-33", 1, 6), ("1E+33", 7, 12)] {
        let (last, value) = history(decimal_operand, int_operand);
        assert_eq!(last, Ok(()), "{decimal_operand}");
        assert_eq!(value, Some(Value::Int32(want)), "{decimal_operand}");
    }
}
