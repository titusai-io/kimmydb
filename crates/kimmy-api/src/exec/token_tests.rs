//! A client's cursor token across a 0.50/0.51 roll (Decimal128 design 7.2): this build
//! issues the schema-4 key of the last id for every id schema 4 can represent, `FF` and the
//! stored key for a `Decimal128` id, and continues either. A token a 0.50 member issued
//! continues here, and one issued here continues on a 0.50 member, whose reader is written
//! below as it pages: strictly after the token's bytes, in schema-4 key order.
//!
//! The ids include the A2 pair (design 6.1): the stored key of [`A`] is the schema-4 key of
//! [`B`], so a token read in the wrong form here resumes after `B` and skips what lies
//! between.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;

use super::tests::live_state;
use super::*;

const A: i64 = 0x5000_0081_0180_0000;
const B: i64 = 0x5000_7F80_8101_8000;

fn root() -> Auth {
    Auth(kimmy_auth::Principal::superuser("test"))
}

/// The schema-4 key of an integer `_id`, from the layout: tag, sign, the exponent of the
/// leading bit biased by 32,768, the magnitude shifted until its top bit is set, all ten
/// inverted when negative. What a 0.50 member's token carries.
fn v1(v: i64) -> Vec<u8> {
    if v == 0 {
        return vec![0x20, 0x03];
    }
    let magnitude = v.unsigned_abs();
    let exp = 63 - magnitude.leading_zeros() as i32;
    let mut body = ((exp + 32_768) as u16).to_be_bytes().to_vec();
    body.extend_from_slice(&(magnitude << magnitude.leading_zeros()).to_be_bytes());
    if v < 0 {
        for byte in &mut body {
            *byte = !*byte;
        }
    }
    let mut out = vec![0x20, if v < 0 { 0x02 } else { 0x04 }];
    out.extend(body);
    out
}

fn token_of(key: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(key)
}

fn ids() -> Vec<i64> {
    vec![-3, 0, 1, 2, A, A + 1, B - 1, B, B + 1, i64::MAX]
}

fn seeded(dir: &tempfile::TempDir) -> SharedState {
    let state = live_state(dir);
    let coll = state.engine.create_collection("app", "docs").unwrap();
    state
        .engine
        .insert_many(&coll, ids().into_iter().map(|id| bson::doc! { "_id": id }).collect())
        .unwrap();
    state
}

/// One page: its ids, and the token it offers.
fn page(state: &SharedState, cursor: Option<String>, limit: usize) -> (Vec<i64>, Option<String>) {
    let out = find(
        state,
        &root(),
        "app",
        "docs",
        FindParams { cursor, limit: Some(limit), ..Default::default() },
    )
    .unwrap();
    let ids = out["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_i64().expect("an integer id"))
        .collect();
    (ids, out.get("nextCursor").and_then(Value::as_str).map(String::from))
}

/// Every token this build issues is the schema-4 key of the page's last id, and a walk by
/// them sees every id once, in order, whatever the page size.
#[test]
fn a_token_is_the_schema_4_key_of_the_last_id_and_a_walk_sees_every_id_once() {
    let dir = tempfile::tempdir().unwrap();
    let state = seeded(&dir);
    for limit in [1, 2, 3, 4] {
        let (mut seen, mut cursor): (Vec<i64>, _) = (Vec::new(), None);
        // Bounded: a token read in the wrong form can resume at the row it names, for ever.
        for _ in 0..=ids().len() {
            let (ids, next) = page(&state, cursor, limit);
            seen.extend(&ids);
            let Some(next) = next else { break };
            let last = *ids.last().unwrap();
            assert_eq!(URL_SAFE_NO_PAD.decode(&next).unwrap(), v1(last), "after {last:#x}");
            cursor = Some(next);
        }
        assert_eq!(seen, ids(), "page size {limit}: every id once, and the walk ends");
    }
}

/// A token a 0.50 member issued resumes here right after the id it names, the A2 pair's
/// included.
#[test]
fn a_token_a_0_50_member_issued_continues_here() {
    let dir = tempfile::tempdir().unwrap();
    let state = seeded(&dir);
    let all = ids();
    for (i, id) in all.iter().enumerate() {
        let (resumed, _) = page(&state, Some(token_of(&v1(*id))), 100);
        assert_eq!(resumed, all[i + 1..].to_vec(), "after {id:#x}");
    }
}

/// A token issued here resumes on a 0.50 member right where this build's next page starts.
#[test]
fn a_token_issued_here_continues_on_a_0_50_member() {
    let dir = tempfile::tempdir().unwrap();
    let state = seeded(&dir);
    let mut by_v1 = ids();
    by_v1.sort_by_key(|id| v1(*id));
    for limit in [1, 2, 3] {
        let mut cursor = None;
        let mut pages = 0;
        loop {
            pages += 1;
            assert!(pages <= ids().len() + 1, "a walk that does not end (page size {limit})");
            let (_, next) = page(&state, cursor, limit);
            let Some(next) = next else { break };
            let bound = URL_SAFE_NO_PAD.decode(&next).unwrap();
            let old: Vec<i64> = by_v1.iter().copied().filter(|id| v1(*id) > bound).collect();
            let (here, _) = page(&state, Some(next.clone()), 100);
            assert_eq!(old, here, "the same continuation on both builds (page size {limit})");
            cursor = Some(next);
        }
    }
}

/// A `Decimal128` id has no schema-4 key: its token is `FF` and the stored key, and it
/// continues here.
#[test]
fn a_decimal_ids_token_is_marked_and_continues() {
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    let coll = state.engine.create_collection("app", "docs").unwrap();
    let decimal = |text: &str| bson::Bson::Decimal128(text.parse().unwrap());
    for id in [bson::Bson::Int64(1), decimal("1.5"), decimal("2.5"), bson::Bson::Int64(3)] {
        state.engine.insert(&coll, bson::doc! { "_id": id }).unwrap();
    }
    let out =
        find(&state, &root(), "app", "docs", FindParams { limit: Some(2), ..Default::default() })
            .unwrap();
    let token = out["nextCursor"].as_str().expect("a token").to_string();
    let raw = URL_SAFE_NO_PAD.decode(&token).unwrap();
    let stored = kimmy_core::keyenc::encode(&decimal("1.5")).unwrap();
    assert_eq!(raw[0], 0xFF, "marked");
    assert_eq!(&raw[1..], stored.as_slice(), "the stored key behind the mark");
    assert_eq!(kimmy_core::Cursor::decode(&token).unwrap().key(), stored.as_slice());
    let rest = find(
        &state,
        &root(),
        "app",
        "docs",
        FindParams { cursor: Some(token), limit: Some(10), ..Default::default() },
    )
    .unwrap();
    assert_eq!(rest["documents"].as_array().unwrap().len(), 2, "2.5 and 3: {rest}");
}
