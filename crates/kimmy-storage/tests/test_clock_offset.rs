//! `KIMMY_TEST_CLOCK_OFFSET_SECS` (docs/testing.md): the process-wide offset of the physical clock the HLC and the clock bound
//! read. Its own test binary, so that setting the offset cannot reach another test's clock, and one test, so that its phases
//! do not race each other.

use std::time::{SystemTime, UNIX_EPOCH};

use kimmy_storage::{
    Engine, OwnStampRefusal, StorageError, parse_test_clock_offset_secs, physical_now_ms,
    set_test_clock_offset_secs,
};

fn wall_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

#[test]
fn the_value_is_a_signed_whole_number_of_seconds() {
    assert_eq!(parse_test_clock_offset_secs("120"), Some(120));
    assert_eq!(parse_test_clock_offset_secs("+120"), Some(120));
    assert_eq!(parse_test_clock_offset_secs("-30"), Some(-30));
    assert_eq!(parse_test_clock_offset_secs(" 7 "), Some(7));
    assert_eq!(parse_test_clock_offset_secs("1.5"), None);
    assert_eq!(parse_test_clock_offset_secs(""), None);
    assert_eq!(parse_test_clock_offset_secs("ahead"), None);
}

#[test]
fn the_offset_moves_the_clock_the_hlc_and_the_bound_read_and_nothing_else() {
    // Unset: the physical clock is the wall clock.
    let skew = physical_now_ms().abs_diff(wall_ms());
    assert!(skew < 2_000, "unset changes nothing: {skew} ms");

    // Set: the clock is shifted by the offset, in seconds.
    set_test_clock_offset_secs(400);
    let shifted = physical_now_ms().saturating_sub(wall_ms());
    assert!(
        (399_000..=401_000).contains(&shifted),
        "+400 s shifts the clock by about 400 s: {shifted} ms"
    );

    // A member that wrote under the shifted clock, restarted without it, leads its own clock by about the offset: the
    // lead gauge shows it, and schema changes are refused once it is past the bound (b1).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = Engine::open(&path).unwrap();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, bson::doc! { "_id": 1 }).unwrap();
        assert_eq!(engine.clock_lead_ms(), 0, "on its own shifted clock the member does not lead");
    }
    set_test_clock_offset_secs(0);
    let engine = Engine::open(&path).unwrap();
    let lead = engine.clock_lead_ms();
    assert!(
        (395_000..=401_000).contains(&lead),
        "restarted without the offset it leads by about 400 s: {lead} ms"
    );
    match engine.create_collection("shop", "more") {
        Err(StorageError::OwnStampsRefused(OwnStampRefusal::ClockAhead { lead_ms, bound_ms })) => {
            assert!(lead_ms > bound_ms, "b1 fires above the bound: {lead_ms} > {bound_ms}");
        }
        other => panic!("a lead of 400 s is past the 300 s bound: {other:?}"),
    }
    drop(engine);

    // Below the bound it does not fire.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    set_test_clock_offset_secs(100);
    {
        let engine = Engine::open(&path).unwrap();
        engine.create_collection("shop", "orders").unwrap();
    }
    set_test_clock_offset_secs(0);
    let engine = Engine::open(&path).unwrap();
    let lead = engine.clock_lead_ms();
    assert!((95_000..=101_000).contains(&lead), "a lead of about 100 s: {lead} ms");
    engine.create_collection("shop", "more").expect("100 s is inside the 300 s bound");
}
