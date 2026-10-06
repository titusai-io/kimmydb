//! The wire additions of ADR-213, at the codec: `class_state`, `class_cause`,
//! `responsive` and `started_ms` in the block, `facts_gen` and `echo` on the
//! frames. Every field is optional and **lenient**: a field of another shape costs
//! the field, never the block or the frame (A8), and a missing class is `unknown`,
//! never `idle`.

use std::sync::Arc;

use bson::{Bson, doc};
use serde::Deserialize;

use crate::facts::{ClassState, Facts, PerClass, StallCause, TtlHeld};
use crate::protocol::{Echo, Message};
use kimmy_core::CollectionId;

fn facts_of(body: bson::Document) -> Facts {
    bson::deserialize_from_slice(&bson::serialize_to_vec(&body).unwrap()).unwrap()
}

fn frame_of(body: bson::Document) -> Message {
    bson::deserialize_from_slice(&bson::serialize_to_vec(&body).unwrap()).unwrap()
}

/// Every case of the probe: each decodes with `catching_up` intact, and what it
/// cost is only the field.
#[test]
fn every_odd_shape_costs_the_field_and_never_the_block() {
    let nested = facts_of(doc! {
        "catching_up": true,
        "class_state": { "ttl": { "nested": 1 }, "webhooks": "ok", "embeddings": "stalled" },
    });
    assert!(nested.catching_up);
    let states = nested.class_state.unwrap();
    assert_eq!(
        (states.ttl, states.webhooks, states.embeddings),
        (ClassState::Unknown, ClassState::Ok, ClassState::Stalled),
        "a nested document where a string was reads unknown for that class only"
    );

    let integer = facts_of(doc! { "catching_up": true, "class_state": 7 });
    assert!(integer.catching_up);
    assert_eq!(integer.class_state, Some(PerClass::all(ClassState::Unknown)));

    let string_responsive = facts_of(doc! { "catching_up": true, "responsive": "yes" });
    assert!(string_responsive.catching_up);
    assert_eq!(string_responsive.responsive, None);
    assert_eq!(facts_of(doc! { "responsive": true }).responsive, Some(true));
    assert_eq!(facts_of(doc! { "responsive": false }).responsive, Some(false));

    let null =
        facts_of(doc! { "catching_up": true, "class_state": Bson::Null, "responsive": Bson::Null });
    assert!(null.catching_up);
    assert_eq!((null.class_state, null.responsive), (None, None));

    // An older sender: the keys are absent, which is `None`, meaning not a target.
    let absent = facts_of(doc! { "catching_up": true });
    assert_eq!(
        (absent.class_state, absent.class_cause, absent.responsive, absent.started_ms),
        (None, None, None, None)
    );
}

/// `writer_wedged` (ADR-220) reads a bool as a bool; a string, a number, a null
/// and an absent key all read `None`, which is *not known*: never `false`, and
/// never `true`. A value that does not read does not spoil the block.
#[test]
fn writer_wedged_reads_a_bool_and_nothing_else() {
    assert_eq!(facts_of(doc! { "writer_wedged": true }).writer_wedged, Some(true));
    assert_eq!(facts_of(doc! { "writer_wedged": false }).writer_wedged, Some(false));
    for bad in [
        Bson::String("true".into()),
        Bson::Int32(1),
        Bson::Int64(1),
        Bson::Double(1.0),
        Bson::Null,
        Bson::Array(vec![Bson::Boolean(true)]),
        Bson::Document(doc! { "wedged": true }),
    ] {
        let facts = facts_of(doc! { "catching_up": true, "writer_wedged": bad.clone() });
        assert!(facts.catching_up, "{bad:?}: the rest of the block still reads");
        assert_eq!(facts.writer_wedged, None, "{bad:?}");
    }
    assert_eq!(facts_of(doc! { "catching_up": true }).writer_wedged, None, "absent");
    // It is set on the wire only when it is known, so a block that does not know
    // adds no key to what 0.44.0 decodes.
    let unknown = bson::serialize_to_bson(&Facts::default()).unwrap();
    assert!(!unknown.as_document().unwrap().contains_key("writer_wedged"));
    let known =
        bson::serialize_to_bson(&Facts { writer_wedged: Some(true), ..Facts::default() }).unwrap();
    assert!(known.as_document().unwrap().get_bool("writer_wedged").unwrap());
}

/// A missing class key, a non-string, or an unknown name is `unknown`, **never
/// `idle`**: a member that said nothing about a class is not a target in it.
#[test]
fn a_missing_class_is_unknown_and_never_idle() {
    let states = facts_of(doc! { "class_state": { "webhooks": "ok" } }).class_state.unwrap();
    assert_eq!(states.webhooks, ClassState::Ok);
    assert_eq!(states.ttl, ClassState::Unknown);
    assert_eq!(states.embeddings, ClassState::Unknown);
    let odd =
        facts_of(doc! { "class_state": { "ttl": "bogus", "webhooks": 3, "embeddings": "IDLE" } })
            .class_state
            .unwrap();
    assert_eq!(
        odd,
        PerClass::all(ClassState::Unknown),
        "unknown names, non-strings and a wrong case"
    );
    for (name, state) in [
        ("ok", ClassState::Ok),
        ("idle", ClassState::Idle),
        ("suspect", ClassState::Suspect),
        ("stalled", ClassState::Stalled),
    ] {
        let read = facts_of(doc! { "class_state": { "ttl": name } }).class_state.unwrap();
        assert_eq!(read.ttl, state);
    }
}

#[test]
fn the_cause_decodes_as_leniently() {
    let causes = facts_of(doc! {
        "class_cause": { "ttl": "local", "webhooks": "runtime", "embeddings": "probation" }
    })
    .class_cause
    .unwrap();
    assert_eq!(
        (causes.ttl, causes.webhooks, causes.embeddings),
        (StallCause::Local, StallCause::Runtime, StallCause::Probation)
    );
    let odd =
        facts_of(doc! { "class_cause": { "ttl": 1, "webhooks": "later" } }).class_cause.unwrap();
    assert_eq!(odd, PerClass::all(StallCause::Unknown));
    assert_eq!(
        facts_of(doc! { "class_cause": "x" }).class_cause,
        Some(PerClass::all(StallCause::Unknown))
    );
}

#[test]
fn the_start_time_is_a_non_negative_integer_or_nothing() {
    assert_eq!(
        facts_of(doc! { "started_ms": 1_700_000_000_000_i64 }).started_ms,
        Some(1_700_000_000_000)
    );
    assert_eq!(facts_of(doc! { "started_ms": 5_i32 }).started_ms, Some(5));
    assert_eq!(facts_of(doc! { "started_ms": -5_i64 }).started_ms, None);
    assert_eq!(facts_of(doc! { "started_ms": "now" }).started_ms, None);
    assert_eq!(facts_of(doc! { "started_ms": 1.5 }).started_ms, None);
}

/// The same cases inside a frame: the frame decodes, the block's odd field costs
/// itself, and the frame's own odd fields cost themselves.
#[test]
fn the_frames_decode_whatever_the_new_fields_hold() {
    let frame = frame_of(doc! { "Vectors": {
        "servable": {}, "witnessed": {},
        "facts": { "catching_up": true, "class_state": 3 },
        "facts_gen": "not a number",
        "echo": 12,
    } });
    let Message::Vectors { facts, facts_gen, echo, .. } = frame else { panic!() };
    let facts = facts.expect("the block decoded");
    assert!(facts.catching_up);
    assert_eq!(facts.class_state, Some(PerClass::all(ClassState::Unknown)));
    assert_eq!((facts_gen, echo), (None, None));

    let frame = frame_of(doc! { "AskVersions": {
        "witnessed": true,
        "facts": { "catching_up": true },
        "facts_gen": -4_i64,
        "echo": { "boot": "not binary", "generation": "x" },
    } });
    let Message::AskVersions { facts, facts_gen, echo, .. } = frame else { panic!() };
    assert!(facts.unwrap().catching_up);
    assert_eq!(facts_gen, None, "a negative generation is none");
    assert_eq!(echo, None, "an echo that does not decode is none: a peer that does not echo");

    let frame = frame_of(doc! { "AskVersions": {
        "facts_gen": 9_i64,
        "echo": { "boot": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![1, 2, 3] }, "generation": 7_i64 },
    } });
    let Message::AskVersions { facts_gen, echo, .. } = frame else { panic!() };
    assert_eq!(facts_gen, Some(9));
    assert_eq!(echo, Some(Echo { boot: vec![1, 2, 3], generation: 7 }));
}

/// 0.43's decoder, which knows none of the new keys, reads every new block and frame.
#[test]
fn a_043_decoder_reads_the_new_block_and_frames() {
    #[derive(Debug, Deserialize)]
    struct Facts043 {
        #[serde(default, with = "serde_bytes")]
        boot: Vec<u8>,
        #[serde(default)]
        catching_up: bool,
        #[serde(default)]
        ttl: Vec<TtlHeld>,
    }
    #[derive(Debug, Deserialize)]
    enum Message043 {
        AskVersions {
            #[serde(default)]
            witnessed: bool,
            #[serde(default)]
            facts: Option<Facts043>,
        },
        Vectors {
            #[serde(default)]
            facts: Option<Facts043>,
        },
    }
    let block = Facts {
        boot: vec![3; 16],
        catching_up: true,
        class_state: Some(PerClass::all(ClassState::Stalled)),
        class_cause: Some(PerClass::all(StallCause::Local)),
        responsive: Some(false),
        writer_wedged: Some(true),
        started_ms: Some(1_700_000_000_000),
        ..Facts::default()
    }
    .with_ttl(vec![TtlHeld { collection: CollectionId(5), digest: vec![1; 8] }]);
    let echo = Some(Echo { boot: vec![4; 16], generation: 12 });
    for message in [
        Message::AskVersions {
            witnessed: true,
            facts: Some(Arc::new(block.clone())),
            facts_gen: Some(3),
            echo: echo.clone(),
        },
        Message::Vectors {
            servable: Default::default(),
            witnessed: Default::default(),
            facts: Some(Arc::new(block.clone())),
            facts_gen: Some(3),
            echo,
        },
    ] {
        let bytes = bson::serialize_to_vec(&message).unwrap();
        let old: Message043 = bson::deserialize_from_slice(&bytes).unwrap();
        let facts = match old {
            Message043::AskVersions { facts, .. } | Message043::Vectors { facts } => facts.unwrap(),
        };
        assert!(facts.catching_up);
        assert_eq!(facts.boot, vec![3; 16]);
        assert_eq!(facts.ttl.len(), 1);
    }
}

/// This build reading what 0.43 sends: no `class_state` (so not a target), no
/// `facts_gen`, and no echo (so a peer that never confirms).
#[test]
fn this_build_reads_a_043_block_and_frames() {
    let frame = frame_of(doc! { "Vectors": {
        "servable": {}, "witnessed": {},
        "facts": { "boot": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![8; 16] }, "yielding": { "ttl": true } },
    } });
    let Message::Vectors { facts, facts_gen, echo, .. } = frame else { panic!() };
    let facts = facts.unwrap();
    assert!(facts.yielding.ttl);
    assert_eq!(
        (facts.class_state, facts.class_cause, facts.responsive, facts.started_ms),
        (None, None, None, None)
    );
    assert_eq!((facts_gen, echo), (None, None));
    let frame = frame_of(doc! { "AskVersions": { "witnessed": true } });
    assert!(matches!(
        frame,
        Message::AskVersions { facts: None, facts_gen: None, echo: None, witnessed: true }
    ));
}

/// A block and a frame that set none of the new fields encode exactly as 0.43's do:
/// no new key appears.
#[test]
fn unset_fields_add_no_keys() {
    let facts = bson::serialize_to_bson(&Facts::default()).unwrap();
    let doc = facts.as_document().unwrap();
    for key in ["class_state", "class_cause", "responsive", "writer_wedged", "started_ms"] {
        assert!(!doc.contains_key(key), "{key}");
    }
    let frame = bson::serialize_to_bson(&Message::AskVersions {
        witnessed: true,
        facts: None,
        facts_gen: None,
        echo: None,
    })
    .unwrap();
    let body = frame.as_document().unwrap().get_document("AskVersions").unwrap();
    assert!(!body.contains_key("facts_gen") && !body.contains_key("echo"));
}

/// Generations are per-process counters, encoded as BSON `Int64`; one above
/// `i64::MAX` fails to serialise, which a counter never reaches.
#[test]
fn a_generation_round_trips_as_an_int64() {
    for generation in [0u64, 1, u64::from(u32::MAX) + 1, 1 << 62, i64::MAX as u64] {
        let message = Message::AskVersions {
            witnessed: true,
            facts: None,
            facts_gen: Some(generation),
            echo: Some(Echo { boot: vec![1; 16], generation }),
        };
        let bytes = bson::serialize_to_vec(&message).unwrap();
        let back: Message = bson::deserialize_from_slice(&bytes).unwrap();
        assert_eq!(back, message, "generation {generation}");
        let raw: bson::Document = bson::deserialize_from_slice(&bytes).unwrap();
        let body = raw.get_document("AskVersions").unwrap();
        assert!(matches!(body.get("facts_gen"), Some(Bson::Int64(_))));
    }
    let too_big = Message::AskVersions {
        witnessed: true,
        facts: None,
        facts_gen: Some(i64::MAX as u64 + 1),
        echo: None,
    };
    assert!(bson::serialize_to_vec(&too_big).is_err(), "never reached by a counter");
}

/// A whole block with every new field set survives a round trip.
#[test]
fn a_full_block_round_trips() {
    let block = Facts {
        boot: vec![3; 16],
        class_state: Some(PerClass {
            ttl: ClassState::Ok,
            webhooks: ClassState::Suspect,
            embeddings: ClassState::Stalled,
        }),
        class_cause: Some(PerClass {
            ttl: StallCause::Unknown,
            webhooks: StallCause::Runtime,
            embeddings: StallCause::Probation,
        }),
        responsive: Some(true),
        started_ms: Some(42),
        ..Facts::default()
    };
    let back: Facts =
        bson::deserialize_from_slice(&bson::serialize_to_vec(&block).unwrap()).unwrap();
    assert_eq!(back, block);
}
