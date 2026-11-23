//! The refusal of a schema change addressed to a stage physical, keyed on the name mark (ADR-272, decision 15; manager ruling of
//! 2026-11-22): any DDL whose collection name carries `.__stg.` is refused before any lookup, create included, so the answer is the
//! same on every member whether or not the stage exists there. A refusal that depended on the stage being held would answer
//! "not found" on one member and the refusal on another.

use bson::doc;
use kimmy_core::{Error as CoreError, Hlc, IndexField, MergePolicy, ValidatorSpec};

use crate::{Engine, StorageError};

const DB: &str = "shop";
const REAL: &str = "orders.__stg.real";
const MADE_UP: &str = "orders.__stg.madeup";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn vector_config() -> kimmy_core::VectorConfig {
    kimmy_core::VectorConfig {
        fields: vec!["text".into()],
        provider: kimmy_core::ProviderConfig::Byo {},
        dim: 4,
        metric: Default::default(),
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

fn validator() -> ValidatorSpec {
    ValidatorSpec {
        schema: doc! { "bsonType": "object" },
        action: Default::default(),
        level: Default::default(),
    }
}

/// What an entry point answered: the stage-collection refusal's (name, logical, message), or a description of what else it did.
type Refusal = Result<(String, String, String), String>;

/// The refusal an entry point gave: its stage-collection error's (name, logical, message), or a description of what else it did.
fn refusal<T: std::fmt::Debug>(result: Result<T, StorageError>) -> Refusal {
    match result {
        Err(StorageError::Core(CoreError::StageCollection { name, logical })) => {
            let message =
                CoreError::StageCollection { name: name.clone(), logical: logical.clone() }
                    .to_string();
            Ok((name, logical, message))
        }
        other => Err(format!("{other:?}")),
    }
}

/// Every schema change a client or an operator can address to a collection by name, run against `name`.
fn every_entry_point(e: &Engine, name: &str) -> Vec<(&'static str, Refusal)> {
    vec![
        ("create_collection", refusal(e.create_collection(DB, name))),
        (
            "create_collection_with_policy",
            refusal(e.create_collection_with_policy(DB, name, MergePolicy::Document, None)),
        ),
        (
            "create_collection_with_validator",
            refusal(e.create_collection_with_validator(DB, name, validator())),
        ),
        ("drop_collection", refusal(e.drop_collection(DB, name))),
        (
            "create_index",
            refusal(e.create_index(
                DB,
                name,
                vec![IndexField { path: "a".into(), descending: false }],
                false,
                None,
            )),
        ),
        ("drop_index", refusal(e.drop_index(DB, name, "a_1"))),
        ("configure_vectors", refusal(e.configure_vectors(DB, name, vector_config()))),
        ("disable_vectors", refusal(e.disable_vectors(DB, name, false))),
        ("set_validator", refusal(e.set_validator(DB, name, Some(validator())))),
    ]
}

#[test]
fn a_made_up_stage_name_and_a_real_one_get_the_same_refusal() {
    let (e, _d) = engine();
    e.create_stage(DB, REAL, MergePolicy::Document, Hlc::new(crate::physical_now_ms() + 60_000, 0))
        .unwrap();
    let real = every_entry_point(&e, REAL);
    let made_up = every_entry_point(&e, MADE_UP);
    for ((what, real), (_, made_up)) in real.into_iter().zip(made_up) {
        let (name, logical, real_message) =
            real.unwrap_or_else(|got| panic!("{what} on a real stage: {got}"));
        assert_eq!((name.as_str(), logical.as_str()), (REAL, "orders"), "{what}");
        let (name, logical, made_up_message) =
            made_up.unwrap_or_else(|got| panic!("{what} on a made-up stage name: {got}"));
        assert_eq!((name.as_str(), logical.as_str()), (MADE_UP, "orders"), "{what}");
        assert_eq!(
            real_message.replace(REAL, "<name>"),
            made_up_message.replace(MADE_UP, "<name>"),
            "{what}: the text is the same whether or not the stage exists here"
        );
    }
    assert!(e.get_collection(DB, MADE_UP).is_err(), "and the refusal created nothing");
}

#[test]
fn a_name_that_only_looks_like_a_stage_is_an_ordinary_name() {
    // `orders.__stg` carries the `__` segment but not the whole mark: the ordinary name rules judge it, not this refusal
    let (e, _d) = engine();
    for fine in ["orders.stg.zz", "orders_stg_zz", "orders.__stg"] {
        e.create_collection(DB, fine)
            .unwrap_or_else(|got| panic!("{fine} carries no stage mark: {got:?}"));
    }
}

#[test]
fn the_system_path_that_makes_a_stage_is_not_refused() {
    let (e, _d) = engine();
    e.create_stage(DB, REAL, MergePolicy::Document, Hlc::new(crate::physical_now_ms() + 60_000, 0))
        .unwrap();
    assert!(e.get_collection(DB, REAL).is_ok());
}

#[test]
fn every_local_schema_change_refuses_a_stage_name_before_anything_else() {
    // Every function that guards a local schema change with the own-stamps refusal (U1 (b)) must refuse the stage name FIRST: the
    // name is the whole question, and a state-dependent refusal ahead of it would answer differently on different members.
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut checked = Vec::new();
    let mut offenders = Vec::new();
    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        let file = path.file_name().unwrap().to_string_lossy().to_string();
        if path.extension().is_none_or(|e| e != "rs") || file.ends_with("_tests.rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let mut current = String::new();
        let mut guarded_by_name = false;
        for line in text.lines() {
            let code = line.trim_start();
            if let Some(rest) = code.strip_prefix("pub fn ") {
                current = rest.split(['(', '<']).next().unwrap_or("").to_string();
                guarded_by_name = false;
            }
            // the order matters inside a line too: a name refusal written AFTER the state guard on the same line is not first
            let is_the_state_guard =
                code.starts_with("self.refuse_ddl_if_own_stamps_would_be_refused()");
            if !is_the_state_guard && code.contains("refuse_stage_name(") {
                guarded_by_name = true;
            }
            // the system path that MAKES a stage is not a client's schema change, and a database's creation or drop takes no collection name
            let system_path =
                ["create_stage", "create_system_collection", "create_database", "drop_database"]
                    .contains(&current.as_str());
            if is_the_state_guard && !current.is_empty() && !system_path {
                if guarded_by_name {
                    checked.push(format!("{file}::{current}"));
                } else {
                    offenders.push(format!("{file}::{current}"));
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these guard a schema change without refusing a stage name first: {offenders:?}"
    );
    assert!(checked.len() >= 9, "the nine local schema changes are scanned: {checked:?}");
}
