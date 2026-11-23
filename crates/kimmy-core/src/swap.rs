//! Payloads and register types of the atomic `$out` swap (ADR-272).
//!
//! A replacing `$out` writes into a hidden physical collection, the *stage*, and then binds the
//! logical name to it with one replicated entry. Three entry kinds carry that, and each names its
//! target by database and collection name, as every payload in [`crate::ddl`] does (the entry's
//! `collection` id is a hash and cannot be turned back into a name):
//!
//! - [`crate::OpKind::BindCollection`], body [`CollectionBind`]: the logical name is bound to a
//!   physical collection. A schema-change kind, decided by `Engine::apply_ddl`.
//! - [`crate::OpKind::AbandonStage`], body [`StageAbandon`]: the writer gives up a stage it never
//!   bound. A schema-change kind as well.
//! - [`crate::OpKind::OutLossRecord`], body [`OutLoss`]: a write the swap made late, lost and
//!   reported. Not a schema change and not a document write; it is its own record kind.
//!
//! The stage's creation is not a kind of its own: it is a `CreateCollection` whose body carries
//! the stage's deadline ([`crate::CollectionCreate::stage`]).
//!
//! [`RegisterEvent`] is what a member keeps of the logical name's timeline: its bindings, its
//! logical drops and its recreations, each under the stamp of the entry that made it. The codec is
//! the stored form of one event (the value of a row keyed by that stamp); it is part of the stored
//! format (storage schema 12).

use serde::{Deserialize, Serialize};

use crate::hlc::{Hlc, Stamp};

/// A logical collection name being bound to a physical collection: the body of a
/// `BindCollection` entry.
///
/// The writer stamps it after its last staging write and the stage's index builds, so its stamp
/// is above every staged entry and at or below the stage's deadline ([`StageInfo::deadline`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionBind {
    pub db: String,
    /// The logical name, the one clients use.
    pub name: String,
    /// The physical collection that answers to the name from this entry's stamp on.
    pub physical: String,
}

/// A writer giving up a stage it never bound: the body of an `AbandonStage` entry.
///
/// An ordinary entry of the writer's own origin, appended and served onward. A member that holds
/// it in position buries the stage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageAbandon {
    pub db: String,
    /// The stage's physical name.
    pub physical: String,
}

/// What a stage's creation entry says beyond a plain creation: the body field
/// [`crate::CollectionCreate::stage`].
///
/// A collection created with it is hidden (its physical name carries `.__stg.`), inherits its
/// target's merge policy (the creation's own `merge_policy`) and may never be bound above
/// `deadline`: a binding stamped above it is invalid, history on every member, by a pure
/// function of the stamps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageInfo {
    /// The writer's clock at the stage's creation plus the request's remaining time, never more
    /// than the request's own deadline. Compared with a binding's `hlc`: a binding is valid at or
    /// below it, and a later event of the name buries the stage only strictly above it.
    pub deadline: Hlc,
}

/// A write the swap made late, lost and reported: the body of an `OutLossRecord` entry.
///
/// The record is keyed `(logical name, _id, loser stamp)`; the entry's `collection` is the logical
/// name's id and its `doc_id` the document's `_id`. Several members can mint the same record and
/// they collapse by key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutLoss {
    pub db: String,
    /// The logical name the lost write was addressed to.
    pub name: String,
    /// The lost write's stamp.
    pub loser: Stamp,
    /// The lost post-image, cut at the collection's cut; `None` for a lost delete.
    #[serde(with = "serde_bytes")]
    pub image: Option<Vec<u8>>,
    /// Whether `image` was cut: part of the record, so two members that cut one image to equal
    /// bytes hold one record.
    pub cut: bool,
}

/// One event of a logical name's timeline (ADR-272, decision 1).
///
/// The register holds the name's whole timeline: the state of the name on a member is the newest
/// event in effect there, by stamp. A binding names the physical collection; a logical drop
/// names none (the name is absent); a recreation names the new physical it made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterEvent {
    Bind { physical: String },
    LogicalDrop,
    Create { physical: String },
}

/// Why a stored register event does not decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegisterDecodeError(pub &'static str);

impl std::fmt::Display for RegisterDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a stored register event does not decode: {}", self.0)
    }
}

impl std::error::Error for RegisterDecodeError {}

/// The stored tags of a [`RegisterEvent`]. Part of the format; never renumbered.
const TAG_BIND: u8 = 1;
const TAG_LOGICAL_DROP: u8 = 2;
const TAG_CREATE: u8 = 3;

/// The longest physical name a register row holds: a stage's name is the target's (at most 120
/// bytes) plus the stage suffix, which is well under this.
pub const MAX_PHYSICAL_NAME_LEN: usize = 512;

impl RegisterEvent {
    /// The stored form: `tag ‖ [length (u16, big endian) ‖ physical (UTF-8)]`. A logical drop is
    /// the tag alone.
    pub fn encode(&self) -> Vec<u8> {
        let (tag, physical) = match self {
            Self::Bind { physical } => (TAG_BIND, Some(physical)),
            Self::LogicalDrop => (TAG_LOGICAL_DROP, None),
            Self::Create { physical } => (TAG_CREATE, Some(physical)),
        };
        let mut out = vec![tag];
        if let Some(physical) = physical {
            let bytes = physical.as_bytes();
            debug_assert!(bytes.len() <= MAX_PHYSICAL_NAME_LEN);
            out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            out.extend_from_slice(bytes);
        }
        out
    }

    /// Reads a stored form, refusing every byte it does not account for.
    pub fn decode(bytes: &[u8]) -> Result<Self, RegisterDecodeError> {
        let Some((&tag, rest)) = bytes.split_first() else {
            return Err(RegisterDecodeError("no tag"));
        };
        if tag == TAG_LOGICAL_DROP {
            return if rest.is_empty() {
                Ok(Self::LogicalDrop)
            } else {
                Err(RegisterDecodeError("a logical drop with trailing bytes"))
            };
        }
        if tag != TAG_BIND && tag != TAG_CREATE {
            return Err(RegisterDecodeError("an unknown tag"));
        }
        let Some((len, name)) = rest.split_first_chunk::<2>() else {
            return Err(RegisterDecodeError("no name length"));
        };
        let len = usize::from(u16::from_be_bytes(*len));
        if len > MAX_PHYSICAL_NAME_LEN || name.len() != len {
            return Err(RegisterDecodeError("the name length is wrong"));
        }
        let physical = String::from_utf8(name.to_vec())
            .map_err(|_| RegisterDecodeError("the name is not UTF-8"))?;
        Ok(if tag == TAG_BIND { Self::Bind { physical } } else { Self::Create { physical } })
    }

    /// The physical collection the event names, if it names one.
    pub fn physical(&self) -> Option<&str> {
        match self {
            Self::Bind { physical } | Self::Create { physical } => Some(physical),
            Self::LogicalDrop => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Epoch, NodeId};

    fn stamp() -> Stamp {
        Stamp::new(Hlc::new(1_700_000_000_000, 7), NodeId::from_bytes([9; 16]), Epoch::from_u64(3))
    }

    /// The bytes a body is stored and sent as: BSON, field names and order as the struct declares
    /// them. A rename, a reorder, a new field or a changed type changes these and is a format
    /// change (storage schema 12), never an edit.
    #[test]
    fn a_collection_bind_is_these_bytes() {
        let body =
            CollectionBind { db: "d".into(), name: "n".into(), physical: "n.__stg.x".into() };
        let golden: Vec<u8> = vec![
            0x33, 0, 0, 0, // document length
            0x02, b'd', b'b', 0, 2, 0, 0, 0, b'd', 0, // "db": "d"
            0x02, b'n', b'a', b'm', b'e', 0, 2, 0, 0, 0, b'n', 0, // "name": "n"
            0x02, b'p', b'h', b'y', b's', b'i', b'c', b'a', b'l', 0, 10, 0, 0,
            0, // "physical"
            b'n', b'.', b'_', b'_', b's', b't', b'g', b'.', b'x', 0, // : "n.__stg.x"
            0, // end
        ];
        let bytes = bson::serialize_to_vec(&body).unwrap();
        assert_eq!(bytes, golden);
        assert_eq!(bson::deserialize_from_slice::<CollectionBind>(&bytes).unwrap(), body);
    }

    #[test]
    fn a_stage_abandon_is_these_bytes() {
        let body = StageAbandon { db: "d".into(), physical: "n.__stg.x".into() };
        let golden: Vec<u8> = vec![
            0x27, 0, 0, 0, // document length
            0x02, b'd', b'b', 0, 2, 0, 0, 0, b'd', 0, // "db": "d"
            0x02, b'p', b'h', b'y', b's', b'i', b'c', b'a', b'l', 0, 10, 0, 0,
            0, // "physical"
            b'n', b'.', b'_', b'_', b's', b't', b'g', b'.', b'x', 0, // : "n.__stg.x"
            0, // end
        ];
        let bytes = bson::serialize_to_vec(&body).unwrap();
        assert_eq!(bytes, golden);
        assert_eq!(bson::deserialize_from_slice::<StageAbandon>(&bytes).unwrap(), body);
    }

    #[test]
    fn a_stage_info_is_these_bytes() {
        let info = StageInfo { deadline: Hlc::new(1_700_000_000_000, 7) };
        let bytes = bson::serialize_to_vec(&info).unwrap();
        assert_eq!(bson::deserialize_from_slice::<StageInfo>(&bytes).unwrap(), info);
        let doc: bson::Document = bson::deserialize_from_slice(&bytes).unwrap();
        assert_eq!(doc.keys().collect::<Vec<_>>(), ["deadline"], "one field, named deadline");
    }

    #[test]
    fn an_out_loss_round_trips_and_names_its_fields() {
        let cut = OutLoss {
            db: "d".into(),
            name: "n".into(),
            loser: stamp(),
            image: Some(vec![1, 2, 3]),
            cut: true,
        };
        let bytes = bson::serialize_to_vec(&cut).unwrap();
        assert_eq!(bson::deserialize_from_slice::<OutLoss>(&bytes).unwrap(), cut);
        let doc: bson::Document = bson::deserialize_from_slice(&bytes).unwrap();
        assert_eq!(doc.keys().collect::<Vec<_>>(), ["db", "name", "loser", "image", "cut"]);
        // The image is BSON binary, not an array of int32s (the reason `serde_bytes` is on it).
        assert!(matches!(doc.get("image"), Some(bson::Bson::Binary(_))));
        let delete = OutLoss { image: None, cut: false, ..cut };
        let back: OutLoss =
            bson::deserialize_from_slice(&bson::serialize_to_vec(&delete).unwrap()).unwrap();
        assert_eq!(back, delete, "a lost delete has no image");
        assert!(!back.cut, "and it was not cut");
    }

    #[test]
    fn a_register_event_is_these_bytes() {
        assert_eq!(
            RegisterEvent::Bind { physical: "n.__stg.x".into() }.encode(),
            [1, 0, 9, b'n', b'.', b'_', b'_', b's', b't', b'g', b'.', b'x']
        );
        assert_eq!(RegisterEvent::LogicalDrop.encode(), [2]);
        assert_eq!(
            RegisterEvent::Create { physical: "R.1".into() }.encode(),
            [3, 0, 3, b'R', b'.', b'1']
        );
    }

    #[test]
    fn a_register_event_round_trips_and_refuses_what_it_does_not_account_for() {
        for event in [
            RegisterEvent::Bind { physical: "orders.__stg.abc".into() },
            RegisterEvent::LogicalDrop,
            RegisterEvent::Create { physical: "orders.R.7".into() },
            RegisterEvent::Bind { physical: String::new() },
            RegisterEvent::Create { physical: "é".repeat(200) },
        ] {
            assert_eq!(RegisterEvent::decode(&event.encode()).unwrap(), event);
        }
        let mut too_long = vec![1, 0x02, 0x01]; // a length of 513, which no physical name has
        too_long.extend(std::iter::repeat_n(b'a', 513));
        assert!(RegisterEvent::decode(&too_long).is_err(), "a name past the longest a row holds");
        for bad in [
            &[][..],
            &[0],
            &[4],
            &[2, 0],                // a logical drop with a byte after it
            &[1],                   // no length
            &[1, 0],                // half a length
            &[1, 0, 2, b'a'],       // short of the length
            &[1, 0, 1, b'a', b'b'], // past the length
            &[3, 0, 1, 0xff],       // not UTF-8
            &[1, 0xff, 0xff, b'a'], // a length no name has
        ] {
            assert!(RegisterEvent::decode(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_register_event_names_its_physical() {
        assert_eq!(RegisterEvent::Bind { physical: "a".into() }.physical(), Some("a"));
        assert_eq!(RegisterEvent::Create { physical: "b".into() }.physical(), Some("b"));
        assert_eq!(RegisterEvent::LogicalDrop.physical(), None);
    }
}
