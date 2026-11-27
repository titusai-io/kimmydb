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

/// What a stage's physical name contains: `{name}.__stg.{node}{epoch}{hlc}` (ADR-272). A name that contains it is hidden:
/// not addressable through the API and left out of listings, MCP, database-scoped streams and wildcard grants. The epoch
/// is part of the name because two epochs of one node can resume an HLC. The name can exceed the 120-byte limit a client's
/// name has, which is allowed because the system path skips `validate_name` and a replicated creation does not validate.
pub const STAGE_NAME_MARK: &str = ".__stg.";

/// Whether `name` is a stage's physical name.
pub fn is_stage_name(name: &str) -> bool {
    name.contains(STAGE_NAME_MARK)
}

/// The logical name a stage's physical name was made from (`orders` for `orders.__stg.0a1b`); `None` for a name that is not a
/// stage's.
pub fn logical_of_stage(name: &str) -> Option<&str> {
    name.split_once(STAGE_NAME_MARK).map(|(logical, _)| logical)
}

/// What a recreation's physical name contains: `{name}.__rc.{stamp}` (ADR-272, decision 6). Every recreate of a once-swapped
/// name makes a NEW physical collection named by its own event, so that its birth is its stamp and the id of an earlier life
/// is never reused: reusing one makes the physical's birth depend on arrival order. The name is hidden as a stage's is; the
/// logical name answers from it once the member has made the recreate visible.
pub const RECREATE_NAME_MARK: &str = ".__rc.";

/// Whether `name` is a recreation's physical name.
pub fn is_recreation_name(name: &str) -> bool {
    name.contains(RECREATE_NAME_MARK)
}

/// The logical name a recreation's physical name was made from (`orders` for `orders.__rc.0a1b`); `None` for a name that is
/// not a recreation's.
pub fn logical_of_recreation(name: &str) -> Option<&str> {
    name.split_once(RECREATE_NAME_MARK).map(|(logical, _)| logical)
}

/// The physical name of the recreation of `logical` that the event stamped `stamp` makes: the name, the mark and the
/// stamp's key in hex, so two recreations never share a name and every member derives the same one.
pub fn recreation_name(logical: &str, stamp: &Stamp) -> String {
    let mut name =
        String::with_capacity(logical.len() + RECREATE_NAME_MARK.len() + 2 * Stamp::KEY_LEN);
    name.push_str(logical);
    name.push_str(RECREATE_NAME_MARK);
    for byte in stamp.to_key_bytes() {
        name.push_str(&format!("{byte:02x}"));
    }
    name
}

/// The stamp of the event that made the recreation `name`, read back from the name: the inverse of
/// [`recreation_name`]. `None` for a name that is not a recreation's and for one whose stamp does
/// not parse (not exactly `2 * Stamp::KEY_LEN` lowercase hex digits).
pub fn recreation_stamp(name: &str) -> Option<Stamp> {
    let (_, digits) = name.split_once(RECREATE_NAME_MARK)?;
    if digits.len() != 2 * Stamp::KEY_LEN {
        return None;
    }
    let nibble = |digit: u8| match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    };
    let mut key = [0u8; Stamp::KEY_LEN];
    let (pairs, _) = digits.as_bytes().as_chunks::<2>();
    for (byte, pair) in key.iter_mut().zip(pairs) {
        *byte = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(Stamp::from_key_bytes(key))
}

/// Why a creation of `name` is refused where it arrives from another member (ADR-272): it carries a mark the swap reserves, and nothing
/// that makes it the swap's. A stage's creation carries the stage's information (`has_stage_info`), and a recreation's name ends in the
/// hex of its event's stamp. A client cannot make either name (the doors of the API and the engine refuse it on the name alone), so one
/// that arrives without is from a member whose own check was bypassed, and would stand as a hidden collection that the register knows
/// nothing of. `None` for every other name, and for each of those two made as it should be.
pub fn reserved_name_without_its_marks(name: &str, has_stage_info: bool) -> Option<&'static str> {
    if is_stage_name(name) && !has_stage_info {
        return Some(
            "the mark `.__stg.` is reserved for a stage, and this creation carries no stage",
        );
    }
    if is_recreation_name(name) && recreation_stamp(name).is_none() {
        return Some(
            "the mark `.__rc.` is reserved for the physical collection of a recreated name, and this name carries no stamp",
        );
    }
    None
}

/// Whether `name` is a physical name no client can address: a stage's or a recreation's. Such a name is never listed or served
/// as itself, and a schema change addressed to it is refused on the name alone.
pub fn is_hidden_name(name: &str) -> bool {
    is_stage_name(name) || is_recreation_name(name)
}

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
    /// than the request's own deadline. Compared with a binding's `hlc`: a binding is valid only at
    /// or below it (and only from the creation's origin), and a later event of the name buries the
    /// stage only strictly above it.
    pub deadline: Hlc,
}

/// What a member keeps of a stage's creation: the stamp of the creation entry (the origin of the stage's own job, and
/// the base its deadline is clamped from) and the [`StageInfo`] the entry carried. The register keeps it for as long as
/// any register event refers to the stage, which is longer than the stage's physical collection lives
/// (`REGISTER_STAGES`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageMeta {
    pub created: Stamp,
    pub info: StageInfo,
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
    fn a_stage_is_named_by_its_mark() {
        assert!(is_stage_name("orders.__stg.0a1b"));
        assert!(!is_stage_name("orders"));
        assert!(!is_stage_name("orders.__vectors"));
        assert!(!is_stage_name("__stg.orders"), "the mark is a suffix of a name, with its dot");
        assert_eq!(STAGE_NAME_MARK, ".__stg.");
        assert_eq!(logical_of_stage("orders.__stg.0a1b"), Some("orders"));
        assert_eq!(logical_of_stage("orders"), None);
    }

    #[test]
    fn a_recreation_is_named_by_its_event() {
        let stamp = Stamp::new(
            Hlc::new(1_700_000_000_123, 4),
            crate::NodeId::from_bytes([7; 16]),
            crate::Epoch::ZERO,
        );
        let name = recreation_name("orders", &stamp);
        assert!(name.starts_with("orders.__rc."));
        assert!(is_recreation_name(&name) && is_hidden_name(&name) && !is_stage_name(&name));
        assert_eq!(logical_of_recreation(&name), Some("orders"));
        assert_eq!(name, recreation_name("orders", &stamp), "every member derives the same name");
        let other = Stamp::new(Hlc::new(1_700_000_000_123, 5), stamp.node, stamp.epoch);
        assert_ne!(name, recreation_name("orders", &other), "two events never share a name");
        assert_eq!(RECREATE_NAME_MARK, ".__rc.");
        assert!(!is_recreation_name("__rc.orders") && !is_recreation_name("orders.rc.x"));
        assert!(is_hidden_name("orders.__stg.a") && !is_hidden_name("orders"));
    }

    #[test]
    fn a_recreations_stamp_is_read_back_from_its_name() {
        let stamp = Stamp::new(
            Hlc::new(1_700_000_000_123, 4),
            crate::NodeId::from_bytes([7; 16]),
            crate::Epoch::ZERO,
        );
        let name = recreation_name("orders", &stamp);
        assert_eq!(recreation_stamp(&name), Some(stamp), "the name carries the stamp that made it");
        let other = Stamp::new(Hlc::new(1_700_000_000_123, 5), stamp.node, stamp.epoch);
        assert_eq!(recreation_stamp(&recreation_name("a.b", &other)), Some(other));
        // names that are not a recreation's, and ones whose stamp does not parse
        assert_eq!(recreation_stamp("orders"), None);
        assert_eq!(recreation_stamp("orders.__stg.a1"), None);
        assert_eq!(recreation_stamp("orders.__rc."), None, "no digits");
        assert_eq!(recreation_stamp(&name[..name.len() - 2]), None, "one byte short");
        assert_eq!(recreation_stamp(&format!("{name}00")), None, "one byte long");
        let digits = &name[name.len() - 2 * Stamp::KEY_LEN..];
        let upper = format!("orders{RECREATE_NAME_MARK}{}", digits.to_uppercase());
        assert_ne!(upper, name, "premise: the digits include letters");
        assert_eq!(recreation_stamp(&upper), None, "only lowercase hex is a name this build made");
        let not_hex = format!("{}zz", &name[..name.len() - 2]);
        assert_eq!(recreation_stamp(&not_hex), None, "a digit that is not hex");
    }

    #[test]
    fn a_register_event_names_its_physical() {
        assert_eq!(RegisterEvent::Bind { physical: "a".into() }.physical(), Some("a"));
        assert_eq!(RegisterEvent::Create { physical: "b".into() }.physical(), Some("b"));
        assert_eq!(RegisterEvent::LogicalDrop.physical(), None);
    }

    #[test]
    fn a_reserved_name_is_refused_unless_it_carries_what_makes_it_the_swaps() {
        let stamp = Stamp::new(Hlc::new(5, 0), NodeId::from_bytes([7; 16]), Epoch::ZERO);
        let recreation = recreation_name("orders", &stamp);
        assert_eq!(reserved_name_without_its_marks("orders", false), None, "an ordinary name");
        assert_eq!(
            reserved_name_without_its_marks("orders.__stg.zz", true),
            None,
            "a stage with its information"
        );
        assert!(
            reserved_name_without_its_marks("orders.__stg.zz", false).is_some(),
            "a stage name with none"
        );
        assert_eq!(
            reserved_name_without_its_marks(&recreation, false),
            None,
            "a recreation with its stamp"
        );
        assert!(
            reserved_name_without_its_marks("orders.__rc.zz", false).is_some(),
            "a recreation name with no stamp"
        );
        assert!(
            reserved_name_without_its_marks("orders.__rc.", true).is_some(),
            "stage information does not make a recreation name the swap's"
        );
    }
}
