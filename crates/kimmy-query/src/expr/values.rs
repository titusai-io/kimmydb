//! `$binarySize`, `$bsonSize`, `$tsSecond` and `$tsIncrement`.
//!
//! The first two answer an `Int32`: the bytes of a string (as UTF-8, with no length prefix and
//! no terminator) or of binary data (old-style binary, subtype 2, counts the 4-byte length it
//! carries inside its payload, as the reference does), and the bytes the document takes encoded as BSON. The
//! last two answer an `Int64`, the seconds and the increment of a timestamp. A null or a
//! missing operand is null; any other type is refused in the reference's words.

use bson::Bson;
use kimmy_core::{Error, Result};

use super::docops::bson_type_name;
use super::{Op, count_bson, sizes};

/// `$binarySize`: the length in bytes of a string or of binary data.
pub(super) fn binary_size(arg: &Bson) -> Result<Bson> {
    match arg {
        Bson::Null | Bson::Undefined => Ok(Bson::Null),
        Bson::String(s) => Ok(count_bson(s.len())),
        Bson::Binary(b) => Ok(count_bson(sizes::binary_length(b))),
        other => Err(Error::InvalidQuery(format!(
            "$binarySize requires a string or BinData argument, found: {}",
            bson_type_name(Some(other))
        ))),
    }
}

/// `$bsonSize`: the size in bytes of a document encoded as BSON.
pub(super) fn bson_size(arg: &Bson) -> Result<Bson> {
    match arg {
        Bson::Null | Bson::Undefined => Ok(Bson::Null),
        Bson::Document(d) => Ok(count_bson(sizes::document_bytes(d))),
        other => Err(Error::InvalidQuery(format!(
            "$bsonSize requires a document input, found: {}",
            bson_type_name(Some(other))
        ))),
    }
}

/// `$tsSecond` and `$tsIncrement`: the two halves of a timestamp, each an `Int64`.
pub(super) fn timestamp_part(op: Op, arg: &Bson) -> Result<Bson> {
    match arg {
        Bson::Null | Bson::Undefined => Ok(Bson::Null),
        Bson::Timestamp(t) => {
            Ok(Bson::Int64(i64::from(if op == Op::TsSecond { t.time } else { t.increment })))
        }
        other => Err(Error::InvalidQuery(format!(
            "Argument to {} must be a timestamp, but is {}",
            op.name(),
            bson_type_name(Some(other))
        ))),
    }
}
