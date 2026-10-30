//! The names a BSON value reports as its type: what `$type` and `bsonType` accept.

use bson::Bson;

/// Every name a value can report: what [`type_name_of`] returns, and so every
/// alias `$type` accepts. Not [`type_name_for_code`]'s table, which has no
/// code for `symbol` or `dbPointer` and would refuse types `$type` finds.
pub const TYPE_NAMES: [&str; 20] = [
    "double",
    "string",
    "object",
    "array",
    "binData",
    "undefined",
    "objectId",
    "bool",
    "date",
    "null",
    "regex",
    "javascript",
    "int",
    "timestamp",
    "long",
    "decimal",
    "minKey",
    "maxKey",
    "symbol",
    "dbPointer",
];

pub fn type_name_of(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::Undefined => "undefined",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => "javascript",
        Bson::Int32(_) => "int",
        Bson::Timestamp(_) => "timestamp",
        Bson::Int64(_) => "long",
        Bson::Decimal128(_) => "decimal",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
        Bson::Symbol(_) => "symbol",
        Bson::DbPointer(_) => "dbPointer",
    }
}
