//! The size caps a client write is held to where it is admitted (ADR-250).
//!
//! Checked on the document a client write is about to store and on the `_id` it
//! names; never on what a peer sends, a snapshot carries or a restore reads,
//! which are state already admitted somewhere.

use crate::{DocId, Error, Result};

/// The most bytes a stored document may encode to: 16 MiB, a quarter of the frame
/// a peer exchange carries, so a document can always cross it.
pub const MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;

/// The most bytes an `_id` may hold: its characters for a string, its bytes for a
/// binary. The other kinds are a few bytes each.
pub const MAX_ID_BYTES: usize = 4096;

/// Refuse a document whose encoding is `bytes` long when that is over the cap.
pub fn check_document(bytes: usize) -> Result<()> {
    if bytes > MAX_DOCUMENT_BYTES {
        return Err(Error::DocumentTooLarge { bytes, max: MAX_DOCUMENT_BYTES });
    }
    Ok(())
}

/// Refuse an `_id` over the cap. The refusal names the id's kind and length and
/// never the id, which may be the very thing that is too long.
pub fn check_id(id: &DocId) -> Result<()> {
    let (kind, bytes) = match id {
        DocId::String(s) => ("string", s.len()),
        DocId::Binary(b) => ("binary", b.len()),
        DocId::ObjectId(_) | DocId::Int64(_) | DocId::Uuid(_) | DocId::Decimal128(_) => {
            return Ok(());
        }
    };
    if bytes > MAX_ID_BYTES {
        return Err(Error::IdTooLarge { kind, bytes, max: MAX_ID_BYTES });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_document_at_the_cap_passes_and_one_byte_over_is_refused() {
        assert!(check_document(MAX_DOCUMENT_BYTES).is_ok());
        let err = check_document(MAX_DOCUMENT_BYTES + 1).unwrap_err();
        assert!(matches!(err, Error::DocumentTooLarge { bytes, max }
            if bytes == MAX_DOCUMENT_BYTES + 1 && max == MAX_DOCUMENT_BYTES));
    }

    #[test]
    fn a_string_or_binary_id_is_measured_in_bytes() {
        assert!(check_id(&DocId::String("a".repeat(MAX_ID_BYTES))).is_ok());
        assert!(check_id(&DocId::Binary(vec![0; MAX_ID_BYTES])).is_ok());
        // A multi-byte character counts as its bytes, not as one.
        let wide = "\u{e9}".repeat(MAX_ID_BYTES / 2 + 1);
        for id in [
            DocId::String("a".repeat(MAX_ID_BYTES + 1)),
            DocId::Binary(vec![0; MAX_ID_BYTES + 1]),
            DocId::String(wide),
        ] {
            let err = check_id(&id).unwrap_err();
            assert!(matches!(err, Error::IdTooLarge { .. }), "{err:?}");
            assert!(!err.to_string().contains("aaaa"), "the id is not echoed: {err}");
        }
        assert!(check_id(&DocId::Int64(i64::MAX)).is_ok());
        assert!(check_id(&DocId::generate()).is_ok());
    }
}
