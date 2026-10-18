//! Idempotency keys (B5): the key a client sends, the scope it is remembered
//! under, the request's fingerprint, and the `_id` a keyed insert derives.
//!
//! No I/O, like the rest of this crate: the API reads the header and hashes the
//! request, the storage engine keeps the record, and both speak these types.
//!
//! **The key.** `Idempotency-Key` is a structured-field String (RFC 9651
//! §3.3.3), as draft-ietf-httpapi-idempotency-key-header-07 §2.1 requires. Once
//! unquoted and unescaped it must be 16 to 255 characters of the sf-string
//! alphabet, printable ASCII (`0x20..=0x7E`): that is the published fixed
//! format (the draft's §5). Parameters on the Item are refused, not ignored.
//! The MCP tools take the same value unquoted, as an argument
//! ([`IdempotencyKey::from_raw`]); both forms reach the same unescaped value, so
//! they share one scope.
//!
//! **The scope** ([`ScopeHash`]) is SHA-256 over a domain tag, the principal's
//! key (`local:<user>` or `oidc:<issuer>:<sub>`) and the unescaped key. The key
//! itself is never stored: the record is keyed by the scope's hash (the draft's
//! §5, a composite key no other principal can reach).
//!
//! **The fingerprint** ([`Fingerprint`]) is 32 bytes the API computes over the
//! request (method, route template, path and query, body). Stored and compared
//! here, never computed.
//!
//! **The derived `_id`** ([`derived_id`]) of a keyed insert without one: a
//! UUID version 8 built as RFC 9562 Appendix B.2 builds a name-based one,
//! SHA-256 over a domain tag, the collection id, the scope, the fingerprint and
//! the position, stored as the UUID's canonical 36-character string. The
//! fingerprint is in it so that a key reused with another request derives
//! another `_id` and never replaces the first request's document (invariant I7).
//!
//! **Every byte here is a protocol constant.** A derived `_id` computed by one
//! member must equal the one a retry computes on another, of any release, and a
//! scope hash must find the record a peer stored. The scope hash's and the
//! derivation's domain tags, field order and widths are pinned by golden
//! vectors in the tests below; changing any of them changes which document a
//! retry lands on. The fingerprint the derivation takes is the API's bytes, not
//! this crate's: `kimmy_api::idempotency` pins them (a golden vector per
//! deriving route and MCP tool form), and the API's and MCP's integration tests
//! pin the derived `_id` of each deriving route and tool end to end.

use sha2::{Digest, Sha256};

use crate::ids::CollectionId;

/// The shortest key, in characters of the unescaped value.
pub const KEY_MIN_LEN: usize = 16;
/// The longest key, in characters of the unescaped value.
pub const KEY_MAX_LEN: usize = 255;

/// The domain tag of [`ScopeHash`]: a hash computed for any other purpose over
/// the same bytes cannot collide with a scope.
const SCOPE_TAG: &[u8] = b"kimmydb/idempotency/scope/v1";
/// The domain tag of [`derived_id`], in the place RFC 9562 Appendix B.2 puts a
/// namespace.
const DERIVED_ID_TAG: &[u8] = b"kimmydb/idempotency/id/v1";

/// Why a key was refused. Each says what was wrong with the value, never the
/// value: the API's `400` names the header and quotes this.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    /// Empty, or not quoted: a bare token is not a String (Decision Q5).
    #[error("the value must be a quoted string, such as \"6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90\"")]
    NotAString,
    /// The opening quote is never closed.
    #[error("the quoted string is not closed")]
    Unterminated,
    /// A backslash followed by anything but `"` or `\`.
    #[error("a backslash may escape only a double quote or a backslash")]
    BadEscape,
    /// A character outside printable ASCII (a control character, DEL, or
    /// anything not ASCII).
    #[error("the value may hold only printable ASCII characters")]
    BadCharacter,
    /// Parameters on the Item (`"k";a=1`), which are refused rather than
    /// ignored.
    #[error("the value takes no parameters")]
    Parameters,
    /// Something after the String that is not a parameter: a second member, a
    /// list.
    #[error("the value must be a single quoted string")]
    TrailingInput,
    /// The unescaped value's length is outside 16 to 255 characters.
    #[error(
        "the value must be {KEY_MIN_LEN} to {KEY_MAX_LEN} characters long, unescaped; it is {found}"
    )]
    Length { found: usize },
}

/// A client's idempotency key, unescaped. Only ever hashed into a
/// [`ScopeHash`]; nothing stores it.
#[derive(Clone, PartialEq, Eq)]
pub struct IdempotencyKey(String);

/// Never printed: a key is a client's own handle on a request, and a log line
/// is no place for it.
impl std::fmt::Debug for IdempotencyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IdempotencyKey({} chars)", self.0.len())
    }
}

/// Printable ASCII: the sf-string alphabet once unescaped (RFC 9651 §3.3.3).
fn printable(c: char) -> bool {
    matches!(c, '\x20'..='\x7e')
}

impl IdempotencyKey {
    /// Parse the `Idempotency-Key` header's value: an RFC 9651 Item whose bare
    /// item is a String, with no parameters.
    ///
    /// Leading and trailing whitespace (SP, HTAB) is not part of a field value
    /// (RFC 9110 §5.5) and is set aside first. The String is then parsed as
    /// RFC 9651 §4.2.5 parses one: `\"` and `\\` are its only escapes, and a
    /// character outside `0x20..=0x7E` fails it. The length rule is applied to
    /// the unescaped value, so each escape counts as one character.
    pub fn parse_header_value(value: &str) -> Result<Self, KeyError> {
        let value = value.trim_matches([' ', '\t']);
        let Some(rest) = value.strip_prefix('"') else {
            return Err(KeyError::NotAString);
        };
        let mut unescaped = String::with_capacity(rest.len());
        let mut chars = rest.char_indices();
        let after = loop {
            let Some((at, c)) = chars.next() else {
                return Err(KeyError::Unterminated);
            };
            match c {
                '\\' => match chars.next() {
                    Some((_, escaped @ ('"' | '\\'))) => unescaped.push(escaped),
                    Some(_) => return Err(KeyError::BadEscape),
                    None => return Err(KeyError::Unterminated),
                },
                '"' => break &rest[at + 1..],
                c if printable(c) => unescaped.push(c),
                _ => return Err(KeyError::BadCharacter),
            }
        };
        // RFC 9651 §4.2: what follows the Item, past any SP, must be nothing.
        // A `;` opens its parameters, which this header does not take.
        let after = after.trim_start_matches(' ');
        if after.starts_with(';') {
            return Err(KeyError::Parameters);
        }
        if !after.is_empty() {
            return Err(KeyError::TrailingInput);
        }
        Self::checked(unescaped)
    }

    /// A key as the MCP tools take it: the value itself, unquoted and
    /// unescaped, held to the same length and alphabet as the header's.
    pub fn from_raw(value: &str) -> Result<Self, KeyError> {
        if !value.chars().all(printable) {
            return Err(KeyError::BadCharacter);
        }
        Self::checked(value.to_string())
    }

    fn checked(unescaped: String) -> Result<Self, KeyError> {
        // Every character is ASCII by now, so bytes are characters.
        let found = unescaped.len();
        if !(KEY_MIN_LEN..=KEY_MAX_LEN).contains(&found) {
            return Err(KeyError::Length { found });
        }
        Ok(Self(unescaped))
    }

    /// The unescaped value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The scope this key is remembered under for `principal`, the caller's
    /// `principal_key` (`local:<user>`, `oidc:<issuer>:<sub>`).
    ///
    /// `SHA-256(tag ‖ len(principal) ‖ principal ‖ len(key) ‖ key)`, each
    /// length a big-endian `u32`: the lengths keep `("ab", "c…")` and
    /// `("a", "bc…")` apart.
    pub fn scope_hash(&self, principal: &str) -> ScopeHash {
        let mut hash = Sha256::new();
        hash.update(SCOPE_TAG);
        hash.update((principal.len() as u32).to_be_bytes());
        hash.update(principal.as_bytes());
        hash.update((self.0.len() as u32).to_be_bytes());
        hash.update(self.0.as_bytes());
        ScopeHash(hash.finalize().into())
    }
}

/// A 32-byte value printed as lowercase hex.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Where a key is remembered: SHA-256 over the principal and the key (see
/// [`IdempotencyKey::scope_hash`]). With the collection id, the record's key.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScopeHash(pub [u8; 32]);

impl ScopeHash {
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// From stored or received bytes; `None` unless exactly 32.
    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok().map(Self)
    }
}

impl std::fmt::Debug for ScopeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScopeHash({})", hex(&self.0))
    }
}

/// A keyed request's fingerprint (the draft's §2.4), computed by the API over
/// the request as sent. A retry with the same key and the same fingerprint is
/// the same request; with another fingerprint, a key reused.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fingerprint(pub [u8; 32]);

impl Fingerprint {
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// From stored or received bytes; `None` unless exactly 32.
    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok().map(Self)
    }
}

impl std::fmt::Debug for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Fingerprint({})", hex(&self.0))
    }
}

/// The 16 bytes of a keyed insert's derived `_id`: a UUID version 8, built as
/// RFC 9562 Appendix B.2 builds a name-based one. SHA-256 over
/// `tag ‖ collection id (u64, big-endian) ‖ scope (32) ‖ fingerprint (32) ‖
/// position (u64, big-endian)`, its leftmost 128 bits, with the version nibble
/// set to 8 and the variant bits to `10`.
///
/// `position` is the document's index in a bulk, and 0 otherwise (an insert,
/// a `find_and_modify` upsert's seed).
pub fn derived_uuid(
    collection: CollectionId,
    scope: &ScopeHash,
    fingerprint: &Fingerprint,
    position: u64,
) -> [u8; 16] {
    let mut hash = Sha256::new();
    hash.update(DERIVED_ID_TAG);
    hash.update(collection.0.to_be_bytes());
    hash.update(scope.0);
    hash.update(fingerprint.0);
    hash.update(position.to_be_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    name_based_v8(&digest)
}

/// RFC 9562 Appendix B.2's last steps: the hash's leftmost 128 bits, version 8
/// in the high nibble of octet 6, the variant `10` in the top bits of octet 8.
fn name_based_v8(digest: &[u8; 32]) -> [u8; 16] {
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out[6] = (out[6] & 0x0F) | 0x80;
    out[8] = (out[8] & 0x3F) | 0x80;
    out
}

/// A keyed insert's derived `_id`, as the stored body carries it: the UUID's
/// canonical 36-character lowercase string (Decision Q1). A path segment that
/// is neither 24 hex characters nor an integer reads as a string, so every
/// by-id route addresses it.
pub fn derived_id(
    collection: CollectionId,
    scope: &ScopeHash,
    fingerprint: &Fingerprint,
    position: u64,
) -> String {
    uuid::Uuid::from_bytes(derived_uuid(collection, scope, fingerprint, position))
        .hyphenated()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes_of(hex: &str) -> Vec<u8> {
        (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect()
    }

    fn key(raw: &str) -> IdempotencyKey {
        IdempotencyKey::from_raw(raw).unwrap()
    }

    #[test]
    fn a_quoted_string_is_a_key() {
        let k =
            IdempotencyKey::parse_header_value("\"6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90\"").unwrap();
        assert_eq!(k.as_str(), "6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90");
        // Whitespace around the field value is not part of it (RFC 9110 §5.5),
        // and SP after the Item is allowed (RFC 9651 §4.2).
        let padded = IdempotencyKey::parse_header_value(" \t\"0123456789abcdef\" ").unwrap();
        assert_eq!(padded.as_str(), "0123456789abcdef");
    }

    #[test]
    fn escapes_unescape_and_count_as_one_character() {
        // 14 plain characters and two escapes: 16 once unescaped, 18 as sent.
        let k = IdempotencyKey::parse_header_value(r#""abcdefghijklmn\"\\""#).unwrap();
        assert_eq!(k.as_str(), "abcdefghijklmn\"\\");
        assert_eq!(k.as_str().len(), KEY_MIN_LEN);
        // 15 plain characters and one escape is 16; 15 alone is too short,
        // however long the escaped form is.
        assert!(IdempotencyKey::parse_header_value(r#""abcdefghijklmno\\""#).is_ok());
        assert_eq!(
            IdempotencyKey::parse_header_value(r#""abcdefghijklm\\""#),
            Err(KeyError::Length { found: 14 })
        );
        // At the top: 255 unescaped is allowed even though 256 bytes are sent.
        let top = format!("\"{}\\\"\"", "a".repeat(KEY_MAX_LEN - 1));
        assert_eq!(IdempotencyKey::parse_header_value(&top).unwrap().as_str().len(), KEY_MAX_LEN);
        let over = format!("\"{}\"", "a".repeat(KEY_MAX_LEN + 1));
        assert_eq!(
            IdempotencyKey::parse_header_value(&over),
            Err(KeyError::Length { found: KEY_MAX_LEN + 1 })
        );
    }

    #[test]
    fn everything_but_a_lone_quoted_string_is_refused() {
        let refused = |v: &str| IdempotencyKey::parse_header_value(v).unwrap_err();
        assert_eq!(refused(""), KeyError::NotAString);
        assert_eq!(refused("   "), KeyError::NotAString);
        // A bare token is not a String (Decision Q5).
        assert_eq!(refused("0123456789abcdef0"), KeyError::NotAString);
        assert_eq!(refused("\"\""), KeyError::Length { found: 0 });
        assert_eq!(refused("\"0123456789abcdef"), KeyError::Unterminated);
        assert_eq!(refused("\"0123456789abcdef\\"), KeyError::Unterminated);
        assert_eq!(refused(r#""0123456789abcdef\n""#), KeyError::BadEscape);
        assert_eq!(refused("\"0123456789abcdef\u{7f}\""), KeyError::BadCharacter);
        assert_eq!(refused("\"0123456789abcdef\u{1}\""), KeyError::BadCharacter);
        assert_eq!(refused("\"0123456789abcdef\t\""), KeyError::BadCharacter);
        assert_eq!(refused("\"0123456789abcdefé\""), KeyError::BadCharacter);
        assert_eq!(refused("\"0123456789abcdef\";a=1"), KeyError::Parameters);
        assert_eq!(refused("\"0123456789abcdef\" ;a"), KeyError::Parameters);
        assert_eq!(refused("\"0123456789abcdef\", \"x\""), KeyError::TrailingInput);
        assert_eq!(refused("\"0123456789abcdef\"x"), KeyError::TrailingInput);
        // A trailing tab after the Item is set aside as the field's own
        // whitespace; one between the Item and more input is not SP.
        assert_eq!(refused("\"0123456789abcdef\"\t;a=1"), KeyError::TrailingInput);
    }

    #[test]
    fn the_raw_form_holds_the_same_rule_and_reaches_the_same_scope() {
        assert_eq!(IdempotencyKey::from_raw("short"), Err(KeyError::Length { found: 5 }));
        assert_eq!(IdempotencyKey::from_raw(&"a".repeat(15)), Err(KeyError::Length { found: 15 }));
        assert!(IdempotencyKey::from_raw(&"a".repeat(16)).is_ok());
        assert!(IdempotencyKey::from_raw(&"a".repeat(255)).is_ok());
        assert_eq!(
            IdempotencyKey::from_raw(&"a".repeat(256)),
            Err(KeyError::Length { found: 256 })
        );
        assert_eq!(IdempotencyKey::from_raw("0123456789abcdef\n"), Err(KeyError::BadCharacter));
        assert_eq!(IdempotencyKey::from_raw("0123456789abcdefé"), Err(KeyError::BadCharacter));
        // Quote and backslash are printable: the raw form carries them as they
        // are, and so reaches the scope the escaped header reaches.
        let raw = key(r#"abcdefghijklmn"\"#);
        let header = IdempotencyKey::parse_header_value(r#""abcdefghijklmn\"\\""#).unwrap();
        assert_eq!(raw, header);
        assert_eq!(raw.scope_hash("local:alice"), header.scope_hash("local:alice"));
    }

    #[test]
    fn a_key_is_never_printed() {
        let printed = format!("{:?}", key("secret-client-key-0001"));
        assert!(!printed.contains("secret"), "{printed}");
    }

    /// The scope's bytes, pinned. Computed independently (Python's hashlib)
    /// over `b"kimmydb/idempotency/scope/v1" ‖ be32(11) ‖ b"local:alice" ‖
    /// be32(36) ‖ the key`.
    #[test]
    fn the_scope_hash_is_pinned() {
        let k = key("6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90");
        assert_eq!(
            k.scope_hash("local:alice").0.to_vec(),
            bytes_of("6d2c7e550043e7e13adffeaef8ba32686de418ebb5e25e4c9b3c52ec0bf55efd"),
        );
        // Scoped to the principal: another one is another scope.
        assert_eq!(
            k.scope_hash("local:bob").0.to_vec(),
            bytes_of("8467b6a3654ecd70221c28ad7f668cae09b9269256b674fe16ddd0090c8434c8")
        );
        // The lengths separate the two fields.
        let a = key("bcdefghijklmnopq").scope_hash("local:a");
        let b = key("cdefghijklmnopq0").scope_hash("local:ab");
        assert_ne!(a, b);
    }

    /// RFC 9562 Appendix B.2's own vector: the namespace DNS and the name
    /// `www.example.com` give `5c146b14-3c52-8afd-938a-375d0df1fbf6`. The
    /// derived `_id` takes these last steps, so this pins that it is built as
    /// B.2 builds one.
    #[test]
    fn the_construction_is_rfc_9562_appendix_b2() {
        let mut hash = Sha256::new();
        hash.update(uuid::Uuid::NAMESPACE_DNS.as_bytes());
        hash.update(b"www.example.com");
        let digest: [u8; 32] = hash.finalize().into();
        let id = uuid::Uuid::from_bytes(name_based_v8(&digest));
        assert_eq!(id.hyphenated().to_string(), "5c146b14-3c52-8afd-938a-375d0df1fbf6");
        assert_eq!(id.get_version_num(), 8);
    }

    /// The derived `_id`, pinned: a retry on any member, of any release, must
    /// land on the document the first attempt wrote. Computed independently
    /// (Python's hashlib) over `b"kimmydb/idempotency/id/v1" ‖ be64(collection)
    /// ‖ scope ‖ fingerprint ‖ be64(position)`, version and variant set.
    #[test]
    fn the_derived_id_is_pinned() {
        let collection = CollectionId::derive("shop", "orders");
        assert_eq!(collection.0, 6_029_630_790_784_399_930);
        let scope = key("6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90").scope_hash("local:alice");
        let fingerprint = Fingerprint([0x11; 32]);
        assert_eq!(
            derived_id(collection, &scope, &fingerprint, 0),
            "841b418a-6803-8dcf-a1ed-6f4e87bfb225"
        );
        assert_eq!(
            derived_id(collection, &scope, &fingerprint, 1),
            "20c7db4d-39ab-8898-a280-c1ce50be4e1e"
        );
        let other = Fingerprint([0x22; 32]);
        assert_eq!(
            derived_id(collection, &scope, &other, 0),
            "d69947db-031b-8df6-9ee0-b0d6b95db0ee"
        );
        let id = uuid::Uuid::parse_str(&derived_id(collection, &scope, &fingerprint, 7)).unwrap();
        assert_eq!(id.get_version_num(), 8);
        assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
    }

    #[test]
    fn every_input_moves_the_derived_id() {
        let c = CollectionId(42);
        let s = key("0123456789abcdef").scope_hash("local:alice");
        let f = Fingerprint([1; 32]);
        let base = derived_id(c, &s, &f, 0);
        assert_eq!(base.len(), 36);
        assert_eq!(base, base.to_lowercase());
        assert_ne!(base, derived_id(CollectionId(43), &s, &f, 0), "collection");
        assert_ne!(base, derived_id(c, &key("0123456789abcdeg").scope_hash("local:alice"), &f, 0));
        assert_ne!(base, derived_id(c, &s, &Fingerprint([2; 32]), 0), "fingerprint (I7)");
        assert_ne!(base, derived_id(c, &s, &f, 1), "position");
        assert_eq!(base, derived_id(c, &s, &f, 0), "and a function of them alone");
    }
}
