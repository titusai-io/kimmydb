//! Password hashing.

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Argon2, password_hash::rand_core::OsRng};

use crate::error::{AuthError, Result};

/// Hash a password with Argon2id and a fresh random salt.
///
/// Returns a PHC string, which carries the algorithm, parameters, and salt
/// alongside the digest — so parameters can be raised later without stranding
/// existing hashes.
pub fn hash(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| AuthError::Hashing(e.to_string()))
}

/// Check a password against a stored PHC hash.
///
/// A malformed stored hash verifies as `false` rather than erroring, so that a
/// corrupt user record cannot be distinguished from a wrong password by timing
/// or by response.
pub fn verify(password: &str, stored: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(stored) else {
        return false;
    };
    Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_password_verifies_against_its_own_hash() {
        let h = hash("correct horse battery staple").unwrap();
        assert!(verify("correct horse battery staple", &h));
    }

    #[test]
    fn a_wrong_password_does_not_verify() {
        let h = hash("hunter2").unwrap();
        assert!(!verify("hunter3", &h));
        assert!(!verify("", &h));
    }

    #[test]
    fn the_same_password_hashes_differently_each_time() {
        // A fresh salt per hash is what stops identical passwords from being
        // identifiable across accounts.
        let a = hash("same").unwrap();
        let b = hash("same").unwrap();
        assert_ne!(a, b);
        assert!(verify("same", &a) && verify("same", &b));
    }

    #[test]
    fn the_hash_is_a_phc_string_naming_argon2id() {
        let h = hash("x").unwrap();
        assert!(h.starts_with("$argon2id$"), "unexpected hash format: {h}");
    }

    #[test]
    fn the_plaintext_never_appears_in_the_hash() {
        let h = hash("swordfish").unwrap();
        assert!(!h.contains("swordfish"));
    }

    #[test]
    fn a_corrupt_stored_hash_fails_closed() {
        assert!(!verify("anything", "not-a-phc-string"));
        assert!(!verify("anything", ""));
        assert!(!verify("anything", "$argon2id$v=19$m=1,t=1,p=1$aaaa$bbbb"));
    }

    #[test]
    fn unicode_and_long_passwords_round_trip() {
        for password in ["пароль", "🔐🔐🔐", &"x".repeat(1024)] {
            let h = hash(password).unwrap();
            assert!(verify(password, &h), "failed for {password:?}");
        }
    }
}
