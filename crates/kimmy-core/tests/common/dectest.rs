//! The General Decimal Arithmetic test suite's files: where they come from,
//! which sets are accepted, and how a line is read.
//!
//! The files are © Mike Cowlishaw and IBM, offered "as is" with no licence
//! grant, so kimmydb **never vendors them**. They are fetched at test time
//! (`scripts/fetch-dectest.sh`) from a public source, checked against the
//! sha256 pinned here, and kept in CI's own cache. A directory holding the
//! files is named by `KIMMY_DECTEST_DIR`.
//!
//! Two sets are accepted, each by the sha256 of every file:
//!
//! - **2.62**, from `https://speleotrove.com/decimal/dectest.zip`, whose own
//!   sha256 is [`ZIP_SHA256`];
//! - **2.59**, the copy CPython ships, at the pinned commit [`CPYTHON_COMMIT`].
//!   The two differ in `dqMultiply` by one case and in comments.
//!
//! Without the variable a test says so and passes locally, and fails when `CI`
//! is set, so that a CI that lost its fetch cannot go quiet.

#![allow(dead_code)]

use sha2::{Digest, Sha256};

pub const ZIP_SHA256: &str = "b70a224cd52e82b7a8150aedac5efa2d0cb3941696fd829bdbe674f9f65c3926";
pub const CPYTHON_COMMIT: &str = "0fb18b02c8ad56299d6a2910be0bab8ad601ef24";

/// One accepted set of the suite's files.
pub struct Set {
    pub version: &'static str,
    /// `(file, sha256)`.
    pub files: [(&'static str, &'static str); 5],
}

pub const SETS: [Set; 2] = [
    Set {
        version: "2.62",
        files: [
            ("dqBase.decTest", "22ee8af01c98c26eb581503bed93d38f9218530d1a7700a6c2a2f73ae972e60a"),
            ("dqAdd.decTest", "17d0213375adc66241ddb6000ecdc124a892396b9d44fffd94207ec92697944f"),
            (
                "dqSubtract.decTest",
                "977398b439fc3014addfdff3427182fe85fd106b725bf723c6567f30dc158d06",
            ),
            (
                "dqMultiply.decTest",
                "70f1f67c5bf8e0d89bd92c83d5ebebd15984bb746e33fca1afc7aff3585b476f",
            ),
            (
                "dqDivide.decTest",
                "a9443f8ea566c61b7f6e2b55d44a242d9f46f98db9304ac2a649fb54b5b754da",
            ),
        ],
    },
    Set {
        version: "2.59",
        files: [
            ("dqBase.decTest", "766b3086d3b98ede72cd5c9f98eca908fd9a72410b2679a0c6aa2e9e39c25430"),
            ("dqAdd.decTest", "c177a8be4d5c325db9c8357907b046bcf3160fe998192c81da2b3b756cc31ed7"),
            (
                "dqSubtract.decTest",
                "922e49be8743f06c4b150a1fce409a53028fca4805e85a19be0f982d246d1ca3",
            ),
            (
                "dqMultiply.decTest",
                "0cf9dd544e740aa467dde13541ad10c942600518cc436b1f5562bdf1be54a7d8",
            ),
            (
                "dqDivide.decTest",
                "e689e4eb4404c3e58229b4fb7b93eef39e2c5deaf757ed813023c20dd3eb09d4",
            ),
        ],
    },
];

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

/// The suite's files, read from `KIMMY_DECTEST_DIR` and held to one accepted
/// set: `(version, [(file, text)])`. `None`, after saying why, when the
/// variable is unset and this is not CI.
///
/// # Panics
/// In CI without the variable, and on a file that is not byte-for-byte one of
/// the pinned ones.
pub fn load() -> Option<(&'static str, Vec<(&'static str, String)>)> {
    let Some(dir) = std::env::var_os("KIMMY_DECTEST_DIR") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "KIMMY_DECTEST_DIR is not set in CI: the dectest files were not fetched \
             (scripts/fetch-dectest.sh)"
        );
        eprintln!(
            "skipped: set KIMMY_DECTEST_DIR to a directory holding the dectest files \
             (scripts/fetch-dectest.sh), or use CPython's Lib/test/decimaltestdata at \
             {CPYTHON_COMMIT} (version 2.59)"
        );
        return None;
    };
    let dir = std::path::Path::new(&dir);
    // The first file decides the set; every file must then match it.
    let first =
        std::fs::read(dir.join("dqBase.decTest")).expect("dqBase.decTest in KIMMY_DECTEST_DIR");
    let digest = sha256_hex(&first);
    let set = SETS
        .iter()
        .find(|set| set.files[0].1 == digest)
        .unwrap_or_else(|| panic!("dqBase.decTest has sha256 {digest}, which is in no pinned set"));
    let mut out = Vec::new();
    for (name, want) in set.files {
        let bytes = std::fs::read(dir.join(name)).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(sha256_hex(&bytes), want, "{name} is not the pinned {} file", set.version);
        out.push((name, String::from_utf8(bytes).expect("the suite's files are ASCII")));
    }
    Some((set.version, out))
}

/// One line of a `.decTest` file, split into its tokens: whitespace separates
/// them, a token in `'` or `"` quotes keeps its spaces and doubles a quote to
/// hold one, and `--` outside quotes starts a comment.
pub fn tokens(line: &str) -> Vec<String> {
    let chars: Vec<char> = line.chars().collect();
    let (mut out, mut i) = (Vec::new(), 0);
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '-' && chars.get(i + 1) == Some(&'-') {
            break;
        } else if c == '\'' || c == '"' {
            let mut token = String::new();
            i += 1;
            while i < chars.len() {
                if chars[i] == c {
                    if chars.get(i + 1) == Some(&c) {
                        token.push(c);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                token.push(chars[i]);
                i += 1;
            }
            out.push(token);
        } else {
            let start = i;
            while i < chars.len() && !chars[i].is_whitespace() {
                i += 1;
            }
            out.push(chars[start..i].iter().collect());
        }
    }
    out
}

/// What the suite's result string says the decQuad is, read by a reader
/// written for its two output forms (scientific and engineering) and sharing
/// nothing with the code under test: the 128 bits of that decQuad.
pub fn result_bits(result: &str) -> u128 {
    let (negative, rest) = match result.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, result),
    };
    let sign = u128::from(negative) << 127;
    if rest == "Infinity" {
        return sign | (0b11110 << 122);
    }
    for (prefix, signalling) in [("sNaN", true), ("NaN", false)] {
        if let Some(payload) = rest.strip_prefix(prefix) {
            let payload: u128 = if payload.is_empty() { 0 } else { payload.parse().unwrap() };
            return sign | (0b11111 << 122) | (u128::from(signalling) << 121) | payload;
        }
    }
    let (mantissa, exponent) = match rest.split_once('E') {
        Some((m, e)) => (m, e.parse::<i64>().unwrap()),
        None => (rest, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let coefficient: u128 = format!("{whole}{fraction}").parse().unwrap();
    let exponent = exponent - fraction.len() as i64;
    assert!(coefficient < 10u128.pow(34) && (-6176..=6111).contains(&exponent), "{result}");
    sign | (((exponent + 6176) as u128) << 113) | coefficient
}
