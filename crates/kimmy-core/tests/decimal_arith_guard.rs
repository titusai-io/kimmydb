//! The decimal arithmetic is integer-only, and this holds the file to it
//! (ADR-252).
//!
//! `decimal/arith.rs` must give the same bytes on every architecture and build of
//! a release, so it may not touch a floating-point type or a platform's byte
//! order, and it may not format a number to digits (a formatter's rounding is
//! not a documented contract). The file's tests live beside it in
//! `arith_tests.rs`, which may use doubles freely: the guard reads this one file.
//! The same holds `decimal/arith/func.rs`, the decimal functions, to the same list.

const BANNED: &[&str] = &[
    "f32",
    "f64",
    "to_ne_bytes",
    "from_ne_bytes",
    "transmute",
    "format!",
    "to_string",
    "write!",
    "{:",
    "powi",
    "powf",
    "sqrt",
];

/// What `decimal/arith/func.rs` may not name. It is the same list but for the
/// word `sqrt`, which is the name of one of its functions (the square root of a
/// decimal); the float method of that name is still refused, as a call.
const BANNED_FUNC: &[&str] = &[
    "f32",
    "f64",
    "to_ne_bytes",
    "from_ne_bytes",
    "transmute",
    "format!",
    "to_string",
    "write!",
    "{:",
    "powi",
    "powf",
    ".sqrt(",
    "unsafe",
];

fn scan(file: &str, banned: &[&str]) -> Vec<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(file);
    let source = std::fs::read_to_string(&path).expect("the arithmetic source");
    let mut found = Vec::new();
    for (number, line) in source.lines().enumerate() {
        for token in banned {
            if line.contains(token) {
                found.push(format!(
                    "{}:{}: `{token}` in `{}`",
                    path.display(),
                    number + 1,
                    line.trim()
                ));
            }
        }
    }
    found
}

#[test]
fn the_arithmetic_names_no_float_and_no_formatter() {
    let found = scan("src/decimal/arith.rs", BANNED);
    assert!(found.is_empty(), "integer-only code names a banned token:\n{}", found.join("\n"));
}

/// The functions (`exp`, `ln`, `pow`, ...) are integer-only too: their tests live
/// in `func_tests.rs`, which may use doubles, and the guard reads this one file.
#[test]
fn the_functions_name_no_float_and_no_formatter() {
    let found = scan("src/decimal/arith/func.rs", BANNED_FUNC);
    assert!(found.is_empty(), "integer-only code names a banned token:\n{}", found.join("\n"));
}

#[test]
fn the_guard_sees_a_banned_token() {
    // The scan above finds nothing in a clean file only if it can find
    // something in a dirty one.
    let dirty = format!("let x = 1.0_{}{};", "f", "64");
    assert!(BANNED.iter().any(|token| dirty.contains(token)));
    assert!(BANNED_FUNC.iter().any(|token| dirty.contains(token)));
    let method = format!("x.{}(", "sqrt");
    assert!(BANNED_FUNC.iter().any(|token| method.contains(token)));
}
