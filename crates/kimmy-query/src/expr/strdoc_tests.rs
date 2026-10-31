//! The string, document and type operators of ADR-260. Each expected value is
//! the reference's answer for the same input; `q8-probes/str.js` runs the same
//! cases against a server.

use bson::{Bson, Document, doc};
use kimmy_core::{Error, Result};

use super::Expr;

fn ev(expr: Document, d: &Document) -> Result<Bson> {
    Expr::parse(&Bson::Document(expr))?.eval(d)
}

fn ok(expr: Document) -> Bson {
    ev(expr, &Document::new()).expect("should evaluate")
}

fn on(expr: Document, d: Document) -> Bson {
    ev(expr, &d).expect("should evaluate")
}

fn s(v: &str) -> Bson {
    Bson::String(v.into())
}

fn regex_value(pattern: &str, options: &str) -> Bson {
    Bson::RegularExpression(bson::Regex {
        pattern: pattern.to_string().try_into().expect("valid pattern"),
        options: options.to_string().try_into().expect("valid options"),
    })
}

fn refused(expr: Document, d: Document, needle: &str) {
    let err = ev(expr, &d).expect_err("should be refused").to_string();
    assert!(err.contains(needle), "{err:?} should mention {needle:?}");
}

// -- $trim, $ltrim, $rtrim ------------------------------------------------

#[test]
fn trim_strips_whitespace_by_default() {
    assert_eq!(ok(doc! {"$trim": {"input": "  \t a b \n"}}), s("a b"));
    assert_eq!(ok(doc! {"$ltrim": {"input": "  a "}}), s("a "));
    assert_eq!(ok(doc! {"$rtrim": {"input": "  a "}}), s("  a"));
}

#[test]
fn trim_default_set_is_the_references() {
    // NUL, no-break space, ogham space mark, en quad .. hair space are in it.
    assert_eq!(
        ok(doc! {"$trim": {"input": "\u{0}\u{a0}\u{1680}\u{2000}x\u{200a}\u{b}\u{c}"}}),
        s("x")
    );
    // U+0085, U+2028, U+202F and U+3000 are not.
    for c in ['\u{85}', '\u{2028}', '\u{202f}', '\u{3000}'] {
        let input = format!("{c}x{c}");
        assert_eq!(ok(doc! {"$trim": {"input": input.clone()}}), s(&input));
    }
}

#[test]
fn trim_chars_are_a_set_of_code_points() {
    assert_eq!(ok(doc! {"$trim": {"input": "xyaxbyx", "chars": "xy"}}), s("axb"));
    assert_eq!(ok(doc! {"$ltrim": {"input": "ééaé", "chars": "é"}}), s("aé"));
    assert_eq!(ok(doc! {"$rtrim": {"input": "ééaé", "chars": "é"}}), s("ééa"));
    // Given `chars`, whitespace is no longer stripped; empty `chars` strips nothing.
    assert_eq!(ok(doc! {"$trim": {"input": " a ", "chars": "x"}}), s(" a "));
    assert_eq!(ok(doc! {"$trim": {"input": " a ", "chars": ""}}), s(" a "));
    assert_eq!(ok(doc! {"$trim": {"input": "", "chars": "x"}}), s(""));
    assert_eq!(ok(doc! {"$trim": {"input": "xxx", "chars": "x"}}), s(""));
}

#[test]
fn trim_null_and_missing_are_null_and_wrong_types_are_refused() {
    assert_eq!(on(doc! {"$trim": {"input": "$nope"}}, doc! {}), Bson::Null);
    assert_eq!(ok(doc! {"$trim": {"input": Bson::Null}}), Bson::Null);
    // A null `chars` is null too, not the default.
    assert_eq!(ok(doc! {"$trim": {"input": " a ", "chars": Bson::Null}}), Bson::Null);
    assert_eq!(on(doc! {"$trim": {"input": " a ", "chars": "$nope"}}, doc! {}), Bson::Null);
    refused(doc! {"$trim": {"input": 5}}, doc! {}, "$trim requires its input to be a string");
    refused(doc! {"$ltrim": {"input": "a", "chars": 5}}, doc! {}, "$ltrim requires 'chars'");
    refused(doc! {"$rtrim": {"input": ["a"]}}, doc! {}, "$rtrim requires its input");
    refused(doc! {"$trim": {"chars": "a"}}, doc! {}, "$trim needs `input`");
    refused(doc! {"$trim": {"input": "a", "char": "a"}}, doc! {}, "does not take `char`");
    refused(doc! {"$trim": "a"}, doc! {}, "takes a document");
}

// -- $replaceOne, $replaceAll -----------------------------------------------

#[test]
fn replace_one_and_all() {
    let f = |op: &str, i: &str, find: &str, with: &str| {
        ok(doc! {op: {"input": i, "find": find, "replacement": with}})
    };
    assert_eq!(f("$replaceOne", "a-b-c", "-", "+"), s("a+b-c"));
    assert_eq!(f("$replaceAll", "a-b-c", "-", "+"), s("a+b+c"));
    assert_eq!(f("$replaceAll", "aaa", "aa", "b"), s("ba"));
    assert_eq!(f("$replaceAll", "abc", "x", "y"), s("abc"));
    assert_eq!(f("$replaceOne", "", "a", "b"), s(""));
    assert_eq!(f("$replaceAll", "éa", "é", "e"), s("ea"));
    assert_eq!(f("$replaceAll", "a.b", ".", ""), s("ab"));
    // The find string is literal, not a pattern.
    assert_eq!(f("$replaceAll", "abc", ".", "x"), s("abc"));
    // An empty `find` matches before the first code point; for All, between each.
    assert_eq!(f("$replaceOne", "abc", "", "-"), s("-abc"));
    assert_eq!(f("$replaceAll", "abc", "", "-"), s("-a-b-c-"));
    assert_eq!(f("$replaceAll", "", "", "-"), s("-"));
    // The reference inserts between the bytes of a multi-byte character, and the
    // result is invalid UTF-8 that its client cannot read; this inserts between
    // code points.
    assert_eq!(f("$replaceAll", "éa", "", "-"), s("-é-a-"));
    assert_eq!(f("$replaceOne", "éa", "", "-"), s("-éa"));
}

#[test]
fn replace_null_and_wrong_types() {
    let d = doc! {};
    assert_eq!(
        on(doc! {"$replaceOne": {"input": "$nope", "find": "a", "replacement": "b"}}, d.clone()),
        Bson::Null
    );
    assert_eq!(
        ok(doc! {"$replaceAll": {"input": "a", "find": Bson::Null, "replacement": "b"}}),
        Bson::Null
    );
    assert_eq!(
        ok(doc! {"$replaceAll": {"input": "a", "find": "a", "replacement": Bson::Null}}),
        Bson::Null
    );
    refused(
        doc! {"$replaceOne": {"input": 1, "find": "a", "replacement": "b"}},
        d.clone(),
        "'input' be a string",
    );
    refused(
        doc! {"$replaceAll": {"input": "a", "find": 1, "replacement": "b"}},
        d.clone(),
        "'find' be a string",
    );
    refused(
        doc! {"$replaceAll": {"input": "a", "find": "a", "replacement": 1}},
        d.clone(),
        "'replacement'",
    );
    // A type error beats a null elsewhere.
    refused(
        doc! {"$replaceAll": {"input": Bson::Null, "find": 1, "replacement": "b"}},
        d.clone(),
        "'find'",
    );
    refused(doc! {"$replaceAll": {"input": "a", "find": "a"}}, d.clone(), "needs `replacement`");
    refused(
        doc! {"$replaceAll": {"input": "a", "find": "a", "replacement": "", "x": 1}},
        d,
        "does not take `x`",
    );
}

// -- $regexMatch, $regexFind, $regexFindAll ----------------------------------

#[test]
fn regex_match_basics_and_options() {
    let m = |input: &str, re: &str, opts: Option<&str>| {
        let mut spec = doc! {"input": input, "regex": re};
        if let Some(o) = opts {
            spec.insert("options", o);
        }
        ok(doc! {"$regexMatch": spec})
    };
    assert_eq!(m("hello", "ell", None), Bson::Boolean(true));
    assert_eq!(m("hello", "^ell", None), Bson::Boolean(false));
    assert_eq!(m("HELLO", "ell", None), Bson::Boolean(false));
    assert_eq!(m("HELLO", "ell", Some("i")), Bson::Boolean(true));
    assert_eq!(m("a\nb", "^b", None), Bson::Boolean(false));
    assert_eq!(m("a\nb", "^b", Some("m")), Bson::Boolean(true));
    assert_eq!(m("a\nb", "a.b", None), Bson::Boolean(false));
    assert_eq!(m("a\nb", "a.b", Some("s")), Bson::Boolean(true));
    assert_eq!(m("ab", "a b", Some("x")), Bson::Boolean(true));
    assert_eq!(m("", "", None), Bson::Boolean(true));
    assert_eq!(m("é", "^.$", None), Bson::Boolean(true));
    assert_eq!(m("abc", "b", Some("")), Bson::Boolean(true));
}

#[test]
fn regex_takes_a_regex_value() {
    let re = regex_value("ABC", "i");
    assert_eq!(
        ok(doc! {"$regexMatch": {"input": "xabcx", "regex": re.clone()}}),
        Bson::Boolean(true)
    );
    // Flags in both places are refused; empty `options` beside a regex's flags is not both.
    refused(
        doc! {"$regexMatch": {"input": "a", "regex": re.clone(), "options": "m"}},
        doc! {},
        "both 'regex' and 'options'",
    );
    assert_eq!(
        ok(doc! {"$regexMatch": {"input": "xabcx", "regex": re, "options": Bson::Null}}),
        Bson::Boolean(true)
    );
}

#[test]
fn regex_null_missing_and_wrong_types() {
    let d = doc! {};
    let each = |input: Bson, regex: Bson| -> [Bson; 3] {
        let spec = doc! {"input": input, "regex": regex};
        [
            ok(doc! {"$regexMatch": spec.clone()}),
            ok(doc! {"$regexFind": spec.clone()}),
            ok(doc! {"$regexFindAll": spec}),
        ]
    };
    let none = [Bson::Boolean(false), Bson::Null, Bson::Array(vec![])];
    assert_eq!(each(Bson::Null, s("a")), none);
    assert_eq!(each(s("a"), Bson::Null), none);
    assert_eq!(each(Bson::Null, Bson::Null), none);
    assert_eq!(
        on(doc! {"$regexMatch": {"input": "$nope", "regex": "a"}}, d.clone()),
        Bson::Boolean(false)
    );
    assert_eq!(
        on(doc! {"$regexFindAll": {"input": "a", "regex": "$nope"}}, d.clone()),
        Bson::Array(vec![])
    );
    refused(
        doc! {"$regexMatch": {"input": 5, "regex": "a"}},
        d.clone(),
        "$regexMatch needs 'input' to be of type string",
    );
    refused(
        doc! {"$regexFind": {"input": "a", "regex": 5}},
        d.clone(),
        "$regexFind needs 'regex' to be of type string or regex",
    );
    refused(
        doc! {"$regexFindAll": {"input": "a", "regex": "a", "options": 5}},
        d.clone(),
        "'options'",
    );
    // The type error comes even when the input is null.
    refused(doc! {"$regexMatch": {"input": Bson::Null, "regex": 5}}, d.clone(), "'regex'");
    refused(
        doc! {"$regexMatch": {"input": "a", "regex": "a", "options": "q"}},
        d.clone(),
        "unknown $regex flag",
    );
    refused(doc! {"$regexMatch": {"input": "a", "regex": "(a"}}, d.clone(), "cannot be used");
    refused(doc! {"$regexMatch": {"input": "a", "regex": "(a)\\1"}}, d.clone(), "cannot be used");
    refused(doc! {"$regexMatch": {"input": "a", "regex": "a\u{0}b"}}, d.clone(), "null byte");
    refused(doc! {"$regexMatch": {"input": "a"}}, d, "needs `regex`");
}

#[test]
fn regex_find_gives_match_idx_and_captures() {
    let got = ok(doc! {"$regexFind": {"input": "ab12cd", "regex": "([a-z])(\\d+)"}});
    assert_eq!(got, Bson::Document(doc! {"match": "b12", "idx": 1i64, "captures": ["b", "12"]}));
    // No groups: an empty captures array. No match: null.
    assert_eq!(
        ok(doc! {"$regexFind": {"input": "xay", "regex": "a"}}),
        Bson::Document(doc! {"match": "a", "idx": 1i64, "captures": []})
    );
    assert_eq!(ok(doc! {"$regexFind": {"input": "xay", "regex": "z"}}), Bson::Null);
    // A group that took no part is null.
    assert_eq!(
        ok(doc! {"$regexFind": {"input": "b", "regex": "(a)?(b)"}}),
        Bson::Document(doc! {"match": "b", "idx": 0i64, "captures": [Bson::Null, "b"]})
    );
    // idx counts code points, not bytes.
    assert_eq!(
        ok(doc! {"$regexFind": {"input": "éé🙂x", "regex": "x"}}),
        Bson::Document(doc! {"match": "x", "idx": 3i64, "captures": []})
    );
    // The leftmost match, greedy.
    assert_eq!(
        ok(doc! {"$regexFind": {"input": "aaa", "regex": "a+"}}),
        Bson::Document(doc! {"match": "aaa", "idx": 0i64, "captures": []})
    );
}

#[test]
fn regex_find_all_several_and_zero_width_matches() {
    let all = ok(doc! {"$regexFindAll": {"input": "a1b22c333", "regex": "(\\d+)"}});
    assert_eq!(
        all,
        Bson::Array(vec![
            Bson::Document(doc! {"match": "1", "idx": 1i64, "captures": ["1"]}),
            Bson::Document(doc! {"match": "22", "idx": 3i64, "captures": ["22"]}),
            Bson::Document(doc! {"match": "333", "idx": 6i64, "captures": ["333"]}),
        ])
    );
    assert_eq!(ok(doc! {"$regexFindAll": {"input": "abc", "regex": "z"}}), Bson::Array(vec![]));
    // Matches do not overlap.
    let idx = |input: &str, re: &str| -> Vec<i64> {
        let Bson::Array(items) = ok(doc! {"$regexFindAll": {"input": input, "regex": re}}) else {
            panic!("an array")
        };
        items.iter().map(|m| m.as_document().unwrap().get_i64("idx").unwrap()).collect()
    };
    assert_eq!(idx("aaaa", "aa"), vec![0, 2]);
    // Zero-width: one per position, each search starting a code point past an
    // empty match, and an empty match found where a non-empty one ended. The
    // loop stops when the next search would start at the end of the input, so
    // there is no empty match at the end unless a search began before it.
    // Every row is the reference's answer.
    assert_eq!(idx("abc", ""), vec![0, 1, 2]);
    assert_eq!(idx("abc", "a*"), vec![0, 1, 2]);
    assert_eq!(idx("aaa", "a*"), vec![0]);
    assert_eq!(idx("ab", "b*"), vec![0, 1]);
    assert_eq!(idx("ab", "a|"), vec![0, 1]);
    assert_eq!(idx("é", "x*"), vec![0]);
    assert_eq!(idx("éa", "x*"), vec![0, 1]);
    // An empty input still gets its one search.
    assert_eq!(idx("", ""), vec![0]);
    assert_eq!(idx("", "x*"), vec![0]);
    // Anchors see the whole string, not just the rest of it.
    assert_eq!(idx("aaa", "^a"), vec![0]);
    assert_eq!(idx("ab cd", "\\b"), vec![0, 2, 3, 5]);
    // The matches themselves, not only where they are.
    assert_eq!(
        ok(doc! {"$regexFindAll": {"input": "abc", "regex": "a*"}}),
        Bson::Array(vec![
            Bson::Document(doc! {"match": "a", "idx": 0i64, "captures": []}),
            Bson::Document(doc! {"match": "", "idx": 1i64, "captures": []}),
            Bson::Document(doc! {"match": "", "idx": 2i64, "captures": []}),
        ])
    );
    assert_eq!(
        ok(doc! {"$regexFindAll": {"input": "", "regex": ""}}),
        Bson::Array(vec![Bson::Document(doc! {"match": "", "idx": 0i64, "captures": []})])
    );
}

#[test]
fn regex_computed_from_the_document() {
    let d = doc! {"s": "Hello", "p": "L+", "o": "i", "re": null};
    let find = doc! {"$regexFind": {"input": "$s", "regex": "$p", "options": "$o"}};
    assert_eq!(
        on(find.clone(), d.clone()),
        Bson::Document(doc! {"match": "ll", "idx": 2i64, "captures": []})
    );
    // The same expression over other rows compiles its pattern again.
    assert_eq!(
        on(find.clone(), doc! {"s": "xx", "p": "x"}),
        Bson::Document(doc! {"match": "x", "idx": 0i64, "captures": []})
    );
    assert_eq!(
        on(find, doc! {"s": "xX", "p": "X"}),
        Bson::Document(doc! {"match": "X", "idx": 1i64, "captures": []})
    );
    assert_eq!(on(doc! {"$regexMatch": {"input": "$s", "regex": "$re"}}, d), Bson::Boolean(false));
}

#[test]
fn a_constant_bad_regex_is_refused_at_parse() {
    let bad = Bson::Document(doc! {"$regexMatch": {"input": "$s", "regex": "(a"}});
    assert!(Expr::parse(&bad).is_err());
    let flag = Bson::Document(doc! {"$regexMatch": {"input": "$s", "regex": "a", "options": "z"}});
    assert!(Expr::parse(&flag).is_err());
}

// -- $indexOfCP, $indexOfBytes ---------------------------------------------

#[test]
fn index_of_cp_counts_code_points() {
    let f = |args: Vec<Bson>| ok(doc! {"$indexOfCP": args});
    assert_eq!(f(vec![s("cafétéria"), s("t")]), Bson::Int64(4));
    assert_eq!(f(vec![s("cafétéria"), s("é"), Bson::Int32(4)]), Bson::Int64(5));
    assert_eq!(f(vec![s("a🙂b"), s("b")]), Bson::Int64(2));
    assert_eq!(f(vec![s("abc"), s("z")]), Bson::Int64(-1));
    assert_eq!(f(vec![s("abcabc"), s("b"), Bson::Int32(2)]), Bson::Int64(4));
    // For code points `end` bounds where a match may start, so the substring may
    // run past it (`$indexOfBytes` is the other way: see below).
    assert_eq!(f(vec![s("abcabc"), s("bc"), Bson::Int32(0), Bson::Int32(2)]), Bson::Int64(1));
    assert_eq!(f(vec![s("abcabc"), s("bc"), Bson::Int32(0), Bson::Int32(1)]), Bson::Int64(-1));
    assert_eq!(f(vec![s("abcabc"), s("bc"), Bson::Int32(0), Bson::Int32(3)]), Bson::Int64(1));
    assert_eq!(f(vec![s("abcabc"), s("bc"), Bson::Int32(2), Bson::Int32(6)]), Bson::Int64(4));
    assert_eq!(f(vec![s("abc"), s("c"), Bson::Int32(0), Bson::Int32(100)]), Bson::Int64(2));
    assert_eq!(f(vec![s("abc"), s("c"), Bson::Int32(5)]), Bson::Int64(-1));
    // The same over multi-byte text: positions are code points.
    assert_eq!(f(vec![s("éééab"), s("ab"), Bson::Int32(0), Bson::Int32(3)]), Bson::Int64(-1));
    assert_eq!(f(vec![s("éééab"), s("ab"), Bson::Int32(0), Bson::Int32(4)]), Bson::Int64(3));
    // An empty substring is found at `start` only where a match may start: before
    // the end of the string and before `end`.
    assert_eq!(f(vec![s("abc"), s("")]), Bson::Int64(0));
    assert_eq!(f(vec![s("abc"), s(""), Bson::Int32(2)]), Bson::Int64(2));
    assert_eq!(f(vec![s("abc"), s(""), Bson::Int32(3)]), Bson::Int64(-1));
    assert_eq!(f(vec![s("abc"), s(""), Bson::Int32(1), Bson::Int32(1)]), Bson::Int64(-1));
    assert_eq!(f(vec![s("abc"), s(""), Bson::Int32(1), Bson::Int32(2)]), Bson::Int64(1));
    assert_eq!(f(vec![s("abc"), s(""), Bson::Int32(4)]), Bson::Int64(-1));
    assert_eq!(f(vec![s(""), s("")]), Bson::Int64(0));
    assert_eq!(f(vec![s(""), s("a")]), Bson::Int64(-1));
    // start > end finds nothing.
    assert_eq!(f(vec![s("abc"), s("a"), Bson::Int32(2), Bson::Int32(1)]), Bson::Int64(-1));
    // Integral doubles are accepted.
    assert_eq!(f(vec![s("abc"), s("c"), Bson::Double(1.0)]), Bson::Int64(2));
}

#[test]
fn index_of_bytes_counts_bytes() {
    let f = |args: Vec<Bson>| ok(doc! {"$indexOfBytes": args});
    assert_eq!(f(vec![s("cafétéria"), s("t")]), Bson::Int64(5));
    assert_eq!(f(vec![s("a🙂b"), s("b")]), Bson::Int64(5));
    assert_eq!(f(vec![s("héllo"), s("llo"), Bson::Int32(2)]), Bson::Int64(3));
    assert_eq!(f(vec![s("héllo"), s("l"), Bson::Int32(0), Bson::Int32(3)]), Bson::Int64(-1));
    assert_eq!(f(vec![s("abc"), s("c"), Bson::Int32(100)]), Bson::Int64(-1));
    assert_eq!(f(vec![s("abc"), s("c"), Bson::Int32(0), Bson::Int32(100)]), Bson::Int64(2));
    // `end` truncates the haystack: the substring must lie wholly before it, where
    // `$indexOfCP` lets it run past.
    assert_eq!(f(vec![s("abcabc"), s("bc"), Bson::Int32(0), Bson::Int32(2)]), Bson::Int64(-1));
    assert_eq!(f(vec![s("abcabc"), s("bc"), Bson::Int32(0), Bson::Int32(3)]), Bson::Int64(1));
    // An empty substring is found at `start` up to the end of the string.
    assert_eq!(f(vec![s("abc"), s(""), Bson::Int32(3)]), Bson::Int64(3));
    assert_eq!(f(vec![s("abc"), s(""), Bson::Int32(4)]), Bson::Int64(-1));
    assert_eq!(f(vec![s(""), s("")]), Bson::Int64(0));
}

#[test]
fn index_of_null_and_errors() {
    assert_eq!(ok(doc! {"$indexOfCP": [Bson::Null, "a"]}), Bson::Null);
    assert_eq!(on(doc! {"$indexOfBytes": ["$nope", "a"]}, doc! {}), Bson::Null);
    let d = doc! {};
    refused(
        doc! {"$indexOfCP": [5, "a"]},
        d.clone(),
        "$indexOfCP requires a string as the first argument",
    );
    refused(doc! {"$indexOfCP": ["a", Bson::Null]}, d.clone(), "second argument");
    refused(doc! {"$indexOfBytes": ["a", 5]}, d.clone(), "second argument");
    refused(doc! {"$indexOfCP": ["a", "a", 1.5]}, d.clone(), "starting");
    refused(doc! {"$indexOfCP": ["a", "a", "x"]}, d.clone(), "integral starting index");
    refused(doc! {"$indexOfCP": ["a", "a", -1]}, d.clone(), "nonnegative starting index");
    refused(doc! {"$indexOfBytes": ["a", "a", 0, -1]}, d.clone(), "nonnegative ending index");
    refused(doc! {"$indexOfCP": ["a"]}, d.clone(), "takes 2 to 4 argument");
    refused(doc! {"$indexOfCP": ["a", "a", 0, 0, 0]}, d, "takes 2 to 4 argument");
}

// -- $strcasecmp, $strLenBytes, $substrBytes ----------------------------------

#[test]
fn strcasecmp_folds_ascii_only() {
    let f = |a: Bson, b: Bson| ok(doc! {"$strcasecmp": [a, b]});
    assert_eq!(f(s("abc"), s("ABC")), Bson::Int32(0));
    assert_eq!(f(s("a"), s("B")), Bson::Int32(-1));
    assert_eq!(f(s("b"), s("A")), Bson::Int32(1));
    assert_eq!(f(s("ab"), s("abc")), Bson::Int32(-1));
    assert_eq!(f(s(""), s("")), Bson::Int32(0));
    assert_eq!(f(s("é"), s("É")), Bson::Int32(1));
    // Null and missing are the empty string; numbers are their text.
    assert_eq!(f(Bson::Null, s("")), Bson::Int32(0));
    assert_eq!(f(Bson::Null, s("a")), Bson::Int32(-1));
    assert_eq!(on(doc! {"$strcasecmp": ["$nope", "a"]}, doc! {}), Bson::Int32(-1));
    assert_eq!(f(Bson::Int32(12), s("12")), Bson::Int32(0));
    // The fold is to upper case, so `_` and `[` (between `Z` and `a`) sort below
    // a letter's lower-case form, not above (folding down would give -1).
    assert_eq!(f(s("_"), s("a")), Bson::Int32(1));
    assert_eq!(f(s("["), s("a")), Bson::Int32(1));
    assert_eq!(f(s("a"), s("_")), Bson::Int32(-1));
    assert_eq!(f(s("_"), s("A")), Bson::Int32(1));
    assert_eq!(f(s("Z"), s("a")), Bson::Int32(1));
    refused(doc! {"$strcasecmp": [["a"], "a"]}, doc! {}, "$strcasecmp can't convert");
    refused(doc! {"$strcasecmp": ["a"]}, doc! {}, "takes exactly 2");
}

#[test]
fn a_double_is_coerced_to_text_with_six_significant_digits() {
    // The reference's `coerceToString` prints a double as `%g` does; `$toString`
    // prints the shortest text that reads back.
    let f = |a: Bson, b: &str| ok(doc! {"$strcasecmp": [a, b]});
    assert_eq!(f(Bson::Double(1e21), "1e+21"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(0.1 + 0.2), "0.3"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(123456789012.0), "1.23457e+11"), Bson::Int32(0));
    // "1.23457E+11" against "123456789012": '.' sorts below '2'.
    assert_eq!(f(Bson::Double(123456789012.0), "123456789012"), Bson::Int32(-1));
    assert_eq!(f(Bson::Double(f64::NAN), "nan"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(f64::INFINITY), "inf"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(f64::NEG_INFINITY), "-inf"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(2.5), "2.5"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(2.0), "2"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(-0.0), "-0"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(100_000.0), "100000"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(1_000_000.0), "1e+06"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(999_999.5), "1e+06"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(0.0001), "0.0001"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(0.00001), "1e-05"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(-1234.5678), "-1234.57"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(1.5e-7), "1.5e-07"), Bson::Int32(0));
    assert_eq!(f(Bson::Double(f64::MAX), "1.79769e+308"), Bson::Int32(0));
    // Integers print in full.
    assert_eq!(f(Bson::Int64(123456789012), "123456789012"), Bson::Int32(0));
    // `$substrBytes` coerces the same way.
    assert_eq!(ok(doc! {"$substrBytes": [Bson::Double(123456789012.0), 0, 7]}), s("1.23457"));
}

#[test]
fn str_len_bytes() {
    assert_eq!(ok(doc! {"$strLenBytes": "abc"}), Bson::Int64(3));
    assert_eq!(ok(doc! {"$strLenBytes": "é"}), Bson::Int64(2));
    assert_eq!(ok(doc! {"$strLenBytes": "🙂"}), Bson::Int64(4));
    assert_eq!(ok(doc! {"$strLenBytes": ""}), Bson::Int64(0));
    // Null is refused, as $strLenCP refuses it.
    refused(doc! {"$strLenBytes": Bson::Null}, doc! {}, "$strLenBytes requires a string");
    refused(doc! {"$strLenBytes": "$nope"}, doc! {}, "$strLenBytes requires a string");
    refused(doc! {"$strLenBytes": 5}, doc! {}, "$strLenBytes requires a string");
}

#[test]
fn substr_bytes_lands_on_code_point_boundaries() {
    let f = |i: &str, a: i32, b: i32| ok(doc! {"$substrBytes": [i, a, b]});
    assert_eq!(f("abcdef", 1, 3), s("bcd"));
    assert_eq!(f("abcdef", 0, 100), s("abcdef"));
    assert_eq!(f("abcdef", 6, 1), s(""));
    assert_eq!(f("abcdef", 10, 1), s(""));
    assert_eq!(f("abcdef", 2, 0), s(""));
    assert_eq!(f("abcdef", 2, -1), s("cdef"));
    assert_eq!(f("", 0, 1), s(""));
    assert_eq!(f("éa", 0, 2), s("é"));
    assert_eq!(f("éa", 2, 1), s("a"));
    assert_eq!(f("aéb", 1, 2), s("é"));
    assert_eq!(f("aéb", 1, 100), s("éb"));
    let d = doc! {};
    refused(
        doc! {"$substrBytes": ["éa", 1, 1]},
        d.clone(),
        "starting index is a UTF-8 continuation byte",
    );
    refused(
        doc! {"$substrBytes": ["éa", 0, 1]},
        d.clone(),
        "ending index is a UTF-8 continuation byte",
    );
    refused(doc! {"$substrBytes": ["🙂", 0, 3]}, d.clone(), "ending index");
    // A negative start is refused, and so is a NaN or an infinity or an
    // out-of-range number for start or length; -0.5 truncates to 0.
    refused(
        doc! {"$substrBytes": ["abcdef", -1, 2]},
        d.clone(),
        "starting index must be non-negative (got: -1)",
    );
    refused(doc! {"$substrBytes": ["abc", Bson::Int64(i64::MIN), 1]}, d.clone(), "non-negative");
    refused(
        doc! {"$substrBytes": ["abc", Bson::Double(f64::NAN), 1]},
        d.clone(),
        "Can't coerce out of range value nan to long",
    );
    refused(
        doc! {"$substrBytes": ["abc", 1, Bson::Double(f64::NAN)]},
        d.clone(),
        "Can't coerce out of range value nan to long",
    );
    refused(
        doc! {"$substrBytes": ["abc", 1, Bson::Double(f64::INFINITY)]},
        d.clone(),
        "out of range value inf",
    );
    refused(doc! {"$substrBytes": ["abc", Bson::Double(1e19), 1]}, d.clone(), "out of range");
    assert_eq!(ok(doc! {"$substrBytes": ["abc", Bson::Double(-0.5), 1]}), s("a"));
    assert_eq!(ok(doc! {"$substrBytes": ["abcdef", 1, Bson::Int64(i64::MIN)]}), s("bcdef"));
    assert_eq!(ok(doc! {"$substrBytes": ["abcdef", Bson::Int64(i64::MAX), 1]}), s(""));
    // A negative length after a multi-byte character reads to the end.
    assert_eq!(f("éa", 2, -1), s("a"));
    // A start beyond the string is "" before the end is looked at.
    assert_eq!(f("é", 5, 1), s(""));
    // Null is the empty string; numbers are their text; doubles truncate.
    assert_eq!(ok(doc! {"$substrBytes": [Bson::Null, 0, 1]}), s(""));
    assert_eq!(ok(doc! {"$substrBytes": [12345, 1, 2]}), s("23"));
    assert_eq!(ok(doc! {"$substrBytes": ["abcdef", 1.9, 2.9]}), s("bc"));
    refused(
        doc! {"$substrBytes": ["a", "0", 1]},
        d.clone(),
        "starting index must be a numeric type",
    );
    refused(
        doc! {"$substrBytes": ["a", 0, Bson::Null]},
        d.clone(),
        "length must be a numeric type",
    );
    refused(doc! {"$substrBytes": [["a"], 0, 1]}, d, "can't convert");
}

// -- the regex operators' classes and flags ---------------------------------

#[test]
fn regex_classes_are_ascii_and_the_u_flag_is_accepted() {
    // Each row is the reference's answer for `$regexMatch`.
    let m = |input: &str, re: &str, opts: &str| {
        ok(doc! {"$regexMatch": {"input": input, "regex": re, "options": opts}})
    };
    let (yes, no) = (Bson::Boolean(true), Bson::Boolean(false));
    assert_eq!(m("\u{663}", "\\d", ""), no);
    assert_eq!(m("\u{663}", "\\D", ""), yes);
    assert_eq!(m("é", "\\w", ""), no);
    assert_eq!(m("é", "[\\w]", ""), no);
    assert_eq!(m("\u{a0}", "\\s", ""), no);
    assert_eq!(m("7", "\\d", ""), yes);
    assert_eq!(m("a_9", "^\\w+$", ""), yes);
    assert_eq!(m("\t\n ", "^\\s+$", ""), yes);
    assert_eq!(m("é", "^.$", ""), yes);
    // Case-insensitive matching is Unicode: É is é, and the Kelvin sign is k.
    assert_eq!(m("É", "é", "i"), yes);
    assert_eq!(m("\u{212a}", "k", "i"), yes);
    // `u` is accepted and changes nothing.
    assert_eq!(m("a", "a", "u"), yes);
    assert_eq!(m("é", "\\w", "u"), no);
    assert_eq!(ok(doc! {"$regexMatch": {"input": "xax", "regex": regex_value("a", "u")}}), yes);
    // What the engine does not run is still refused.
    for re in ["(a)\\1", "(?=a)", "(?<!a)b", "\\Qa\\E", "(?>a)"] {
        refused(doc! {"$regexMatch": {"input": "a", "regex": re}}, doc! {}, "cannot be used");
    }
}

// -- the string-length check and the cap on $regexFindAll ---------------------

#[test]
fn concat_refuses_a_string_over_the_limit_before_building_it() {
    use super::concat_within;
    let sized = |bytes: usize, max: usize| concat_within(&[s("aaaa"), s(&"b".repeat(bytes))], max);
    assert_eq!(sized(4, 8).unwrap(), s("aaaabbbb"));
    let err = sized(5, 8).unwrap_err();
    assert!(
        matches!(err, Error::AnswerTooLarge { bytes: 9, max: 8, .. }),
        "{err:?}: the limit is exact, and the answer is the 413 kind"
    );
    // A null makes the result null whatever the sizes, and a type error comes in
    // argument order, as before.
    assert_eq!(concat_within(&[s("aaaa"), s("bbbbbbbb"), Bson::Null], 8).unwrap(), Bson::Null);
    let err = concat_within(&[s("aaaa"), Bson::Int32(1), s("bbbbbbbb")], 8).unwrap_err();
    assert!(err.to_string().contains("$concat needs strings"), "{err}");
    assert_eq!(concat_within(&[], 0).unwrap(), s(""));
}

#[test]
fn a_reduce_that_doubles_a_string_stops_at_16_mib() {
    use kimmy_core::limits::MAX_DOCUMENT_BYTES;
    let double = |steps: usize| {
        ev(
            doc! {"$reduce": {
                "input": vec![Bson::Int32(0); steps],
                "initialValue": "a",
                "in": {"$concat": ["$$value", "$$value"]},
            }},
            &doc! {},
        )
    };
    // 2^24 bytes is exactly the limit; one more doubling is over it.
    let Bson::String(at_limit) = double(24).unwrap() else { panic!("a string") };
    assert_eq!(at_limit.len(), MAX_DOCUMENT_BYTES);
    drop(at_limit);
    let err = double(25).unwrap_err();
    assert!(
        matches!(&err, Error::AnswerTooLarge { bytes, max, .. }
            if *bytes == 2 * MAX_DOCUMENT_BYTES && *max == MAX_DOCUMENT_BYTES),
        "{err:?}"
    );
}

#[test]
fn replace_refuses_a_result_over_the_limit_before_building_it() {
    use super::strings::replace;
    let refused_at = |input: &str, find: &str, with: &str, all: bool, max: usize| -> usize {
        match replace(input, find, with, all, max) {
            Err(Error::AnswerTooLarge { bytes, max: m, .. }) if m == max => bytes,
            other => panic!("expected AnswerTooLarge, got {other:?}"),
        }
    };
    // The limit is exact: "-a-b-c-" is 7 bytes.
    assert_eq!(replace("abc", "", "-", true, 7).unwrap(), "-a-b-c-");
    assert_eq!(refused_at("abc", "", "-", true, 6), 7);
    // $replaceOne: 3 - 1 + 4 = 6.
    assert_eq!(replace("abc", "b", "xxxx", false, 6).unwrap(), "axxxxc");
    assert_eq!(refused_at("abc", "b", "xxxx", false, 5), 6);
    assert_eq!(replace("abc", "", "xx", false, 5).unwrap(), "xxabc");
    assert_eq!(refused_at("abc", "", "xx", false, 4), 5);
    // $replaceAll counts every occurrence: "aaa" with "a" -> "bb" is 6 bytes.
    assert_eq!(replace("aaa", "a", "bb", true, 6).unwrap(), "bbbbbb");
    assert_eq!(refused_at("aaa", "a", "bb", true, 5), 6);
    // Non-overlapping occurrences, and a shrinking replacement is not refused.
    assert_eq!(replace("aaaaa", "aa", "b", true, 3).unwrap(), "bba");
    assert_eq!(replace("aaaaa", "aa", "", true, 1).unwrap(), "a");
    assert_eq!(replace("é", "", "-", true, 5).unwrap(), "-é-");
    // Nothing found: the input's own length.
    assert_eq!(replace("abc", "z", "yyyyyyyy", true, 3).unwrap(), "abc");
    assert_eq!(replace("abc", "z", "yyyyyyyy", false, 3).unwrap(), "abc");
    // An empty `find` over a megabyte with a megabyte of replacement asks for a
    // terabyte: refused at once, with the size it asked for.
    let mib = "x".repeat(1 << 20);
    let asked = refused_at(&mib, "", &mib, true, super::MAX_STRING_BYTES);
    assert_eq!(asked, (mib.len() + 1) * mib.len() + mib.len());
}

#[test]
fn the_operators_refuse_a_result_over_16_mib() {
    let big = "x".repeat(1 << 20);
    let d = doc! {"a": big.clone(), "b": big};
    for op in ["$replaceAll", "$replaceOne"] {
        let err = ev(doc! {op: {"input": "$a", "find": "", "replacement": "$b"}}, &d);
        // $replaceOne asks for 2 MiB and is fine.
        if op == "$replaceAll" {
            assert!(matches!(err, Err(Error::AnswerTooLarge { .. })), "{err:?}");
        } else {
            assert!(matches!(err, Ok(Bson::String(ref r)) if r.len() == 2 << 20), "{op}");
        }
    }
}

#[test]
fn regex_find_all_counts_idx_from_the_previous_match() {
    // Two-byte and four-byte characters: idx counts code points, and the count
    // carries from one match to the next.
    let text = format!("{}x{}x{}", "é".repeat(3), "🙂".repeat(2), "ab");
    let Bson::Array(found) = ok(doc! {"$regexFindAll": {"input": text, "regex": "x|b"}}) else {
        panic!("an array")
    };
    let at: Vec<(&str, i64)> = found
        .iter()
        .map(|m| {
            let m = m.as_document().unwrap();
            (m.get_str("match").unwrap(), m.get_i64("idx").unwrap())
        })
        .collect();
    assert_eq!(at, vec![("x", 3), ("x", 6), ("b", 8)]);

    // An empty pattern over many two-byte characters: one match per character,
    // each with its own index.
    let n = 20_000;
    let Bson::Array(found) = ok(doc! {"$regexFindAll": {"input": "é".repeat(n), "regex": ""}})
    else {
        panic!("an array")
    };
    assert_eq!(found.len(), n);
    for i in [0, 1, 777, n - 1] {
        assert_eq!(found[i].as_document().unwrap().get_i64("idx").unwrap(), i as i64);
    }
}

#[test]
fn regex_find_all_is_bounded_by_the_bytes_of_its_answer() {
    use super::strings::{find_all_documents, match_bytes, match_document};
    use kimmy_core::regex::RegexPattern;

    // The count is the BSON size of each element of the answer's array.
    let text = "éa🙂bc";
    for (pattern, index) in [("a", 0usize), ("(a)(x)?(🙂)", 9), ("b|c", 123), ("", 1000)] {
        let p = RegexPattern::new(pattern, "").unwrap();
        for m in p.find_all(text) {
            let doc = match_document(text, &m, 0);
            let Bson::Document(doc) = doc else { panic!("a document") };
            let element =
                1 + index.to_string().len() + 1 + bson::serialize_to_vec(&doc).unwrap().len();
            assert_eq!(match_bytes(text, &m, index), element, "{pattern:?} at {index}");
        }
    }

    let p = RegexPattern::new("a", "").unwrap();
    let one = match_bytes("a", &p.find_all("a")[0], 0);
    let refusal = |text: &str, max: usize| match find_all_documents(&p, text, max) {
        Err(Error::AnswerTooLarge { bytes, max: m, .. }) if m == max => bytes,
        other => panic!("expected AnswerTooLarge, got {other:?}"),
    };
    // Exactly at the limit is answered; one byte under refuses at the match that
    // passes it, naming the bytes counted by then.
    let Bson::Array(three) = find_all_documents(&p, "aaa", 3 * one).unwrap() else { panic!() };
    assert_eq!(three.len(), 3);
    assert_eq!(refusal("aaa", 3 * one - 1), 3 * one);
    assert_eq!(refusal("aaaa", 3 * one), 4 * one);
}

// -- $mergeObjects -----------------------------------------------------------

#[test]
fn merge_objects_later_wins_and_keys_keep_first_place() {
    let got = ok(doc! {"$mergeObjects": [{"a": 1, "b": 2}, {"c": 3, "a": 9}, {"d": 4}]});
    let want = doc! {"a": 9, "b": 2, "c": 3, "d": 4};
    assert_eq!(got, Bson::Document(want.clone()));
    // Key order is part of the answer.
    assert_eq!(
        got.as_document().unwrap().keys().collect::<Vec<_>>(),
        want.keys().collect::<Vec<_>>()
    );
}

#[test]
fn merge_objects_nulls_and_missing_are_skipped() {
    assert_eq!(
        ok(doc! {"$mergeObjects": [Bson::Null, {"a": 1}, Bson::Null]}),
        Bson::Document(doc! {"a": 1})
    );
    assert_eq!(
        on(doc! {"$mergeObjects": ["$nope", "$o"]}, doc! {"o": {"x": 1}}),
        Bson::Document(doc! {"x": 1})
    );
    assert_eq!(ok(doc! {"$mergeObjects": [Bson::Null, Bson::Null]}), Bson::Document(doc! {}));
    assert_eq!(ok(doc! {"$mergeObjects": Bson::Array(vec![])}), Bson::Document(doc! {}));
    assert_eq!(ok(doc! {"$mergeObjects": Bson::Null}), Bson::Document(doc! {}));
    assert_eq!(
        on(doc! {"$mergeObjects": "$o"}, doc! {"o": {"x": 1}}),
        Bson::Document(doc! {"x": 1})
    );
    // A merge replaces a nested document whole.
    assert_eq!(
        ok(doc! {"$mergeObjects": [{"a": {"x": 1, "y": 2}}, {"a": {"x": 5}}]}),
        Bson::Document(doc! {"a": {"x": 5}})
    );
}

#[test]
fn merge_objects_refuses_a_non_document() {
    refused(
        doc! {"$mergeObjects": [{"a": 1}, 5]},
        doc! {},
        "$mergeObjects requires object inputs, but input 5 is of type int",
    );
    refused(doc! {"$mergeObjects": ["$arr"]}, doc! {"arr": [1]}, "of type array");
    refused(doc! {"$mergeObjects": "$s"}, doc! {"s": "x"}, "of type string");
}

#[test]
fn merge_objects_as_an_accumulator() {
    use crate::aggregate::{Limits, apply, parse};
    let run = |pipeline: Vec<Document>, input: Vec<Document>| -> Result<Vec<Document>> {
        let mut current = input;
        for stage in parse(&pipeline)? {
            current = apply(&stage, current, &Limits::default())?;
        }
        Ok(current)
    };
    let input = vec![
        doc! {"k": 1, "o": {"a": 1, "b": 1}},
        doc! {"k": 1, "o": null},
        doc! {"k": 1},
        doc! {"k": 1, "o": {"c": 3, "a": 2}},
        doc! {"k": 2},
        doc! {"k": 3, "o": null},
    ];
    let out = run(
        vec![
            doc! {"$group": {"_id": "$k", "m": {"$mergeObjects": "$o"}}},
            doc! {"$sort": {"_id": 1}},
        ],
        input.clone(),
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": 1, "m": {"a": 2, "b": 1, "c": 3}},
            doc! {"_id": 2, "m": {}},
            doc! {"_id": 3, "m": {}},
        ]
    );
    let err =
        run(vec![doc! {"$group": {"_id": "$k", "m": {"$mergeObjects": "$k"}}}], input).unwrap_err();
    assert!(err.to_string().contains("$mergeObjects requires object inputs"));
}

// -- $getField, $setField, $unsetField -----------------------------------------

#[test]
fn get_field_shorthand_and_named() {
    let d = doc! {"a": 1, "x.y": 2, "$price": 3, "o": {"k": 4}, "n": null, "s": "Hello"};
    assert_eq!(on(doc! {"$getField": "a"}, d.clone()), Bson::Int32(1));
    // A dotted name is a name, not a path.
    assert_eq!(on(doc! {"$getField": "x.y"}, d.clone()), Bson::Int32(2));
    assert_eq!(
        on(doc! {"$getField": {"field": "x.y", "input": "$$ROOT"}}, d.clone()),
        Bson::Int32(2)
    );
    assert_eq!(on(doc! {"$getField": {"field": "s", "input": "$$CURRENT"}}, d.clone()), s("Hello"));
    // A `$`-prefixed name needs $literal.
    assert_eq!(on(doc! {"$getField": {"$literal": "$price"}}, d.clone()), Bson::Int32(3));
    assert_eq!(
        on(doc! {"$getField": {"field": {"$literal": "$price"}, "input": "$$ROOT"}}, d.clone()),
        Bson::Int32(3)
    );
    assert_eq!(on(doc! {"$getField": {"field": "k", "input": "$o"}}, d.clone()), Bson::Int32(4));
    // An absent field is missing: null where a value is needed, $type "missing".
    assert_eq!(on(doc! {"$getField": "zz"}, d.clone()), Bson::Null);
    assert_eq!(on(doc! {"$type": {"$getField": "zz"}}, d.clone()), s("missing"));
    assert_eq!(on(doc! {"$type": {"$getField": "n"}}, d.clone()), s("null"));
    // A null input is null.
    assert_eq!(on(doc! {"$getField": {"field": "k", "input": "$n"}}, d.clone()), Bson::Null);
    assert_eq!(
        on(doc! {"$type": {"$getField": {"field": "k", "input": "$n"}}}, d.clone()),
        s("null")
    );
}

#[test]
fn get_field_of_an_input_that_is_missing_or_not_a_document_is_missing() {
    let d = doc! {"o": {"a": 1}, "arr": [1, 2], "str": "x", "num": 7};
    for input in ["$nope", "$arr", "$str", "$num"] {
        let got = on(doc! {"$getField": {"field": "a", "input": input}}, d.clone());
        assert_eq!(got, Bson::Null, "{input}");
        let kind = on(doc! {"$type": {"$getField": {"field": "a", "input": input}}}, d.clone());
        assert_eq!(kind, s("missing"), "{input}");
    }
    // A literal that is not a document too.
    assert_eq!(ok(doc! {"$type": {"$getField": {"field": "a", "input": "str"}}}), s("missing"));
    assert_eq!(ok(doc! {"$type": {"$getField": {"field": "a", "input": 5}}}), s("missing"));
    // And it is left out of a projection rather than failing the pipeline.
    use crate::aggregate::{Limits, apply, parse};
    let stages = parse(&[doc! {"$addFields": {
        "g": {"$getField": {"field": "a", "input": "$v"}},
    }}])
    .unwrap();
    let rows = vec![doc! {"v": {"a": 1}}, doc! {"v": "text"}, doc! {"v": [1]}, doc! {}];
    let out = apply(&stages[0], rows, &Limits::default()).unwrap();
    assert_eq!(
        out,
        vec![doc! {"v": {"a": 1}, "g": 1}, doc! {"v": "text"}, doc! {"v": [1]}, doc! {}]
    );
}

#[test]
fn field_names_are_constants() {
    let d = doc! {"a": 1, "o": {"a": 1}, "p": "a"};
    // The reference's refusals (5654600, 5654601, 5654602 / 4161106, 4161107, 4161108).
    // The named form has no default input.
    refused(doc! {"$getField": {"field": "a"}}, d.clone(), "needs `input`");
    // A computed name, even one that could be folded.
    refused(
        doc! {"$getField": {"field": {"$concat": ["a"]}, "input": "$o"}},
        d.clone(),
        "$getField requires 'field' to evaluate to a constant, but got a non-constant argument",
    );
    refused(
        doc! {"$getField": {"$concat": ["x", ".", "y"]}},
        d.clone(),
        "to evaluate to a constant",
    );
    // A field path, in either form.
    refused(
        doc! {"$getField": "$p"},
        d.clone(),
        "'$p' is a field path reference which is not allowed in this context",
    );
    refused(
        doc! {"$getField": {"field": "$p", "input": "$o"}},
        d.clone(),
        "Did you mean {$literal: '$p'}?",
    );
    refused(doc! {"$getField": "$$CURRENT.a"}, d.clone(), "is a field path reference");
    // Not a string.
    refused(
        doc! {"$getField": {"field": 5, "input": "$o"}},
        d.clone(),
        "$getField requires 'field' to evaluate to type String, but got int",
    );
    refused(
        doc! {"$getField": {"field": Bson::Null, "input": "$o"}},
        d.clone(),
        "to evaluate to type String, but got null",
    );
    refused(doc! {"$getField": 5}, d.clone(), "but got int");
    refused(doc! {"$getField": {"input": "$o"}}, d.clone(), "needs `field`");
    refused(doc! {"$getField": {"field": "a", "x": 1}}, d.clone(), "does not take `x`");
    refused(doc! {"$getField": ["a"]}, d.clone(), "not an array");
    // The same for $setField and $unsetField.
    refused(
        doc! {"$setField": {"field": {"$concat": ["a", "b"]}, "input": "$o", "value": 1}},
        d.clone(),
        "$setField requires 'field' to evaluate to a constant",
    );
    refused(
        doc! {"$unsetField": {"field": {"$concat": ["a"]}, "input": "$o"}},
        d.clone(),
        "$unsetField requires 'field' to evaluate to a constant",
    );
    refused(
        doc! {"$setField": {"field": "$p", "input": "$o", "value": 1}},
        d.clone(),
        "$setField: '$p' is a field path reference",
    );
    refused(
        doc! {"$setField": {"field": Bson::Null, "input": "$o", "value": 1}},
        d.clone(),
        "$setField requires 'field' to evaluate to type String, but got null",
    );
    refused(
        doc! {"$unsetField": {"field": 5, "input": "$o"}},
        d,
        "$unsetField requires 'field' to evaluate to type String, but got int",
    );
}

#[test]
fn set_field_adds_replaces_and_removes() {
    let d = doc! {"o": {"a": 1, "b": 2}, "n": null};
    let set = |field: &str, value: Bson| {
        on(doc! {"$setField": {"field": field, "input": "$o", "value": value}}, d.clone())
    };
    // An existing field keeps its place; a new one goes last.
    let got = set("a", Bson::Int32(9));
    assert_eq!(got, Bson::Document(doc! {"a": 9, "b": 2}));
    assert_eq!(got.as_document().unwrap().keys().collect::<Vec<_>>(), ["a", "b"]);
    let got = set("c", Bson::Int32(3));
    assert_eq!(got.as_document().unwrap().keys().collect::<Vec<_>>(), ["a", "b", "c"]);
    // Dots and `$` are part of the name.
    assert_eq!(set("x.y", Bson::Int32(1)), Bson::Document(doc! {"a": 1, "b": 2, "x.y": 1}));
    assert_eq!(
        on(doc! {"$setField": {"field": {"$literal": "$p"}, "input": "$o", "value": 1}}, d.clone()),
        Bson::Document(doc! {"a": 1, "b": 2, "$p": 1})
    );
    // $$REMOVE removes; a null value is stored.
    assert_eq!(set("a", Bson::String("$$REMOVE".into())), Bson::Document(doc! {"b": 2}));
    assert_eq!(set("zz", Bson::String("$$REMOVE".into())), Bson::Document(doc! {"a": 1, "b": 2}));
    assert_eq!(set("a", Bson::Null), Bson::Document(doc! {"a": null, "b": 2}));
    // The value may be any expression, and is read from the document.
    assert_eq!(
        on(
            doc! {"$setField": {"field": "t", "input": "$$ROOT", "value": {"$add": [1, 2]}}},
            doc! {"q": 1}
        ),
        Bson::Document(doc! {"q": 1, "t": 3i64})
    );
    // Null input is null; a non-document is refused.
    assert_eq!(
        on(doc! {"$setField": {"field": "a", "input": "$n", "value": 1}}, d.clone()),
        Bson::Null
    );
    assert_eq!(
        on(doc! {"$setField": {"field": "a", "input": "$nope", "value": 1}}, d.clone()),
        Bson::Null
    );
    refused(
        doc! {"$setField": {"field": "a", "input": 5, "value": 1}},
        d.clone(),
        "$setField requires 'input'",
    );
    refused(
        doc! {"$setField": {"field": 1, "input": "$o", "value": 1}},
        d.clone(),
        "'field' to evaluate to type String",
    );
    refused(doc! {"$setField": {"field": "a", "input": "$o"}}, d, "needs `value`");
}

#[test]
fn set_field_with_a_missing_value_removes() {
    // UNSURE in the probe: a value that is a missing field path.
    let d = doc! {"o": {"a": 1, "b": 2}};
    assert_eq!(
        on(doc! {"$setField": {"field": "a", "input": "$o", "value": "$nope"}}, d),
        Bson::Document(doc! {"b": 2})
    );
}

#[test]
fn unset_field_removes() {
    let d = doc! {"o": {"a": 1, "b": 2, "c.d": 3}, "n": null};
    assert_eq!(
        on(doc! {"$unsetField": {"field": "a", "input": "$o"}}, d.clone()),
        Bson::Document(doc! {"b": 2, "c.d": 3})
    );
    assert_eq!(
        on(doc! {"$unsetField": {"field": "c.d", "input": "$o"}}, d.clone()),
        Bson::Document(doc! {"a": 1, "b": 2})
    );
    assert_eq!(
        on(doc! {"$unsetField": {"field": "zz", "input": "$o"}}, d.clone()),
        Bson::Document(doc! {"a": 1, "b": 2, "c.d": 3})
    );
    assert_eq!(on(doc! {"$unsetField": {"field": "a", "input": "$n"}}, d.clone()), Bson::Null);
    refused(
        doc! {"$unsetField": {"field": "a", "input": 5}},
        d.clone(),
        "$unsetField requires 'input'",
    );
    refused(doc! {"$unsetField": {"field": "a"}}, d.clone(), "needs `input`");
    refused(
        doc! {"$unsetField": {"field": "a", "input": "$o", "value": 1}},
        d,
        "does not take `value`",
    );
}

#[test]
fn get_field_missing_is_left_out_of_a_projection() {
    use crate::aggregate::{Limits, apply, parse};
    let stages = parse(&[doc! {"$addFields": {
        "g": {"$getField": "nope"},
        "h": {"$getField": "a"},
        "t": {"$type": "$nope"},
    }}])
    .unwrap();
    let out = apply(&stages[0], vec![doc! {"a": 1}], &Limits::default()).unwrap();
    assert_eq!(out, vec![doc! {"a": 1, "h": 1, "t": "missing"}]);
}

// -- $type, $isNumber ---------------------------------------------------------

#[test]
fn type_names_every_value() {
    let d = doc! {
        "d": 1.5, "s": "x", "o": {}, "a": [], "b": true, "n": null, "i": 1i32, "l": 1i64,
        "dec": bson::Decimal128::from_bytes([0; 16]), "dt": bson::DateTime::from_millis(0),
        "oid": bson::oid::ObjectId::new(),
        "re": regex_value("a", ""),
        "bin": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![1] }),
        "ts": Bson::Timestamp(bson::Timestamp { time: 1, increment: 1 }),
        "min": Bson::MinKey, "max": Bson::MaxKey,
    };
    let t = |field: &str| on(doc! {"$type": format!("${field}")}, d.clone());
    for (field, want) in [
        ("d", "double"),
        ("s", "string"),
        ("o", "object"),
        ("a", "array"),
        ("b", "bool"),
        ("n", "null"),
        ("i", "int"),
        ("l", "long"),
        ("dec", "decimal"),
        ("dt", "date"),
        ("oid", "objectId"),
        ("re", "regex"),
        ("bin", "binData"),
        ("ts", "timestamp"),
        ("min", "minKey"),
        ("max", "maxKey"),
        ("nope", "missing"),
        ("o.x", "missing"),
    ] {
        assert_eq!(t(field), s(want), "{field}");
    }
    // The one-element array form, a literal, and an expression.
    assert_eq!(ok(doc! {"$type": ["$nope"]}), s("missing"));
    assert_eq!(ok(doc! {"$type": "abc"}), s("string"));
    assert_eq!(ok(doc! {"$type": Bson::Null}), s("null"));
    assert_eq!(ok(doc! {"$type": {"$literal": "$x"}}), s("string"));
    assert_eq!(ok(doc! {"$type": "$$REMOVE"}), s("missing"));
    assert_eq!(on(doc! {"$type": "$$ROOT.nope"}, doc! {"a": 1}), s("missing"));
    assert_eq!(on(doc! {"$type": "$$ROOT.a"}, doc! {"a": 1}), s("int"));
    assert_eq!(on(doc! {"$type": {"$cond": [true, "$nope", 1]}}, doc! {}), s("missing"));
    assert_eq!(on(doc! {"$type": {"$ifNull": ["$nope", 1]}}, doc! {}), s("int"));
    refused(doc! {"$type": [1, 2]}, doc! {}, "takes exactly 1");
}

#[test]
fn is_number() {
    let d = doc! {"i": 1i32, "l": 1i64, "d": 1.5, "dec": bson::Decimal128::from_bytes([0; 16]),
    "s": "1", "b": true, "n": null, "a": [1], "dt": bson::DateTime::from_millis(0)};
    for f in ["i", "l", "d", "dec"] {
        assert_eq!(on(doc! {"$isNumber": format!("${f}")}, d.clone()), Bson::Boolean(true), "{f}");
    }
    for f in ["s", "b", "n", "a", "dt", "nope"] {
        assert_eq!(on(doc! {"$isNumber": format!("${f}")}, d.clone()), Bson::Boolean(false), "{f}");
    }
    assert_eq!(ok(doc! {"$isNumber": 5}), Bson::Boolean(true));
    assert_eq!(ok(doc! {"$isNumber": [Bson::Double(f64::NAN)]}), Bson::Boolean(true));
}

#[test]
fn new_operators_see_the_variables_and_the_document() {
    let e = Bson::Document(doc! {"$trim": {"input": "$s", "chars": "$$c"}});
    let parsed = Expr::parse_with_vars(&e, &["c".to_string()]).unwrap();
    assert!(parsed.reads_document());
    assert!(parsed.reads_variable(&["c".to_string()]));
    assert!(!parsed.reads_variable(&["z".to_string()]));
    let lit = Expr::parse(&Bson::Document(doc! {"$getField": {"$literal": "a"}})).unwrap();
    assert!(lit.reads_document(), "the default input is $$CURRENT");
}
