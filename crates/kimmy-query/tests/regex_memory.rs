//! What a parsed filter holds for its `$regex` patterns, measured.
//!
//! A filter keeps each pattern compiled, for the life of the request, so the
//! compiled size is held rather than passed through. With `regex`'s own
//! limits, one `{"$regex": "\\w{209}"}` held 11 MiB after parse and a 1.4 KB
//! `$or` of forty of them 446 MiB. This counts the heap through the global
//! allocator and holds every filter the parser accepts — the largest ones it
//! accepts included — under a bound, after parse and after matching. The
//! second bound is mostly the lazy DFA caches, one per pattern, each at most
//! `REGEX_CACHE_LIMIT_BYTES`. A pattern keeps up to two of those (one for
//! each direction it searches in) and the smaller caches of the slower
//! engines; the most measured is about 0.53 MiB per pattern. These are
//! measurements, not a proof of the worst case: each ceiling sits just above
//! the largest filter measured, 1.95 MiB after parse (three `\w{12}`) and
//! 17.7 MiB after matching (a `\w{18}` and thirty-one reverse-anchored
//! alternations, over the documents below).
//!
//! One test in this binary, because the count is process-wide: a second test
//! running in parallel would allocate into it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering::Relaxed};

use bson::{Bson, Document, doc};
use kimmy_query::filter;

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

// SAFETY: every call is forwarded to `System`, unchanged; the counter only
// observes the sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc` is passed on as is.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE.fetch_add(layout.size() as isize, Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: as above.
        unsafe { System.dealloc(p, layout) };
        LIVE.fetch_sub(layout.size() as isize, Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const MIB: isize = 1 << 20;

/// `n` `$regex` clauses on `s` under one `$or`.
fn or_of(n: usize, pattern: impl Fn(usize) -> String) -> Document {
    let clauses: Vec<Bson> =
        (0..n).map(|i| Bson::Document(doc! { "s": { "$regex": pattern(i) } })).collect();
    doc! { "$or": clauses }
}

#[test]
fn a_parsed_filter_holds_its_regex_patterns_within_a_bound() {
    // Strings that drive the lazy DFAs of the alternation patterns below
    // through as many states as they have — `[ab]*a[ab]{n}` needs 2^(n+1) —
    // in each mix a cache can be filled by: `a` and `b` alone; with `é`, which
    // makes a Unicode `\b` give the DFA up for the slower engines; with spaces
    // and line ends, which `$` under `m` and `\b` stop at; and short strings,
    // which the backtracker takes.
    let mut x: u64 = 0x2545_f491_4f6c_dd1d;
    let mut random = |alphabet: &[char], len: usize| -> String {
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                alphabet[(x % alphabet.len() as u64) as usize]
            })
            .collect()
    };
    let mut docs: Vec<Document> = Vec::new();
    for alphabet in [
        &['a', 'b'][..],
        &['a', 'b', 'é'],
        &['a', 'b', ' ', '\n'],
        &['a', 'b', 'é', ' ', '\n', 'Z', '_'],
    ] {
        for _ in 0..2 {
            docs.push(doc! { "s": random(alphabet, 20_000) });
        }
    }
    for len in [8, 30, 100, 300, 1_000, 3_000] {
        docs.push(doc! { "s": random(&['a', 'b', 'é', ' '], len) });
    }

    let cases: Vec<(String, Document)> = vec![
        ("one \\w{209}".into(), or_of(1, |_| r"\w{209}".into())),
        ("ten \\w{209}".into(), or_of(10, |_| r"\w{209}".into())),
        ("forty \\w{209}".into(), or_of(40, |_| r"\w{209}".into())),
        ("one \\w{20}".into(), or_of(1, |i| format!(r"\w{{20}}{i}"))),
        ("two \\w{20}".into(), or_of(2, |i| format!(r"\w{{20}}{i}"))),
        ("three \\w{20}".into(), or_of(3, |i| format!(r"\w{{20}}{i}"))),
        ("thirty-two \\w{20}".into(), or_of(32, |i| format!(r"\w{{20}}{i}"))),
        ("forty \\w{20}".into(), or_of(40, |i| format!(r"\w{{20}}{i}"))),
        ("three \\w{12}".into(), or_of(3, |i| format!(r"\w{{12}}{i}"))),
        ("four \\w{12}".into(), or_of(4, |i| format!(r"\w{{12}}{i}"))),
        ("thirty-two short".into(), or_of(32, |i| format!("^a{i}"))),
        ("thirty-three short".into(), or_of(33, |i| format!("^a{i}"))),
        ("a hundred short".into(), or_of(100, |i| format!("^a{i}"))),
        (
            "thirty-two alternations".into(),
            or_of(32, |i| format!("[ab]*a[ab]{{{}}}[^ab]", 6 + i % 10)),
        ),
        (
            "thirty-two reverse-anchored".into(),
            or_of(32, |i| format!("(a|b)*a(a|b){{13}}(a|b)*$|q{i}z")),
        ),
        ("thirty-two multi-line".into(), or_of(32, |i| format!("(?m)^(a|b)*a(a|b){{13}}$|q{i}z"))),
        (
            "thirty-two multi-line with é".into(),
            or_of(32, |i| format!("(?m)(a|b|é)*a(a|b){{12}}$|q{i}z")),
        ),
        ("thirty-two word-bounded".into(), or_of(32, |i| format!(r"\b[ab]*a[ab]{{14}}\b|q{i}z"))),
        (
            "thirty-two \\w-led reverse-anchored".into(),
            or_of(32, |i| format!(r"[\w--\d]?(a|b)*a(a|b){{13}}(a|b)*$|q{i}z")),
        ),
        (
            "one \\w{18} and thirty-one reverse-anchored".into(),
            or_of(32, |i| match i {
                0 => r"\w{18}(a|b)*$".to_string(),
                _ => format!("(a|b)*a(a|b){{13}}(a|b)*$|q{i}z"),
            }),
        ),
        (
            "a hundred alternations".into(),
            or_of(100, |i| format!("[ab]*a[ab]{{{}}}[^ab]", 6 + i % 10)),
        ),
    ];

    let mut accepted = Vec::new();
    for (name, query) in &cases {
        let base = LIVE.load(Relaxed);
        let Ok(parsed) = filter::parse(query) else {
            continue;
        };
        let after_parse = LIVE.load(Relaxed) - base;
        // Each branch of an `$or` on its own, because the `$or` stops at the
        // first that matches and would leave the later patterns' caches cold.
        let branches = match &parsed {
            filter::Filter::Or(branches) => branches.iter().collect(),
            other => vec![other],
        };
        for d in &docs {
            for branch in &branches {
                filter::matches(branch, d).unwrap();
            }
        }
        let after_match = LIVE.load(Relaxed) - base;
        eprintln!(
            "{name}: {:.2} MiB held after parse, {:.2} MiB after matching {} documents",
            after_parse as f64 / MIB as f64,
            after_match as f64 / MIB as f64,
            docs.len()
        );
        assert!(
            after_parse < 3 * MIB,
            "{name} holds {} MiB after parse, past the 3 MiB bound",
            after_parse / MIB
        );
        assert!(
            after_match < 20 * MIB,
            "{name} holds {} MiB after matching, past the 20 MiB ceiling",
            after_match / MIB
        );
        accepted.push(name.as_str());
        drop(parsed);
    }
    // Premise: the largest filters the limits allow were accepted, so the
    // bounds above were measured against them and not only against refusals.
    for name in ["one \\w{20}", "three \\w{12}", "thirty-two short", "thirty-two alternations"] {
        assert!(accepted.contains(&name), "premise: {name} is accepted; accepted {accepted:?}");
    }
}
