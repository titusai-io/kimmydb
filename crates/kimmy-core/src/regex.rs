//! The `$regex` facility: a pattern compiled once under its flags, and the budget
//! that bounds what a filter, a pipeline, an update or a schema may hold of them.
//!
//! It lives here, beside the `$jsonSchema` engine that compiles `pattern`s with it
//! (ADR-257), so that `kimmy-storage` can parse a validator without depending on
//! the query crate (ADR-181). `kimmy-query` re-exports it from `filter`.

use regex_automata::meta;
use regex_automata::util::syntax;

use crate::error::{Error, Result};

/// The most memory one `$regex` pattern may compile to: 1 MiB, as the engine
/// counts the automaton it builds (`regex`'s own default is 10 MiB).
///
/// A compiled pattern is kept in the parsed filter for the life of the
/// request, so its size is held, not passed through. Most patterns compile to
/// a few kilobytes; what reaches the limit is a Unicode class repeated many
/// times — `\w` alone is some 55 KiB, so `\w{20}` is about 1 MiB and
/// `\w{209}` would be 11 MiB.
pub const REGEX_PATTERN_LIMIT_BYTES: usize = 1 << 20;

/// The most a compiled pattern's lazy DFA may grow to while it matches:
/// 256 KiB (`regex`'s default is 2 MiB).
///
/// The cache stays with the pattern once grown, so a filter can hold one per
/// `$regex` until the request ends: a hundred patterns of the shape
/// `[ab]*a[ab]{14}[^ab]` over strings of `a` and `b` held 92 MiB after matching
/// at 1 MiB each. Past the capacity the engine clears the cache or falls back
/// to a slower search, and the answer is the same. Measured on 10,000 short
/// strings, only a pattern near [`REGEX_PATTERN_LIMIT_BYTES`] slows down
/// (`\w{20}` from 0.3 ms to 6 ms); an ordinary one is as fast as at 2 MiB.
pub const REGEX_CACHE_LIMIT_BYTES: usize = 256 << 10;

/// The most `$regex` conditions one filter may hold.
///
/// Every `$match` of a pipeline, its `$lookup` sub-pipelines included, counts
/// as one filter, and so do all the entries of an update's `arrayFilters`; an
/// update's or a `find_and_modify`'s query filter is a budget of its own,
/// apart from its `arrayFilters`.
///
/// Each pattern that has matched keeps its search caches until the request
/// ends: up to two lazy DFA caches of [`REGEX_CACHE_LIMIT_BYTES`], one for
/// each direction it searches in, and the smaller caches of the slower
/// engines. So this is what limits a request's memory after matching, not
/// only after parse. **Measured, not bounded:** the most measured for
/// thirty-two patterns is 17.7 MiB after matching, about 0.53 MiB of caches
/// per pattern, from reverse-anchored alternations such as
/// `(a|b)*a(a|b){13}(a|b)*$` over long strings of `a` and `b`
/// (`tests/regex_memory.rs`). A filter of more than a couple of dozen
/// patterns is not a workload this serves; alternatives fit in one pattern,
/// `"^(abc|def)"`.
pub const REGEX_CONDITIONS_LIMIT: usize = 32;

/// The most memory the compiled patterns of one filter may hold together,
/// counted over the same scope as [`REGEX_CONDITIONS_LIMIT`]: 2 MiB.
pub const REGEX_TOTAL_LIMIT_BYTES: usize = 2 << 20;

/// What the `$regex` conditions parsed so far under one budget have cost.
#[derive(Clone, Copy, Default)]
struct RegexSpend {
    conditions: usize,
    bytes: usize,
}

thread_local! {
    /// The budget being charged, while one is open on this thread.
    ///
    /// Parsing is synchronous and never yields, so a thread-local scope is
    /// exactly one parse; it lets a pipeline or an update charge every filter
    /// it parses to one budget without threading it through each parser.
    static REGEX_SPEND: std::cell::Cell<Option<RegexSpend>> =
        const { std::cell::Cell::new(None) };
}

/// One budget for the `$regex` conditions parsed until it is dropped.
///
/// Opening one while another is open joins it, so a filter parsed inside a
/// pipeline charges the pipeline's budget. Only the outermost closes it.
pub struct RegexBudget {
    outermost: bool,
}

impl RegexBudget {
    pub fn open() -> Self {
        REGEX_SPEND.with(|spend| {
            let outermost = spend.get().is_none();
            if outermost {
                spend.set(Some(RegexSpend::default()));
            }
            Self { outermost }
        })
    }
}

impl Drop for RegexBudget {
    fn drop(&mut self) {
        if self.outermost {
            REGEX_SPEND.with(|spend| spend.set(None));
        }
    }
}

/// A `$regex` pattern under its flags, compiled once, when the filter is
/// parsed, and kept in the condition.
///
/// Matching used to compile the pattern again for every document it was
/// tested against — after parsing had already compiled it once to check it
/// and thrown the result away. Compiling is the expensive half of a regex
/// match, so a scan paid for it once per document. Now nothing compiles after
/// parse: the matcher only runs the compiled program, which the engine shares
/// between clones, so cloning a filter copies no program either.
///
/// Keeping it means holding it, so what one pattern and one filter may hold
/// is bounded: [`REGEX_PATTERN_LIMIT_BYTES`], [`REGEX_CACHE_LIMIT_BYTES`],
/// [`REGEX_CONDITIONS_LIMIT`] and [`REGEX_TOTAL_LIMIT_BYTES`].
///
/// The fields are private, so the only way to build one is [`Self::new`],
/// which refuses what the matcher cannot use. A condition carrying a flag
/// nothing implements, or a pattern that does not compile, cannot exist.
///
/// Two patterns are equal when their source and flags are: the compiled form
/// is a function of those two.
#[derive(Clone)]
pub struct RegexPattern {
    pattern: String,
    options: String,
    compiled: meta::Regex,
}

impl RegexPattern {
    /// Compile `pattern` under `options`.
    ///
    /// Both are decided here, when the filter is parsed, so a pattern the
    /// matcher cannot use is a `400` naming what is wrong with it. It used to
    /// match nothing, which reads as an empty result: a backreference or a
    /// lookaround, which the engine does not support, and a plain typo look
    /// the same as "no document matches". While a filter is being parsed the
    /// pattern is charged to its budget, and refused past it.
    pub fn new(pattern: &str, options: &str) -> Result<Self> {
        let spent = REGEX_SPEND.with(std::cell::Cell::get);
        // Counted before compiling, so a filter of many patterns stops at the
        // limit rather than compiling them all first.
        if spent.is_some_and(|s| s.conditions >= REGEX_CONDITIONS_LIMIT) {
            return Err(Error::InvalidQuery(format!(
                "a filter may hold at most {REGEX_CONDITIONS_LIMIT} $regex conditions, the stages \
                 of a pipeline counting as one filter and the arrayFilters of an update as \
                 another, apart from its query filter; this one holds more: write alternatives \
                 as one pattern, as in \"^(abc|def)\""
            )));
        }
        let compiled = build_regex(pattern, options)?;
        if let Some(spent) = spent {
            let bytes = spent.bytes + compiled.memory_usage();
            if bytes > REGEX_TOTAL_LIMIT_BYTES {
                return Err(Error::InvalidQuery(format!(
                    "the $regex patterns of this filter compile to more than {} MiB together, \
                     the limit for one filter, the stages of a pipeline counting as one filter \
                     and the arrayFilters of an update as another, apart from its query \
                     filter; {pattern:?} is the one past it: a Unicode class \
                     repeated many times, such as \\w{{20}}, is the usual cause, and an ASCII \
                     class such as [A-Za-z0-9_] compiles far smaller",
                    REGEX_TOTAL_LIMIT_BYTES >> 20
                )));
            }
            REGEX_SPEND.with(|s| {
                s.set(Some(RegexSpend { conditions: spent.conditions + 1, bytes }));
            });
        }
        Ok(Self { pattern: pattern.to_string(), options: options.to_string(), compiled })
    }

    /// The pattern as the filter wrote it.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// The flags it was compiled under.
    pub fn options(&self) -> &str {
        &self.options
    }

    /// Whether the pattern matches somewhere in `s`.
    pub fn is_match(&self, s: &str) -> bool {
        self.compiled.is_match(s)
    }
}

impl PartialEq for RegexPattern {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.options == other.options
    }
}

impl std::fmt::Debug for RegexPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegexPattern")
            .field("pattern", &self.pattern)
            .field("options", &self.options)
            .finish()
    }
}

/// Refuse a `$regex` flag this database does not implement. The four it does
/// (`i`, `m`, `s`, `x`) are all there are: an unknown letter used to be
/// dropped, so `"I"` compiled the pattern case-sensitively and answered an
/// empty result with nothing said.
pub fn check_regex_options(options: &str) -> Result<()> {
    match options.chars().find(|flag| !matches!(flag, 'i' | 'm' | 's' | 'x')) {
        Some(flag) => Err(unknown_regex_flag(flag)),
        None => Ok(()),
    }
}

fn unknown_regex_flag(flag: char) -> Error {
    Error::InvalidQuery(format!("unknown $regex flag {flag:?}; the flags are i, m, s and x"))
}

/// The pattern under its flags, compiled within [`REGEX_PATTERN_LIMIT_BYTES`]
/// and [`REGEX_CACHE_LIMIT_BYTES`].
///
/// The flags are checked here, before anything is compiled, and this is the
/// check a `$regex` meets; a lone `$options` meets [`check_regex_options`].
/// The engine is `regex`'s own, configured as `regex::RegexBuilder` configures
/// it but for the two limits, and used directly because it reports how much
/// memory a compiled pattern holds, which the filter's budget counts.
///
/// Called only from [`RegexPattern::new`]: once per `$regex` in a filter, when
/// it is parsed, and never while matching.
fn build_regex(pattern: &str, options: &str) -> Result<meta::Regex> {
    REGEX_COMPILATIONS.with(|n| n.set(n.get() + 1));
    let mut syntax = syntax::Config::new();
    for flag in options.chars() {
        syntax = match flag {
            'i' => syntax.case_insensitive(true),
            'm' => syntax.multi_line(true),
            's' => syntax.dot_matches_new_line(true),
            'x' => syntax.ignore_whitespace(true),
            other => return Err(unknown_regex_flag(other)),
        };
    }
    meta::Builder::new()
        .syntax(syntax)
        .configure(
            meta::Config::new()
                .nfa_size_limit(Some(REGEX_PATTERN_LIMIT_BYTES))
                .hybrid_cache_capacity(REGEX_CACHE_LIMIT_BYTES),
        )
        .build(pattern)
        .map_err(|e| {
            let reason = match (e.syntax_error(), e.size_limit()) {
                // The engine's message is a multi-line rendering of the
                // pattern with a caret under the fault. The reason is its
                // last line.
                (Some(syntax), _) => {
                    let message = syntax.to_string();
                    message
                        .lines()
                        .last()
                        .unwrap_or_default()
                        .trim_start_matches("error: ")
                        .to_string()
                }
                (None, Some(limit)) => format!(
                    "it compiles to more than {} MiB, the limit for one pattern; a Unicode class \
                     repeated many times, such as \\w{{100}}, is the usual cause, and an ASCII \
                     class such as [A-Za-z0-9_] or a smaller count compiles far smaller",
                    limit >> 20
                ),
                (None, None) => e.to_string(),
            };
            Error::InvalidQuery(format!("$regex pattern {pattern:?} cannot be used: {reason}"))
        })
}

thread_local! {
    /// How many regexes this thread has compiled, for the tests that hold
    /// matching to compiling none.
    static REGEX_COMPILATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Regexes this thread has compiled so far.
#[doc(hidden)]
pub fn regex_compilations() -> usize {
    REGEX_COMPILATIONS.with(std::cell::Cell::get)
}
