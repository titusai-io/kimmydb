//! The `$regex` facility: a pattern compiled once under its flags, and the budget
//! that bounds what a filter, a pipeline, an update or a schema may hold of them.
//!
//! It lives here, beside the `$jsonSchema` engine that compiles `pattern`s with it
//! (ADR-257), so that `kimmy-storage` can parse a validator without depending on
//! the query crate (ADR-181). `kimmy-query` re-exports it from `filter`.

use regex_automata::meta;
use regex_syntax::ast::{self, Ast};
use regex_syntax::hir;

use crate::error::{Error, Result};

/// The most memory one `$regex` pattern may compile to: 1 MiB, as the engine
/// counts the automaton it builds (`regex`'s own default is 10 MiB).
///
/// A compiled pattern is kept in the parsed filter for the life of the
/// request, so its size is held, not passed through. Most patterns compile to
/// a few kilobytes; what reaches the limit is a Unicode class repeated many
/// times — a Unicode word class such as `[\p{Alphabetic}\p{M}\p{Nd}\p{Pc}]` (what
/// `\w` meant before it became ASCII, ADR-260) is some 55 KiB, so twenty of them
/// are about 1 MiB and two hundred and nine would be 11 MiB.
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
/// (twenty Unicode word classes from 0.3 ms to 6 ms); an ordinary one is as fast
/// as at 2 MiB.
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
                     repeated many times, such as \\p{{L}}{{20}}, is the usual cause, and an ASCII \
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

    /// The first match in `s`, with its capture groups, or none.
    pub fn first_match(&self, s: &str) -> Option<RegexMatch> {
        let mut first = None;
        let _ = self.scan::<()>(s, true, &mut |m| {
            first = Some(m.clone());
            Ok(())
        });
        first
    }

    /// Every match in `s`, left to right and not overlapping, with the capture
    /// groups of each. See [`Self::try_for_each_match`] for the loop.
    pub fn find_all(&self, s: &str) -> Vec<RegexMatch> {
        let mut found = Vec::new();
        let _ = self.scan::<()>(s, false, &mut |m| {
            found.push(m.clone());
            Ok(())
        });
        found
    }

    /// Calls `f` with every match in `s` in order, stopping at the first error
    /// it returns, so that a caller can bound what it collects.
    ///
    /// This is the reference's `$regexFindAll` loop, not `regex`'s iterator:
    /// each search starts where the previous match ended, and after a match of
    /// no width one code point past it. The loop runs once for any input, and
    /// again only while the next search would start before the end of `s`; so
    /// an empty match at the end of `s` is found only when the search that
    /// found it began earlier. `a*` over `"abc"` matches `"a"` at 0, then `""`
    /// at 1 and at 2 (not at 3); `a*` over `"aaa"` matches `"aaa"` only; and
    /// `""` over `""` matches once.
    pub fn try_for_each_match<E>(
        &self,
        s: &str,
        mut f: impl FnMut(&RegexMatch) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        self.scan(s, false, &mut f)
    }

    fn scan<E>(
        &self,
        s: &str,
        first_only: bool,
        f: &mut dyn FnMut(&RegexMatch) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        use regex_automata::Input;
        let mut caps = self.compiled.create_captures();
        let mut at = 0;
        loop {
            self.compiled.search_captures(&Input::new(s).span(at..s.len()), &mut caps);
            let Some(whole) = caps.get_match() else { return Ok(()) };
            let span = |g: usize| caps.get_group(g).map(|m| (m.start, m.end));
            f(&RegexMatch {
                start: whole.start(),
                end: whole.end(),
                groups: (1..caps.group_len()).map(span).collect(),
            })?;
            if first_only {
                return Ok(());
            }
            at = if whole.is_empty() {
                // One code point past an empty match.
                whole.end() + s[whole.end()..].chars().next().map_or(1, char::len_utf8)
            } else {
                whole.end()
            };
            if at >= s.len() {
                return Ok(());
            }
        }
    }
}

/// One match of a [`RegexPattern`]: byte offsets into the searched string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegexMatch {
    pub start: usize,
    pub end: usize,
    /// Capture groups 1.., `None` for one that took no part in the match.
    pub groups: Vec<Option<(usize, usize)>>,
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

/// Refuse a `$regex` flag this database does not implement. The flags it does
/// are `i`, `m`, `s` and `x`, and `u`, which the reference accepts and which
/// changes nothing here: a pattern is always matched as UTF-8 text. An unknown
/// letter used to be dropped, so `"I"` compiled the pattern case-sensitively
/// and answered an empty result with nothing said.
pub fn check_regex_options(options: &str) -> Result<()> {
    match options.chars().find(|flag| !matches!(flag, 'i' | 'm' | 's' | 'x' | 'u')) {
        Some(flag) => Err(unknown_regex_flag(flag)),
        None => Ok(()),
    }
}

fn unknown_regex_flag(flag: char) -> Error {
    Error::InvalidQuery(format!("unknown $regex flag {flag:?}; the flags are i, m, s, x and u"))
}

/// The pattern under its flags, compiled within [`REGEX_PATTERN_LIMIT_BYTES`]
/// and [`REGEX_CACHE_LIMIT_BYTES`].
///
/// The flags are checked here, before anything is compiled, and this is the
/// check a `$regex` meets; a lone `$options` meets [`check_regex_options`].
/// The engine is `regex`'s own, configured as `regex::RegexBuilder` configures
/// it but for the two limits and for the classes (see [`ascii_classes`]), and
/// used directly because it reports how much memory a compiled pattern holds,
/// which the filter's budget counts.
///
/// Called only from [`RegexPattern::new`]: once per `$regex` in a filter, when
/// it is parsed, and never while matching.
fn build_regex(pattern: &str, options: &str) -> Result<meta::Regex> {
    REGEX_COMPILATIONS.with(|n| n.set(n.get() + 1));
    let mut parser = ast::parse::ParserBuilder::new();
    let mut translator = hir::translate::TranslatorBuilder::new();
    for flag in options.chars() {
        match flag {
            'i' => {
                translator.case_insensitive(true);
            }
            'm' => {
                translator.multi_line(true);
            }
            's' => {
                translator.dot_matches_new_line(true);
            }
            'x' => {
                parser.ignore_whitespace(true);
            }
            // Accepted, as the reference does, and nothing to set.
            'u' => {}
            other => return Err(unknown_regex_flag(other)),
        }
    }
    let cannot_be_used = |reason: String| {
        Error::InvalidQuery(format!("$regex pattern {pattern:?} cannot be used: {reason}"))
    };
    // The engine's message is a multi-line rendering of the pattern with a caret
    // under the fault. The reason is its last line.
    let last_line = |e: regex_syntax::Error| {
        e.to_string().lines().last().unwrap_or_default().trim_start_matches("error: ").to_string()
    };
    let parsed = parser
        .build()
        .parse(pattern)
        .map_err(|e| cannot_be_used(last_line(regex_syntax::Error::from(e))))?;
    let ast = ascii_classes(parsed);
    let hir = translator
        .build()
        .translate(pattern, &ast)
        .map_err(|e| cannot_be_used(last_line(regex_syntax::Error::from(e))))?;
    meta::Builder::new()
        .configure(
            meta::Config::new()
                .nfa_size_limit(Some(REGEX_PATTERN_LIMIT_BYTES))
                .hybrid_cache_capacity(REGEX_CACHE_LIMIT_BYTES),
        )
        .build_from_hir(&hir)
        .map_err(|e| match e.size_limit() {
            Some(limit) => cannot_be_used(format!(
                "it compiles to more than {} MiB, the limit for one pattern; a Unicode class \
                 repeated many times, such as \\p{{L}}{{100}}, is the usual cause, and an ASCII \
                 class such as [A-Za-z0-9_] or a smaller count compiles far smaller",
                limit >> 20
            )),
            None => cannot_be_used(e.to_string()),
        })
}

/// What `\d`, `\w`, `\s` and the word boundaries `\b` and `\B` mean in a
/// pattern: ASCII only, as in the reference, where `\d` does not match
/// U+0663, `\w` does not match `é` and `\s` does not match U+00A0. `regex`
/// reads them as Unicode classes, so each is rewritten, in the parsed pattern
/// and before it is compiled, to the ASCII class it stands for. Everything
/// else keeps its Unicode meaning: `.` is one code point, `\p{L}` is a
/// Unicode property, and `i` folds `é` with `É`, as the reference's does.
///
/// A class outside brackets becomes `(?-i:[0-9A-Za-z_])`, with case folding
/// off, so that `\w` under `i` does not take in `ſ` (U+017F) and `K`
/// (U+212A), which fold to ASCII letters. Inside brackets no flag can be
/// set, and under `i` a `[\w]` does match those two.
fn ascii_classes(mut ast: Ast) -> Ast {
    rewrite_node(&mut ast);
    ast
}

fn snippet(pattern: &str) -> Ast {
    ast::parse::Parser::new().parse(pattern).expect("a fixed pattern parses")
}

fn ascii_class(kind: &ast::ClassPerlKind, negated: bool) -> &'static str {
    use ast::ClassPerlKind::{Digit, Space, Word};
    match (kind, negated) {
        (Digit, false) => "[0-9]",
        (Digit, true) => "[^0-9]",
        (Word, false) => "[0-9A-Za-z_]",
        (Word, true) => "[^0-9A-Za-z_]",
        // Space, horizontal tab, line feed, vertical tab, form feed, carriage return.
        (Space, false) => r"[\x20\x09\x0A\x0B\x0C\x0D]",
        (Space, true) => r"[^\x20\x09\x0A\x0B\x0C\x0D]",
    }
}

fn is_word_assertion(kind: &ast::AssertionKind) -> bool {
    use ast::AssertionKind::{
        NotWordBoundary, WordBoundary, WordBoundaryEnd, WordBoundaryEndAngle, WordBoundaryEndHalf,
        WordBoundaryStart, WordBoundaryStartAngle, WordBoundaryStartHalf,
    };
    matches!(
        kind,
        WordBoundary
            | NotWordBoundary
            | WordBoundaryStart
            | WordBoundaryEnd
            | WordBoundaryStartAngle
            | WordBoundaryEndAngle
            | WordBoundaryStartHalf
            | WordBoundaryEndHalf
    )
}

fn rewrite_node(node: &mut Ast) {
    match node {
        Ast::ClassPerl(class) => {
            let source = format!("(?-i:{})", ascii_class(&class.kind, class.negated));
            *node = snippet(&source);
        }
        Ast::Assertion(assertion) if is_word_assertion(&assertion.kind) => {
            let span = assertion.span;
            let flags = ast::Flags {
                span,
                items: vec![
                    ast::FlagsItem { span, kind: ast::FlagsItemKind::Negation },
                    ast::FlagsItem { span, kind: ast::FlagsItemKind::Flag(ast::Flag::Unicode) },
                ],
            };
            let inner = std::mem::replace(node, Ast::empty(span));
            *node = Ast::group(ast::Group {
                span,
                kind: ast::GroupKind::NonCapturing(flags),
                ast: Box::new(inner),
            });
        }
        Ast::ClassBracketed(class) => rewrite_set(&mut class.kind),
        Ast::Repetition(repetition) => rewrite_node(&mut repetition.ast),
        Ast::Group(group) => rewrite_node(&mut group.ast),
        Ast::Alternation(alternation) => alternation.asts.iter_mut().for_each(rewrite_node),
        Ast::Concat(concat) => concat.asts.iter_mut().for_each(rewrite_node),
        _ => {}
    }
}

fn rewrite_set(set: &mut ast::ClassSet) {
    match set {
        ast::ClassSet::Item(item) => rewrite_item(item),
        ast::ClassSet::BinaryOp(op) => {
            rewrite_set(&mut op.lhs);
            rewrite_set(&mut op.rhs);
        }
    }
}

fn rewrite_item(item: &mut ast::ClassSetItem) {
    match item {
        ast::ClassSetItem::Perl(class) => {
            let Ast::ClassBracketed(ref nested) = snippet(ascii_class(&class.kind, class.negated))
            else {
                unreachable!("a bracketed class parses to one")
            };
            *item = ast::ClassSetItem::Bracketed(nested.clone());
        }
        ast::ClassSetItem::Bracketed(class) => rewrite_set(&mut class.kind),
        ast::ClassSetItem::Union(union) => union.items.iter_mut().for_each(rewrite_item),
        _ => {}
    }
}

thread_local! {
    /// How many regexes this thread has compiled, for the tests that hold
    /// matching to compiling none.
    static REGEX_COMPILATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The Unicode word class, spelled out: what `\w` meant before it became ASCII.
/// It compiles to some 55 KiB, which the tests of the size limits and the budgets
/// repeat to reach them.
#[doc(hidden)]
pub const UNICODE_WORD_CLASS: &str = r"[\p{Alphabetic}\p{M}\p{Nd}\p{Pc}\p{Join_Control}]";

/// Regexes this thread has compiled so far.
#[doc(hidden)]
pub fn regex_compilations() -> usize {
    REGEX_COMPILATIONS.with(std::cell::Cell::get)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matches(pattern: &str, options: &str, text: &str) -> bool {
        RegexPattern::new(pattern, options).expect("pattern compiles").is_match(text)
    }

    fn found(pattern: &str, text: &str) -> Vec<(usize, usize)> {
        RegexPattern::new(pattern, "")
            .expect("pattern compiles")
            .find_all(text)
            .iter()
            .map(|m| (m.start, m.end))
            .collect()
    }

    #[test]
    fn digit_word_and_space_classes_are_ascii() {
        // The reference: `\d` does not match U+0663, `\w` not `é`, `\s` not U+00A0.
        assert!(!matches(r"\d", "", "\u{663}"));
        assert!(matches(r"\d", "", "x7"));
        assert!(matches(r"\D", "", "\u{663}"));
        assert!(!matches(r"\w", "", "é"));
        assert!(matches(r"\w", "", "_"));
        assert!(matches(r"^\w+$", "", "Az09_"));
        assert!(matches(r"\W", "", "é"));
        assert!(!matches(r"\s", "", "\u{a0}"));
        assert!(!matches(r"\s", "", "\u{2003}"));
        assert!(matches(r"^\s+$", "", " \t\n\u{b}\u{c}\r"));
        assert!(matches(r"\S", "", "\u{a0}"));
        // The same inside brackets, and negated.
        assert!(!matches(r"[\d]", "", "\u{663}"));
        assert!(!matches(r"[\w-]", "", "é"));
        assert!(matches(r"[\w-]", "", "-"));
        assert!(matches(r"[^\w]", "", "é"));
        assert!(matches(r"[\W]", "", "é"));
        assert!(matches(r"[\D]", "", "\u{663}"));
        assert!(!matches(r"[^\S]", "", "\u{a0}"));
        assert!(matches(r"[a[\d]]", "", "5"));
        assert!(!matches(r"[a[\d]]", "", "\u{663}"));
        // An escaped backslash before a `d` is not a class.
        assert!(matches(r"\\d", "", r"\d"));
        // Properties stay Unicode.
        assert!(matches(r"\p{L}", "", "é"));
        assert!(matches(r"\p{Nd}", "", "\u{663}"));
    }

    #[test]
    fn word_boundaries_are_ascii() {
        // `é` is not a word character, so a boundary sits before the `x` after it.
        assert!(matches(r"\bx", "", "éx"));
        assert!(!matches(r"\Bx", "", "éx"));
        assert!(matches(r"\Bx", "", "ax"));
        assert!(!matches(r"\b", "", "é"));
        assert!(matches(r"\B", "", "é"));
        // Boundaries of "ab cd" in the reference's order.
        assert_eq!(found(r"\b", "ab cd"), vec![(0, 0), (2, 2), (3, 3), (5, 5)]);
        // `a`, then `é` (two bytes, not a word character), a space, `b`: the
        // boundary at the very end is not searched for.
        assert_eq!(found(r"\b", "aé b"), vec![(0, 0), (1, 1), (4, 4)]);
    }

    #[test]
    fn case_insensitive_matching_stays_unicode_and_the_classes_stay_ascii() {
        assert!(matches("É", "i", "é"));
        assert!(matches("é", "i", "É"));
        assert!(matches("k", "i", "\u{212a}"));
        assert!(matches("σ", "i", "ς"));
        assert!(matches(r"\w", "i", "k"));
        // `\w` under `i` does not take in the letters that fold to ASCII ones.
        assert!(!matches(r"\w", "i", "\u{17f}"));
        assert!(!matches(r"\w", "i", "\u{212a}"));
        assert!(!matches(r"\W", "i", "k"));
        assert!(matches(r"\W", "i", "\u{17f}"));
    }

    #[test]
    fn the_u_flag_is_accepted_and_changes_nothing() {
        assert!(check_regex_options("u").is_ok());
        assert!(check_regex_options("imsxu").is_ok());
        assert!(matches("a", "u", "a"));
        assert!(!matches(r"\d", "u", "\u{663}"));
        assert!(check_regex_options("I").is_err());
        assert!(RegexPattern::new("a", "q").is_err());
        let message = check_regex_options("q").unwrap_err().to_string();
        assert!(message.contains("i, m, s, x and u"), "{message}");
    }

    #[test]
    fn still_refused_are_what_the_engine_cannot_run() {
        for pattern in [r"\Qa.b\E", "(?>a)", r"(a)\1", "(?=a)", "(?<!a)b", r"\Z", r"\G"] {
            assert!(RegexPattern::new(pattern, "").is_err(), "{pattern}");
        }
    }

    #[test]
    fn a_possessive_quantifier_is_read_as_a_nested_greedy_repetition() {
        // A documented difference: the reference never gives back what `a*+`
        // took, so `a*+a` matches nothing there.
        assert!(matches("a*+a", "", "aa"));
        assert!(matches("a++", "", "aa"));
    }

    #[test]
    fn find_all_stops_when_the_next_search_would_start_at_the_end() {
        // Each row is the reference's answer for `$regexFindAll`.
        type Case<'a> = (&'a str, &'a str, &'a [(usize, usize)]);
        let cases: &[Case] = &[
            ("abc", "", &[(0, 0), (1, 1), (2, 2)]),
            ("abc", "a*", &[(0, 1), (1, 1), (2, 2)]),
            ("aaa", "a*", &[(0, 3)]),
            ("ab", "b*", &[(0, 0), (1, 2)]),
            ("ab", "a|", &[(0, 1), (1, 1)]),
            ("é", "x*", &[(0, 0)]),
            ("", "", &[(0, 0)]),
            ("", "x*", &[(0, 0)]),
            ("a", "a", &[(0, 1)]),
            ("aaaa", "aa", &[(0, 2), (2, 4)]),
            // An empty match straight after a non-empty one is found.
            ("baac", "a*", &[(0, 0), (1, 3), (3, 3)]),
            ("éa", "", &[(0, 0), (2, 2)]),
        ];
        for (text, pattern, want) in cases {
            assert_eq!(found(pattern, text), want.to_vec(), "{pattern:?} over {text:?}");
        }
    }

    #[test]
    fn the_match_callback_can_stop_the_scan() {
        let p = RegexPattern::new("a", "").unwrap();
        let mut seen = 0;
        let out = p.try_for_each_match("aaaa", |_| {
            seen += 1;
            if seen == 2 { Err("stop") } else { Ok(()) }
        });
        assert_eq!(out, Err("stop"));
        assert_eq!(seen, 2);
        assert_eq!(p.first_match("xxa").map(|m| m.start), Some(2));
        assert_eq!(p.first_match("xxx"), None);
    }
}
