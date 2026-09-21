//! Every link between this repository's Markdown files resolves.
//!
//! Nothing else checks them. The compiler does not read Markdown, the doc
//! tests run Rust, and the conflict-marker scan looks for markers rather than
//! targets. So a heading renamed in `docs/decisions.md` leaves every
//! `#anchor` that pointed at it dangling, and nothing says so: a reader
//! following the record's own cross-references lands at the top of the file.
//!
//! Three kinds of link are checked: to an anchor in the same file, to another
//! file, and to an anchor in another Markdown file. Anchors are computed the
//! way GitHub computes them, because GitHub is where these files are read:
//! the heading's rendered text, lowercased, with every character that is not a
//! letter, a digit, a space, a hyphen or an underscore removed, and each space
//! turned into a hyphen. So an em-dash between two spaces becomes `--`, and a
//! repeated heading is numbered `-1`, `-2` after the first.
//!
//! **It refuses to pass by finding nothing.** The real-tree test asserts how
//! much it read before it asserts that nothing was broken, and the fixture
//! test holds it to reporting a broken link of every kind, with its line. A
//! link checker that silently scans no files passes on every tree there is.
//!
//! What it does not check: external URLs (a test must not need the network),
//! and links from Rust doc comments into `docs/`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

/// Directories never walked: build output, version control, and what a
/// client's own tooling creates, which holds other projects' Markdown. Hidden
/// directories are skipped too, except `.github`.
const SKIPPED_DIRS: &[&str] =
    &["target", "node_modules", ".venv", "venv", "__pycache__", ".pytest_cache", ".mypy_cache"];

/// What one Markdown file offers and asks for.
#[derive(Debug, Default)]
struct Parsed {
    /// Every anchor the rendered page has, in GitHub's spelling.
    anchors: BTreeSet<String>,
    /// Every link destination, with the 1-based line it is on.
    links: Vec<(usize, String)>,
    /// How many headings were read.
    headings: usize,
    /// The line of a code fence that is never closed. GitHub renders the rest
    /// of the file as code, and this parser would read no link after it.
    unclosed_fence: Option<usize>,
}

/// The opening of a fenced code block: its character and length.
fn fence_open(body: &str) -> Option<(char, usize)> {
    let ch = body.chars().next().filter(|c| *c == '`' || *c == '~')?;
    let len = body.chars().take_while(|c| *c == ch).count();
    if len < 3 {
        return None;
    }
    // A backtick fence's info string cannot itself hold a backtick.
    if ch == '`' && body[len..].contains('`') {
        return None;
    }
    Some((ch, len))
}

/// The text of an ATX heading (`## Text ##`), if the line is one.
fn atx(body: &str) -> Option<&str> {
    let level = body.chars().take_while(|c| *c == '#').count();
    if !(1..=6).contains(&level) {
        return None;
    }
    let rest = &body[level..];
    if !(rest.is_empty() || rest.starts_with(' ') || rest.starts_with('\t')) {
        return None;
    }
    let text = rest.trim();
    // An optional closing sequence of `#`s, preceded by a space.
    let trimmed = text.trim_end_matches('#');
    Some(if trimmed.is_empty() || trimmed.ends_with(' ') { trimmed.trim_end() } else { text })
}

/// Whether a line opens a block a setext underline cannot turn into a
/// heading: a list item, a quote, a table row or HTML. Under those, `---` is
/// a thematic break.
fn starts_non_paragraph(body: &str) -> bool {
    let list = |marker: char| {
        body.strip_prefix(marker).is_some_and(|rest| rest.is_empty() || rest.starts_with(' '))
    };
    let ordered = {
        let digits = body.chars().take_while(char::is_ascii_digit).count();
        digits > 0
            && body[digits..].starts_with(['.', ')'])
            && body[digits + 1..].chars().next().is_none_or(|c| c == ' ')
    };
    list('-')
        || list('*')
        || list('+')
        || ordered
        || body.starts_with('>')
        || body.starts_with('|')
        || body.starts_with('<')
}

/// `text` with its code spans replaced by spaces, so a link written inside
/// backticks is not read as a link.
fn without_code_spans(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '`' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        let run = chars[i..].iter().take_while(|c| **c == '`').count();
        // The closing run is exactly as long as the opening one.
        let mut j = i + run;
        let mut close = None;
        while j < chars.len() {
            if chars[j] == '`' {
                let r = chars[j..].iter().take_while(|c| **c == '`').count();
                if r == run {
                    close = Some(j + r);
                    break;
                }
                j += r;
            } else {
                j += 1;
            }
        }
        match close {
            Some(end) => {
                out.extend(std::iter::repeat_n(' ', end - i));
                i = end;
            }
            // No closing run: the backticks are literal.
            None => {
                out.extend(std::iter::repeat_n('`', run));
                i += run;
            }
        }
    }
    out
}

/// Every inline link or image destination on a line, `[text](dest "title")`.
fn inline_destinations(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(found) = line[from..].find("](") {
        let start = from + found + 2;
        let rest = &line[start..];
        let dest = if let Some(angled) = rest.strip_prefix('<') {
            angled.split_once('>').map(|(d, _)| d.to_string())
        } else {
            let mut depth = 0usize;
            let mut end = None;
            for (j, c) in rest.char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' if depth == 0 => {
                        end = Some(j);
                        break;
                    }
                    ')' => depth -= 1,
                    c if c.is_whitespace() => {
                        end = Some(j);
                        break;
                    }
                    _ => {}
                }
            }
            end.map(|e| rest[..e].to_string())
        };
        if let Some(dest) = dest.filter(|d| !d.is_empty()) {
            out.push(dest);
        }
        from = start;
    }
    out
}

/// The destination of a reference definition, `[label]: dest`.
fn reference_destination(line: &str) -> Option<String> {
    let indent = line.len() - line.trim_start_matches(' ').len();
    if indent > 3 {
        return None;
    }
    let body = line.trim_start().strip_prefix('[')?;
    let (label, rest) = body.split_once("]:")?;
    if label.is_empty() || label.contains(']') {
        return None;
    }
    rest.split_whitespace().next().map(|d| d.trim_matches(['<', '>']).to_string())
}

/// A heading's text as GitHub renders it, before slugging: code spans keep
/// their content, a link or image keeps its text, HTML tags go, and
/// underscores used for emphasis go while those inside a word stay.
fn rendered_text(heading: &str) -> String {
    let mut out = String::new();
    // Alternating outside and inside code spans; only the outside is markup.
    for (k, part) in heading.split('`').enumerate() {
        if k % 2 == 1 {
            out.push_str(part);
            continue;
        }
        let mut text = String::new();
        let mut rest = part;
        // `[text](dest)` and `![alt](dest)` keep only their text.
        while let Some(open) = rest.find('[') {
            let Some(close) = rest[open..].find("](").map(|c| open + c) else { break };
            let Some(end) = rest[close..].find(')').map(|e| close + e) else { break };
            text.push_str(rest[..open].trim_end_matches('!'));
            text.push_str(&rest[open + 1..close]);
            rest = &rest[end + 1..];
        }
        text.push_str(rest);
        // HTML tags.
        let mut plain = String::new();
        let mut in_tag = false;
        for c in text.chars() {
            match c {
                '<' => in_tag = true,
                '>' if in_tag => in_tag = false,
                c if !in_tag => plain.push(c),
                _ => {}
            }
        }
        // An escaped underscore is literal; any other at a word's edge is
        // emphasis.
        let chars: Vec<char> = plain.replace("\\_", "\u{0}").chars().collect();
        for (i, c) in chars.iter().enumerate() {
            if *c == '_' {
                let before = i.checked_sub(1).map(|p| chars[p]);
                let after = chars.get(i + 1);
                let inside = before.is_some_and(char::is_alphanumeric)
                    && after.is_some_and(|a| a.is_alphanumeric());
                if !inside {
                    continue;
                }
            }
            out.push(if *c == '\u{0}' { '_' } else { *c });
        }
    }
    out
}

/// GitHub's anchor for a heading, before numbering repeats.
fn github_slug(heading: &str) -> String {
    rendered_text(heading)
        .to_lowercase()
        .chars()
        .filter_map(|c| match c {
            ' ' => Some('-'),
            c if c.is_alphanumeric() || c == '-' || c == '_' => Some(c),
            _ => None,
        })
        .collect()
}

/// Add a heading's anchor as GitHub numbers it: the first occurrence plain,
/// then `-1`, `-2`, skipping any spelling already taken.
fn add_anchor(anchors: &mut BTreeSet<String>, taken: &mut BTreeMap<String, usize>, heading: &str) {
    let base = github_slug(heading);
    let mut slug = base.clone();
    while taken.contains_key(&slug) {
        let n = taken.get_mut(&base).expect("the base is taken first");
        *n += 1;
        slug = format!("{base}-{n}");
    }
    taken.insert(slug.clone(), 0);
    anchors.insert(slug);
}

fn parse(text: &str) -> Parsed {
    let mut out = Parsed::default();
    let mut taken = BTreeMap::new();
    let mut fence: Option<(char, usize, usize)> = None;
    // The paragraph the current line continues, for a setext heading, and
    // whether it is a plain paragraph.
    let mut paragraph: Vec<&str> = Vec::new();
    let mut plain = false;
    for (i, line) in text.lines().enumerate() {
        let n = i + 1;
        let indent = line.len() - line.trim_start_matches(' ').len();
        let body = line.trim_start_matches(' ');
        if let Some((ch, len, _)) = fence {
            let closing = body.trim_end();
            if indent <= 3 && closing.len() >= len && closing.chars().all(|c| c == ch) {
                fence = None;
            }
            continue;
        }
        if indent <= 3
            && let Some((ch, len)) = fence_open(body)
        {
            fence = Some((ch, len, n));
            paragraph.clear();
            continue;
        }
        if body.trim().is_empty() {
            paragraph.clear();
            continue;
        }
        if indent <= 3
            && let Some(heading) = atx(body)
        {
            out.headings += 1;
            add_anchor(&mut out.anchors, &mut taken, heading);
            paragraph.clear();
            let spans = without_code_spans(line);
            out.links.extend(inline_destinations(&spans).into_iter().map(|d| (n, d)));
            continue;
        }
        let underline = body.trim_end();
        if indent <= 3
            && plain
            && !paragraph.is_empty()
            && !underline.is_empty()
            && (underline.chars().all(|c| c == '=') || underline.chars().all(|c| c == '-'))
        {
            out.headings += 1;
            add_anchor(&mut out.anchors, &mut taken, &paragraph.join("\n"));
            paragraph.clear();
            continue;
        }
        if paragraph.is_empty() {
            plain = indent <= 3 && !starts_non_paragraph(body);
        }
        paragraph.push(line.trim());
        if let Some(dest) = reference_destination(line) {
            out.links.push((n, dest));
        }
        let spans = without_code_spans(line);
        out.links.extend(inline_destinations(&spans).into_iter().map(|d| (n, d)));
    }
    out.unclosed_fence = fence.map(|(_, _, line)| line);
    out
}

/// `%XX` escapes decoded.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Some(b) = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
        {
            out.push(b);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Whether a destination names a scheme (`https:`, `mailto:`): not a path.
fn has_scheme(dest: &str) -> bool {
    let Some((scheme, _)) = dest.split_once(':') else { return false };
    let mut chars = scheme.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

/// `path` with `.` and `..` resolved lexically.
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

fn is_markdown(path: &Path) -> bool {
    path.extension().is_some_and(|e| e.eq_ignore_ascii_case("md"))
}

/// A GitHub line anchor into a source file, `#L10` or `#L10-L20`.
fn is_line_anchor(fragment: &str) -> bool {
    let line = |s: &str| {
        s.strip_prefix('L').is_some_and(|d| !d.is_empty() && d.chars().all(|c| c.is_ascii_digit()))
    };
    match fragment.split_once('-') {
        Some((a, b)) => line(a) && line(b),
        None => line(fragment),
    }
}

/// What a check read, and what it found broken.
#[derive(Debug, Default)]
struct Report {
    files: usize,
    same_file_anchors: usize,
    cross_file_anchors: usize,
    to_files: usize,
    external: usize,
    broken: Vec<String>,
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root).unwrap_or(path).to_string_lossy().replace('\\', "/")
}

/// Check every link in `files`, which live under `root`.
fn check(root: &Path, files: &[PathBuf]) -> Report {
    let mut report = Report { files: files.len(), ..Report::default() };
    let parsed: BTreeMap<PathBuf, Parsed> = files
        .iter()
        .map(|f| {
            let text = std::fs::read_to_string(f).unwrap_or_else(|e| panic!("reading {f:?}: {e}"));
            (normalize(f), parse(&text))
        })
        .collect();
    // Markdown outside the walked set, parsed when something links to it.
    let mut elsewhere: BTreeMap<PathBuf, Parsed> = BTreeMap::new();
    for (file, doc) in &parsed {
        let here = relative(root, file);
        if let Some(line) = doc.unclosed_fence {
            report.broken.push(format!(
                "{here}:{line}: a code fence opened here is never closed, so the rest of the file \
                 renders as code"
            ));
        }
        for (line, dest) in &doc.links {
            if has_scheme(dest) {
                report.external += 1;
                continue;
            }
            let (path_part, fragment) = match dest.split_once('#') {
                Some((p, f)) => (p, Some(percent_decode(f))),
                None => (dest.as_str(), None),
            };
            let target = if path_part.is_empty() {
                file.clone()
            } else if let Some(from_root) = path_part.strip_prefix('/') {
                normalize(&root.join(percent_decode(from_root)))
            } else {
                normalize(
                    &file.parent().expect("a file has a directory").join(percent_decode(path_part)),
                )
            };
            if !target.exists() {
                report.broken.push(format!("{here}:{line}: {dest}: no such file"));
                continue;
            }
            let Some(fragment) = fragment else {
                report.to_files += 1;
                continue;
            };
            if !is_markdown(&target) || target.is_dir() {
                if is_line_anchor(&fragment) && target.is_file() {
                    report.to_files += 1;
                } else {
                    report.broken.push(format!(
                        "{here}:{line}: {dest}: an anchor into something that is not a Markdown file"
                    ));
                }
                continue;
            }
            let anchors = match parsed.get(&target) {
                Some(doc) => &doc.anchors,
                None => {
                    &elsewhere
                        .entry(target.clone())
                        .or_insert_with(|| {
                            parse(&std::fs::read_to_string(&target).unwrap_or_default())
                        })
                        .anchors
                }
            };
            if target == *file {
                report.same_file_anchors += 1;
            } else {
                report.cross_file_anchors += 1;
            }
            if !anchors.contains(&fragment) {
                report.broken.push(format!(
                    "{here}:{line}: {dest}: no heading in {} makes the anchor #{fragment}",
                    relative(root, &target)
                ));
            }
        }
    }
    report
}

/// Every Markdown file under `root`, sorted.
fn markdown_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("reading {dir:?}: {e}")) {
            let path = entry.expect("a directory entry").path();
            let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
            if path.is_dir() {
                let hidden = name.starts_with('.') && name != ".github";
                if !hidden && !SKIPPED_DIRS.contains(&name.as_str()) {
                    pending.push(path);
                }
            } else if is_markdown(&path) {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn repo_root() -> PathBuf {
    normalize(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
}

#[test]
fn a_heading_is_slugged_the_way_github_slugs_it() {
    // An em-dash between two spaces leaves both spaces, so two hyphens.
    assert_eq!(
        github_slug("ADR-158 — A collection drop is chunked, and its tombstone is written first"),
        "adr-158--a-collection-drop-is-chunked-and-its-tombstone-is-written-first"
    );
    // Code keeps its underscores; punctuation, dots included, goes.
    assert_eq!(github_slug("`delete_guarded` re-reads it"), "delete_guarded-re-reads-it");
    assert_eq!(github_slug("What's new in 0.33.0?"), "whats-new-in-0330");
    // A link keeps its text, emphasis goes, and a word's own underscore stays.
    assert_eq!(
        github_slug("See [ADR-183](decisions.md), **bold** and _stressed_ snake_case"),
        "see-adr-183-bold-and-stressed-snake_case"
    );
    assert_eq!(github_slug("Café au lait"), "café-au-lait");
}

#[test]
fn a_repeated_heading_is_numbered_after_the_first() {
    let doc = parse("# Notes\n\n## Notes\n\n### Notes\n\n## Notes-1\n");
    // The third `Notes` would be `notes-2`; a heading already spelling
    // `notes-1` pushes the numbering past it, as GitHub does.
    assert_eq!(
        doc.anchors,
        ["notes", "notes-1", "notes-2", "notes-1-1"].into_iter().map(String::from).collect()
    );
}

#[test]
fn links_in_code_are_not_links_and_a_setext_heading_is_a_heading() {
    let doc = parse(
        "Title\n=====\n\n```sh\n[in a fence](#no)\n```\n\n`[in a span](#no)` and [real](#title)\n\n\
         - a list item\n---\n\n[ref]: other.md#there\n",
    );
    assert_eq!(doc.anchors, ["title".to_string()].into_iter().collect());
    assert_eq!(doc.headings, 1, "a `---` under a list item is a thematic break");
    assert_eq!(
        doc.links,
        vec![(8, "#title".to_string()), (13, "other.md#there".to_string())],
        "the fenced link and the one in backticks are not read"
    );
    assert_eq!(parse("```\n[x](#y)\n").unclosed_fence, Some(1));
}

#[test]
fn every_kind_of_broken_link_is_reported_with_its_line() {
    // The checker's positive control: a tree with one link broken in each way
    // it can be, beside one that works of each kind. If this passes and the
    // real tree's test passes, the real tree's links were read and resolved;
    // if only the second passed, nothing would say which.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir(root.join("docs")).unwrap();
    std::fs::write(
        root.join("docs/a.md"),
        "# A page\n\n## Part — two\n\n\
         [ok](#part--two) [bad](#part-two)\n\
         [ok](b.md#b-page) [bad](b.md#nowhere)\n\
         [ok](b.md) [bad](missing.md) [ok](../README.md)\n\
         [bad](b.rs#section) [ok](b.rs#L3)\n\
         [ok](https://example.com/#anything)\n\
         [ref]: b.md#also-nowhere\n\
         ```\nunclosed [x](#never-read)\n",
    )
    .unwrap();
    std::fs::write(root.join("docs/b.md"), "# B page\n").unwrap();
    std::fs::write(root.join("docs/b.rs"), "fn main() {}\n").unwrap();
    std::fs::write(root.join("README.md"), "[into docs](docs/a.md#a-page)\n").unwrap();

    let files = markdown_files(root);
    let report = check(root, &files);
    assert_eq!(
        report.broken,
        [
            "docs/a.md:11: a code fence opened here is never closed, so the rest of the file \
             renders as code",
            "docs/a.md:5: #part-two: no heading in docs/a.md makes the anchor #part-two",
            "docs/a.md:6: b.md#nowhere: no heading in docs/b.md makes the anchor #nowhere",
            "docs/a.md:7: missing.md: no such file",
            "docs/a.md:8: b.rs#section: an anchor into something that is not a Markdown file",
            "docs/a.md:10: b.md#also-nowhere: no heading in docs/b.md makes the anchor \
             #also-nowhere",
        ]
        .map(String::from),
        "{report:#?}"
    );
    // The counts the real-tree test's premises rest on, held to a tree whose
    // answer is known: README's link into docs/a.md is a cross-file anchor
    // that resolves, and the link after the unclosed fence is never read.
    assert_eq!(
        (
            report.files,
            report.same_file_anchors,
            report.cross_file_anchors,
            report.to_files,
            report.external
        ),
        (3, 2, 4, 3, 1),
        "{report:#?}"
    );
}

#[test]
fn every_link_between_the_repositorys_markdown_files_resolves() {
    let root = repo_root();
    let files = markdown_files(&root);
    let report = check(&root, &files);

    // What it read, before what it found: a checker that read nothing, or
    // lost the rest of a file to a fence it misparsed, passes on any tree.
    let names: BTreeSet<String> = files.iter().map(|f| relative(&root, f)).collect();
    for must in ["README.md", "CHANGELOG.md", "docs/decisions.md", "docs/operations.md"] {
        assert!(names.contains(must), "premise: {must} was read; read {names:?}");
    }
    assert!(report.files >= 20, "premise: the tree's Markdown was walked: {report:#?}");
    assert!(
        report.same_file_anchors >= 50 && report.cross_file_anchors >= 5 && report.to_files >= 20,
        "premise: links of every kind were resolved: {report:#?}"
    );
    eprintln!(
        "read {} files: {} same-file anchors, {} cross-file anchors, {} file links, {} external \
         skipped",
        report.files,
        report.same_file_anchors,
        report.cross_file_anchors,
        report.to_files,
        report.external
    );
    // Every ADR heading was read *as that heading*, by anchor, not by count.
    // Counting cannot see this: decisions.md has far more headings than ADRs,
    // so a parser that lost a whole region -- a mis-paired fence swallowing it,
    // say -- still reports more headings than there are ADRs and passes. The
    // unclosed-fence report only catches a fence left open to the end of file.
    let decisions = std::fs::read_to_string(root.join("docs/decisions.md")).unwrap();
    let adr_slugs: Vec<String> = decisions
        .lines()
        .filter(|l| l.starts_with("## ADR-"))
        .map(|l| github_slug(atx(l).expect("a `## ` line is an ATX heading")))
        .collect();
    assert!(adr_slugs.len() > 100, "premise: the ADRs were found ({})", adr_slugs.len());
    let anchors = parse(&decisions).anchors;
    let missing: Vec<&str> =
        adr_slugs.iter().filter(|s| !anchors.contains(*s)).map(String::as_str).collect();
    assert!(
        missing.is_empty(),
        "premise: {} of {} ADR headings in docs/decisions.md were not read as headings, so a \
         region of the file was parsed as something other than what it is, and no link inside \
         it was checked: {missing:?}",
        missing.len(),
        adr_slugs.len()
    );

    assert!(
        report.broken.is_empty(),
        "{} broken link(s) among {} same-file anchors, {} cross-file anchors and {} file links \
         in {} files:\n{}",
        report.broken.len(),
        report.same_file_anchors,
        report.cross_file_anchors,
        report.to_files,
        report.files,
        report.broken.join("\n")
    );
}
