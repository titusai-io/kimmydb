//! Dot-path resolution with MongoDB array semantics.
//!
//! The rule that surprises people, and that the rest of the query layer is
//! built around: a path traverses *into* arrays implicitly. Given
//! `{items: [{sku: "a"}, {sku: "b"}]}`, the path `items.sku` resolves to both
//! `"a"` and `"b"`, and a filter matches if *any* of them matches.
//!
//! A numeric segment is ambiguous — `a.0` could mean "index 0" or "the field
//! named 0" — so both interpretations are resolved, matching Mongo. A read's
//! position is `0` or a digit 1-9 followed by digits ([`read_position`]); a
//! write's is any run of digits ([`array_index`]).

use bson::{Bson, Document};

/// The most elements a write through an index may add to an array past its end.
pub const MAX_BACKFILL: usize = 1_500_000;

/// The array position a path segment names, if it names one.
///
/// A position is a non-empty run of ASCII digits, leading zeros allowed (`00`
/// and `01` are positions), and nothing else: a sign (`+0`) makes it a field
/// name. A run of digits too long for a `usize` names no position either.
pub fn array_index(segment: &str) -> Option<usize> {
    if segment.is_empty() || !segment.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    segment.parse::<usize>().ok()
}

/// The array position a path segment names **for a read**: a filter, a sort key, an index
/// key. `0`, or a digit 1-9 followed by digits. `00`, `01`, `+0` and `-1` are field names
/// (q7s3f.jsonl 48-82; g:372-373). A run of digits too long for a `usize` is a position past
/// every end, never an error (g:331-378; j:157-162).
///
/// A write reads a position differently: [`array_index`] takes `00` and `01` as positions
/// (q7s2g2.jsonl 34, 37). The two functions are not interchangeable.
pub fn read_position(segment: &str) -> Option<usize> {
    let ok = match segment.as_bytes() {
        [b'0'] => true,
        [b'1'..=b'9', rest @ ..] => rest.iter().all(|b| b.is_ascii_digit()),
        _ => false,
    };
    ok.then(|| segment.parse::<usize>().unwrap_or(usize::MAX))
}

/// Why a write cannot address a position of an array.
enum IndexRefusal {
    /// The segment is no position at all.
    NotAnIndex,
    /// A position, but too large to be an index of a document's array: a value
    /// that does not fit an `i32`. UNRECORDED where the reference draws this
    /// line, the recording has 99999999999 only.
    TooLarge,
    /// A position more than [`MAX_BACKFILL`] past the end of the array.
    Backfill,
}

/// The position `segment` names for a write into an array of `len` elements.
/// Checked before any element is added, by both [`set`] and [`set_checked`].
fn index_to_write(segment: &str, len: usize) -> Result<usize, IndexRefusal> {
    let index = array_index(segment).ok_or(IndexRefusal::NotAnIndex)?;
    if i32::try_from(index).is_err() {
        return Err(IndexRefusal::TooLarge);
    }
    if index > len && index - len > MAX_BACKFILL {
        return Err(IndexRefusal::Backfill);
    }
    Ok(index)
}

fn backfill_refusal() -> String {
    format!("can't backfill more than {MAX_BACKFILL} elements")
}

/// Split a dot path into its segments.
pub fn segments(path: &str) -> Vec<&str> {
    path.split('.').collect()
}

/// All values reachable at `path`, following arrays implicitly.
///
/// An empty result means the path is absent, which callers must distinguish
/// from a path present with value `null` — `$exists` and `$eq: null` depend on
/// the difference.
pub fn resolve<'a>(doc: &'a Document, path: &str) -> Vec<&'a Bson> {
    resolve_by(doc, path, Positions::Read)
}

/// [`resolve`] with a write's positions: `00` and `01` are positions, as in an update path.
/// For the one reader of a path that keeps them, `$graphLookup`'s `connectFromField`
/// (q7s4i 343); every other reader of a path takes [`resolve`]'s rule.
pub fn resolve_with_written_positions<'a>(doc: &'a Document, path: &str) -> Vec<&'a Bson> {
    resolve_by(doc, path, Positions::Written)
}

fn resolve_by<'a>(doc: &'a Document, path: &str, positions: Positions) -> Vec<&'a Bson> {
    let segs = segments(path);
    let mut out = Vec::new();
    resolve_in_document(doc, &segs, positions, &mut out);
    out
}

fn resolve_in_document<'a>(
    doc: &'a Document,
    segs: &[&str],
    positions: Positions,
    out: &mut Vec<&'a Bson>,
) {
    let Some((head, rest)) = segs.split_first() else {
        return;
    };
    if let Some(value) = doc.get(*head) {
        descend(value, rest, positions, out);
    }
}

fn descend<'a>(value: &'a Bson, segs: &[&str], positions: Positions, out: &mut Vec<&'a Bson>) {
    if segs.is_empty() {
        out.push(value);
        return;
    }
    let (head, rest) = segs.split_first().expect("non-empty");

    match value {
        Bson::Document(doc) => {
            if let Some(next) = doc.get(*head) {
                descend(next, rest, positions, out);
            }
        }
        Bson::Array(items) => {
            // A numeric segment may address a position (a read's: `00` and `01` are field
            // names here, q7s3f 48-82)...
            if let Some(index) = positions.of(head)
                && let Some(item) = items.get(index)
            {
                descend(item, rest, positions, out);
            }
            // ...and independently, the segment may name a field inside each
            // element. Both are valid readings, and Mongo honours both.
            for item in items {
                if let Bson::Document(doc) = item
                    && let Some(next) = doc.get(*head)
                {
                    descend(next, rest, positions, out);
                }
            }
        }
        _ => {}
    }
}

/// The distinct arrays `path` goes through or ends in, each named by the
/// dotted prefix it sits at (`x` for `x.a` over `x: [{a: 1}, {a: 2}]`, and `x.a`
/// as well when those `a` are arrays themselves).
///
/// An array is named by its place in the path, not by its elements, so two
/// paths through one array (`x.a` and `x.b` over `x`) share it, which is how the
/// reference counts the arrays a compound index spans. An array of one element
/// is an array: it is named although the path resolves to a single value.
///
/// `positions` is the rule the index key walk reads a position by, and the arrays are the ones
/// that walk goes through: a position inside an array reads that element only, and any other
/// segment names the field in each document element.
pub fn array_prefixes(doc: &Document, path: &str, positions: Positions) -> Vec<String> {
    let segs = segments(path);
    let mut out = std::collections::BTreeSet::new();
    if let Some(value) = doc.get(segs[0]) {
        visit_arrays(value, &segs, 1, positions, &mut out);
    }
    out.into_iter().collect()
}

fn visit_arrays(
    value: &Bson,
    segs: &[&str],
    depth: usize,
    positions: Positions,
    out: &mut std::collections::BTreeSet<String>,
) {
    if let Bson::Array(_) = value {
        out.insert(segs[..depth].join("."));
    }
    if depth == segs.len() {
        return;
    }
    let head = segs[depth];
    match value {
        Bson::Document(doc) => {
            if let Some(next) = doc.get(head) {
                visit_arrays(next, segs, depth + 1, positions, out);
            }
        }
        Bson::Array(items) => {
            // The key walk's reading of a segment: a position inside the array is that
            // element only, and any other segment names the field in each document element.
            if let Some(item) = positions.of(head).and_then(|index| items.get(index)) {
                visit_arrays(item, segs, depth + 1, positions, out);
            } else {
                for item in items {
                    if let Bson::Document(doc) = item
                        && let Some(next) = doc.get(head)
                    {
                        visit_arrays(next, segs, depth + 1, positions, out);
                    }
                }
            }
        }
        _ => {}
    }
}

/// How [`walk`] reads the values a path reaches (the resolver rule, q7s3-resolver-rule.md).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// A filter: a non-document array element contributes nothing, and an element lacking the
    /// field sets [`Walk::lacked`].
    Filter,
    /// A sort or an index key: a non-document array element counts as lacking the field, and a
    /// position inside the array reads that element only, never a field of that name.
    Key,
}

/// What [`walk`] does in [`Mode::Key`] when a position is inside the array and some direct
/// element is a document holding a field named by the digits (g:133; j:151-186).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnAmbiguous {
    /// Refuse: [`Ambiguous16746`]. A sort and a local write.
    Error,
    /// Read the position only and set [`Walk::ambiguous`]: the caller files the document
    /// unkeyed (a replicated write).
    Unkeyed,
    /// Read the position only and say nothing.
    Ignore,
}

/// Which function says what a segment's position is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Positions {
    /// [`read_position`]: the rule of a read.
    Read,
    /// [`array_index`]: the write rule, which takes `00` and `01` as positions. It is what an
    /// index key read until the read-position switch, and is kept for the one commit between.
    Written,
}

impl Positions {
    /// The position `segment` names under this rule, if it names one.
    pub fn of(self, segment: &str) -> Option<usize> {
        match self {
            Positions::Read => read_position(segment),
            Positions::Written => array_index(segment),
        }
    }
}

/// How the root of a [`walk`] was reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Root {
    /// A field value: an array is traversed and positions are read.
    Field,
    /// An array element reached by traversal: an array is not descended, and a non-document
    /// contributes nothing in a filter and a null in a key.
    Item,
}

/// The options of one [`walk`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rule {
    pub mode: Mode,
    pub on_ambiguous: OnAmbiguous,
    pub positions: Positions,
}

/// "Ambiguous field name found in array (do not use numeric field names in embedded elements in
/// an array)", the reference's 16746.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ambiguous16746;

impl std::fmt::Display for Ambiguous16746 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "Ambiguous field name found in array (do not use numeric field names in embedded \
             elements in an array)",
        )
    }
}

/// What a path reaches: see [`walk`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Walk<'a> {
    /// Each value reached, and whether it was reached by a position (a leaf array reached by a
    /// position is compared whole, not expanded).
    pub values: Vec<(&'a Bson, bool)>,
    /// Some document on the path lacked the next field (or, in [`Mode::Key`], some array
    /// element was not a document): a null for the comparisons that read a missing field as one.
    pub lacked: bool,
    /// [`Mode::Key`] with [`OnAmbiguous::Unkeyed`]: a position was inside the array and a
    /// direct element held a field of that name.
    pub ambiguous: bool,
}

impl<'a> Walk<'a> {
    /// A path none of whose values exists: a document without the field.
    pub fn missing() -> Self {
        Walk { values: Vec::new(), lacked: true, ambiguous: false }
    }

    /// One value that is the whole answer: an element a condition is tested against.
    pub fn single(value: &'a Bson) -> Self {
        Walk { values: vec![(value, false)], lacked: false, ambiguous: false }
    }

    /// Whether `predicate` holds for a value, or, for an array value not reached by a
    /// position, for one of its items.
    pub fn any(&self, predicate: impl Fn(&Bson) -> bool) -> bool {
        self.values.iter().any(|(value, by_position)| {
            predicate(value)
                || match value {
                    Bson::Array(items) if !by_position => items.iter().any(&predicate),
                    _ => false,
                }
        })
    }
}

/// The rule of a filter: [`Mode::Filter`] with the read positions. Nothing in this mode refuses.
pub const FILTER: Rule =
    Rule { mode: Mode::Filter, on_ambiguous: OnAmbiguous::Ignore, positions: Positions::Read };

/// What a filter reads at `path` in a stored document (`FILTER`, which cannot refuse).
pub fn read_document<'a>(doc: &'a Document, path: &str) -> Walk<'a> {
    walk_document(doc, path, FILTER).unwrap_or_default()
}

/// What a filter reads at `path` below `root`, which was reached as `root_how` says.
pub fn read_value<'a>(root: &'a Bson, root_how: Root, path: &str) -> Walk<'a> {
    walk(root, root_how, path, FILTER).unwrap_or_default()
}

/// The values `path` reaches in a stored document, with the resolver rule: see [`walk`].
pub fn walk_document<'a>(
    doc: &'a Document,
    path: &str,
    rule: Rule,
) -> Result<Walk<'a>, Ambiguous16746> {
    let segs = segments(path);
    let mut walker = Walker { rule, out: Walk::default() };
    walker.in_document(doc, &segs)?;
    Ok(walker.out)
}

/// The values `path` reaches below `root`, which was reached as `root_how` says: the resolver
/// rule of q7s3-resolver-rule.md. `path` must not be empty.
///
/// A segment on a document reads that field (present: carry on; absent: `lacked`). A segment on
/// an array reached by a field or a position reads, when it is a position inside the array, that
/// element (and, in a filter, also the field of that name in each document element; in a key
/// never, refusing or filing the document when a direct element holds it), and otherwise the
/// field of that name in each element. A scalar with segments left lacks the field.
pub fn walk<'a>(
    root: &'a Bson,
    root_how: Root,
    path: &str,
    rule: Rule,
) -> Result<Walk<'a>, Ambiguous16746> {
    let segs = segments(path);
    let mut walker = Walker { rule, out: Walk::default() };
    let how = match root_how {
        Root::Field => Reach::Field,
        Root::Item => Reach::Item,
    };
    walker.go(root, &segs, how)?;
    Ok(walker.out)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Reach {
    Field,
    Item,
    Position,
}

struct Walker<'a> {
    rule: Rule,
    out: Walk<'a>,
}

impl<'a> Walker<'a> {
    fn position(&self, segment: &str) -> Option<usize> {
        self.rule.positions.of(segment)
    }

    fn in_document(&mut self, doc: &'a Document, segs: &[&str]) -> Result<(), Ambiguous16746> {
        let Some((head, rest)) = segs.split_first() else { return Ok(()) };
        match doc.get(*head) {
            Some(next) => self.go(next, rest, Reach::Field),
            None => {
                self.out.lacked = true;
                Ok(())
            }
        }
    }

    fn go(&mut self, v: &'a Bson, segs: &[&str], how: Reach) -> Result<(), Ambiguous16746> {
        let Some((head, rest)) = segs.split_first() else {
            self.out.values.push((v, how == Reach::Position));
            return Ok(());
        };
        match v {
            Bson::Document(doc) => self.in_document(doc, segs),
            Bson::Array(items) if how != Reach::Item => self.in_array(items, head, rest),
            _ => {
                if how == Reach::Field || self.rule.mode == Mode::Key {
                    self.out.lacked = true;
                }
                Ok(())
            }
        }
    }

    fn in_array(
        &mut self,
        items: &'a [Bson],
        head: &str,
        rest: &[&str],
    ) -> Result<(), Ambiguous16746> {
        let inside = self.position(head).filter(|p| *p < items.len());
        let Some(at) = inside else {
            // No position, or one past the end (an over-long one included): the field of that
            // name in each element.
            for item in items {
                match item {
                    Bson::Document(doc) => self.named(doc, head, rest)?,
                    _ if self.rule.mode == Mode::Key => self.out.lacked = true,
                    _ => {}
                }
            }
            return Ok(());
        };
        self.go(&items[at], rest, Reach::Position)?;
        match self.rule.mode {
            Mode::Filter => {
                for item in items {
                    if let Bson::Document(doc) = item {
                        self.named(doc, head, rest)?;
                    }
                }
            }
            Mode::Key => {
                let clash =
                    items.iter().any(|i| matches!(i, Bson::Document(d) if d.contains_key(head)));
                if clash {
                    match self.rule.on_ambiguous {
                        OnAmbiguous::Error => return Err(Ambiguous16746),
                        OnAmbiguous::Unkeyed => self.out.ambiguous = true,
                        OnAmbiguous::Ignore => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// The field `head` of a document element: present, carry on; absent, `lacked`.
    fn named(
        &mut self,
        doc: &'a Document,
        head: &str,
        rest: &[&str],
    ) -> Result<(), Ambiguous16746> {
        match doc.get(head) {
            Some(next) => self.go(next, rest, Reach::Field),
            None => {
                self.out.lacked = true;
                Ok(())
            }
        }
    }
}

/// Set a value at a dot path, creating intermediate documents as needed.
///
/// Traverses existing arrays only by numeric index; it will not fan a write out
/// across array elements, because a write must have exactly one destination.
pub fn set(doc: &mut Document, path: &str, value: Bson) -> Result<(), String> {
    let segs = segments(path);
    set_in_document(doc, &segs, value)
}

fn set_in_document(doc: &mut Document, segs: &[&str], value: Bson) -> Result<(), String> {
    let (head, rest) = segs.split_first().expect("non-empty path");
    if rest.is_empty() {
        doc.insert(*head, value);
        return Ok(());
    }

    // Vivify a missing or non-traversable intermediate as a document, which is
    // what Mongo's $set does.
    let needs_new = !matches!(doc.get(*head), Some(Bson::Document(_)) | Some(Bson::Array(_)));
    if needs_new {
        doc.insert(*head, Bson::Document(Document::new()));
    }

    match doc.get_mut(*head).expect("just ensured present") {
        Bson::Document(child) => set_in_document(child, rest, value),
        Bson::Array(items) => {
            let (index_seg, tail) = rest.split_first().expect("rest is non-empty");
            let index = index_to_write(index_seg, items.len()).map_err(|why| match why {
                IndexRefusal::NotAnIndex => {
                    format!("cannot traverse array with non-numeric segment {index_seg:?}")
                }
                IndexRefusal::TooLarge => cannot_create_in_array(index_seg, head, items),
                IndexRefusal::Backfill => backfill_refusal(),
            })?;
            while items.len() <= index {
                items.push(Bson::Null);
            }
            if tail.is_empty() {
                items[index] = value;
                return Ok(());
            }
            if !matches!(items[index], Bson::Document(_)) {
                items[index] = Bson::Document(Document::new());
            }
            match &mut items[index] {
                Bson::Document(child) => set_in_document(child, tail, value),
                _ => unreachable!("just ensured a document"),
            }
        }
        _ => unreachable!("just ensured a container"),
    }
}

/// The keys an update created, each as the full path of keys and array
/// positions from the document root down to the new key.
pub type Created = Vec<Vec<String>>;

/// Set a value at a dot path the way an update writes it: a numeric segment
/// addresses an array element at any depth, and a write below a scalar, a
/// `null` or an array element that is not a document is refused rather than
/// replacing the value.
///
/// Every key this call adds to a document is appended to `created`, for
/// [`settle_created`] to put in order once the whole update has been applied.
/// Unlike [`set`], which the aggregation stages share and which makes room by
/// replacing a scalar with a document, this is the update language's write.
pub fn set_checked(
    doc: &mut Document,
    path: &str,
    value: Bson,
    created: &mut Created,
) -> Result<(), String> {
    let segs = segments(path);
    write_in_document(doc, &segs, value, &mut Vec::new(), created)
}

fn cannot_create(field: &str, under: &str, element: &Bson) -> String {
    match element {
        Bson::Array(items) => cannot_create_in_array(field, under, items),
        _ => format!("cannot create field '{field}' in element {{{under}: {element}}}"),
    }
}

/// [`cannot_create`] for an array, which the reference shows with a space inside
/// its brackets: `{arr: [ 7, 8 ]}`.
fn cannot_create_in_array(field: &str, under: &str, items: &[Bson]) -> String {
    if items.is_empty() {
        return format!("cannot create field '{field}' in element {{{under}: []}}");
    }
    let shown: Vec<String> = items.iter().map(ToString::to_string).collect();
    format!("cannot create field '{field}' in element {{{under}: [ {} ]}}", shown.join(", "))
}

fn write_in_document(
    doc: &mut Document,
    segs: &[&str],
    value: Bson,
    trail: &mut Vec<String>,
    created: &mut Created,
) -> Result<(), String> {
    let (head, rest) = segs.split_first().expect("non-empty path");
    let present = doc.contains_key(*head);
    if !present {
        let mut key = trail.clone();
        key.push((*head).to_string());
        created.push(key);
    }
    if rest.is_empty() {
        doc.insert(*head, value);
        return Ok(());
    }
    if !present {
        doc.insert(*head, Bson::Document(Document::new()));
    }
    trail.push((*head).to_string());
    let outcome = match doc.get_mut(*head).expect("just ensured present") {
        Bson::Document(child) => write_in_document(child, rest, value, trail, created),
        Bson::Array(items) => write_in_array(items, head, rest, value, trail, created),
        other => Err(cannot_create(rest[0], head, other)),
    };
    trail.pop();
    outcome
}

fn write_in_array(
    items: &mut Vec<Bson>,
    under: &str,
    segs: &[&str],
    value: Bson,
    trail: &mut Vec<String>,
    created: &mut Created,
) -> Result<(), String> {
    let (index_seg, tail) = segs.split_first().expect("non-empty path");
    let index = index_to_write(index_seg, items.len()).map_err(|why| match why {
        IndexRefusal::NotAnIndex | IndexRefusal::TooLarge => {
            cannot_create_in_array(index_seg, under, items)
        }
        IndexRefusal::Backfill => backfill_refusal(),
    })?;
    let existed = index < items.len();
    while items.len() <= index {
        items.push(Bson::Null);
    }
    if tail.is_empty() {
        items[index] = value;
        return Ok(());
    }
    if !existed {
        items[index] = Bson::Document(Document::new());
    }
    trail.push(index.to_string());
    let outcome = match &mut items[index] {
        Bson::Document(child) => write_in_document(child, tail, value, trail, created),
        Bson::Array(inner) => write_in_array(inner, index_seg, tail, value, trail, created),
        other => Err(cannot_create(tail[0], index_seg, other)),
    };
    trail.pop();
    outcome
}

/// Put the keys an update created in order: at each level, the fields that
/// were there stay where they were and the new ones follow, sorted by
/// [`new_key_order`]: the names made of digits by their numeric value, then the
/// others in byte order.
///
/// A document an update was given as a value is not touched, only the levels
/// the update's own paths created or wrote into.
pub fn settle_created(doc: &mut Document, created: &[Vec<String>]) {
    let mut levels: std::collections::BTreeMap<&[String], std::collections::HashSet<&str>> =
        std::collections::BTreeMap::new();
    for key in created {
        if let Some((name, parent)) = key.split_last() {
            levels.entry(parent).or_default().insert(name.as_str());
        }
    }
    for (parent, names) in levels {
        if let Some(level) = document_at(doc, parent) {
            sort_new_keys(level, &names);
        }
    }
}

fn document_at<'a>(doc: &'a mut Document, trail: &[String]) -> Option<&'a mut Document> {
    let Some((head, rest)) = trail.split_first() else { return Some(doc) };
    match doc.get_mut(head)? {
        Bson::Document(child) => document_at(child, rest),
        Bson::Array(items) => array_document_at(items, rest),
        _ => None,
    }
}

fn array_document_at<'a>(items: &'a mut [Bson], trail: &[String]) -> Option<&'a mut Document> {
    let (head, rest) = trail.split_first()?;
    match items.get_mut(array_index(head)?)? {
        Bson::Document(child) => document_at(child, rest),
        Bson::Array(inner) => array_document_at(inner, rest),
        _ => None,
    }
}

/// The order the fields an update creates take at a level: a name made only of
/// ASCII digits sorts before any other, by its numeric value (`9` before `10`),
/// and the rest follow in byte order. UNRECORDED: two digit names of one value
/// (`01` and `1`) are told apart by their bytes.
pub fn new_key_order(a: &str, b: &str) -> std::cmp::Ordering {
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit());
    match (digits(a), digits(b)) {
        (true, true) => {
            let (x, y) = (a.trim_start_matches('0'), b.trim_start_matches('0'));
            x.len().cmp(&y.len()).then_with(|| x.cmp(y)).then_with(|| a.cmp(b))
        }
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => a.cmp(b),
    }
}

fn sort_new_keys(level: &mut Document, new: &std::collections::HashSet<&str>) {
    let keys: Vec<&String> = level.keys().collect();
    let first_new = keys.iter().position(|k| new.contains(k.as_str()));
    let settled = match first_new {
        None => true,
        Some(at) => {
            keys[at..].iter().all(|k| new.contains(k.as_str()))
                && keys[at..].windows(2).all(|w| new_key_order(w[0], w[1]).is_lt())
        }
    };
    if settled {
        return;
    }
    let (mut fresh, kept): (Vec<_>, Vec<_>) =
        std::mem::take(level).into_iter().partition(|(k, _)| new.contains(k.as_str()));
    fresh.sort_by(|a, b| new_key_order(&a.0, &b.0));
    level.extend(kept.into_iter().chain(fresh));
}

/// Sort by name the keys of every sub-document a dot path goes through, the
/// root and the path's own last key excepted: the levels a write at `path`
/// created. A document that is the value at the path is left as it is.
pub fn sort_levels_through(doc: &mut Document, path: &str) {
    let segs = segments(path);
    let mut level = doc;
    for seg in &segs[..segs.len() - 1] {
        let Some(Bson::Document(child)) = level.get_mut(*seg) else { return };
        let mut fields: Vec<(String, Bson)> = std::mem::take(child).into_iter().collect();
        fields.sort_by(|a, b| a.0.cmp(&b.0));
        child.extend(fields);
        level = child;
    }
}

/// Remove the value at a dot path. Returns whether anything was removed.
pub fn unset(doc: &mut Document, path: &str) -> bool {
    let segs = segments(path);
    unset_in_document(doc, &segs)
}

fn unset_in_document(doc: &mut Document, segs: &[&str]) -> bool {
    let (head, rest) = segs.split_first().expect("non-empty path");
    if rest.is_empty() {
        return doc.remove(*head).is_some();
    }
    match doc.get_mut(*head) {
        Some(Bson::Document(child)) => unset_in_document(child, rest),
        Some(Bson::Array(items)) => {
            let (index_seg, tail) = rest.split_first().expect("rest is non-empty");
            let Some(index) = array_index(index_seg) else {
                return false;
            };
            match items.get_mut(index) {
                // Mongo leaves a null hole rather than shifting the array, so
                // that other paths into it keep their indices.
                Some(slot) if tail.is_empty() => {
                    *slot = Bson::Null;
                    true
                }
                Some(Bson::Document(child)) => unset_in_document(child, tail),
                _ => false,
            }
        }
        _ => false,
    }
}

/// The value at a dot path, read as [`set_checked`] writes it: a document by key, an array
/// by position only (`array_index`), and nothing else. A segment that is a position
/// addresses an element and never a field of the elements, a name never addresses an
/// array, nothing is read through a scalar, and there is no fan-out: one place, the one a
/// write to the same path would change. `None` where nothing is there.
///
/// An update reads the value it computes from here, so that what it reads and what it
/// writes are one place.
pub fn get_checked<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let segs = segments(path);
    let (head, rest) = segs.split_first()?;
    let mut value = doc.get(*head)?;
    for seg in rest {
        value = match value {
            Bson::Document(child) => child.get(*seg)?,
            Bson::Array(items) => items.get(array_index(seg)?)?,
            _ => return None,
        };
    }
    Some(value)
}

/// Remove the value at a dot path as an update does: through a numeric
/// segment at any depth of nested arrays, leaving a `null` hole where an
/// element was. Returns whether anything was removed.
pub fn unset_checked(doc: &mut Document, path: &str) -> bool {
    let segs = segments(path);
    unset_deep_in_document(doc, &segs)
}

fn unset_deep_in_document(doc: &mut Document, segs: &[&str]) -> bool {
    let (head, rest) = segs.split_first().expect("non-empty path");
    if rest.is_empty() {
        return doc.remove(*head).is_some();
    }
    match doc.get_mut(*head) {
        Some(Bson::Document(child)) => unset_deep_in_document(child, rest),
        Some(Bson::Array(items)) => unset_deep_in_array(items, rest),
        _ => false,
    }
}

fn unset_deep_in_array(items: &mut [Bson], segs: &[&str]) -> bool {
    let (index_seg, tail) = segs.split_first().expect("non-empty path");
    let Some(index) = array_index(index_seg) else {
        return false;
    };
    match items.get_mut(index) {
        Some(slot) if tail.is_empty() => {
            *slot = Bson::Null;
            true
        }
        Some(Bson::Document(child)) => unset_deep_in_document(child, tail),
        Some(Bson::Array(inner)) => unset_deep_in_array(inner, tail),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;

    #[test]
    fn get_checked_reads_the_place_a_write_addresses() {
        let d = doc! { "a": [{ "3": 5, "x": 1 }, [7, 8]], "b": { "c": [10, 11] }, "n": 1 };
        let at = |p: &str| get_checked(&d, p).cloned();
        // Documents by key, arrays by position only.
        assert_eq!(at("b.c.1"), Some(Bson::Int32(11)));
        assert_eq!(at("a.1.0"), Some(Bson::Int32(7)));
        assert_eq!(at("a.0.x"), Some(Bson::Int32(1)));
        assert_eq!(at("a.0.3"), Some(Bson::Int32(5)));
        // A position never reads a field of the elements, and a name never reads an array.
        assert_eq!(at("a.3"), None);
        assert_eq!(at("a.x"), None);
        assert_eq!(at("b.c.x"), None);
        // Nothing through a scalar, nothing past the end, a sign is a name, a leading zero a position.
        assert_eq!(at("n.x"), None);
        assert_eq!(at("n.0"), None);
        assert_eq!(at("a.9"), None);
        assert_eq!(at("b.c.+1"), None);
        assert_eq!(at("b.c.01"), Some(Bson::Int32(11)));
        assert_eq!(at("zzz"), None);
    }

    use super::*;

    #[test]
    fn array_prefixes_name_each_array_by_its_place_in_the_path() {
        let d =
            doc! { "x": [{ "a": 1, "b": [1] }, { "a": 2 }], "c": [1, 2], "n": { "m": 1 }, "e": [] };
        assert_eq!(array_prefixes(&d, "x.a", Positions::Read), vec!["x"], "the array of documents");
        assert_eq!(
            array_prefixes(&d, "x.b", Positions::Read),
            vec!["x", "x.b"],
            "and the array inside it"
        );
        assert_eq!(array_prefixes(&d, "c", Positions::Read), vec!["c"]);
        assert_eq!(
            array_prefixes(&d, "e", Positions::Read),
            vec!["e"],
            "an empty array is an array"
        );
        assert!(array_prefixes(&d, "n.m", Positions::Read).is_empty());
        assert!(array_prefixes(&d, "missing.a", Positions::Read).is_empty());
        let one = doc! { "x": [{ "a": 1 }] };
        assert_eq!(
            array_prefixes(&one, "x.a", Positions::Read),
            vec!["x"],
            "one element is still an array"
        );
        assert_eq!(
            array_prefixes(&one, "x.0.a", Positions::Read),
            vec!["x"],
            "a position reads the same array"
        );
    }

    fn values(doc: &Document, path: &str) -> Vec<Bson> {
        resolve(doc, path).into_iter().cloned().collect()
    }

    #[test]
    fn resolves_a_top_level_field() {
        let d = doc! { "a": 1 };
        assert_eq!(values(&d, "a"), vec![Bson::Int32(1)]);
    }

    #[test]
    fn resolves_a_nested_field() {
        let d = doc! { "a": { "b": { "c": 7 } } };
        assert_eq!(values(&d, "a.b.c"), vec![Bson::Int32(7)]);
    }

    #[test]
    fn a_missing_path_resolves_to_nothing() {
        let d = doc! { "a": 1 };
        assert!(values(&d, "b").is_empty());
        assert!(values(&d, "a.b").is_empty());
        assert!(values(&d, "a.b.c.d").is_empty());
    }

    #[test]
    fn an_explicit_null_is_distinct_from_a_missing_field() {
        // $exists and `$eq: null` both hinge on this distinction.
        let d = doc! { "a": Bson::Null };
        assert_eq!(values(&d, "a"), vec![Bson::Null]);
        assert!(values(&d, "b").is_empty());
    }

    #[test]
    fn paths_traverse_into_arrays_of_documents() {
        let d = doc! { "items": [ { "sku": "a" }, { "sku": "b" } ] };
        assert_eq!(
            values(&d, "items.sku"),
            vec![Bson::String("a".into()), Bson::String("b".into())]
        );
    }

    #[test]
    fn a_terminal_array_resolves_to_the_array_itself() {
        // Element-wise matching is the comparison layer's job, not the path's.
        let d = doc! { "tags": ["x", "y"] };
        assert_eq!(values(&d, "tags").len(), 1);
        assert!(matches!(values(&d, "tags")[0], Bson::Array(_)));
    }

    #[test]
    fn numeric_segments_address_positions_and_field_names_alike() {
        let by_index = doc! { "a": [10, 20] };
        assert_eq!(values(&by_index, "a.0"), vec![Bson::Int32(10)]);

        // The same segment can also name a field inside array elements.
        let by_name = doc! { "a": [ { "0": "named" } ] };
        assert!(values(&by_name, "a.0").contains(&Bson::String("named".into())));
    }

    #[test]
    fn nested_arrays_flatten_across_levels() {
        let d = doc! { "a": [ { "b": [ { "c": 1 }, { "c": 2 } ] } ] };
        assert_eq!(values(&d, "a.b.c"), vec![Bson::Int32(1), Bson::Int32(2)]);
    }

    #[test]
    fn set_creates_missing_intermediates() {
        let mut d = doc! {};
        set(&mut d, "a.b.c", Bson::Int32(1)).unwrap();
        assert_eq!(d, doc! { "a": { "b": { "c": 1 } } });
    }

    #[test]
    fn set_overwrites_a_scalar_intermediate() {
        let mut d = doc! { "a": 5 };
        set(&mut d, "a.b", Bson::Int32(1)).unwrap();
        assert_eq!(d, doc! { "a": { "b": 1 } });
    }

    #[test]
    fn set_writes_into_arrays_by_index() {
        let mut d = doc! { "a": [1, 2, 3] };
        set(&mut d, "a.1", Bson::Int32(99)).unwrap();
        assert_eq!(d, doc! { "a": [1, 99, 3] });
    }

    #[test]
    fn set_pads_an_array_when_writing_past_its_end() {
        let mut d = doc! { "a": [1] };
        set(&mut d, "a.3", Bson::Int32(9)).unwrap();
        assert_eq!(d, doc! { "a": [1, Bson::Null, Bson::Null, 9] });
    }

    #[test]
    fn set_rejects_a_non_numeric_segment_into_an_array() {
        // A write must have exactly one destination; fanning out across
        // elements would be ambiguous.
        let mut d = doc! { "a": [ { "b": 1 } ] };
        assert!(set(&mut d, "a.b", Bson::Int32(2)).is_err());
    }

    #[test]
    fn unset_removes_fields_and_reports_whether_it_did() {
        let mut d = doc! { "a": { "b": 1, "c": 2 } };
        assert!(unset(&mut d, "a.b"));
        assert_eq!(d, doc! { "a": { "c": 2 } });
        assert!(!unset(&mut d, "a.zzz"));
        assert!(!unset(&mut d, "nope.deep"));
    }

    #[test]
    fn unset_leaves_a_null_hole_in_an_array() {
        // Compacting would shift every later element, silently changing what
        // other paths into the array refer to.
        let mut d = doc! { "a": [1, 2, 3] };
        assert!(unset(&mut d, "a.1"));
        assert_eq!(d, doc! { "a": [1, Bson::Null, 3] });
    }

    #[test]
    fn both_writes_refuse_an_index_far_past_the_end_before_pushing() {
        // q7s2g.out lines 59-76. A modest index (5000000): a build that does not
        // refuse allocates it, which a larger one would not survive.
        let mut d = doc! { "arr": [7] };
        let err = set(&mut d, "arr.5000000", Bson::Int32(1)).unwrap_err();
        assert_eq!(err, "can't backfill more than 1500000 elements");
        assert_eq!(d, doc! { "arr": [7] });
        let err = set_checked(&mut d, "arr.5000000.x", Bson::Int32(1), &mut Vec::new());
        assert_eq!(err.unwrap_err(), "can't backfill more than 1500000 elements");
        assert_eq!(d, doc! { "arr": [7] });
    }

    #[test]
    fn the_gap_that_is_allowed_is_exactly_one_and_a_half_million() {
        // q7s2g.out lines 52-61: length 1, so index 1500001 is written and
        // 1500002 is not.
        let mut d = doc! { "arr": [7] };
        set(&mut d, "arr.1500001", Bson::Int32(1)).unwrap();
        assert_eq!(d.get_array("arr").unwrap().len(), 1_500_002);
        let mut d = doc! { "arr": [7] };
        set_checked(&mut d, "arr.1500001", Bson::Int32(1), &mut Vec::new()).unwrap();
        assert_eq!(d.get_array("arr").unwrap().len(), 1_500_002);
        let mut d = doc! { "arr": [7] };
        assert!(set(&mut d, "arr.1500002", Bson::Int32(1)).is_err());
        assert!(set_checked(&mut d, "arr.1500002", Bson::Int32(1), &mut Vec::new()).is_err());
        assert_eq!(d.get_array("arr").unwrap().len(), 1);
    }

    #[test]
    fn a_segment_too_large_for_an_index_is_refused_by_both_writes() {
        // q7s2g.out lines 76-116 (99999999999). UNRECORDED: the line between this
        // and the backfill refusal; a value that does not fit an i32 is this one.
        for index in ["99999999999", "2147483648"] {
            let want = format!("cannot create field '{index}' in element {{arr: [ 7 ]}}");
            let mut d = doc! { "arr": [7] };
            assert_eq!(
                set_checked(&mut d, &format!("arr.{index}.x"), Bson::Null, &mut vec![]),
                Err(want)
            );
            assert_eq!(d, doc! { "arr": [7] });
            assert!(set(&mut d, &format!("arr.{index}"), Bson::Null).is_err());
            assert_eq!(d, doc! { "arr": [7] });
        }
        let mut d = doc! { "arr": [7] };
        let err = set_checked(&mut d, "arr.2147483647", Bson::Null, &mut vec![]).unwrap_err();
        assert_eq!(err, "can't backfill more than 1500000 elements");
    }

    #[test]
    fn a_read_takes_zero_and_a_digit_run_without_a_leading_zero_as_a_position() {
        // q7s3f.jsonl 44-82 and q7s3g 372-373: in a read `00` and `01` are field names, and
        // `+0` is one too. A write reads `00` and `01` as positions (q7s2g2.jsonl 34, 37), which
        // `array_index` still does.
        let d = doc! { "arr": [7, 8], "o": { "+0": 1, "00": 2, "01": 3 } };
        assert_eq!(resolve(&d, "arr.0"), vec![&Bson::Int32(7)]);
        assert_eq!(resolve(&d, "arr.1"), vec![&Bson::Int32(8)]);
        assert!(resolve(&d, "arr.00").is_empty());
        assert!(resolve(&d, "arr.01").is_empty());
        assert!(resolve(&d, "arr.+0").is_empty());
        assert_eq!(resolve(&d, "o.+0"), vec![&Bson::Int32(1)]);
        assert_eq!(resolve(&d, "o.00"), vec![&Bson::Int32(2)]);
        assert_eq!(resolve(&d, "o.01"), vec![&Bson::Int32(3)]);
        // A document whose own fields are named `00` and `01`, inside an array.
        let named = doc! { "arr": [{ "00": "w", "01": "z" }] };
        assert_eq!(resolve(&named, "arr.00"), vec![&Bson::String("w".into())]);
        // The write still takes them as positions.
        let mut w = doc! { "arr": [7, 8] };
        set_checked(&mut w, "arr.01", Bson::Int32(9), &mut Vec::new()).unwrap();
        assert_eq!(w, doc! { "arr": [7, 9] });
    }

    // ---- the walker (C1 of the resolver rule; q7s3-resolver-rule.md) ----

    const FILTER: Rule =
        Rule { mode: Mode::Filter, on_ambiguous: OnAmbiguous::Ignore, positions: Positions::Read };
    const KEY: Rule =
        Rule { mode: Mode::Key, on_ambiguous: OnAmbiguous::Error, positions: Positions::Read };
    const KEY_UNKEYED: Rule =
        Rule { mode: Mode::Key, on_ambiguous: OnAmbiguous::Unkeyed, positions: Positions::Read };

    fn a(value: Bson) -> Document {
        doc! { "a": value }
    }

    /// The values and `lacked` of a filter walk, the values as owned.
    fn filter_walk(d: &Document, path: &str) -> (Vec<(Bson, bool)>, bool) {
        let w = walk_document(d, path, FILTER).unwrap();
        (w.values.iter().map(|(v, p)| ((*v).clone(), *p)).collect(), w.lacked)
    }

    fn key_walk(d: &Document, path: &str) -> (Vec<(Bson, bool)>, bool) {
        let w = walk_document(d, path, KEY).unwrap();
        (w.values.iter().map(|(v, p)| ((*v).clone(), *p)).collect(), w.lacked)
    }

    /// `{path: null}` over a filter walk: a lacked field or a null value (f:1).
    fn null_matches(d: &Document, path: &str) -> bool {
        let w = walk_document(d, path, FILTER).unwrap();
        w.lacked || w.any(|v| matches!(v, Bson::Null))
    }

    fn ad() -> Vec<Document> {
        vec![
            doc! { "_id": 1, "a": [{ "b": Bson::Null }, { "c": 1 }] },
            doc! { "_id": 2, "a": [{ "c": 5 }, { "d": 1 }] },
            doc! { "_id": 3, "a": [1, 2] },
            doc! { "_id": 4, "a": [] },
            doc! { "_id": 5, "a": [{ "c": Bson::Null }] },
            doc! { "_id": 6, "a": [{ "c": 1 }, { "c": Bson::Null }] },
            doc! { "_id": 7, "a": [{ "c": []}] },
            doc! { "_id": 8, "a": { "c": 1 } },
            doc! { "_id": 9, "a": [{ "c": 3 }, { "c": 1 }] },
            doc! { "_id": 10 },
            doc! { "_id": 11, "a": [{ "c": 2 }, { "d": 0 }] },
            doc! { "_id": 12, "a": [{ "d": 7 }] },
            doc! { "_id": 13, "a": [{ "c": [5, 1] }, { "c": 4 }] },
            doc! { "_id": 14, "a": [Bson::Null, { "c": 9 }] },
            doc! { "_id": 15, "a": [[{ "c": 1 }]] },
            doc! { "_id": 16, "a": { "c": Bson::Null } },
        ]
    }

    fn ids(docs: &[Document], keep: impl Fn(&Document) -> bool) -> Vec<i32> {
        docs.iter().filter(|d| keep(d)).map(|d| d.get_i32("_id").unwrap()).collect()
    }

    #[test]
    fn a_read_position_is_zero_or_a_digit_one_to_nine_and_more_digits() {
        // q7s3f.jsonl 44-82: `00`, `01`, `+0`, `-1` and a zero-led run are field names.
        assert_eq!(read_position("0"), Some(0));
        assert_eq!(read_position("1"), Some(1));
        assert_eq!(read_position("10"), Some(10));
        for name in ["", "00", "01", "+0", "-1", "0000000000001", "1a", " 1", "b"] {
            assert_eq!(read_position(name), None, "{name:?} is a field name");
        }
        // j:157-162, g:331-378: too long for a usize is a position past every end.
        assert_eq!(read_position("99999999999999999999999"), Some(usize::MAX));
        // The write rule is the other one and is untouched (q7s2g2.jsonl 34, 37).
        assert_eq!(array_index("00"), Some(0));
        assert_eq!(array_index("01"), Some(1));
        assert_eq!(array_index("+0"), None);
    }

    #[test]
    fn the_filter_walk_of_a_c_over_ad_reads_null_as_f1_does() {
        // f:1, 2, 4, 8, 9, 23: `{"a.c": null}` and its relatives.
        assert_eq!(ids(&ad(), |d| null_matches(d, "a.c")), vec![1, 2, 5, 6, 10, 11, 12, 16]);
        // f:6 `$exists: true` is "some value was reached", f:7 its complement.
        let exists = |d: &Document| !walk_document(d, "a.c", FILTER).unwrap().values.is_empty();
        assert_eq!(ids(&ad(), exists), vec![1, 2, 5, 6, 7, 8, 9, 11, 13, 14, 16]);
        assert_eq!(ids(&ad(), |d| !exists(d)), vec![3, 4, 10, 12, 15]);
        // f:14 `$type: "null"`: a stored null, not a lacking field.
        let is_null = |d: &Document| {
            walk_document(d, "a.c", FILTER).unwrap().any(|v| matches!(v, Bson::Null))
        };
        assert_eq!(ids(&ad(), is_null), vec![5, 6, 16]);
    }

    #[test]
    fn a_digit_position_over_ad_reads_the_element_and_the_field_alike() {
        // f:20, 21: `a.0.c` and `a.1.c` match null for the same thirteen documents.
        let want = vec![1, 2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16];
        assert_eq!(ids(&ad(), |d| null_matches(d, "a.0.c")), want);
        assert_eq!(ids(&ad(), |d| null_matches(d, "a.1.c")), want);
    }

    #[test]
    fn a_leaf_array_reached_by_position_is_kept_whole_and_one_reached_by_name_is_not() {
        // f:41, 44, 58; g:152.
        let d = a(Bson::Array(vec![bson::bson!([1, 2]), bson::bson!([3, 4])]));
        let (values, lacked) = filter_walk(&d, "a.0");
        assert_eq!(values, vec![(bson::bson!([1, 2]), true)]);
        assert!(!lacked, "no document element, so no field to lack");
        let d = doc! { "a": { "b": [5, 6] } };
        assert_eq!(filter_walk(&d, "a.b"), (vec![(bson::bson!([5, 6]), false)], false));
        // `any` expands the array reached by name and not the one reached by position.
        let by_name = walk_document(&d, "a.b", FILTER).unwrap();
        assert!(by_name.any(|v| v == &Bson::Int32(5)));
        let d = a(Bson::Array(vec![bson::bson!([1, 2])]));
        let by_position = walk_document(&d, "a.0", FILTER).unwrap();
        assert!(!by_position.any(|v| v == &Bson::Int32(1)));
        assert!(by_position.any(|v| v == &bson::bson!([1, 2])));
    }

    #[test]
    fn leading_zeros_are_field_names_in_a_read_walk() {
        // f:48-82 over POS 4 `{"0":"x","1":"y","01":"z","00":"w"}` and POS 1 `[5,6,7]`.
        let named = a(bson::bson!({ "0": "x", "1": "y", "01": "z", "00": "w" }));
        assert_eq!(filter_walk(&named, "a.01").0, vec![(Bson::String("z".into()), false)]);
        assert_eq!(filter_walk(&named, "a.00").0, vec![(Bson::String("w".into()), false)]);
        let list = a(bson::bson!([5, 6, 7]));
        let (values, lacked) = filter_walk(&list, "a.01");
        assert!(values.is_empty() && !lacked, "`01` names no element of an array of scalars");
        assert_eq!(filter_walk(&list, "a.1").0, vec![(Bson::Int32(6), true)]);
    }

    #[test]
    fn a_position_past_the_end_reads_field_names_and_never_refuses() {
        // j:153, 156, 175-178: PE 1 `[{"1":5}]` reads the field "1" of its element.
        let pe1 = a(bson::bson!([{ "1": 5 }]));
        assert_eq!(filter_walk(&pe1, "a.1").0, vec![(Bson::Int32(5), false)]);
        assert_eq!(key_walk(&pe1, "a.1"), (vec![(Bson::Int32(5), false)], false));
        // PE 3 `[5, 6]` under `a.2`: nothing in a filter, a null in a key (j:169-170 `a.2`).
        let pe3 = a(bson::bson!([5, 6]));
        assert_eq!(filter_walk(&pe3, "a.2"), (vec![], false));
        assert_eq!(key_walk(&pe3, "a.2"), (vec![], true));
        // PE 5, an over-long position (j:157-158).
        let pe5 = a(bson::bson!([{ "99999999999": 1 }]));
        assert_eq!(key_walk(&pe5, "a.99999999999"), (vec![(Bson::Int32(1), false)], false));
        // PE 4, missing.
        assert_eq!(filter_walk(&doc! {}, "a.1"), (vec![], true));
    }

    #[test]
    fn a_key_reads_null_for_every_element_that_is_not_a_document_with_the_field() {
        // f:27, 31; g:93-96, 100: SD 1 `[5,{c:9}]`, 2, 3, 8 `[{d:1},7]`, 15 missing.
        assert_eq!(
            key_walk(&a(bson::bson!([5, { "c": 9 }])), "a.c"),
            (vec![(Bson::Int32(9), false)], true)
        );
        assert_eq!(
            key_walk(&a(bson::bson!([[1], { "c": 9 }])), "a.c"),
            (vec![(Bson::Int32(9), false)], true)
        );
        assert_eq!(key_walk(&a(bson::bson!([{ "d": 1 }, 7])), "a.c"), (vec![], true));
        assert_eq!(
            key_walk(&a(bson::bson!([{ "c": 1 }])), "a.c"),
            (vec![(Bson::Int32(1), false)], false)
        );
        assert_eq!(key_walk(&doc! {}, "a.c"), (vec![], true));
        // AD 15 `[[{c:1}]]`: a nested array is not descended in a key.
        assert_eq!(key_walk(&a(bson::bson!([[{ "c": 1 }]])), "a.c"), (vec![], true));
        // AD 14 `[null, {c:9}]` (f:27).
        assert_eq!(
            key_walk(&a(bson::bson!([null, { "c": 9 }])), "a.c"),
            (vec![(Bson::Int32(9), false)], true)
        );
    }

    #[test]
    fn a_key_reads_a_position_inside_the_array_as_that_element_only() {
        // f:37; g:107; EP: a leaf array reached by position is whole (f:282).
        let ep1 = a(bson::bson!([[1, 2]]));
        assert_eq!(key_walk(&ep1, "a.0"), (vec![(bson::bson!([1, 2]), true)], false));
        // CL 5 `[{b:1}]` under `a.0`: the element at 0 is the document, and no field "0" is read.
        let cl5 = a(bson::bson!([{ "b": 1 }]));
        assert_eq!(key_walk(&cl5, "a.0"), (vec![(bson::bson!({ "b": 1 }), true)], false));
        assert_eq!(key_walk(&cl5, "a.0.b"), (vec![(Bson::Int32(1), false)], false));
        // The same document in a filter also reads the field "0" of its element: it lacks it.
        assert!(filter_walk(&cl5, "a.0").1);
    }

    #[test]
    fn a_clash_is_ambiguous_exactly_when_the_position_is_inside_and_an_element_holds_the_field() {
        let cl: Vec<(i32, Bson)> = vec![
            (1, bson::bson!([{ "0": 9 }])),
            (2, bson::bson!([{ "b": 1 }, { "0": 2 }])),
            (3, bson::bson!([[5], { "0": 9 }])),
        ];
        // g:133, 134, 136, 137 (CL 1-3 under `a.0`, `a.0.0`, `a.0.b`).
        for (id, value) in &cl {
            for path in ["a.0", "a.0.0", "a.0.b"] {
                let d = a(value.clone());
                assert_eq!(
                    walk_document(&d, path, KEY).unwrap_err(),
                    Ambiguous16746,
                    "CL {id} {path}"
                );
                let w = walk_document(&d, path, KEY_UNKEYED).unwrap();
                assert!(w.ambiguous, "CL {id} {path} is filed unkeyed");
            }
        }
        // PE 2 `[{"1":5},{b:1}]` under `a.1` (j:151, 152).
        let pe2 = a(bson::bson!([{ "1": 5 }, { "b": 1 }]));
        assert_eq!(walk_document(&pe2, "a.1", KEY).unwrap_err(), Ambiguous16746);
        // CL 4-8 (g:135, 138), PE 1 and 3-6 (j:153-174): never.
        let free: Vec<Bson> = vec![
            bson::bson!([5, 6]),
            bson::bson!([{ "b": 1 }]),
            bson::bson!({ "0": 3 }),
            bson::bson!([[{ "0": 1 }]]),
        ];
        for value in &free {
            let d = a(value.clone());
            assert!(walk_document(&d, "a.0", KEY).is_ok(), "{value}");
        }
        assert!(walk_document(&doc! {}, "a.0", KEY).is_ok(), "missing");
        let pe1 = a(bson::bson!([{ "1": 5 }]));
        assert!(walk_document(&pe1, "a.1", KEY).is_ok(), "PE 1: the position is past the end");
        let pe5 = a(bson::bson!([{ "99999999999": 1 }]));
        assert!(walk_document(&pe5, "a.99999999999", KEY).is_ok(), "an over-long position");
        // `a.01` is a field name, and `a.b.0` has no document element holding "0".
        let cl1 = a(bson::bson!([{ "01": 9 }]));
        assert!(walk_document(&cl1, "a.01", KEY).is_ok());
        let nested = a(bson::bson!([{ "b": [5, 6] }]));
        assert!(walk_document(&nested, "a.b.0", KEY).is_ok());
        // Ignore reads the position and says nothing.
        let ignore = Rule { on_ambiguous: OnAmbiguous::Ignore, ..KEY };
        let cl1 = a(bson::bson!([{ "0": 9 }]));
        let w = walk_document(&cl1, "a.0", ignore).unwrap();
        assert!(!w.ambiguous);
        assert_eq!(w.values.len(), 1);
    }

    #[test]
    fn an_item_root_contributes_nothing_in_a_filter_and_null_in_a_key() {
        // The root of `arrayFilters` is a field (F3); an item is a root reached by traversal.
        let scalar = Bson::Int32(5);
        let w = walk(&scalar, Root::Item, "c", FILTER).unwrap();
        assert!(w.values.is_empty() && !w.lacked);
        let w = walk(&scalar, Root::Item, "c", KEY).unwrap();
        assert!(w.values.is_empty() && w.lacked);
        let w = walk(&scalar, Root::Field, "c", FILTER).unwrap();
        assert!(w.lacked, "a scalar that is a field value lacks the field (b:10, b:24)");
        let array = bson::bson!([{ "c": 1 }]);
        assert!(walk(&array, Root::Item, "c", FILTER).unwrap().values.is_empty());
        assert_eq!(walk(&array, Root::Field, "c", FILTER).unwrap().values.len(), 1);
    }

    #[test]
    fn a_written_position_rule_takes_zero_led_digits_and_the_read_rule_does_not() {
        let list = a(bson::bson!([7, 8]));
        let written = Rule { positions: Positions::Written, ..FILTER };
        let w = walk_document(&list, "a.01", written).unwrap();
        assert_eq!(w.values, vec![(&Bson::Int32(8), true)]);
        let w = walk_document(&list, "a.01", FILTER).unwrap();
        assert!(w.values.is_empty());
    }
}
