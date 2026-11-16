//! What the recorded-table tests share.

#![allow(dead_code)]

use bson::{Bson, Document};
use kimmy_query::{aggregate, filter, shape, update};

/// The value a JSON text holds: objects, arrays, strings without escapes, whole numbers, `null`
/// and booleans, which is all the recorded data uses (the `bson!` macro runs out of recursion on
/// a table that long).
pub fn json(text: &str) -> bson::Bson {
    let bytes = text.as_bytes();
    let mut at = 0;
    let value = parse_json(bytes, &mut at);
    skip_space(bytes, &mut at);
    assert_eq!(at, bytes.len(), "trailing text in {text}");
    value
}

fn skip_space(b: &[u8], at: &mut usize) {
    while b.get(*at).is_some_and(u8::is_ascii_whitespace) {
        *at += 1;
    }
}

fn parse_json(b: &[u8], at: &mut usize) -> bson::Bson {
    use bson::Bson;
    skip_space(b, at);
    match b[*at] {
        b'{' => {
            *at += 1;
            let mut out = Document::new();
            loop {
                skip_space(b, at);
                match b[*at] {
                    b'}' => {
                        *at += 1;
                        return extended(out);
                    }
                    b',' => *at += 1,
                    _ => {
                        let Bson::String(key) = parse_json(b, at) else { panic!("a key") };
                        skip_space(b, at);
                        assert_eq!(b[*at], b':');
                        *at += 1;
                        let value = parse_json(b, at);
                        out.insert(key, value);
                    }
                }
            }
        }
        b'[' => {
            *at += 1;
            let mut out = Vec::new();
            loop {
                skip_space(b, at);
                match b[*at] {
                    b']' => {
                        *at += 1;
                        return Bson::Array(out);
                    }
                    b',' => *at += 1,
                    _ => out.push(parse_json(b, at)),
                }
            }
        }
        b'"' => {
            let mut out: Vec<u8> = Vec::new();
            *at += 1;
            loop {
                match b[*at] {
                    b'"' => break,
                    b'\\' => {
                        *at += 1;
                        match b[*at] {
                            b'n' => out.push(b'\n'),
                            b't' => out.push(b'\t'),
                            b'r' => out.push(b'\r'),
                            b'u' => {
                                let hex = std::str::from_utf8(&b[*at + 1..*at + 5]).unwrap();
                                let ch = char::from_u32(u32::from_str_radix(hex, 16).unwrap())
                                    .expect("a scalar value");
                                let mut buf = [0u8; 4];
                                out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                                *at += 4;
                            }
                            other => out.push(other),
                        }
                    }
                    other => out.push(other),
                }
                *at += 1;
            }
            *at += 1;
            Bson::String(String::from_utf8(out).expect("utf-8"))
        }
        _ => {
            let start = *at;
            while b
                .get(*at)
                .is_some_and(|&c| !matches!(c, b',' | b']' | b'}') && !c.is_ascii_whitespace())
            {
                *at += 1;
            }
            match &b[start..*at] {
                b"null" => Bson::Null,
                b"true" => Bson::Boolean(true),
                b"false" => Bson::Boolean(false),
                number => {
                    let text = std::str::from_utf8(number).unwrap();
                    match text.parse::<i32>() {
                        Ok(n) => Bson::Int32(n),
                        Err(_) => Bson::Double(
                            text.parse().unwrap_or_else(|_| panic!("a number: {text}")),
                        ),
                    }
                }
            }
        }
    }
}

/// A document that is one canonical extended JSON value (`{"$numberLong": "5"}`, `$date`, `$oid`,
/// `$numberDouble`, `$numberDecimal`, `$numberInt`, `$binary`, `$minKey`, `$maxKey`,
/// `$regularExpression`) is that value; any other is a document.
fn extended(doc: Document) -> Bson {
    use bson::Bson;
    if doc.len() != 1 {
        return Bson::Document(doc);
    }
    let (key, value) = doc.iter().next().unwrap();
    match (key.as_str(), value) {
        ("$numberInt", Bson::String(t)) => Bson::Int32(t.parse().unwrap()),
        ("$numberLong", Bson::String(t)) => Bson::Int64(t.parse().unwrap()),
        ("$numberDouble", Bson::String(t)) => Bson::Double(match t.as_str() {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            other => other.parse().unwrap(),
        }),
        ("$numberDecimal", Bson::String(t)) => Bson::Decimal128(t.parse().unwrap()),
        ("$oid", Bson::String(t)) => Bson::ObjectId(bson::oid::ObjectId::parse_str(t).unwrap()),
        ("$date", Bson::Int64(ms)) => Bson::DateTime(bson::DateTime::from_millis(*ms)),
        ("$minKey", _) => Bson::MinKey,
        ("$maxKey", _) => Bson::MaxKey,
        ("$binary", Bson::Document(d)) => {
            let subtype = u8::from_str_radix(d.get_str("subType").unwrap(), 16).unwrap();
            Bson::Binary(
                bson::Binary::from_base64(d.get_str("base64").unwrap(), Some(subtype.into()))
                    .unwrap(),
            )
        }
        ("$regularExpression", Bson::Document(d)) => Bson::RegularExpression(bson::Regex {
            pattern: d.get_str("pattern").unwrap().to_string().try_into().unwrap(),
            options: d.get_str("options").unwrap().to_string().try_into().unwrap(),
        }),
        _ => Bson::Document(doc),
    }
}

pub fn documents(value: &Bson) -> Vec<Document> {
    value.as_array().unwrap().iter().map(|d| d.as_document().unwrap().clone()).collect()
}

pub fn ids(docs: &[Document]) -> Vec<Bson> {
    docs.iter().map(|d| d.get("_id").cloned().unwrap()).collect()
}

/// The matches of `query` among `docs` and the sort of a command, as `find` and `findAndModify`
/// read them: a query that is exactly one key `_id` with a literal value ([`shape::is_one_by_id`]) sorts nothing.
fn matched_and_sort<'a>(
    docs: &'a [Document],
    query: &Document,
    sort: &Document,
) -> Result<(Vec<&'a Document>, Vec<shape::SortKey>), String> {
    let parsed = filter::parse(query).map_err(|e| e.to_string())?;
    let mut keys = shape::parse_sort(sort).map_err(|e| e.to_string())?;
    if shape::is_one_by_id(query) {
        keys.clear();
    }
    let mut matched = Vec::new();
    for doc in docs {
        if filter::matches(&parsed, doc).map_err(|e| e.to_string())? {
            matched.push(doc);
        }
    }
    Ok((matched, keys))
}

/// A `find` with `filter`, `sort`, `skip` and `limit`: the `_id`s, or the refusal. The keys of
/// every match are taken with `shape::sort_keys`, as the executor's bounded sort takes them.
pub fn find_ids(docs: &[Document], command: &Document) -> Result<Vec<Bson>, String> {
    let empty = Document::new();
    let query = command.get_document("filter").unwrap_or(&empty);
    let (matched, sort) = matched_and_sort(docs, query, command.get_document("sort").unwrap())?;
    let mut keyed = Vec::new();
    for doc in matched {
        keyed.push((shape::sort_keys(&sort, doc).map_err(|e| e.to_string())?, doc));
    }
    keyed.sort_by(|a, b| shape::compare_keys(&sort, &a.0, &b.0));
    let count = |name: &str| {
        command.get(name).map_or(0, |n| match n {
            Bson::Int32(n) => *n as usize,
            Bson::Int64(n) => *n as usize,
            other => panic!("{name}: {other}"),
        })
    };
    let skip = count("skip");
    let limit = if command.contains_key("limit") { count("limit") } else { usize::MAX };
    Ok(keyed
        .into_iter()
        .skip(skip)
        .take(limit)
        .map(|(_, d)| d.get("_id").cloned().unwrap())
        .collect())
}

/// A `findAndModify`: whether it answered. The keys of every match are taken before one is
/// chosen; an upsert that matches nothing inserts and sorts nothing.
pub fn find_and_modify_answers(docs: &[Document], command: &Document) -> Result<(), String> {
    let query = command.get_document("query").unwrap();
    let (matched, sort) = matched_and_sort(docs, query, command.get_document("sort").unwrap())?;
    for doc in matched {
        shape::sort_keys(&sort, doc).map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// A `findAndModify` of the first match in the order of the command's sort, as the returned document:
/// the document after the update (`new: true`) or before it, `None` where nothing matched (the
/// reference answers `value: null`). The refusal of a sort over parallel arrays is checked on every
/// match first, as in [`find_and_modify_answers`].
pub fn find_and_modify_value(
    docs: &[Document],
    command: &Document,
) -> Result<Option<Document>, String> {
    const NOW: i64 = 1_700_000_000_000;
    let query = command.get_document("query").unwrap();
    let (matched, sort) = matched_and_sort(docs, query, command.get_document("sort").unwrap())?;
    for doc in &matched {
        shape::sort_keys(&sort, doc).map_err(|e| e.to_string())?;
    }
    let mut chosen: Vec<Document> = matched.into_iter().cloned().collect();
    shape::sort(&sort, &mut chosen);
    let Some(before) = chosen.into_iter().next() else { return Ok(None) };
    let mut after = before.clone();
    let parsed =
        update::parse(command.get_document("update").unwrap()).map_err(|e| e.to_string())?;
    update::apply(&parsed, &mut after, NOW).map_err(|e| e.to_string())?;
    Ok(Some(if command.get_bool("new").unwrap_or(false) { after } else { before }))
}

pub fn pipeline_ids(docs: &[Document], command: &Document) -> Result<Vec<Bson>, String> {
    let stages = documents(&Bson::Array(command.get_array("pipeline").unwrap().clone()));
    let mut current = docs.to_vec();
    for stage in aggregate::parse(&stages).map_err(|e| e.to_string())? {
        current = aggregate::apply(&stage, current, &aggregate::Limits::default())
            .map_err(|e| e.to_string())?;
    }
    Ok(ids(&current))
}
