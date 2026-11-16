//! What the recorded-table tests share.

#![allow(dead_code)]

use bson::{Bson, Document};
use kimmy_query::{aggregate, filter, shape};

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
                        return Bson::Document(out);
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
            let start = *at + 1;
            let end = start + b[start..].iter().position(|c| *c == b'"').expect("a closing quote");
            *at = end + 1;
            Bson::String(String::from_utf8(b[start..end].to_vec()).expect("utf-8"))
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
                    Bson::Int32(text.parse().unwrap_or_else(|_| panic!("a number: {text}")))
                }
            }
        }
    }
}

pub fn documents(value: &Bson) -> Vec<Document> {
    value.as_array().unwrap().iter().map(|d| d.as_document().unwrap().clone()).collect()
}

pub fn ids(docs: &[Document]) -> Vec<Bson> {
    docs.iter().map(|d| d.get("_id").cloned().unwrap()).collect()
}

/// The matches of `query` among `docs` and the sort of a command, as `find` and `findAndModify`
/// read them: a query that is exactly `{_id: <number>}` ([`shape::is_one_by_id`]) sorts nothing.
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

pub fn pipeline_ids(docs: &[Document], command: &Document) -> Result<Vec<Bson>, String> {
    let stages = documents(&Bson::Array(command.get_array("pipeline").unwrap().clone()));
    let mut current = docs.to_vec();
    for stage in aggregate::parse(&stages).map_err(|e| e.to_string())? {
        current = aggregate::apply(&stage, current, &aggregate::Limits::default())
            .map_err(|e| e.to_string())?;
    }
    Ok(ids(&current))
}
