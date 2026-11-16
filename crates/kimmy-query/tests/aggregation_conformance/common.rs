//! What the recorded cases are run with.

#![allow(dead_code)]

use bson::Document;
use kimmy_query::aggregate::{self, Limits};

/// The pipeline run over `input` stage by stage, or the refusal as its message.
pub fn run(pipeline: &[Document], input: Vec<Document>) -> Result<Vec<Document>, String> {
    let stages = aggregate::parse(pipeline).map_err(|e| e.to_string())?;
    let limits = Limits::default();
    let mut current = input;
    for stage in &stages {
        current = aggregate::apply(stage, current, &limits).map_err(|e| e.to_string())?;
    }
    Ok(current)
}

/// The message of the refusal of a pipeline at parse; a pipeline that parses fails the test.
pub fn refused(pipeline: &[Document]) -> String {
    match aggregate::parse(pipeline) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("{pipeline:?} must be refused at parse"),
    }
}

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
