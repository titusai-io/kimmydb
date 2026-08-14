//! Reading a collection, rather than reading its first hundred documents.

use serde_json::{Value, json};

use crate::{Client, Result};

/// What to ask a collection for.
///
/// Every field is optional and the defaults are the server's, with one
/// exception worth stating: **omitting `limit` means 100 documents, not all of
/// them.** That is the server's behaviour, and hiding it here would only move
/// the surprise.
#[derive(Clone, Debug, Default)]
pub struct Query {
    filter: Option<Value>,
    sort: Option<Value>,
    projection: Option<Value>,
    limit: Option<usize>,
    skip: Option<usize>,
    explain: bool,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn filter(mut self, filter: Value) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn sort(mut self, sort: Value) -> Self {
        self.sort = Some(sort);
        self
    }

    pub fn projection(mut self, projection: Value) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Documents per page. Clamped by the server at 10,000.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Offset. Costs work proportional to what it skips, and cannot be combined
    /// with paging — [`Client::pages`] refuses a query carrying one rather than
    /// letting the server refuse it on the second page.
    pub fn skip(mut self, skip: usize) -> Self {
        self.skip = Some(skip);
        self
    }

    /// Ask how the query was answered.
    pub fn explain(mut self, explain: bool) -> Self {
        self.explain = explain;
        self
    }

    pub(crate) fn to_body(&self) -> Value {
        let mut body = json!({ "explain": self.explain });
        for (key, value) in [
            ("filter", self.filter.clone()),
            ("sort", self.sort.clone()),
            ("projection", self.projection.clone()),
        ] {
            if let Some(value) = value {
                body[key] = value;
            }
        }
        if let Some(limit) = self.limit {
            body["limit"] = json!(limit);
        }
        if let Some(skip) = self.skip {
            body["skip"] = json!(skip);
        }
        body
    }

    /// Whether this query is one a cursor can continue.
    ///
    /// The server's rule, applied here so a walk fails on the first page with
    /// an explanation rather than on the second with a refusal.
    fn is_pageable(&self) -> Option<&'static str> {
        if self.skip.is_some_and(|s| s > 0) {
            return Some("`skip` and a cursor both say where to resume; use one");
        }
        match &self.sort {
            None => None,
            Some(sort) => {
                let id_ascending = sort.as_object().is_some_and(|o| {
                    o.len() == 1 && o.get("_id").and_then(Value::as_i64) == Some(1)
                });
                (!id_ascending)
                    .then_some("a cursor pages in _id order, so it takes no other `sort`")
            }
        }
    }
}

/// A walk through a collection, one page at a time.
///
/// ```no_run
/// # async fn example(client: kimmy_client::Client) -> kimmy_client::Result<()> {
/// let mut pages = client.pages("shop", "orders", kimmy_client::Query::new().limit(500));
/// while let Some(page) = pages.next().await? {
///     println!("{} documents", page.len());
/// }
/// # Ok(()) }
/// ```
///
/// **The walk ends on a short or empty page, not on a missing token.** A final
/// page that is exactly full still carries one — the server cannot know it is
/// the last without looking further — so a client that stopped when a token
/// stopped arriving would read one page too few. This handles that; a
/// hand-rolled loop is where it gets forgotten.
pub struct Pages {
    client: Client,
    db: String,
    collection: String,
    query: Query,
    cursor: Option<String>,
    /// Set once the server stops offering a continuation, so `next` returns
    /// `None` forever rather than restarting the walk.
    finished: bool,
}

impl Pages {
    pub(crate) fn new(client: Client, db: String, collection: String, query: Query) -> Self {
        Self { client, db, collection, query, cursor: None, finished: false }
    }

    /// The next page, or `None` at the end of the walk.
    pub async fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.finished {
            return Ok(None);
        }
        if let Some(why) = self.query.is_pageable() {
            self.finished = true;
            return Err(crate::Error::Stream(format!("this query cannot be paged: {why}")));
        }

        let mut body = self.query.to_body();
        if let Some(cursor) = &self.cursor {
            body["cursor"] = json!(cursor);
        }

        let response = self
            .client
            .request(
                crate::Method::Post,
                &format!("/v1/db/{}/coll/{}/find", self.db, self.collection),
                Some(body),
                crate::Safety::Idempotent,
            )
            .await?;

        let documents: Vec<Value> = response["documents"].as_array().cloned().unwrap_or_default();
        self.cursor = response["nextCursor"].as_str().map(str::to_string);
        if self.cursor.is_none() || documents.is_empty() {
            self.finished = true;
        }
        if documents.is_empty() {
            return Ok(None);
        }
        Ok(Some(documents))
    }

    /// Every remaining document, collected.
    ///
    /// Convenient and honest about what it is: this holds the whole result in
    /// memory. Use [`Pages::next`] for anything whose size you do not know.
    pub async fn collect_all(&mut self) -> Result<Vec<Value>> {
        let mut all = Vec::new();
        while let Some(page) = self.next().await? {
            all.extend(page);
        }
        Ok(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_query_sends_only_what_it_was_given() {
        let body = Query::new().filter(json!({ "a": 1 })).limit(10).to_body();
        assert_eq!(body["filter"], json!({ "a": 1 }));
        assert_eq!(body["limit"], 10);
        assert!(body.get("sort").is_none(), "an unset field is absent, not null");
        assert!(body.get("skip").is_none());
    }

    #[test]
    fn a_query_a_cursor_cannot_page_says_so_before_the_walk_starts() {
        // The server would refuse this too. Catching it here means the caller
        // hears about it on page one, with the reason, rather than as a 400
        // partway through a loop.
        assert!(Query::new().skip(5).is_pageable().is_some());
        assert!(Query::new().sort(json!({ "qty": 1 })).is_pageable().is_some());

        // `_id` ascending is the order a cursor already pages in.
        assert!(Query::new().sort(json!({ "_id": 1 })).is_pageable().is_none());
        assert!(Query::new().is_pageable().is_none());
        assert!(Query::new().skip(0).is_pageable().is_none(), "skip 0 is not a skip");
    }
}
