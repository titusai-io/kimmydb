//! Pipeline-style updates (ADR-265): an update written as an array of aggregation stages.
//!
//! `[{$set: {total: {$add: ["$a", "$b"]}}}, {$unset: "scratch"}]` runs the stages over
//! the document being updated and stores what comes out. Only the three stage families
//! that reshape one document are allowed (`$addFields`/`$set`, `$project`/`$unset`,
//! `$replaceRoot`/`$replaceWith`); they are the aggregation stages themselves, parsed and
//! run by [`crate::aggregate`], so an expression means here what it means there.

use bson::{Bson, Document};
use kimmy_core::{Error, Result};

use crate::aggregate::{self, Limits, Stage};

/// The stage names a pipeline update may use.
pub const ALLOWED_STAGES: [&str; 6] =
    ["$addFields", "$set", "$project", "$unset", "$replaceRoot", "$replaceWith"];

const ID_FIELD: &str = "_id";

/// A parsed pipeline update.
#[derive(Clone, Debug)]
pub struct UpdatePipeline {
    /// The stages as written, which is what two pipelines are equal by.
    source: Vec<Document>,
    stages: Vec<Stage>,
}

impl PartialEq for UpdatePipeline {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}

impl UpdatePipeline {
    /// Parse the stages of a pipeline update. Refused, `400`: an empty array, an element
    /// that is not a document, and any stage but the six names in [`ALLOWED_STAGES`]; a
    /// stage that is wrong inside is refused as it is in an aggregation. `$$NOW` is the
    /// request's one date (the parse joins the clock the request already opened).
    pub fn parse(stages: &[Bson]) -> Result<Self> {
        let _clock = crate::expr::RequestClock::open();
        if stages.is_empty() {
            return Err(Error::InvalidUpdate(
                "a pipeline update needs at least one stage; an empty array is not an update"
                    .into(),
            ));
        }
        let mut source = Vec::with_capacity(stages.len());
        for stage in stages {
            let Bson::Document(doc) = stage else {
                return Err(Error::InvalidUpdate(format!(
                    "each stage of a pipeline update is a document, found {}",
                    crate::expr::type_name(stage)
                )));
            };
            if let Some(name) = doc.keys().next().filter(|_| doc.len() == 1)
                && !ALLOWED_STAGES.contains(&name.as_str())
            {
                return Err(Error::InvalidUpdate(format!(
                    "a pipeline update allows only $addFields (or $set), $project (or $unset) \
                     and $replaceRoot (or $replaceWith), found {name}"
                )));
            }
            source.push(doc.clone());
        }
        // One parse for all the stages, so the pipeline's regular expressions share one
        // budget as they do in an aggregation. A stage with a number of keys other than
        // one is refused there.
        let parsed = aggregate::parse(&source)?;
        debug_assert!(parsed.iter().all(is_allowed));
        if !parsed.iter().all(is_allowed) {
            return Err(Error::Internal(
                "a pipeline update parsed to a stage it does not allow".into(),
            ));
        }
        Ok(Self { source, stages: parsed })
    }

    /// Run the stages over `doc` and put the result back in it.
    ///
    /// `_id` is the document's identity and does not move. A `$replaceRoot` or
    /// `$replaceWith` whose result has no `_id` keeps the document's own (first, where
    /// every write path puts it); a result whose `_id` differs from it, or a stage that
    /// takes it out (`$project: {_id: 0}`, `$unset: "_id"`), is refused as an operator
    /// update refuses a write to `_id`. A document with no `_id` yet (the seed of an
    /// upsert) may not gain one.
    pub fn apply(&self, doc: &mut Document) -> Result<()> {
        let original = doc.get(ID_FIELD).cloned();
        let limits = Limits::default();
        let mut current = std::mem::take(doc);
        for stage in &self.stages {
            let mut out = aggregate::apply(stage, vec![current], &limits)?;
            current = match (out.pop(), out.is_empty()) {
                (Some(next), true) => next,
                _ => {
                    return Err(Error::Internal(
                        "a pipeline update stage did not answer exactly one document".into(),
                    ));
                }
            };
            if matches!(stage, Stage::ReplaceRoot(_))
                && !current.contains_key(ID_FIELD)
                && let Some(id) = &original
            {
                current = with_id_first(id.clone(), current);
            }
        }
        if current.get(ID_FIELD) != original.as_ref() {
            return Err(Error::InvalidUpdate("_id is immutable and cannot be updated".into()));
        }
        // `_id` is stored first, where every write path puts it: a result that left it
        // elsewhere would be a different document, byte for byte, from the same fields
        // stored by any other write, and would count as a change every time (ADR-208).
        if let Some(id) = original
            && current.keys().next().map(String::as_str) != Some(ID_FIELD)
        {
            current.remove(ID_FIELD);
            current = with_id_first(id, current);
        }
        *doc = current;
        Ok(())
    }
}

fn is_allowed(stage: &Stage) -> bool {
    matches!(stage, Stage::AddFields(_) | Stage::Project { .. } | Stage::ReplaceRoot(_))
}

fn with_id_first(id: Bson, rest: Document) -> Document {
    let mut out = Document::new();
    out.insert(ID_FIELD, id);
    for (key, value) in rest {
        if key != ID_FIELD {
            out.insert(key, value);
        }
    }
    out
}
