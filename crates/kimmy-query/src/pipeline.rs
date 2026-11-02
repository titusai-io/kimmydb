//! The push pipeline (ADR-270): a pipeline's stages as a chain of sinks, each taking one
//! document and pushing zero or more on to the next, and answering whether it still wants
//! input.
//!
//! The executor used to run a pipeline as a loop of `docs = stage(docs)`, each stage taking
//! the whole input and building the whole output. A chain is the same pipeline with the
//! stages' hands joined: a source pushes documents into the head, and the tail collects
//! what comes out. A stage that holds nothing between documents is a step of a sink that
//! passes each document on, and a full `$limit` tells the source to stop reading ([`Flow`]).
//!
//! # Two kinds of stage
//!
//! A stage that [streams](streams) (`$match`, `$project`, `$addFields`, `$replaceRoot`,
//! `$redact`, `$unwind`, `$skip`, `$limit`) holds one document at a time. A run of them is
//! **one** sink, [`Streaming`], that takes each document through the run's steps in a loop:
//! the depth of a `push` is constant (a run pushes to a barrier that only stores, or to the
//! collector), whatever the number of stages,
//! and a pipeline of a hundred thousand `$skip` stages (the body limit allows about that
//! many) is no deeper than one. Each step is the stage's own per-document body in
//! [`crate::aggregate`], the one [`crate::aggregate::apply`] loops over, so there is one
//! copy of each stage and the differential test (`tests/pipeline_differential.rs`) compares
//! one implementation with itself run in two orders.
//!
//! Every other stage is a [`Barrier`]: it collects its input, and when the input is complete
//! ([`Sink::finish`]) runs the stage as it always ran, through the runner the caller
//! supplies, and pushes the output on. The runner is where the stage's code lives, so what a
//! stage charges to the request's budget, the order in which stages fail and the deadline
//! checks are those of the loop this replaced, not a copy of them.
//!
//! The chain is not recursive in its length: a sink does not finish the sink after it,
//! [`Chain::finish`] walks from the head to the tail, and a chain is taken apart the same
//! way.
//!
//! # What a sink holds, and what it is charged (ADR-269)
//!
//! A streaming sink holds one document, and charges nothing that lasts but what it holds
//! beside it: the fans of `$unwind` that keep their input (the document and the elements still
//! to come, one fan to a level, charged while the fan stands), and a `Vec` pushed whole to it,
//! which it charges on arrival as the input of its first stage and gives back as it takes each
//! document. A barrier, and the collector that ends the chain, charge **as the documents
//! arrive** one at a time: the
//! lease grows by a document's estimate when it is pushed, and a document that takes the
//! request over its limit is refused at once, so a streamed `$addFields` feeding a `$group`
//! is held to the limit as it goes and not when it ends. The barrier hands the lease to the
//! stage as the hold on its input, and the stage does not charge the same bytes again. A
//! `Vec` pushed whole ([`Sink::push_all`]) is not charged on arrival: it was built by a
//! stage that charged it as it built it, and the stage that takes it charges it as its
//! input, as the loop did (a barrier at its run, a streaming sink on arrival).
//!
//! A barrier, and the collector, also refuse when they would hold more than the pipeline's
//! `max_documents`, where the stages that stream have no such ceiling.
//!
//! # The request deadline (ADR-266)
//!
//! A sink that loops over documents ticks a `deadline::Ticker`, as every loop does (see
//! [`crate::aggregate`]): the default [`Sink::push_all`] does, and a streaming sink ticks in
//! its `push`. A barrier does not loop; the stage it runs checks and ticks for itself.

use std::cell::RefCell;
use std::rc::Rc;

use bson::Document;
use kimmy_core::Error;

use crate::aggregate::{self, Fan, Limits, Stage, UnwindSpec};
use crate::budget::{Lease, held_bytes, input_of};
use crate::deadline::Ticker;
use crate::expr::Binding;
use crate::filter;
use crate::stages;

/// What a sink answers to a document it was pushed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use = "a Done is the source's cue to stop reading"]
pub enum Flow {
    /// Send the next one.
    More,
    /// This sink needs nothing further (a `$limit` that is full), so nothing before it need
    /// produce anything more.
    Done,
}

/// A stage of a chain, or its end. `'a` is how long the stages and the runner it was built
/// from live, and `E` the error a stage fails with: the executor's own, where the stages
/// that read storage run.
pub trait Sink<'a, E: From<Error>> {
    /// One more document.
    fn push(&mut self, doc: Document) -> Result<Flow, E>;

    /// Many more, in order: the documents of a stage that has finished, or a source that is
    /// already in memory. A sink that holds what it is pushed takes the `Vec` without
    /// copying it, which is why this is not only a loop over [`Sink::push`] (and why a
    /// request that holds the source in one `Vec` does not hold it twice).
    ///
    /// Stops at the first [`Flow::Done`], leaving the rest unpushed.
    fn push_all(&mut self, docs: Vec<Document>) -> Result<Flow, E> {
        let mut ticker = Ticker::new();
        for doc in docs {
            ticker.tick()?;
            if self.push(doc)? == Flow::Done {
                return Ok(Flow::Done);
            }
        }
        Ok(Flow::More)
    }

    /// Whether this sink wants no document at all, before one is pushed (a `$limit` of 0): a
    /// source that asks need not read one.
    fn is_full(&self) -> bool {
        false
    }

    /// The input is complete: do what waits for the end of it, and push the result to the
    /// next sink. Does **not** finish the next sink, which [`Chain::finish`] does after
    /// this one, so that a long chain is not a deep call.
    fn finish(&mut self) -> Result<(), E>;

    /// The sink this one pushes to, `None` at the end of the chain.
    fn next_mut(&mut self) -> Option<&mut dyn Sink<'a, E>>;

    /// Hand over the sink this one pushes to, leaving none, so that [`Chain`] can drop a
    /// chain one sink at a time.
    fn detach(&mut self) -> Option<Link<'a, E>>;
}

/// A sink, boxed: the next stage of a chain.
pub type Link<'a, E> = Box<dyn Sink<'a, E> + 'a>;

/// How a barrier stage is run: the stage, its whole input, and the lease that charged that
/// input as the documents arrived one by one (`None` when it was handed over whole and is
/// not yet charged), to its whole output. The executor that owns a storage handle supplies
/// one that sends the stages that read storage to the functions that do, and the rest to
/// [`crate::aggregate::apply_charged`].
pub type Runner<'a, E> =
    &'a dyn Fn(&Stage, Vec<Document>, Option<Lease>) -> Result<Vec<Document>, E>;

/// Whether `stage` holds one document at a time, and so runs as a step of a [`Streaming`]
/// sink rather than as a [`Barrier`]. A leading `$match` is the source's filter and is not
/// given to the chain.
pub fn streams(stage: &Stage) -> bool {
    matches!(
        stage,
        Stage::Match(_)
            | Stage::Project { .. }
            | Stage::AddFields(_)
            | Stage::ReplaceRoot(_)
            | Stage::Redact(_)
            | Stage::Unwind { .. }
            | Stage::Skip(_)
            | Stage::Limit(_)
    )
}

/// How many documents of the source a pipeline whose stages are `stages` will take, when that
/// is known before it runs: the stages begin with a run of `$skip`s and a `$limit`, which take
/// that many whatever the documents are (a `$limit` of 0 included, which takes none). `None`
/// for any other beginning, which may need every document the source has.
///
/// Exact, never a guess: a source that stops there has delivered what the pipeline takes.
pub fn source_bound(stages: &[Stage]) -> Option<usize> {
    let mut skipped = 0usize;
    for stage in stages {
        match stage {
            Stage::Skip(n) => skipped = skipped.saturating_add(*n),
            Stage::Limit(n) => return Some(skipped.saturating_add(*n)),
            _ => return None,
        }
    }
    None
}

/// A pipeline made into sinks, ready for documents. Push the source into it, then
/// [`Chain::finish`] it for the output.
pub struct Chain<'a, E: From<Error>> {
    head: Link<'a, E>,
    out: Rc<RefCell<Collected<'a>>>,
}

/// Make `stages` into a chain: a sink for each run of stages that stream and one for each
/// of the others, in order, ending in a collector. `limits` is the request's budget and
/// ceilings, and `vars` what an enclosing `$lookup` `let` bound, which the stages that
/// stream evaluate their expressions with.
pub fn build<'a, E: From<Error> + 'a>(
    stages: &'a [Stage],
    run: Runner<'a, E>,
    limits: &'a Limits,
    vars: &'a [Binding<'a>],
) -> Chain<'a, E> {
    build_by(stages, run, limits, vars, streams)
}

/// [`build`] with every stage a [`Barrier`], as the chain was first built (ADR-270, P0): it
/// answers what the old loop of `docs = stage(docs)` answered, error for error. It is the
/// oracle the differential test holds [`build`] to; the executor does not use it.
pub fn build_barriers<'a, E: From<Error> + 'a>(
    stages: &'a [Stage],
    run: Runner<'a, E>,
    limits: &'a Limits,
    vars: &'a [Binding<'a>],
) -> Chain<'a, E> {
    build_by(stages, run, limits, vars, |_| false)
}

fn build_by<'a, E: From<Error> + 'a>(
    stages: &'a [Stage],
    run: Runner<'a, E>,
    limits: &'a Limits,
    vars: &'a [Binding<'a>],
    by: fn(&Stage) -> bool,
) -> Chain<'a, E> {
    let last = stages.last().map_or("the source", Stage::name);
    let out = Rc::new(RefCell::new(Collected::new("the output of the pipeline", last, limits)));
    let mut head: Link<'a, E> = Box::new(Collector { out: Rc::clone(&out) });
    let mut end = stages.len();
    while end > 0 {
        // The run of stages that stream that ends at `end`, if the stage there is one.
        let mut start = end;
        while start > 0 && by(&stages[start - 1]) {
            start -= 1;
        }
        if start < end {
            head = Box::new(Streaming::new(&stages[start..end], vars, limits, head));
            end = start;
        } else {
            let stage = &stages[end - 1];
            let before = end.checked_sub(2).map_or("the source", |at| stages[at].name());
            let held = Collected::new(input_of(stage.name()), before, limits);
            head = Box::new(Barrier { stage, run, held, next: Some(head) });
            end -= 1;
        }
    }
    Chain { head, out }
}

impl<'a, E: From<Error>> Chain<'a, E> {
    /// One more source document: the head's [`Sink::push`].
    pub fn push(&mut self, doc: Document) -> Result<Flow, E> {
        self.head.push(doc)
    }

    /// The whole source at once, handed to the head without a copy: the head's
    /// [`Sink::push_all`].
    pub fn push_all(&mut self, docs: Vec<Document>) -> Result<Flow, E> {
        self.head.push_all(docs)
    }

    /// Whether the chain wants no document at all: the head's [`Sink::is_full`].
    pub fn is_full(&self) -> bool {
        self.head.is_full()
    }

    /// The source is complete: finish every sink in order, and answer what came out of the
    /// last. The first sink to fail ends it, and the ones after it have run nothing.
    pub fn finish(mut self) -> Result<Vec<Document>, E> {
        let mut at: Option<&mut dyn Sink<'a, E>> = Some(&mut *self.head);
        while let Some(sink) = at {
            sink.finish()?;
            at = sink.next_mut();
        }
        let (docs, _lease) = self.out.borrow_mut().take();
        Ok(docs)
    }
}

impl<'a, E: From<Error>> Drop for Chain<'a, E> {
    fn drop(&mut self) {
        // Each sink owns the next, so dropping the head would drop the rest as deep as the
        // chain is long. Taking them apart from the front keeps every drop shallow.
        let mut rest = self.head.detach();
        while let Some(mut sink) = rest {
            rest = sink.detach();
        }
    }
}

/// Documents a sink holds until its input is complete, and the budget they are charged to.
///
/// A document pushed one at a time is charged as it arrives, to a lease made when the first
/// does, and refused at the one that takes the request over its limit (ADR-269); so is the
/// one that would be the first over `max_documents`, in the words a stage that produced
/// that many has always been refused in (`aggregate::check_limit`). A `Vec` pushed whole is taken without a
/// copy and not charged, while nothing has been pushed one at a time: it is charged by the
/// stage that takes it, as its input. Either way `lease` is `Some` only when every document
/// held is charged to it.
struct Collected<'a> {
    /// How the holder is named in a refusal for its bytes, and the lease with it.
    what: &'static str,
    /// What produced the documents, named in a refusal for their number.
    producer: &'static str,
    limits: &'a Limits,
    docs: Vec<Document>,
    lease: Option<Lease>,
}

impl<'a> Collected<'a> {
    fn new(what: &'static str, producer: &'static str, limits: &'a Limits) -> Self {
        Collected { what, producer, limits, docs: Vec::new(), lease: None }
    }

    /// One more document, charged as it arrives.
    fn push(&mut self, doc: Document) -> Result<(), Error> {
        if self.lease.is_none() {
            // What came whole before is charged now, so the lease holds all of it.
            let mut lease = self.limits.buffer.lease(self.what);
            lease.charge_docs(&self.docs)?;
            self.lease = Some(lease);
        }
        let lease = self.lease.as_mut().expect("the lease was made above");
        lease.charge_doc(&doc)?;
        aggregate::check_limit(self.producer, self.docs.len() + 1, self.limits)?;
        self.docs.push(doc);
        Ok(())
    }

    /// Many more, in order.
    fn push_all(&mut self, docs: Vec<Document>) -> Result<(), Error> {
        if self.lease.is_none() {
            take_or_extend(&mut self.docs, docs);
            return Ok(());
        }
        docs.into_iter().try_for_each(|doc| self.push(doc))
    }

    /// What is held, and the lease that charged it, if it was charged on arrival.
    fn take(&mut self) -> (Vec<Document>, Option<Lease>) {
        (std::mem::take(&mut self.docs), self.lease.take())
    }
}

/// A run of stages that stream, as one sink. A document goes through the steps in a loop; an
/// `$unwind` stands a [`Fan`] on `fans` and the loop takes its documents on from the step
/// after it, one at a time, so that a `Done` ends the fan-out in the middle of an array and
/// the stack does not grow with the number of stages.
struct Streaming<'a, E: From<Error>> {
    steps: Vec<Step<'a>>,
    vars: &'a [Binding<'a>],
    limits: &'a Limits,
    ticker: Ticker,
    /// The `$unwind`s with documents still to hand out, by the index of the step and with the
    /// bytes each holds, which `held` is charged; the last is the innermost, and runs first.
    fans: Vec<(usize, Fan, usize)>,
    /// What the fans on `fans` hold: the document an array is unwound from and the elements
    /// still to come, one fan to a level of `$unwind`, so that a run of them over a large
    /// document is held to the request's budget (ADR-269) as it descends.
    held: Lease,
    /// The index of the last step that is a `$limit` with none left to take: everything
    /// before it is no longer needed, and a fan made by a step before it is dropped.
    full: Option<usize>,
    /// `None` only once [`Sink::detach`] has taken it.
    next: Option<Link<'a, E>>,
}

/// A stage of a [`Streaming`] run, and the count it keeps: what a `$skip` still drops or a
/// `$limit` still takes.
struct Step<'a> {
    stage: &'a Stage,
    left: usize,
}

/// Where a document went in a run of steps.
enum Walk {
    /// Through every step, to be pushed to the next sink.
    Out(Document),
    /// Dropped by a step.
    Dropped,
    /// Into an `$unwind`, whose documents are on `fans`.
    Fanned,
}

impl<'a, E: From<Error>> Streaming<'a, E> {
    fn new(
        stages: &'a [Stage],
        vars: &'a [Binding<'a>],
        limits: &'a Limits,
        next: Link<'a, E>,
    ) -> Self {
        let steps: Vec<Step<'a>> = stages
            .iter()
            .map(|stage| Step {
                stage,
                left: match stage {
                    Stage::Skip(n) | Stage::Limit(n) => *n,
                    _ => 0,
                },
            })
            .collect();
        let full = steps.iter().rposition(|s| matches!(s.stage, Stage::Limit(_)) && s.left == 0);
        Streaming {
            steps,
            vars,
            limits,
            ticker: Ticker::new(),
            fans: Vec::new(),
            held: limits.buffer.lease(input_of("$unwind")),
            full,
            next: Some(next),
        }
    }

    fn next_sink(&mut self) -> &mut Link<'a, E> {
        self.next.as_mut().expect("a sink is attached until its chain is dropped")
    }

    /// Take `doc` through the steps from `from`: out of the end, or dropped, or into a fan.
    fn walk(&mut self, mut doc: Document, from: usize) -> Result<Walk, Error> {
        let vars = self.vars;
        for at in from..self.steps.len() {
            let step = &mut self.steps[at];
            match step.stage {
                Stage::Match(f) => {
                    if !filter::matches_with(f, &doc, vars)? {
                        return Ok(Walk::Dropped);
                    }
                }
                Stage::Project { projection, computed } => {
                    doc = aggregate::project_doc(projection.as_ref(), computed, &doc, vars)?;
                }
                Stage::AddFields(computed) => {
                    doc = aggregate::add_fields_doc(computed, &doc, vars)?;
                }
                Stage::ReplaceRoot(expr) => {
                    doc = aggregate::replace_root_doc(expr, &doc, vars)?;
                }
                Stage::Redact(expr) => match stages::redact_doc(&doc, expr, vars)? {
                    Some(kept) => doc = kept,
                    None => return Ok(Walk::Dropped),
                },
                Stage::Skip(_) => {
                    if step.left > 0 {
                        step.left -= 1;
                        return Ok(Walk::Dropped);
                    }
                }
                Stage::Limit(_) => {
                    if step.left == 0 {
                        return Ok(Walk::Dropped);
                    }
                    step.left -= 1;
                    if step.left == 0 {
                        self.full = Some(self.full.map_or(at, |full| full.max(at)));
                    }
                }
                Stage::Unwind { .. } => {
                    let spec = unwind_spec(step.stage, self.limits);
                    let fan = Fan::start(&spec, doc)?;
                    let bytes = fan.held_bytes();
                    self.held.charge(bytes)?;
                    self.fans.push((at, fan, bytes));
                    return Ok(Walk::Fanned);
                }
                // `streams` is what puts a stage in a run.
                _ => unreachable!("{} does not stream", step.stage.name()),
            }
        }
        Ok(Walk::Out(doc))
    }

    /// The next document of the innermost fan that is still needed, and the step to take it
    /// on from.
    fn resume(&mut self) -> Result<Option<(Document, usize)>, Error> {
        while let Some((at, fan, bytes)) = self.fans.last_mut() {
            let (at, bytes) = (*at, *bytes);
            if self.full.is_some_and(|full| at < full) {
                self.held.release(bytes);
                self.fans.pop();
                continue;
            }
            let spec = unwind_spec(self.steps[at].stage, self.limits);
            match fan.next(&spec, &mut self.ticker)? {
                Some((doc, _built)) => return Ok(Some((doc, at + 1))),
                None => {
                    self.held.release(bytes);
                    self.fans.pop();
                }
            }
        }
        Ok(None)
    }
}

/// What the `$unwind` at `stage` does.
fn unwind_spec<'s>(stage: &'s Stage, limits: &Limits) -> UnwindSpec<'s> {
    match stage {
        Stage::Unwind { path, preserve_null_and_empty, include_array_index } => UnwindSpec {
            field: path,
            preserve: *preserve_null_and_empty,
            include_index: include_array_index.as_deref(),
            max_document_bytes: limits.max_document_bytes,
        },
        _ => unreachable!("{} is not an $unwind", stage.name()),
    }
}

impl<'a, E: From<Error>> Sink<'a, E> for Streaming<'a, E> {
    fn push(&mut self, doc: Document) -> Result<Flow, E> {
        if self.full.is_some() {
            return Ok(Flow::Done);
        }
        self.ticker.tick()?;
        let mut item = Some((doc, 0));
        loop {
            let (doc, from) = match item.take() {
                Some(item) => item,
                None => match self.resume()? {
                    Some(item) => item,
                    None => break,
                },
            };
            if let Walk::Out(doc) = self.walk(doc, from)? {
                // The fans end with the run: nothing is wanted past a `Done`.
                if self.next_sink().push(doc)? == Flow::Done {
                    self.fans.clear();
                    self.held.release(self.held.bytes());
                    return Ok(Flow::Done);
                }
            }
        }
        Ok(if self.full.is_some() { Flow::Done } else { Flow::More })
    }

    fn push_all(&mut self, docs: Vec<Document>) -> Result<Flow, E> {
        // The documents are in memory, whole, and nothing has charged them: the stage that
        // built them has given its hold back, and the caller of a sub-pipeline leaves them to
        // the first stage. They are charged as the input of the first stage of the run, as
        // that stage charged them when it held its input, and each is given back as it is
        // taken, so that what is left to drain is held to the request's budget (ADR-269).
        let first = self.steps.first().map_or("a stage", |step| step.stage.name());
        let mut arrived = self.limits.buffer.lease(input_of(first));
        arrived.charge_docs(&docs)?;
        // `push` ticks; a tick of its own would count each document twice.
        for doc in docs {
            arrived.release(held_bytes(&doc));
            if self.push(doc)? == Flow::Done {
                return Ok(Flow::Done);
            }
        }
        Ok(Flow::More)
    }

    fn is_full(&self) -> bool {
        self.full.is_some()
    }

    fn finish(&mut self) -> Result<(), E> {
        Ok(())
    }

    fn next_mut(&mut self) -> Option<&mut dyn Sink<'a, E>> {
        match &mut self.next {
            Some(next) => Some(&mut **next),
            None => None,
        }
    }

    fn detach(&mut self) -> Option<Link<'a, E>> {
        self.next.take()
    }
}

/// A stage that cannot answer before it has seen everything it is given: it collects its
/// input, and at [`Sink::finish`] runs the stage on all of it.
struct Barrier<'a, E: From<Error>> {
    stage: &'a Stage,
    run: Runner<'a, E>,
    held: Collected<'a>,
    /// `None` only once [`Sink::detach`] has taken it.
    next: Option<Link<'a, E>>,
}

impl<'a, E: From<Error>> Sink<'a, E> for Barrier<'a, E> {
    fn push(&mut self, doc: Document) -> Result<Flow, E> {
        self.held.push(doc)?;
        Ok(Flow::More)
    }

    fn push_all(&mut self, docs: Vec<Document>) -> Result<Flow, E> {
        self.held.push_all(docs)?;
        Ok(Flow::More)
    }

    fn finish(&mut self) -> Result<(), E> {
        let (input, arrived) = self.held.take();
        let output = (self.run)(self.stage, input, arrived)?;
        // A barrier's output is built whatever the next sink wants, so its `Done` ends
        // nothing here.
        let _ = self
            .next
            .as_mut()
            .expect("a sink is attached until its chain is dropped")
            .push_all(output)?;
        Ok(())
    }

    fn next_mut(&mut self) -> Option<&mut dyn Sink<'a, E>> {
        match &mut self.next {
            Some(next) => Some(&mut **next),
            None => None,
        }
    }

    fn detach(&mut self) -> Option<Link<'a, E>> {
        self.next.take()
    }
}

/// The end of a chain: what is pushed to it is the pipeline's output.
struct Collector<'a> {
    out: Rc<RefCell<Collected<'a>>>,
}

impl<'a, E: From<Error>> Sink<'a, E> for Collector<'a> {
    fn push(&mut self, doc: Document) -> Result<Flow, E> {
        self.out.borrow_mut().push(doc)?;
        Ok(Flow::More)
    }

    fn push_all(&mut self, docs: Vec<Document>) -> Result<Flow, E> {
        self.out.borrow_mut().push_all(docs)?;
        Ok(Flow::More)
    }

    fn finish(&mut self) -> Result<(), E> {
        Ok(())
    }

    fn next_mut(&mut self) -> Option<&mut dyn Sink<'a, E>> {
        None
    }

    fn detach(&mut self) -> Option<Link<'a, E>> {
        None
    }
}

/// Append `docs` to `held`; to an empty `held` that is `docs` itself, not a copy of it.
fn take_or_extend(held: &mut Vec<Document>, docs: Vec<Document>) {
    if held.is_empty() {
        *held = docs;
    } else {
        held.extend(docs);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use bson::{Bson, doc};

    use super::*;
    use crate::budget::{BufferBudget, held_docs_bytes};
    use crate::deadline::Deadline;

    type Calls = RefCell<Vec<(&'static str, Vec<i32>)>>;

    fn docs(ids: &[i32]) -> Vec<Document> {
        ids.iter().map(|i| doc! {"_id": *i}).collect()
    }

    fn ids(docs: &[Document]) -> Vec<i32> {
        docs.iter().map(|d| d.get_i32("_id").unwrap()).collect()
    }

    /// A runner that records which stage ran on which input, and passes the input through
    /// but for `$sample`, which keeps the first `n`. It stands in for stages that block.
    fn recording(
        calls: &Calls,
    ) -> impl Fn(&Stage, Vec<Document>, Option<Lease>) -> Result<Vec<Document>, Error> {
        move |stage, input, _| {
            calls.borrow_mut().push((stage.name(), ids(&input)));
            Ok(match stage {
                Stage::Sample(n) => input.into_iter().take(*n).collect(),
                _ => input,
            })
        }
    }

    /// Three stages that block.
    fn pipeline() -> Vec<Stage> {
        vec![Stage::Sample(3), Stage::Sample(2), Stage::Count("n".into())]
    }

    fn parsed(specs: &[Document]) -> Vec<Stage> {
        crate::aggregate::parse(specs).unwrap()
    }

    /// A runner for a chain that has no stage that blocks.
    fn unused(_: &Stage, _: Vec<Document>, _: Option<Lease>) -> Result<Vec<Document>, Error> {
        panic!("there is no stage that blocks")
    }

    #[test]
    fn no_stages_answer_the_source() {
        let limits = Limits::default();
        let mut chain = build(&[], &unused, &limits, &[]);
        assert_eq!(chain.push_all(docs(&[3, 1, 2])).unwrap(), Flow::More);
        assert_eq!(ids(&chain.finish().unwrap()), [3, 1, 2]);
    }

    #[test]
    fn each_stage_runs_once_on_everything_the_one_before_answered() {
        let calls = Calls::default();
        let run = recording(&calls);
        let stages = pipeline();
        let limits = Limits::default();
        let mut chain = build(&stages, &run, &limits, &[]);
        assert_eq!(chain.push_all(docs(&[1, 2, 3, 4])).unwrap(), Flow::More);
        let out = chain.finish().unwrap();
        assert_eq!(ids(&out), [1, 2]);
        assert_eq!(
            *calls.borrow(),
            [("$sample", vec![1, 2, 3, 4]), ("$sample", vec![1, 2, 3]), ("$count", vec![1, 2])]
        );
    }

    #[test]
    fn nothing_runs_until_the_chain_is_finished() {
        let calls = Calls::default();
        let run = recording(&calls);
        let stages = pipeline();
        let limits = Limits::default();
        let mut chain = build(&stages, &run, &limits, &[]);
        assert_eq!(chain.push(doc! {"_id": 1}).unwrap(), Flow::More);
        assert_eq!(chain.push(doc! {"_id": 2}).unwrap(), Flow::More);
        assert!(calls.borrow().is_empty());
        drop(chain);
        // Dropped unfinished, it ran nothing either.
        assert!(calls.borrow().is_empty());
    }

    #[test]
    fn documents_pushed_one_by_one_and_all_at_once_arrive_in_order() {
        let calls = Calls::default();
        let run = recording(&calls);
        let stages = [Stage::Sample(10)];
        let limits = Limits::default();
        let mut chain = build(&stages, &run, &limits, &[]);
        let _ = chain.push(doc! {"_id": 5}).unwrap();
        let _ = chain.push_all(docs(&[4, 3])).unwrap();
        let _ = chain.push(doc! {"_id": 2}).unwrap();
        let _ = chain.push_all(docs(&[1])).unwrap();
        assert_eq!(ids(&chain.finish().unwrap()), [5, 4, 3, 2, 1]);
        assert_eq!(calls.borrow().len(), 1, "one run, on all five");
    }

    #[test]
    fn the_source_vec_is_handed_on_without_a_copy() {
        let source = docs(&[1, 2, 3]);
        let at = source.as_ptr();
        // The runner hands its input back as it got it, and the second stage likewise.
        let run = |_: &Stage, input: Vec<Document>, arrived: Option<Lease>| {
            assert_eq!(input.as_ptr(), at, "the stage was given a copy");
            assert!(arrived.is_none(), "a Vec pushed whole was charged on arrival");
            Ok::<_, Error>(input)
        };
        let stages = [Stage::Sample(5), Stage::Sample(10)];
        let limits = Limits::default();
        let mut chain = build(&stages, &run, &limits, &[]);
        let _ = chain.push_all(source).unwrap();
        let out = chain.finish().unwrap();
        assert_eq!(out.as_ptr(), at, "the collector was given a copy");
    }

    #[test]
    fn the_first_failure_ends_the_run_and_later_stages_run_nothing() {
        let calls = Calls::default();
        let run = |stage: &Stage, input: Vec<Document>, _: Option<Lease>| {
            calls.borrow_mut().push((stage.name(), ids(&input)));
            match stage {
                Stage::Sample(2) => Err(Error::InvalidQuery("the sample failed".into())),
                _ => Ok(input),
            }
        };
        let stages = pipeline();
        let limits = Limits::default();
        let mut chain = build(&stages, &run, &limits, &[]);
        let _ = chain.push_all(docs(&[1, 2])).unwrap();
        let err = chain.finish().unwrap_err();
        assert_eq!(err.to_string(), "invalid query: the sample failed");
        let names: Vec<_> = calls.borrow().iter().map(|(name, _)| *name).collect();
        assert_eq!(names, ["$sample", "$sample"]);
    }

    #[test]
    fn a_runner_error_of_another_type_comes_through_as_that_type() {
        #[derive(Debug, PartialEq)]
        enum Mine {
            Core(String),
            Stage(&'static str),
        }
        impl From<Error> for Mine {
            fn from(e: Error) -> Self {
                Mine::Core(e.to_string())
            }
        }
        let run = |_: &Stage, _: Vec<Document>, _: Option<Lease>| -> Result<Vec<Document>, Mine> {
            Err(Mine::Stage("mine"))
        };
        let stages = [Stage::Sample(1)];
        let limits = Limits::default();
        let mut chain = build(&stages, &run, &limits, &[]);
        let _ = chain.push_all(docs(&[1])).unwrap();
        assert_eq!(chain.finish().unwrap_err(), Mine::Stage("mine"));
        // And a failure of the pipeline's own, converted.
        let stages = [Stage::Limit(1)];
        let run = |_: &Stage, _: Vec<Document>, _: Option<Lease>| -> Result<Vec<Document>, Mine> {
            panic!("a $limit streams")
        };
        let mut chain = build(&stages, &run, &limits, &[]);
        let guard = Deadline::scope(Some(std::time::Duration::ZERO));
        let err = chain.push(doc! {"_id": 1}).unwrap_err();
        drop(guard);
        assert_eq!(err, Mine::Core(Error::DeadlineExceeded.to_string()));
    }

    #[test]
    fn a_pipeline_of_many_stages_that_block_is_neither_a_deep_call_nor_a_deep_drop() {
        let run = |_: &Stage, input: Vec<Document>, _: Option<Lease>| Ok::<_, Error>(input);
        let stages = vec![Stage::Sample(10); 300_000];
        let limits = Limits::default();
        let mut chain = build(&stages, &run, &limits, &[]);
        let _ = chain.push_all(docs(&[1, 2])).unwrap();
        assert_eq!(ids(&chain.finish().unwrap()), [1, 2]);
        // And dropped unfinished.
        let chain = build(&stages, &run, &limits, &[]);
        drop(chain);
    }

    #[test]
    fn stages_that_stream_and_stages_that_block_in_turn_are_neither_a_deep_call_nor_a_deep_drop() {
        // A sink of each kind a hundred thousand times over: the drop of the chain takes the
        // sinks apart one by one whichever kind it meets, and a document goes through the lot.
        let run = |_: &Stage, input: Vec<Document>, _: Option<Lease>| Ok::<_, Error>(input);
        let stages: Vec<Stage> = (0..200_000)
            .map(|at| if at % 2 == 0 { Stage::Skip(0) } else { Stage::Sample(10) })
            .collect();
        let limits = Limits::default();
        let mut chain = build(&stages, &run, &limits, &[]);
        let _ = chain.push_all(docs(&[1, 2])).unwrap();
        assert_eq!(ids(&chain.finish().unwrap()), [1, 2]);
        drop(build(&stages, &run, &limits, &[]));
    }

    #[test]
    fn a_hundred_thousand_stages_that_stream_are_neither_a_deep_call_nor_a_deep_drop() {
        // The stages are built directly, so that no parse limit stands between a hundred
        // thousand of them and the chain: the stack must not depend on that limit.
        let mut stages = Vec::new();
        for at in 0..100_000 {
            stages.push(match at % 5 {
                0 => Stage::Skip(0),
                1 => Stage::Limit(usize::MAX),
                2 => Stage::AddFields(Vec::new()),
                3 => Stage::Unwind {
                    path: "a".into(),
                    preserve_null_and_empty: false,
                    include_array_index: None,
                },
                _ => parsed(&[doc! {"$match": {}}]).remove(0),
            });
        }
        let limits = Limits::default();
        let mut chain = build(&stages, &unused, &limits, &[]);
        for id in [1, 2, 3] {
            assert_eq!(chain.push(doc! {"_id": id, "a": [1, 2]}).unwrap(), Flow::More);
        }
        let out = chain.finish().unwrap();
        assert_eq!(out.len(), 6);
        assert_eq!(ids(&out), [1, 1, 2, 2, 3, 3]);
        // And dropped unfinished.
        drop(build(&stages, &unused, &limits, &[]));
    }

    /// A sink that takes `room` documents and then says `Done`, and keeps the `_id` of each in
    /// `got`, which the test holds a handle on as well.
    struct Fills {
        room: usize,
        got: Rc<RefCell<Vec<i32>>>,
    }

    impl Fills {
        fn with_room(room: usize) -> Self {
            Fills { room, got: Rc::default() }
        }
    }

    impl<'a> Sink<'a, Error> for Fills {
        fn push(&mut self, doc: Document) -> Result<Flow, Error> {
            self.got.borrow_mut().push(doc.get_i32("_id").unwrap());
            self.room -= 1;
            Ok(if self.room == 0 { Flow::Done } else { Flow::More })
        }
        fn finish(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn next_mut(&mut self) -> Option<&mut dyn Sink<'a, Error>> {
            None
        }
        fn detach(&mut self) -> Option<Link<'a, Error>> {
            None
        }
    }

    #[test]
    fn push_all_stops_at_the_first_done_and_pushes_nothing_after_it() {
        let mut sink = Fills::with_room(2);
        assert_eq!(sink.push_all(docs(&[7, 8, 9, 10])).unwrap(), Flow::Done);
        assert_eq!(*sink.got.borrow(), [7, 8]);
        // Never Done: More, with everything pushed.
        let mut roomy = Fills::with_room(10);
        assert_eq!(roomy.push_all(docs(&[7, 8, 9])).unwrap(), Flow::More);
        assert_eq!(*roomy.got.borrow(), [7, 8, 9]);
        // Nothing to push is More, too.
        assert_eq!(roomy.push_all(Vec::new()).unwrap(), Flow::More);
    }

    #[test]
    fn push_all_ticks_the_deadline() {
        use std::time::Duration;

        let mut sink = Fills::with_room(usize::MAX);
        let _guard = Deadline::scope(Some(Duration::ZERO));
        let err = sink.push_all(docs(&[1, 2, 3])).unwrap_err();
        assert!(matches!(err, Error::DeadlineExceeded), "{err:?}");
        assert!(sink.got.borrow().is_empty(), "a document was pushed after the deadline");
    }

    /// `stage` as a run of its own that pushes to a [`Fills`] of `room`, and what the sink got.
    fn run_of<'a>(
        stage: &'a Stage,
        limits: &'a Limits,
        room: usize,
    ) -> (Streaming<'a, Error>, Rc<RefCell<Vec<i32>>>) {
        let fills = Fills::with_room(room);
        let got = Rc::clone(&fills.got);
        (Streaming::new(std::slice::from_ref(stage), &[], limits, Box::new(fills)), got)
    }

    /// Each stage that streams, as the stage it is, and a document it passes on.
    fn passing_stages() -> Vec<(Stage, Document)> {
        let one = doc! {"_id": 1, "a": 5, "sub": {"_id": 1}};
        let stages = parsed(&[
            doc! {"$match": {"a": 5}},
            doc! {"$project": {"a": 1}},
            doc! {"$addFields": {"b": 1}},
            doc! {"$replaceRoot": {"newRoot": "$sub"}},
            doc! {"$redact": "$$KEEP"},
            doc! {"$skip": 0},
            doc! {"$limit": 9},
        ]);
        stages.into_iter().map(|stage| (stage, one.clone())).collect()
    }

    #[test]
    fn a_stage_that_streams_says_done_exactly_when_its_next_sink_does() {
        let limits = Limits::default();
        for (stage, doc) in passing_stages() {
            let name = stage.name();
            // Room for one: the first document it passes on fills the next sink.
            let (mut run, got) = run_of(&stage, &limits, 1);
            assert!(streams(&stage), "{name}");
            assert_eq!(run.push(doc.clone()).unwrap(), Flow::Done, "{name} hid a Done");
            assert_eq!(got.borrow().len(), 1, "{name} did not pass the document on");
            // Room for two: the first document is not the last.
            let (mut run, _) = run_of(&stage, &limits, 2);
            assert_eq!(run.push(doc.clone()).unwrap(), Flow::More, "{name} made up a Done");
            assert_eq!(run.push(doc.clone()).unwrap(), Flow::Done, "{name} hid a Done");
        }
    }

    #[test]
    fn a_document_a_stage_drops_is_not_the_next_sinks_to_fill() {
        let limits = Limits::default();
        let dropped = [
            (doc! {"$match": {"a": 6}}, doc! {"_id": 1, "a": 5}),
            (doc! {"$skip": 1}, doc! {"_id": 1, "a": 5}),
            (doc! {"$redact": "$$PRUNE"}, doc! {"_id": 1, "a": 5}),
            (doc! {"$unwind": "$a"}, doc! {"_id": 1}),
        ];
        for (spec, doc) in dropped {
            let stage = parsed(&[spec]).remove(0);
            let (mut run, got) = run_of(&stage, &limits, 1);
            assert_eq!(run.push(doc).unwrap(), Flow::More, "{}", stage.name());
            // And it said so without pushing anything: the next sink has all its room.
            assert!(got.borrow().is_empty(), "{}", stage.name());
            assert!(!Sink::<Error>::is_full(&run));
        }
    }

    #[test]
    fn a_limit_is_done_after_its_last_document_and_pushes_no_more() {
        let limits = Limits::default();
        let stage = Stage::Limit(2);
        let (mut run, got) = run_of(&stage, &limits, 10);
        assert!(!Sink::<Error>::is_full(&run));
        assert_eq!(run.push(doc! {"_id": 1}).unwrap(), Flow::More);
        assert!(!Sink::<Error>::is_full(&run));
        assert_eq!(run.push(doc! {"_id": 2}).unwrap(), Flow::Done);
        assert!(Sink::<Error>::is_full(&run));
        // A document sent after the Done is dropped, and the answer is Done again.
        assert_eq!(run.push(doc! {"_id": 3}).unwrap(), Flow::Done);
        assert_eq!(*got.borrow(), [1, 2]);
    }

    #[test]
    fn a_limit_of_nothing_wants_no_document() {
        let limits = Limits::default();
        let stage = Stage::Limit(0);
        let (mut run, got) = run_of(&stage, &limits, 10);
        // Full before any is pushed, so a source that asks reads none.
        assert!(Sink::<Error>::is_full(&run));
        assert_eq!(run.push(doc! {"_id": 1}).unwrap(), Flow::Done);
        assert!(got.borrow().is_empty());
        // Behind a stage that streams, and at the head of a chain.
        let stages = [Stage::Skip(1), Stage::Limit(0), Stage::Count("n".into())];
        let calls = Calls::default();
        let counting = recording(&calls);
        let mut chain = build(&stages, &counting, &limits, &[]);
        assert!(chain.is_full());
        assert_eq!(chain.push(doc! {"_id": 1}).unwrap(), Flow::Done);
        assert_eq!(ids(&chain.finish().unwrap()), Vec::<i32>::new());
        assert_eq!(*calls.borrow(), [("$count", Vec::<i32>::new())]);
    }

    #[test]
    fn a_chain_wants_documents_unless_a_limit_in_its_first_run_is_full() {
        let limits = Limits::default();
        let calls = Calls::default();
        let run = recording(&calls);
        for (stages, full) in [
            (vec![], false),
            (vec![Stage::Limit(1)], false),
            (vec![Stage::Limit(0)], true),
            (vec![Stage::Skip(0), Stage::Limit(0)], true),
            // Behind a stage that blocks, a limit does not stop the source.
            (vec![Stage::Sample(1), Stage::Limit(0)], false),
            (vec![Stage::Skip(0), Stage::Sample(1), Stage::Limit(0)], false),
        ] {
            let chain = build(&stages, &run, &limits, &[]);
            assert_eq!(chain.is_full(), full, "{stages:?}");
        }
    }

    #[test]
    fn a_chain_says_done_through_a_run_of_stages_and_never_through_a_barrier() {
        let limits = Limits::default();
        let calls = Calls::default();
        let run = recording(&calls);
        // A run of stages that stream ends with the limit's last document.
        let stages = [Stage::Skip(1), Stage::Limit(2), Stage::Sample(9)];
        let mut chain = build(&stages, &run, &limits, &[]);
        assert_eq!(chain.push(doc! {"_id": 1}).unwrap(), Flow::More);
        assert_eq!(chain.push(doc! {"_id": 2}).unwrap(), Flow::More);
        assert_eq!(chain.push(doc! {"_id": 3}).unwrap(), Flow::Done);
        assert_eq!(ids(&chain.finish().unwrap()), [2, 3]);
        // A stage that blocks wants all of its input, whatever follows it.
        let stages = [Stage::Sample(9), Stage::Limit(1)];
        let mut chain = build(&stages, &run, &limits, &[]);
        for id in 1..=5 {
            assert_eq!(chain.push(doc! {"_id": id}).unwrap(), Flow::More);
        }
        assert_eq!(ids(&chain.finish().unwrap()), [1]);
        // And one that is pushed a whole Vec says More as well.
        let mut chain = build(&stages, &run, &limits, &[]);
        assert_eq!(chain.push_all(docs(&[1, 2, 3])).unwrap(), Flow::More);
        // The same Vec pushed to a run that fills: Done, and the rest unread.
        let stages = [Stage::Limit(1)];
        let mut chain = build(&stages, &run, &limits, &[]);
        assert_eq!(chain.push_all(docs(&[1, 2, 3])).unwrap(), Flow::Done);
        assert_eq!(ids(&chain.finish().unwrap()), [1]);
    }

    #[test]
    fn an_unwind_stops_in_the_middle_of_an_array_when_its_next_sink_is_done() {
        let limits = Limits::default();
        let stage = parsed(&[doc! {"$unwind": "$a"}]).remove(0);
        let fan = doc! {"_id": 1, "a": [10, 20, 30, 40]};
        // Room for two of the four: the fan-out stops there, and says so.
        let (mut run, got) = run_of(&stage, &limits, 2);
        assert_eq!(run.push(fan.clone()).unwrap(), Flow::Done);
        assert_eq!(*got.borrow(), [1, 1], "two of the four elements went on");
        assert!(run.fans.is_empty(), "the rest of the array is still waiting");
        // Room for all four and one more: More, and every element went on.
        let (mut run, got) = run_of(&stage, &limits, 5);
        assert_eq!(run.push(fan.clone()).unwrap(), Flow::More);
        assert_eq!(*got.borrow(), [1, 1, 1, 1]);
        // Room for exactly four: Done at the last, not before.
        let (mut run, _) = run_of(&stage, &limits, 4);
        assert_eq!(run.push(fan).unwrap(), Flow::Done);
    }

    #[test]
    fn a_full_limit_after_an_unwind_ends_its_array_and_one_before_it_does_not() {
        let limits = Limits::default();
        let calls = Calls::default();
        let run = recording(&calls);
        let fan = doc! {"_id": 1, "a": [10, 20, 30, 40]};
        // The limit is full after two elements: the other two are never made.
        let stages = parsed(&[doc! {"$unwind": "$a"}, doc! {"$limit": 2}]);
        let mut chain = build(&stages, &run, &limits, &[]);
        assert_eq!(chain.push(fan.clone()).unwrap(), Flow::Done);
        let out = chain.finish().unwrap();
        assert_eq!(out.iter().map(|d| d.get_i32("a").unwrap()).collect::<Vec<_>>(), [10, 20]);
        // A limit before the unwind is full with the document, and the document's array is
        // still wanted whole.
        let stages = parsed(&[doc! {"$limit": 1}, doc! {"$unwind": "$a"}]);
        let mut chain = build(&stages, &run, &limits, &[]);
        assert_eq!(chain.push(fan.clone()).unwrap(), Flow::Done);
        assert_eq!(chain.finish().unwrap().len(), 4);
        // Two unwinds under a limit: the inner array is cut as the outer one is.
        let nested = doc! {"_id": 1, "a": [1, 2], "b": [10, 20, 30]};
        let stages = parsed(&[doc! {"$unwind": "$a"}, doc! {"$unwind": "$b"}, doc! {"$limit": 4}]);
        let mut chain = build(&stages, &run, &limits, &[]);
        assert_eq!(chain.push(nested).unwrap(), Flow::Done);
        let pairs: Vec<_> = chain
            .finish()
            .unwrap()
            .iter()
            .map(|d| (d.get_i32("a").unwrap(), d.get_i32("b").unwrap()))
            .collect();
        assert_eq!(pairs, [(1, 10), (1, 20), (1, 30), (2, 10)]);
    }

    #[test]
    fn a_stage_that_streams_ticks_the_deadline_for_each_document() {
        let limits = Limits::default();
        let stage = Stage::Skip(0);
        let (mut run, got) = run_of(&stage, &limits, 10);
        let guard = Deadline::scope(Some(std::time::Duration::ZERO));
        let err = run.push(doc! {"_id": 1}).unwrap_err();
        drop(guard);
        assert!(matches!(err, Error::DeadlineExceeded), "{err:?}");
        assert!(got.borrow().is_empty());
        // Pushed whole, it ticks once for each document and not twice.
        let (_, looks) = crate::deadline::looks_of(|| {
            let (mut run, _) = run_of(&stage, &limits, 1000);
            let _ = run.push_all(docs(&(0..50).collect::<Vec<_>>())).unwrap();
        });
        assert_eq!(looks, 50);
    }

    #[test]
    fn the_stages_that_stream_are_these_and_no_others() {
        let streaming = parsed(&[
            doc! {"$match": {}},
            doc! {"$project": {"a": 1}},
            doc! {"$addFields": {"a": 1}},
            doc! {"$set": {"a": 1}},
            doc! {"$replaceRoot": {"newRoot": "$a"}},
            doc! {"$replaceWith": "$a"},
            doc! {"$unset": "a"},
            doc! {"$redact": "$$KEEP"},
            doc! {"$unwind": "$a"},
            doc! {"$skip": 1},
            doc! {"$limit": 1},
        ]);
        assert!(streaming.iter().all(streams), "{streaming:?}");
        let blocking = parsed(&[
            doc! {"$sort": {"a": 1}},
            doc! {"$group": {"_id": "$a"}},
            doc! {"$count": "n"},
            doc! {"$sortByCount": "$a"},
            doc! {"$sample": {"size": 1}},
            doc! {"$bucket": {"groupBy": "$a", "boundaries": [0, 1]}},
            doc! {"$bucketAuto": {"groupBy": "$a", "buckets": 2}},
            doc! {"$facet": {"x": []}},
            doc! {"$lookup": {"from": "b", "localField": "a", "foreignField": "a", "as": "j"}},
            doc! {"$graphLookup": {"from": "b", "startWith": "$a", "connectFromField": "a",
            "connectToField": "a", "as": "g", "maxDepth": 1}},
            doc! {"$unionWith": "b"},
            doc! {"$setWindowFields": {"output": {"n": {"$sum": 1}}}},
            doc! {"$densify": {"field": "a", "range": {"step": 1, "bounds": "full"}}},
            doc! {"$fill": {"output": {"a": {"value": 0}}}},
            doc! {"$out": "o"},
        ]);
        for stage in &blocking {
            assert!(!streams(stage), "{}", stage.name());
        }
    }

    #[test]
    fn a_stage_that_streams_reads_the_bindings_of_the_pipeline() {
        let stages = crate::aggregate::parse_with_vars(
            &[
                doc! {"$addFields": {"x": "$$k"}},
                doc! {"$project": {"y": "$$k", "x": 1}},
                doc! {"$replaceRoot": {"newRoot": {"_id": "$_id", "z": {"$add": ["$x", "$$k"]}}}},
                doc! {"$redact": {"$cond": [{"$eq": ["$$k", 7]}, "$$KEEP", "$$PRUNE"]}},
            ],
            &["k".to_string()],
        )
        .unwrap();
        let seven = Bson::Int32(7);
        let vars = [("k", &seven)];
        let limits = Limits::default();
        let mut chain = build(&stages, &unused, &limits, &vars);
        let _ = chain.push(doc! {"_id": 1}).unwrap();
        assert_eq!(chain.finish().unwrap(), [doc! {"_id": 1, "z": 14}]);
        // And the bindings are not the pipeline's: with another value the redact prunes.
        let six = Bson::Int32(6);
        let vars = [("k", &six)];
        let mut chain = build(&stages, &unused, &limits, &vars);
        let _ = chain.push(doc! {"_id": 1}).unwrap();
        assert!(chain.finish().unwrap().is_empty());
    }

    /// The bytes of `n` documents of about `pad` bytes, as the budget counts them.
    fn padded(n: i32, pad: usize) -> Vec<Document> {
        (0..n).map(|i| doc! {"_id": i, "pad": "x".repeat(pad)}).collect()
    }

    #[test]
    fn a_stage_that_blocks_charges_the_documents_streamed_to_it_as_they_arrive() {
        let all = padded(10, 1000);
        let one = held_docs_bytes(&all[..1]);
        // Room for four and a half documents: the fifth is the one refused, and the sixth
        // is never pushed.
        let limits = Limits { buffer: BufferBudget::new(one * 9 / 2), ..Limits::default() };
        let run = |_: &Stage, _: Vec<Document>, _: Option<Lease>| -> Result<Vec<Document>, Error> {
            panic!("refused before the stage ran")
        };
        let stages = parsed(&[doc! {"$addFields": {"c": 1}}, doc! {"$sort": {"_id": 1}}]);
        let mut chain = build(&stages, &run, &limits, &[]);
        let mut pushed = 0;
        let err = loop {
            pushed += 1;
            if let Err(e) = chain.push(all[pushed - 1].clone()) {
                break e;
            }
        };
        assert!(matches!(err, Error::ValueTooLarge { what: "the input of $sort", .. }), "{err:?}");
        assert_eq!(pushed, 5);
        drop(chain);
        assert_eq!(limits.buffer.used(), 0, "the lease went back with the chain");
    }

    #[test]
    fn the_stage_is_given_the_lease_that_charged_its_input_and_charges_nothing_more() {
        let all = padded(4, 1000);
        let bytes = held_docs_bytes(&all);
        let limits = Limits { buffer: BufferBudget::new(bytes * 10), ..Limits::default() };
        let seen = RefCell::new(None);
        let run = |_: &Stage, input: Vec<Document>, arrived: Option<Lease>| {
            *seen.borrow_mut() = Some((input.len(), arrived.map(|l| l.bytes())));
            Ok::<_, Error>(input)
        };
        let stages = parsed(&[doc! {"$addFields": {"c": 1}}, doc! {"$sort": {"_id": 1}}]);
        let mut chain = build(&stages, &run, &limits, &[]);
        for doc in &all {
            let _ = chain.push(doc.clone()).unwrap();
        }
        // The documents the `$addFields` built are what the lease charged.
        let built: Vec<Document> = all
            .iter()
            .map(|d| {
                let mut d = d.clone();
                d.insert("c", 1);
                d
            })
            .collect();
        assert_eq!(limits.buffer.used(), held_docs_bytes(&built), "charged as they arrived");
        let _ = chain.finish().unwrap();
        assert_eq!(*seen.borrow(), Some((4, Some(held_docs_bytes(&built)))));
    }

    #[test]
    fn a_vec_pushed_whole_is_not_charged_on_arrival_and_one_pushed_after_it_is_charged_with_it() {
        let limits = Limits { buffer: BufferBudget::new(usize::MAX / 2), ..Limits::default() };
        let seen = RefCell::new(None);
        let run = |_: &Stage, input: Vec<Document>, arrived: Option<Lease>| {
            *seen.borrow_mut() = Some(arrived.map(|l| l.bytes()));
            Ok::<_, Error>(input)
        };
        let stages = [Stage::Sample(100)];
        let mut chain = build(&stages, &run, &limits, &[]);
        let _ = chain.push_all(padded(3, 100)).unwrap();
        assert_eq!(limits.buffer.used(), 0);
        let _ = chain.push(doc! {"_id": 9, "pad": "x"}).unwrap();
        // The three that came whole are charged with the fourth, so the lease holds all.
        let mut all = padded(3, 100);
        all.push(doc! {"_id": 9, "pad": "x"});
        assert_eq!(limits.buffer.used(), held_docs_bytes(&all));
        let _ = chain.finish().unwrap();
        assert_eq!(*seen.borrow(), Some(Some(held_docs_bytes(&all))));
    }

    #[test]
    fn a_stage_that_blocks_and_the_answer_refuse_past_the_documents_the_pipeline_may_hold() {
        let limits = Limits { max_documents: 3, ..Limits::default() };
        let run = |_: &Stage, input: Vec<Document>, _: Option<Lease>| Ok::<_, Error>(input);
        // Three documents streamed to a stage that blocks are held; the fourth is refused.
        let stages = parsed(&[doc! {"$addFields": {"c": 1}}, doc! {"$sort": {"_id": 1}}]);
        let mut chain = build(&stages, &run, &limits, &[]);
        for id in 0..3 {
            let _ = chain.push(doc! {"_id": id}).unwrap();
        }
        let err = chain.push(doc! {"_id": 3}).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid query: $addFields produced 4 documents, over the pipeline limit of 3. Narrow \
             the pipeline with an earlier $match"
        );
        // The answer likewise.
        let stages = parsed(&[doc! {"$addFields": {"c": 1}}]);
        let mut chain = build(&stages, &run, &limits, &[]);
        for id in 0..3 {
            let _ = chain.push(doc! {"_id": id}).unwrap();
        }
        let err = chain.push(doc! {"_id": 3}).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid query: $addFields produced 4 documents, over the pipeline limit of 3. Narrow \
             the pipeline with an earlier $match"
        );
        // A `Vec` is not counted on arrival: the stage that built it counted it.
        let stages = [Stage::Sample(100)];
        let mut chain = build(&stages, &run, &limits, &[]);
        let _ = chain.push_all(docs(&[1, 2, 3, 4, 5])).unwrap();
        assert_eq!(chain.finish().unwrap().len(), 5);
        // And a stage that streams has no ceiling of its own: five documents went through
        // two of them, holding nothing, with the pipeline limit at three.
        let stages = parsed(&[doc! {"$skip": 0}, doc! {"$match": {"_id": {"$gt": 100}}}]);
        let mut chain = build(&stages, &run, &limits, &[]);
        for id in 0..5 {
            assert_eq!(chain.push(doc! {"_id": id}).unwrap(), Flow::More);
        }
        assert!(chain.finish().unwrap().is_empty());
    }

    /// A sink that records what the request's budget is charged each time it is pushed to.
    struct Probe<'a> {
        limits: &'a Limits,
        used: Rc<RefCell<Vec<usize>>>,
    }

    impl<'a> Sink<'a, Error> for Probe<'a> {
        fn push(&mut self, _: Document) -> Result<Flow, Error> {
            self.used.borrow_mut().push(self.limits.buffer.used());
            Ok(Flow::More)
        }
        fn finish(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn next_mut(&mut self) -> Option<&mut dyn Sink<'a, Error>> {
            None
        }
        fn detach(&mut self) -> Option<Link<'a, Error>> {
            None
        }
    }

    #[test]
    fn a_vec_drained_into_a_run_of_stages_that_stream_is_charged_and_given_back_as_it_is_taken() {
        let input = padded(6, 1000);
        let each: Vec<usize> = input.iter().map(held_bytes).collect();
        let total: usize = each.iter().sum();
        let limits = Limits { buffer: BufferBudget::new(total), ..Limits::default() };
        let used = Rc::new(RefCell::new(Vec::new()));
        let probe = Probe { limits: &limits, used: Rc::clone(&used) };
        let stages = parsed(&[doc! {"$project": {"pad": 1}}]);
        let mut run: Streaming<'_, Error> = Streaming::new(&stages, &[], &limits, Box::new(probe));
        assert_eq!(run.push_all(input.clone()).unwrap(), Flow::More);
        // When the first document reaches the sink, the others are still held, and each one
        // taken is given back: the budget reads what is left to drain, to the byte.
        let left: Vec<usize> =
            (1..=6).map(|taken| total - each[..taken].iter().sum::<usize>()).collect();
        assert_eq!(*used.borrow(), left);
        assert_eq!(limits.buffer.used(), 0);
        // A byte under what the Vec holds is refused on arrival, naming the stage, and
        // nothing is pushed.
        let limits = Limits { buffer: BufferBudget::new(total - 1), ..Limits::default() };
        let used = Rc::new(RefCell::new(Vec::new()));
        let probe = Probe { limits: &limits, used: Rc::clone(&used) };
        let mut run: Streaming<'_, Error> = Streaming::new(&stages, &[], &limits, Box::new(probe));
        let err = run.push_all(input).unwrap_err();
        assert!(
            matches!(&err, Error::ValueTooLarge { what: "the input of $project", bytes, .. }
                if *bytes == total),
            "{err:?}"
        );
        assert!(used.borrow().is_empty());
        drop(run);
        assert_eq!(limits.buffer.used(), 0);
    }

    #[test]
    fn a_stage_streamed_documents_are_charged_to_is_named_whatever_it_is() {
        // `input_of` names every stage that blocks, the windows and the fills included.
        for (spec, name) in [
            (
                doc! {"$setWindowFields": {"sortBy": {"_id": 1},
                "output": {"n": {"$documentNumber": {}}}}},
                "the input of $setWindowFields",
            ),
            (
                doc! {"$densify": {"field": "_id", "range": {"step": 1, "bounds": "full"}}},
                "the input of $densify",
            ),
            (doc! {"$fill": {"output": {"x": {"value": 1}}}}, "the input of $fill"),
        ] {
            let stages = parsed(&[doc! {"$skip": 0}, spec]);
            let limits = Limits { buffer: BufferBudget::new(2000), ..Limits::default() };
            let mut chain = build(&stages, &unused, &limits, &[]);
            let err = padded(10, 1000)
                .into_iter()
                .find_map(|doc| chain.push(doc).err())
                .expect("refused as the documents arrive");
            assert!(matches!(&err, Error::ValueTooLarge { what, .. } if *what == name), "{err:?}");
        }
    }

    #[test]
    fn the_source_a_pipeline_takes_is_known_for_a_skip_and_a_limit_at_its_head_and_no_other() {
        let bound = |specs: &[Document]| source_bound(&parsed(specs));
        assert_eq!(bound(&[doc! {"$limit": 5}]), Some(5));
        assert_eq!(bound(&[doc! {"$limit": 0}]), Some(0));
        assert_eq!(bound(&[doc! {"$skip": 3}, doc! {"$limit": 5}]), Some(8));
        assert_eq!(
            bound(&[doc! {"$skip": 3}, doc! {"$skip": 4}, doc! {"$limit": 5}, doc! {"$limit": 1}]),
            Some(12)
        );
        // Anything else before a limit, or no limit, may need every document.
        assert_eq!(bound(&[]), None);
        assert_eq!(bound(&[doc! {"$skip": 3}]), None);
        assert_eq!(bound(&[doc! {"$match": {"a": 1}}, doc! {"$limit": 5}]), None);
        assert_eq!(bound(&[doc! {"$sort": {"a": 1}}, doc! {"$limit": 5}]), None);
        assert_eq!(
            bound(&[doc! {"$project": {"a": 1}}, doc! {"$skip": 1}, doc! {"$limit": 5}]),
            None
        );
    }
}
