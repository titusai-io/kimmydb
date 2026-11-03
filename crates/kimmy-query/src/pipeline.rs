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
//! # Three kinds of stage
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
//! A stage that needs all of its input but not to keep it (`$count`, `$group`, `$bucket`,
//! `$sortByCount`) is a [`Fused`] sink: it holds a state, a counter or its buckets, takes each
//! document into the state as it arrives, and at [`Sink::finish`] pushes its answer on. It
//! never answers `Done`, since it has not seen its input until the end, and it holds what the
//! stage's state holds and not what it was given. The state is the stage's own code
//! ([`crate::aggregate::GroupState`]) which [`crate::aggregate::apply`] runs a whole input
//! through, so there is one implementation of each.
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
//! document.
//!
//! A fused sink charges what its state holds as it grows, to a lease named for the state
//! ([`crate::budget::state_of`]): the buckets of a `$group` and the values they keep, and a
//! `$count` nothing. It gives it back when it finishes, as the barrier it replaces gave back its
//! buckets before its output was pushed on, and the sink that takes the output charges it. It
//! charges a `Vec` pushed whole as a streaming sink does.
//!
//! A barrier, and the collector that ends the chain, charge **as the documents arrive** one
//! at a time: the lease grows by a document's estimate when it is pushed, and a document that
//! takes the request over its limit is refused at once, so a streamed `$addFields` feeding a
//! `$sort` is held to the limit as it goes and not when it ends. The barrier hands the lease
//! to the stage as the hold on its input, and the stage does not charge the same bytes again.
//! A `Vec` pushed whole ([`Sink::push_all`]) is not charged on arrival: it was built by a
//! stage that charged it as it built it, and the stage that takes it charges it as its
//! input, as the loop did (a barrier at its run, a streaming or fused sink on arrival).
//!
//! A barrier, and the collector, also refuse when they would hold more than the pipeline's
//! `max_documents`, where the stages that stream have no such ceiling; a fused sink refuses
//! when its state would hold more groups than that ([`crate::aggregate::check_limit`]), which
//! is what the number of documents is a bound on.
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
use crate::budget::{Lease, held_bytes, input_of, state_of};
use crate::deadline::Ticker;
use crate::expr::Binding;
use crate::filter;
use crate::shape;
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

/// What a stage holds while it runs: how the chain built from a pipeline treats it, which is
/// what `explain` reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hold {
    /// One document at a time: a step of a [`Streaming`] sink ([`streams`]).
    Streams,
    /// A state in place of its input: a [`Fused`] sink.
    State(Kept),
    /// Its whole input: a [`Barrier`].
    Blocks,
}

/// The state a stage that holds one keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kept {
    /// A count (`$count`).
    Counter,
    /// The groups, or the buckets, with what their accumulators keep (`$group`, `$bucket`,
    /// `$sortByCount`).
    Groups,
    /// The first `skip + limit` documents of the sort (`$sort` and the `$skip` and `$limit` that
    /// follow it).
    Heap { skip: usize, limit: usize },
    /// The documents of the sample (`$sample`).
    Reservoir { size: usize },
}

/// Some of a pipeline's stages and what they hold: a run of stages that stream, one stage
/// that keeps a state (a `$sort` with its `$skip` and `$limit` is three), or one stage that
/// blocks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    /// Which stages, by place in the pipeline given to [`segments`].
    pub stages: std::ops::Range<usize>,
    /// What they hold.
    pub hold: Hold,
}

/// How a chain built from `stages` by [`build`] is laid out: its segments in the order of the
/// pipeline, which cover every stage. `build` takes its sinks from this and `explain` reports
/// it, so the plan cannot say a stage streams while the chain blocks on it. A leading `$match`
/// is the source's filter and is not given to the chain, nor a trailing `$merge` or `$out`
/// (the sink that writes): `stages` is what the chain runs.
pub fn segments(stages: &[Stage]) -> Vec<Segment> {
    segments_by(stages, true)
}

/// [`segments`] when `streaming`, and a barrier for every stage when not ([`build_barriers`]).
fn segments_by(stages: &[Stage], streaming: bool) -> Vec<Segment> {
    let mut out = Vec::new();
    let mut end = stages.len();
    while end > 0 {
        // The stages that hold their state and not their input end here.
        if let Some((len, kept)) = streaming.then(|| kept(&stages[..end])).flatten() {
            out.push(Segment { stages: end - len..end, hold: Hold::State(kept) });
            end -= len;
            continue;
        }
        // The run of stages that stream that ends at `end`, if the stage there is one.
        let mut start = end;
        // (A `$limit` that ends a `$sort` held as a heap is not in the run.)
        while streaming
            && start > 0
            && streams(&stages[start - 1])
            && heap_len(&stages[..start]).is_none()
        {
            start -= 1;
        }
        if start < end {
            out.push(Segment { stages: start..end, hold: Hold::Streams });
            end = start;
        } else {
            out.push(Segment { stages: end - 1..end, hold: Hold::Blocks });
            end -= 1;
        }
    }
    out.reverse();
    out
}

/// Whether a pipeline whose stages are `stages` takes its source a document at a time, and
/// holds none of it: the first stage streams, or holds a state in place of its input
/// ([`kept`]). Any other pipeline begins with a stage that needs the source whole, and the
/// executor reads it whole.
pub fn streams_source(stages: &[Stage]) -> bool {
    segments(stages).first().is_some_and(|first| first.hold != Hold::Blocks)
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
    build_by(stages, run, limits, vars, true, &system_below, OUTPUT)
}

/// How the end of a chain is named in a refusal for the bytes it holds: the output of the
/// pipeline the request answers, which the answer is built from (ADR-269).
pub const OUTPUT: &str = "the output of the pipeline";

/// [`build`] for a pipeline that is not the request's: a `$facet` branch or the sub-pipeline of
/// a `$lookup` or `$unionWith`. Its documents are held at the end of the chain as the
/// request's are, and `output` is what a refusal for them names, so that one inside a
/// sub-pipeline does not read as the request's answer.
pub fn build_into<'a, E: From<Error> + 'a>(
    stages: &'a [Stage],
    run: Runner<'a, E>,
    limits: &'a Limits,
    vars: &'a [Binding<'a>],
    output: &'static str,
) -> Chain<'a, E> {
    build_by(stages, run, limits, vars, true, &system_below, output)
}

/// A source of random positions for each `$sample` of a chain ([`stages::Below`]): a function
/// that makes one.
pub type BelowSource<'a> = &'a dyn Fn() -> Box<dyn FnMut(usize) -> usize + 'a>;

/// The positions the system draws ([`stages::system_below`]).
fn system_below<'a>() -> Box<dyn FnMut(usize) -> usize + 'a> {
    Box::new(stages::system_below())
}

/// [`build`], with the positions a `$sample` draws given by `below` and not by the system, so
/// that a test can script them.
pub fn build_with<'a, E: From<Error> + 'a>(
    stages: &'a [Stage],
    run: Runner<'a, E>,
    limits: &'a Limits,
    vars: &'a [Binding<'a>],
    below: BelowSource<'a>,
) -> Chain<'a, E> {
    build_by(stages, run, limits, vars, true, below, OUTPUT)
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
    build_by(stages, run, limits, vars, false, &system_below, OUTPUT)
}

/// [`build`] when `streaming`, and [`build_barriers`] when not.
fn build_by<'a, E: From<Error> + 'a>(
    stages: &'a [Stage],
    run: Runner<'a, E>,
    limits: &'a Limits,
    vars: &'a [Binding<'a>],
    streaming: bool,
    below: BelowSource<'a>,
    output: &'static str,
) -> Chain<'a, E> {
    let last = stages.last().map_or("the source", Stage::name);
    let out = Rc::new(RefCell::new(Collected::new(output, last, limits)));
    let mut head: Link<'a, E> = Box::new(Collector { out: Rc::clone(&out) });
    // From the end, each sink owns the one after it.
    for Segment { stages: at, hold } in segments_by(stages, streaming).into_iter().rev() {
        head = match hold {
            Hold::State(_) => {
                let fusion = fuse(&stages[..at.end], vars, limits, below)
                    .expect("the segments name a state where `fuse` builds one");
                let held = limits.buffer.lease(state_of(fusion.name));
                Box::new(Fused {
                    name: fusion.name,
                    limits,
                    state: Some(fusion.state),
                    held,
                    next: Some(head),
                })
            }
            Hold::Streams => Box::new(Streaming::new(&stages[at], vars, limits, head)),
            Hold::Blocks => {
                let stage = &stages[at.start];
                let before = at.start.checked_sub(1).map_or("the source", |at| stages[at].name());
                let held = Collected::new(input_of(stage.name()), before, limits);
                Box::new(Barrier { stage, run, held, next: Some(head) })
            }
        };
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
    pub fn finish(self) -> Result<Vec<Document>, E> {
        self.finish_held().map(|(docs, _lease)| docs)
    }

    /// [`Chain::finish`], also handing over the lease that charged the output as it arrived,
    /// if it did: every document of the output is charged to it, and its bytes stay in the
    /// request's budget until it is dropped. `None` when the output came whole to the end of
    /// the chain and nothing was charged on arrival.
    pub fn finish_held(mut self) -> Result<(Vec<Document>, Option<Lease>), E> {
        let mut at: Option<&mut dyn Sink<'a, E>> = Some(&mut *self.head);
        while let Some(sink) = at {
            sink.finish()?;
            at = sink.next_mut();
        }
        Ok(self.out.borrow_mut().take())
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
        while let Some((at, fan, charged)) = self.fans.last_mut() {
            let at = *at;
            if self.full.is_some_and(|full| at < full) {
                self.held.release(*charged);
                self.fans.pop();
                continue;
            }
            let spec = unwind_spec(self.steps[at].stage, self.limits);
            match fan.next(&spec, &mut self.ticker)? {
                Some((doc, _built)) => {
                    // What the fan still holds is what it is charged: the element that was
                    // handed out is the caller's now, and the last takes the document with it.
                    let now = fan.held_bytes().min(*charged);
                    self.held.release(*charged - now);
                    *charged = now;
                    return Ok(Some((doc, at + 1)));
                }
                None => {
                    self.held.release(*charged);
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
        // `push` ticks; a tick of its own would count each document twice.
        let first = self.steps.first().map_or("a stage", |step| step.stage.name());
        let limits = self.limits;
        drain(limits, first, docs, |doc| self.push(doc))
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

/// Take `docs`, which are in memory whole and which nothing has charged (the stage that built
/// them has given its hold back, and the caller of a sub-pipeline leaves them to the first
/// stage), through `push` one at a time. They are charged on arrival as the input of `first`,
/// the stage that takes them, as that stage charged them when it held its input, and each is
/// given back as it is taken, so that what is left to drain is held to the request's budget
/// (ADR-269).
fn drain<E: From<Error>>(
    limits: &Limits,
    first: &str,
    docs: Vec<Document>,
    mut push: impl FnMut(Document) -> Result<Flow, E>,
) -> Result<Flow, E> {
    let mut arrived = limits.buffer.lease(input_of(first));
    arrived.charge_docs(&docs)?;
    for doc in docs {
        arrived.release(held_bytes(&doc));
        if push(doc)? == Flow::Done {
            return Ok(Flow::Done);
        }
    }
    Ok(Flow::More)
}

/// What a stage that does not need its input holds in place of it (ADR-270): a counter, the
/// buckets of a `$group`. It takes one document at a time, charges what it keeps to the lease
/// it is given, and at the end of the input answers what the stage answers.
trait State {
    /// One more document. A state that loops ticks the request's deadline.
    fn add(&mut self, doc: Document, held: &mut Lease) -> Result<(), Error>;

    /// Whether the stage answers nothing whatever it is given (a heap that keeps no document),
    /// so that a source need not read any.
    fn is_full(&self) -> bool {
        false
    }

    /// The input is complete: the stage's output.
    fn finish(self: Box<Self>) -> Result<Vec<Document>, Error>;
}

/// `$count`: one number, and nothing charged.
struct Counter<'a> {
    field: &'a str,
    seen: usize,
    ticker: Ticker,
}

impl State for Counter<'_> {
    fn add(&mut self, _: Document, _: &mut Lease) -> Result<(), Error> {
        self.ticker.tick()?;
        self.seen += 1;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<Vec<Document>, Error> {
        // An `Int32` while it fits, as the reference counts (ADR-268).
        Ok(vec![bson::doc! { self.field: crate::expr::count_bson(self.seen) }])
    }
}

/// `$sort` and `$limit n` with `$skip m` between them, if there is one: the first `m + n`
/// documents of the sort ([`shape::TopK`]), of which the answer is the last `n`.
struct Heap<'a> {
    top: shape::TopK<'a>,
    skip: usize,
    limit: usize,
    limits: &'a Limits,
    ticker: Ticker,
}

impl<'a> Heap<'a> {
    fn new(keys: &'a [shape::SortKey], skip: usize, limit: usize, limits: &'a Limits) -> Self {
        let top = shape::TopK::new(keys, skip.saturating_add(limit));
        Heap { top, skip, limit, limits, ticker: Ticker::new() }
    }
}

impl State for Heap<'_> {
    fn add(&mut self, doc: Document, held: &mut Lease) -> Result<(), Error> {
        self.ticker.tick()?;
        self.top.offer(doc, held, self.limits)
    }

    fn is_full(&self) -> bool {
        self.limit == 0
    }

    fn finish(self: Box<Self>) -> Result<Vec<Document>, Error> {
        Ok(self.top.into_sorted().into_iter().skip(self.skip).take(self.limit).collect())
    }
}

/// `$sample`: the reservoir of the sample, and the positions it draws.
struct Sampling<'a> {
    reservoir: stages::Reservoir,
    below: Box<dyn FnMut(usize) -> usize + 'a>,
    limits: &'a Limits,
    ticker: Ticker,
}

impl State for Sampling<'_> {
    fn add(&mut self, doc: Document, held: &mut Lease) -> Result<(), Error> {
        self.ticker.tick()?;
        let Some(slot) = self.reservoir.place(&mut *self.below) else { return Ok(()) };
        if slot == stages::Slot::Fill {
            aggregate::check_limit("$sample", self.reservoir.len() + 1, self.limits)?;
        }
        held.charge(held_bytes(&doc))?;
        if let Some(turned_out) = self.reservoir.put(slot, doc) {
            held.release(held_bytes(&turned_out));
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<Vec<Document>, Error> {
        self.reservoir.finish(&mut *self.below)
    }
}

impl State for aggregate::GroupState<'_> {
    fn add(&mut self, doc: Document, held: &mut Lease) -> Result<(), Error> {
        aggregate::GroupState::add(self, &doc, held)
    }

    fn finish(self: Box<Self>) -> Result<Vec<Document>, Error> {
        aggregate::GroupState::finish(*self)
    }
}

/// The stages that a [`Fused`] sink stands for, and the state it holds.
struct Fusion<'a> {
    /// How many stages, counted back from the end of those given to [`fuse`].
    len: usize,
    /// The name its lease is given by ([`state_of`]).
    name: &'static str,
    state: Box<dyn State + 'a>,
}

/// `$sort` and the `$limit` after it (and the `$skip` between them, if there is one) that end
/// `stages`, which hold the first documents of the sort and not the whole input, and how many
/// stages they are. A `$sort` that is not followed at once by a `$limit` needs all of its input.
fn heap_len(stages: &[Stage]) -> Option<usize> {
    match kept(stages) {
        Some((len, Kept::Heap { .. })) => Some(len),
        _ => None,
    }
}

/// The stages at the end of `stages` that hold a state and not their input, if there are any,
/// how many they are and what they keep: `$count` and the groups hold a counter or their
/// buckets, `$sort` with a `$limit` a heap of the first documents, and `$sample` its
/// reservoir. This is the one list of them: [`fuse`] builds what it names, [`segments`] lays
/// the chain out by it and `explain` reports it.
fn kept(stages: &[Stage]) -> Option<(usize, Kept)> {
    match stages {
        [.., Stage::Sort(_), Stage::Skip(skip), Stage::Limit(limit)] => {
            Some((3, Kept::Heap { skip: *skip, limit: *limit }))
        }
        [.., Stage::Sort(_), Stage::Limit(limit)] => {
            Some((2, Kept::Heap { skip: 0, limit: *limit }))
        }
        [.., Stage::Count(_)] => Some((1, Kept::Counter)),
        [.., Stage::Sample(size)] => Some((1, Kept::Reservoir { size: *size })),
        [.., Stage::Group { .. } | Stage::Bucket { .. } | Stage::SortByCount(_)] => {
            Some((1, Kept::Groups))
        }
        _ => None,
    }
}

/// The sink-side state for the stages at the end of `stages`, if [`kept`] names one.
fn fuse<'a>(
    stages: &'a [Stage],
    vars: &'a [Binding<'a>],
    limits: &'a Limits,
    below: BelowSource<'a>,
) -> Option<Fusion<'a>> {
    let (len, kept) = kept(stages)?;
    let stood_for = &stages[stages.len() - len..];
    let state: Box<dyn State + 'a> = match (kept, stood_for) {
        (Kept::Heap { skip, limit }, [Stage::Sort(keys), ..]) => {
            Box::new(Heap::new(keys, skip, limit, limits))
        }
        (Kept::Counter, [Stage::Count(field)]) => {
            Box::new(Counter { field, seen: 0, ticker: Ticker::new() })
        }
        (Kept::Reservoir { size }, _) => Box::new(Sampling {
            reservoir: stages::Reservoir::new(size),
            below: below(),
            limits,
            ticker: Ticker::new(),
        }),
        (Kept::Groups, [stage]) => Box::new(aggregate::GroupState::new(stage, limits, vars)?),
        _ => return None,
    };
    Some(Fusion { len, name: stood_for[0].name(), state })
}

/// A stage that holds a state and not its input: it takes the documents as they arrive and
/// keeps what the stage needs of them, and at [`Sink::finish`] pushes its answer on. It
/// needs all of its input, so it never answers `Done` to the source.
struct Fused<'a, E: From<Error>> {
    name: &'static str,
    limits: &'a Limits,
    /// `None` once it has finished.
    state: Option<Box<dyn State + 'a>>,
    /// What the state holds, named for it ([`state_of`]).
    held: Lease,
    /// `None` only once [`Sink::detach`] has taken it.
    next: Option<Link<'a, E>>,
}

impl<'a, E: From<Error>> Sink<'a, E> for Fused<'a, E> {
    /// A `$sort` with a `$limit` of 0 answers nothing, so a source that is asked reads none. (It
    /// is asked of the head only, so nothing before it is left unrun: a stage that could fail
    /// on a document runs on every one.)
    fn is_full(&self) -> bool {
        self.state.as_ref().is_some_and(|state| state.is_full())
    }

    fn push(&mut self, doc: Document) -> Result<Flow, E> {
        if let Some(state) = self.state.as_mut() {
            state.add(doc, &mut self.held)?;
        }
        Ok(Flow::More)
    }

    fn push_all(&mut self, docs: Vec<Document>) -> Result<Flow, E> {
        let (limits, name) = (self.limits, self.name);
        drain(limits, name, docs, |doc| self.push(doc))
    }

    fn finish(&mut self) -> Result<(), E> {
        let Some(state) = self.state.take() else { return Ok(()) };
        let output = state.finish()?;
        // The state is built into the output, which the next sink charges as it takes it.
        self.held.release(self.held.bytes());
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
    /// but for `$bucketAuto`, which keeps the first `n` buckets. It stands in for stages that block.
    fn recording(
        calls: &Calls,
    ) -> impl Fn(&Stage, Vec<Document>, Option<Lease>) -> Result<Vec<Document>, Error> {
        move |stage, input, _| {
            calls.borrow_mut().push((stage.name(), ids(&input)));
            Ok(match stage {
                Stage::BucketAuto { buckets, .. } => input.into_iter().take(*buckets).collect(),
                _ => input,
            })
        }
    }

    /// A stage that blocks and that nothing here holds a state for, which the recording runner
    /// answers the first `n` of its input for (`$bucketAuto`).
    fn blocker(n: usize) -> Stage {
        Stage::BucketAuto {
            group_by: crate::expr::Expr::Literal(Bson::Null),
            buckets: n,
            fields: Vec::new(),
        }
    }

    /// A `$sort` with no `$limit` after it, which blocks.
    fn sort() -> Stage {
        Stage::Sort(vec![crate::shape::SortKey { path: "_id".into(), descending: false }])
    }

    /// Three stages that block.
    fn pipeline() -> Vec<Stage> {
        vec![blocker(3), blocker(2), sort()]
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
            [
                ("$bucketAuto", vec![1, 2, 3, 4]),
                ("$bucketAuto", vec![1, 2, 3]),
                ("$sort", vec![1, 2])
            ]
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
        let stages = [blocker(10)];
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
        let stages = [blocker(5), blocker(10)];
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
                Stage::BucketAuto { buckets: 2, .. } => {
                    Err(Error::InvalidQuery("the sample failed".into()))
                }
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
        assert_eq!(names, ["$bucketAuto", "$bucketAuto"]);
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
        let stages = [blocker(1)];
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
        let stages = vec![blocker(10); 300_000];
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
        let stages: Vec<Stage> =
            (0..200_000).map(|at| if at % 2 == 0 { Stage::Skip(0) } else { blocker(10) }).collect();
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
    fn a_limit_that_is_full_before_any_document_wants_none() {
        // The parser refuses `$limit: 0`, as the reference does, so these are stages made by
        // hand: what a chain does for a limit that is full before it is pushed anything.
        let limits = Limits::default();
        let stage = Stage::Limit(0);
        let (mut run, got) = run_of(&stage, &limits, 10);
        // Full before any is pushed, so a source that asks reads none.
        assert!(Sink::<Error>::is_full(&run));
        assert_eq!(run.push(doc! {"_id": 1}).unwrap(), Flow::Done);
        assert!(got.borrow().is_empty());
        // Behind a stage that streams, and at the head of a chain.
        let stages = [Stage::Skip(1), Stage::Limit(0), sort()];
        let calls = Calls::default();
        let counting = recording(&calls);
        let mut chain = build(&stages, &counting, &limits, &[]);
        assert!(chain.is_full());
        assert_eq!(chain.push(doc! {"_id": 1}).unwrap(), Flow::Done);
        assert_eq!(ids(&chain.finish().unwrap()), Vec::<i32>::new());
        assert_eq!(*calls.borrow(), [("$sort", Vec::<i32>::new())]);
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
            (vec![blocker(1), Stage::Limit(0)], false),
            (vec![Stage::Skip(0), blocker(1), Stage::Limit(0)], false),
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
        let stages = [Stage::Skip(1), Stage::Limit(2), blocker(9)];
        let mut chain = build(&stages, &run, &limits, &[]);
        assert_eq!(chain.push(doc! {"_id": 1}).unwrap(), Flow::More);
        assert_eq!(chain.push(doc! {"_id": 2}).unwrap(), Flow::More);
        assert_eq!(chain.push(doc! {"_id": 3}).unwrap(), Flow::Done);
        assert_eq!(ids(&chain.finish().unwrap()), [2, 3]);
        // A stage that blocks wants all of its input, whatever follows it.
        let stages = [blocker(9), Stage::Limit(1)];
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
        let stages = [blocker(100)];
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
        let stages = [blocker(100)];
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
        // A limit of 0 cannot be parsed, but a stage made by hand takes none.
        assert_eq!(source_bound(&[Stage::Skip(2), Stage::Limit(0)]), Some(2));
        assert_eq!(bound(&[doc! {"$skip": 3}, doc! {"$limit": 5}]), Some(8));
        // Made by hand, a run of them adds up and the first limit ends it.
        assert_eq!(
            source_bound(&[Stage::Skip(3), Stage::Skip(4), Stage::Limit(5), Stage::Limit(1)]),
            Some(12)
        );
        // Parsed, a run of them is one window: skip 7 and a limit of 1.
        assert_eq!(
            bound(&[doc! {"$skip": 3}, doc! {"$skip": 4}, doc! {"$limit": 5}, doc! {"$limit": 1}]),
            Some(8)
        );
        // A window that has moved ahead of the stages that neither filter nor change the count
        // is at the head: `[$project, $skip 1, $limit 5]` takes six documents of the source.
        assert_eq!(
            bound(&[doc! {"$project": {"a": 1}}, doc! {"$skip": 1}, doc! {"$limit": 5}]),
            Some(6)
        );
        assert_eq!(
            bound(&[
                doc! {"$addFields": {"a": 1}},
                doc! {"$unset": "b"},
                doc! {"$replaceWith": "$$ROOT"},
                doc! {"$limit": 5},
            ]),
            Some(5)
        );
        // Anything else before a limit, or no limit, may need every document.
        assert_eq!(bound(&[]), None);
        assert_eq!(bound(&[doc! {"$skip": 3}]), None);
        assert_eq!(bound(&[doc! {"$match": {"a": 1}}, doc! {"$limit": 5}]), None);
        assert_eq!(bound(&[doc! {"$sort": {"a": 1}}, doc! {"$limit": 5}]), None);
        assert_eq!(bound(&[doc! {"$unwind": "$a"}, doc! {"$limit": 5}]), None);
        assert_eq!(
            bound(&[doc! {"$redact": "$$KEEP"}, doc! {"$skip": 1}, doc! {"$limit": 5}]),
            None
        );
    }
    /// The stages that hold a state and not their input, with a `$limit` after them that the
    /// stage does not stand for.
    fn fused_specs() -> Vec<Document> {
        vec![
            doc! {"$count": "n"},
            doc! {"$group": {"_id": "$g", "n": {"$sum": 1}, "p": {"$push": "$pad"}}},
            doc! {"$bucket": {"groupBy": "$g", "boundaries": [0, 2, 4], "default": "other",
            "output": {"p": {"$push": "$pad"}}}},
            doc! {"$sortByCount": "$g"},
        ]
    }

    /// `n` documents `{_id, g: i % 3, pad}`.
    fn grouped(n: i32, pad: usize) -> Vec<Document> {
        (0..n).map(|i| doc! {"_id": i, "g": i % 3, "pad": "x".repeat(pad)}).collect()
    }

    #[test]
    fn a_stage_that_holds_a_state_and_not_its_input_never_reaches_the_runner() {
        // A runner that is never called: the stages are sinks of their own.
        for spec in fused_specs() {
            let stages = parsed(std::slice::from_ref(&spec));
            let limits = Limits::default();
            let mut chain = build(&stages, &unused, &limits, &[]);
            for doc in grouped(7, 10) {
                assert_eq!(chain.push(doc).unwrap(), Flow::More, "{spec}");
            }
            assert!(!chain.finish().unwrap().is_empty(), "{spec}");
            // The same stage as a barrier is run by the runner, and answers the same.
            let calls = Calls::default();
            let run = |stage: &Stage, input: Vec<Document>, _: Option<Lease>| {
                calls.borrow_mut().push((stage.name(), ids(&input)));
                aggregate::apply(stage, input, &Limits::default())
            };
            let mut barrier = build_barriers(&stages, &run, &limits, &[]);
            let _ = barrier.push_all(grouped(7, 10)).unwrap();
            assert_eq!(calls.borrow().len(), 0, "nothing runs until the chain is finished");
            let mut sink = build(&stages, &unused, &limits, &[]);
            let _ = sink.push_all(grouped(7, 10)).unwrap();
            assert_eq!(barrier.finish().unwrap(), sink.finish().unwrap(), "{spec}");
            assert_eq!(calls.borrow().len(), 1, "{spec}");
        }
    }

    #[test]
    fn the_stages_that_hold_a_state_are_those_a_source_streams_into() {
        // `streams_source` and `fuse` agree on every stage, each at the head of a pipeline.
        let mut all = fused_specs();
        all.extend([
            doc! {"$sort": {"a": 1}},
            doc! {"$sample": {"size": 1}},
            doc! {"$bucketAuto": {"groupBy": "$a", "buckets": 2}},
            doc! {"$facet": {"x": []}},
            doc! {"$match": {}},
            doc! {"$limit": 1},
        ]);
        for spec in all {
            let stages = parsed(std::slice::from_ref(&spec));
            let fused = fuse(&stages, &[], &Limits::default(), &system_below).is_some();
            assert_eq!(streams_source(&stages), fused || streams(&stages[0]), "{spec}");
        }
        assert!(!streams_source(&[]));
    }

    #[test]
    fn a_source_streams_into_a_sort_exactly_when_its_limit_is_the_sorts() {
        // The stages a `$sort` is followed by, as the parse leaves them: `fuse` says the head is
        // one stage that keeps a state exactly when `streams_source` says the source streams, so
        // that one cannot call a `$sort` fused while the other builds a barrier.
        let sort = || doc! {"$sort": {"a": 1}};
        for (specs, heap) in [
            (vec![sort(), doc! {"$limit": 3}], true),
            (vec![sort(), doc! {"$skip": 2}, doc! {"$limit": 3}], true),
            // The limit is moved ahead of the `$project`, so it is the sort's.
            (vec![sort(), doc! {"$project": {"a": 1}}, doc! {"$limit": 3}], true),
            (vec![sort(), doc! {"$limit": 150_000}, doc! {"$skip": 200_000}], true),
            (vec![sort(), doc! {"$skip": 2}], false),
            (vec![sort()], false),
            (vec![sort(), doc! {"$match": {"a": 1}}, doc! {"$limit": 3}], false),
            (vec![sort(), doc! {"$unwind": "$a"}, doc! {"$limit": 3}], false),
            (vec![sort(), doc! {"$sort": {"b": 1}}, doc! {"$limit": 3}], false),
        ] {
            let stages = parsed(&specs);
            let at_the_head = (1..=stages.len()).any(|end| {
                fuse(&stages[..end], &[], &Limits::default(), &system_below)
                    .is_some_and(|fusion| fusion.len == end)
            });
            assert_eq!(at_the_head, heap, "{specs:?}");
            assert_eq!(streams_source(&stages), heap, "{specs:?}");
        }
    }

    #[test]
    fn a_sort_that_keeps_no_document_is_full_before_any_is_pushed() {
        let limits = Limits::default();
        // A `$limit` of 0 behind a `$sort` is a window that takes nothing (the rewrite makes it of
        // `$limit` then `$skip`); nothing is read for it, and the answer is empty.
        let stages = parsed(&[sort_by_a(), doc! {"$limit": 5}, doc! {"$skip": 9}]);
        let chain = build(&stages, &unused, &limits, &[]);
        assert!(chain.is_full());
        assert!(chain.finish().unwrap().is_empty());
        // One that keeps something is not, and neither is a heap behind a stage that could fail.
        for specs in [
            vec![sort_by_a(), doc! {"$limit": 5}],
            vec![doc! {"$match": {"a": 1}}, sort_by_a(), doc! {"$limit": 5}, doc! {"$skip": 9}],
        ] {
            let stages = parsed(&specs);
            assert!(!build(&stages, &unused, &limits, &[]).is_full(), "{specs:?}");
        }
    }

    fn sort_by_a() -> Document {
        doc! {"$sort": {"a": 1}}
    }

    #[test]
    fn a_count_holds_a_counter_and_charges_nothing_however_many_documents_reach_it() {
        let limits = Limits { buffer: BufferBudget::new(0), ..Limits::default() };
        let stages = parsed(&[doc! {"$count": "n"}]);
        let mut chain = build(&stages, &unused, &limits, &[]);
        for doc in grouped(300, 200) {
            assert_eq!(chain.push(doc).unwrap(), Flow::More);
            assert_eq!(limits.buffer.used(), 0);
        }
        assert_eq!(chain.finish().unwrap(), [doc! {"n": 300}]);
    }

    #[test]
    fn the_state_of_a_group_is_charged_as_it_grows_and_given_back_when_it_finishes() {
        let limits = Limits { buffer: BufferBudget::new(usize::MAX / 2), ..Limits::default() };
        let stages = parsed(&[doc! {"$group": {"_id": "$g", "p": {"$push": "$pad"}}}]);
        let mut chain = build(&stages, &unused, &limits, &[]);
        let mut seen = Vec::new();
        for doc in grouped(9, 400) {
            let _ = chain.push(doc).unwrap();
            seen.push(limits.buffer.used());
        }
        // Each new bucket and each value kept is charged, and nothing held from the input: the
        // charge grows with the buckets and the padding they keep, a document at a time.
        assert!(seen[0] > 400, "the first bucket and its value: {seen:?}");
        assert!(seen.windows(2).all(|w| w[1] > w[0] + 400), "{seen:?}");
        assert!(seen[8] < 9 * held_docs_bytes(&grouped(1, 400)), "the input is not held: {seen:?}");
        let out = chain.finish().unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(limits.buffer.used(), 0, "given back at the finish");
    }

    #[test]
    fn the_state_of_a_stage_is_refused_at_the_byte_past_the_limit_and_named() {
        let specs = [
            fused_specs().remove(1),
            fused_specs().remove(2),
            // A bucket for each document.
            doc! {"$sortByCount": "$_id"},
        ];
        for spec in specs {
            let stage = parsed(std::slice::from_ref(&spec)).remove(0);
            let name = state_of(stage.name());
            // Room for a few documents' padding: refused where the buckets pass it.
            let cap = 5 * held_docs_bytes(&grouped(1, 400));
            let limits = Limits { buffer: BufferBudget::new(cap), ..Limits::default() };
            let stages = [stage];
            let mut chain = build(&stages, &unused, &limits, &[]);
            let mut pushed = 0;
            let err = grouped(200, 400)
                .into_iter()
                .find_map(|doc| {
                    pushed += 1;
                    chain.push(doc).err()
                })
                .unwrap_or_else(|| panic!("{spec}: never refused"));
            let Error::ValueTooLarge { what, bytes, max, .. } = &err else { panic!("{err:?}") };
            assert_eq!((*what, *max), (name, cap), "{spec}");
            assert!(*bytes > cap && *bytes < 2 * cap, "{spec}: refused at {bytes} of {cap}");
            assert!(pushed < 20, "{spec}: refused after {pushed}, not after the whole stream");
            drop(chain);
            assert_eq!(limits.buffer.used(), 0, "{spec}: gives everything back when refused");
        }
    }

    #[test]
    fn the_buckets_of_a_group_are_held_to_the_pipeline_limit_and_its_input_is_not() {
        // Two thousand documents of two keys through a limit of ten documents: answered.
        let limits = Limits { max_documents: 10, ..Limits::default() };
        let stages = parsed(&[doc! {"$group": {"_id": {"$mod": ["$_id", 2]}, "n": {"$sum": 1}}}]);
        let mut chain = build(&stages, &unused, &limits, &[]);
        for id in 0..2000 {
            assert_eq!(chain.push(doc! {"_id": id}).unwrap(), Flow::More);
        }
        assert_eq!(chain.finish().unwrap().len(), 2);
        // The eleventh bucket is refused, naming the stage that made it.
        let stages = parsed(&[doc! {"$group": {"_id": "$_id"}}]);
        let mut chain = build(&stages, &unused, &limits, &[]);
        for id in 0..10 {
            let _ = chain.push(doc! {"_id": id}).unwrap();
        }
        let err = chain.push(doc! {"_id": 10}).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid query: $group produced 11 documents, over the pipeline limit of 10. Narrow \
             the pipeline with an earlier $match"
        );
    }

    #[test]
    fn a_vec_pushed_whole_to_a_stage_that_holds_a_state_is_charged_and_given_back_as_it_is_taken() {
        let input = grouped(20, 200);
        let held_in = held_docs_bytes(&input);
        for spec in fused_specs() {
            let stages = parsed(std::slice::from_ref(&spec));
            let name = input_of(stages[0].name());
            // The input and the buckets it makes do not fit together (a `$group` that keeps the
            // padding holds as much as it was given), but the input given back as it is taken
            // leaves room for them.
            let room = held_in + held_in / 10;
            let limits = Limits { buffer: BufferBudget::new(room), ..Limits::default() };
            let mut chain = build(&stages, &unused, &limits, &[]);
            let pushed = chain.push_all(input.clone());
            assert!(pushed.is_ok(), "{spec}: {pushed:?}");
            drop(chain);
            assert_eq!(limits.buffer.used(), 0, "{spec}");
            let limits = Limits { buffer: BufferBudget::new(held_in - 1), ..Limits::default() };
            let err = build(&stages, &unused, &limits, &[]).push_all(input.clone()).unwrap_err();
            assert!(
                matches!(&err, Error::ValueTooLarge { what, bytes, .. }
                    if *what == name && *bytes == held_in),
                "{spec}: {err:?}"
            );
            assert_eq!(limits.buffer.used(), 0, "{spec}");
        }
    }

    #[test]
    fn a_stage_that_holds_a_state_ticks_the_deadline_for_each_document() {
        for spec in fused_specs() {
            let stages = parsed(std::slice::from_ref(&spec));
            let limits = Limits::default();
            let (_, looks) = crate::deadline::looks_of(|| {
                let mut chain = build(&stages, &unused, &limits, &[]);
                for doc in grouped(1000, 1) {
                    let _ = chain.push(doc).unwrap();
                }
            });
            // A group looks again for each accumulator, so it is not exactly one a document.
            assert!(looks >= 1000, "{spec}: {looks} looks for 1,000 documents");
            if stages[0].name() == "$count" {
                assert_eq!(looks, 1000);
            }
        }
    }

    /// `n` documents `{_id, g: i % 3, pad}`, in an order that is not the sort order of `g`.
    fn heap_docs(n: i32) -> Vec<Document> {
        (0..n).map(|i| doc! {"_id": i, "g": (i * 7) % 5, "pad": "x".repeat(300)}).collect()
    }

    #[test]
    fn a_sort_with_a_limit_after_it_is_a_heap_and_never_reaches_the_runner() {
        let limits = Limits::default();
        for specs in [
            vec![doc! {"$sort": {"g": 1}}, doc! {"$limit": 4}],
            vec![doc! {"$sort": {"g": -1}}, doc! {"$skip": 2}, doc! {"$limit": 4}],
        ] {
            let stages = parsed(&specs);
            let mut chain = build(&stages, &unused, &limits, &[]);
            for doc in heap_docs(12) {
                assert_eq!(chain.push(doc).unwrap(), Flow::More, "never Done");
            }
            let out = chain.finish().unwrap();
            assert_eq!(out.len(), 4, "{specs:?}");
            // The same answer as the barriers: the sort, then the window.
            let mut barriers = build_barriers(&stages, &aggregate_runner, &limits, &[]);
            let _ = barriers.push_all(heap_docs(12)).unwrap();
            assert_eq!(out, barriers.finish().unwrap(), "{specs:?}");
        }
    }

    /// A runner that runs the stage as the pure pipeline does.
    fn aggregate_runner(
        stage: &Stage,
        input: Vec<Document>,
        arrived: Option<Lease>,
    ) -> Result<Vec<Document>, Error> {
        aggregate::apply_charged(stage, input, &Limits::default(), &[], arrived)
    }

    #[test]
    fn a_sort_that_is_not_followed_at_once_by_a_limit_is_a_barrier() {
        let limits = Limits::default();
        let calls = Calls::default();
        let run = recording(&calls);
        // Nothing after it, a stage that streams between it and the limit, and a limit that
        // the sort does not end.
        for specs in [
            vec![doc! {"$sort": {"g": 1}}],
            vec![doc! {"$sort": {"g": 1}}, doc! {"$match": {"g": {"$gte": 0}}}, doc! {"$limit": 4}],
            vec![doc! {"$sort": {"g": 1}}, doc! {"$skip": 1}],
            vec![
                doc! {"$sort": {"g": 1}},
                doc! {"$skip": 1},
                doc! {"$match": {"g": {"$gte": 0}}},
                doc! {"$limit": 2},
            ],
        ] {
            calls.borrow_mut().clear();
            let stages = parsed(&specs);
            let mut chain = build(&stages, &run, &limits, &[]);
            let _ = chain.push_all(heap_docs(6)).unwrap();
            let _ = chain.finish().unwrap();
            assert_eq!(calls.borrow().len(), 1, "{specs:?}: the sort runs as a barrier");
            assert_eq!(calls.borrow()[0].0, "$sort", "{specs:?}");
        }
    }

    #[test]
    fn the_stages_after_a_heap_are_a_run_and_the_limit_that_ends_it_is_not() {
        let limits = Limits::default();
        let stages = parsed(&[
            doc! {"$sort": {"g": 1}},
            doc! {"$limit": 5},
            doc! {"$project": {"g": 1}},
            doc! {"$limit": 2},
        ]);
        let mut chain = build(&stages, &unused, &limits, &[]);
        for doc in heap_docs(12) {
            let _ = chain.push(doc).unwrap();
        }
        let out = chain.finish().unwrap();
        // The five least by `g`, in arrival order among equals, and then two of them.
        assert_eq!(out, [doc! {"_id": 0, "g": 0}, doc! {"_id": 5, "g": 0}]);
        // A heap before a `$limit` it does not stand for: the `$skip` and `$limit` of a run
        // that begins after a `$project` are the run's.
        assert_eq!(heap_len(&stages[..2]), Some(2));
        assert_eq!(heap_len(&stages[..3]), None);
        assert_eq!(heap_len(&stages), None);
        assert_eq!(heap_len(&parsed(&[doc! {"$sort": {"a": 1}}, doc! {"$skip": 1}])), None);
    }

    #[test]
    fn a_heap_streams_its_source_and_a_sort_alone_does_not() {
        for (specs, streams) in [
            (vec![doc! {"$sort": {"a": 1}}, doc! {"$limit": 3}], true),
            (vec![doc! {"$sort": {"a": 1}}, doc! {"$skip": 2}, doc! {"$limit": 3}], true),
            (vec![doc! {"$sort": {"a": 1}}], false),
            (vec![doc! {"$sort": {"a": 1}}, doc! {"$skip": 2}], false),
            // A stage that blocks or filters between them: the limit is not the sort's.
            (vec![doc! {"$sort": {"a": 1}}, doc! {"$match": {"a": 1}}, doc! {"$limit": 3}], false),
            (vec![doc! {"$sort": {"a": 1}}, doc! {"$unwind": "$a"}, doc! {"$limit": 3}], false),
            // A stage that neither filters nor changes the count is passed by the limit when the
            // pipeline is parsed (the window is moved ahead of it), so the two are together.
            (vec![doc! {"$sort": {"a": 1}}, doc! {"$project": {"a": 1}}, doc! {"$limit": 3}], true),
        ] {
            assert_eq!(streams_source(&parsed(&specs)), streams, "{specs:?}");
        }
    }

    #[test]
    fn the_heap_of_a_sort_is_charged_as_it_fills_and_turns_out_what_it_replaces() {
        let one = held_docs_bytes(&heap_docs(1));
        let limits = Limits { buffer: BufferBudget::new(usize::MAX / 2), ..Limits::default() };
        let stages = parsed(&[doc! {"$sort": {"g": 1}}, doc! {"$limit": 3}]);
        let mut chain = build(&stages, &unused, &limits, &[]);
        let mut seen = Vec::new();
        // Documents that arrive in falling order of `g`: each is kept and turns one out.
        for g in (0..10).rev() {
            let _ = chain.push(doc! {"_id": g, "g": g, "pad": "x".repeat(300)}).unwrap();
            seen.push(limits.buffer.used());
        }
        assert!(seen[0] > one && seen[0] < 2 * one, "one document and its key: {seen:?}");
        assert!(seen[2] > 2 * seen[0] && seen[2] < 4 * seen[0], "three held: {seen:?}");
        assert!(seen[3..].iter().all(|&used| used == seen[2]), "no more than three: {seen:?}");
        // A document the heap does not keep is not charged.
        let _ = chain.push(doc! {"_id": 99, "g": 99, "pad": "x".repeat(300)}).unwrap();
        assert_eq!(limits.buffer.used(), seen[2]);
        assert_eq!(ids(&chain.finish().unwrap()), [0, 1, 2]);
        assert_eq!(limits.buffer.used(), 0);
    }

    #[test]
    fn a_sample_is_a_reservoir_of_its_size_and_never_reaches_the_runner() {
        let limits = Limits::default();
        let stages = parsed(&[doc! {"$sample": {"size": 4}}]);
        let mut chain = build(&stages, &unused, &limits, &[]);
        for doc in grouped(50, 10) {
            assert_eq!(chain.push(doc).unwrap(), Flow::More, "never Done");
        }
        let out = chain.finish().unwrap();
        assert_eq!(out.len(), 4);
        let mut seen = ids(&out);
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), 4, "four different documents of the fifty");
        // A size past the input is the input, shuffled; a size of 0 is nothing.
        for (size, len) in [(80, 50), (0, 0)] {
            let stages = parsed(&[doc! {"$sample": {"size": size}}]);
            let mut chain = build(&stages, &unused, &limits, &[]);
            let _ = chain.push_all(grouped(50, 10)).unwrap();
            assert_eq!(chain.finish().unwrap().len(), len, "size {size}");
        }
    }

    #[test]
    fn the_reservoir_of_a_sample_is_charged_as_it_fills_and_turns_out_what_it_replaces() {
        let one = held_docs_bytes(&grouped(1, 300));
        let limits = Limits { buffer: BufferBudget::new(usize::MAX / 2), ..Limits::default() };
        let stages = parsed(&[doc! {"$sample": {"size": 3}}]);
        let mut chain = build(&stages, &unused, &limits, &[]);
        let mut seen = Vec::new();
        for doc in grouped(200, 300) {
            let _ = chain.push(doc).unwrap();
            seen.push(limits.buffer.used());
        }
        // Three documents of 300 bytes and no more, though the input is two hundred.
        assert!(seen[0] >= one && seen[0] < 2 * one, "{seen:?}");
        assert!(seen[2] >= 3 * one && seen[2] < 4 * one, "{seen:?}");
        let most = *seen.iter().max().unwrap();
        assert!(most < 4 * one, "never more than the three it keeps: {most}");
        // A document that replaces another is charged and the other given back, so the sizes
        // stay within a few bytes of each other (the ids differ in width).
        assert!(seen[3..].iter().all(|&used| used.abs_diff(seen[2]) < one / 4), "{seen:?}");
        assert_eq!(chain.finish().unwrap().len(), 3);
        assert_eq!(limits.buffer.used(), 0, "given back at the finish");
    }

    #[test]
    fn the_reservoir_of_a_sample_is_refused_at_the_byte_and_at_the_pipeline_limit() {
        let one = held_docs_bytes(&grouped(1, 300));
        let stages = parsed(&[doc! {"$sample": {"size": 10}}]);
        // Room for three of the ten it would keep: refused as the fourth arrives.
        let limits = Limits { buffer: BufferBudget::new(3 * one + one / 2), ..Limits::default() };
        let mut chain = build(&stages, &unused, &limits, &[]);
        let err =
            grouped(10, 300).into_iter().find_map(|doc| chain.push(doc).err()).expect("refused");
        assert!(
            matches!(&err, Error::ValueTooLarge { what: "the reservoir of $sample", .. }),
            "{err:?}"
        );
        drop(chain);
        assert_eq!(limits.buffer.used(), 0);
        // And a sample larger than the documents the pipeline may hold is refused when the
        // reservoir would hold more, which is not when it is asked for.
        let limits = Limits { max_documents: 3, ..Limits::default() };
        let mut chain = build(&stages, &unused, &limits, &[]);
        for doc in grouped(3, 10) {
            let _ = chain.push(doc).unwrap();
        }
        let err = chain.push(grouped(4, 10).remove(3)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid query: $sample produced 4 documents, over the pipeline limit of 3. Narrow \
             the pipeline with an earlier $match"
        );
    }

    /// Each stage kind alone, as it is written in a request.
    fn every_stage_spec() -> Vec<Document> {
        let mut all = vec![
            doc! {"$match": {"g": {"$gte": 0}}},
            doc! {"$project": {"g": 1}},
            doc! {"$addFields": {"y": 1}},
            doc! {"$set": {"y": 1}},
            doc! {"$replaceRoot": {"newRoot": "$$ROOT"}},
            doc! {"$replaceWith": "$$ROOT"},
            doc! {"$unset": "pad"},
            doc! {"$redact": "$$KEEP"},
            doc! {"$unwind": "$arr"},
            doc! {"$skip": 1},
            doc! {"$limit": 3},
            doc! {"$sort": {"g": 1}},
            doc! {"$sample": {"size": 2}},
            doc! {"$bucketAuto": {"groupBy": "$g", "buckets": 2}},
            doc! {"$setWindowFields": {"sortBy": {"g": 1}, "output": {
            "s": {"$sum": "$g", "window": {"documents": ["unbounded", "current"]}}}}},
            doc! {"$densify": {"field": "g", "range": {"step": 1, "bounds": "full"}}},
            doc! {"$fill": {"output": {"y": {"value": 0}}}},
            doc! {"$fill": {"sortBy": {"g": 1}, "output": {"y": {"method": "locf"}}}},
            doc! {"$facet": {"f": [{"$limit": 1}]}},
            doc! {"$lookup": {"from": "b", "localField": "g", "foreignField": "g", "as": "j"}},
            doc! {"$graphLookup": {"from": "b", "startWith": "$g", "connectFromField": "g",
            "connectToField": "g", "as": "w", "maxDepth": 1}},
            doc! {"$unionWith": "b"},
        ];
        all.extend(fused_specs());
        all
    }

    /// The documents the layout tests push: `{_id, g, pad, arr}`.
    fn laid_out(n: i32) -> Vec<Document> {
        (0..n).map(|i| doc! {"_id": i, "g": i % 3, "pad": "x", "arr": [i]}).collect()
    }

    /// What a chain built from `stages` does with documents, observed: the positions of the
    /// stages the runner was asked to run (the stages that block), and whether anything had
    /// reached the end of the chain before it was finished (the stages that stream pass
    /// documents on as they arrive, a state holds its answer to the end).
    fn observed(stages: &[Stage]) -> (Vec<usize>, bool) {
        let ran = RefCell::new(Vec::new());
        let run = |stage: &Stage, input: Vec<Document>, _: Option<Lease>| {
            ran.borrow_mut().push(std::ptr::from_ref(stage));
            Ok::<_, Error>(input)
        };
        let limits = Limits::default();
        let mut chain = build(stages, &run, &limits, &[]);
        for doc in laid_out(6) {
            let _ = chain.push(doc).unwrap();
        }
        let before_finish = !chain.out.borrow().docs.is_empty();
        let _ = chain.finish().unwrap();
        let blocks = (0..stages.len())
            .filter(|&at| ran.borrow().contains(&std::ptr::from_ref(&stages[at])))
            .collect();
        (blocks, before_finish)
    }

    #[test]
    fn what_a_stage_holds_is_what_the_chain_built_from_it_does() {
        for spec in every_stage_spec() {
            let stages = parsed(std::slice::from_ref(&spec));
            let [segment] = &segments(&stages)[..] else {
                panic!("{spec}: one stage, one segment")
            };
            assert_eq!(segment.stages, 0..stages.len(), "{spec}");
            let (blocks, before_finish) = observed(&stages);
            match segment.hold {
                Hold::Blocks => assert_eq!(blocks, [0], "{spec}: the runner runs it"),
                Hold::Streams => {
                    assert!(blocks.is_empty() && before_finish, "{spec}: it passes documents on");
                    assert!(streams(&stages[0]), "{spec}");
                }
                Hold::State(_) => {
                    assert!(blocks.is_empty() && !before_finish, "{spec}: it holds its answer");
                    assert!(
                        !streams(&stages[0]) || matches!(stages[0], Stage::Sample(_)),
                        "{spec}"
                    );
                }
            }
            // The head of a pipeline: the source streams into it unless it blocks.
            assert_eq!(streams_source(&stages), segment.hold != Hold::Blocks, "{spec}");
        }
    }

    #[test]
    fn the_state_a_stage_keeps_is_the_one_the_layout_names() {
        let kept_by = |spec: Document| match segments(&parsed(&[spec]))[0].hold {
            Hold::State(kept) => kept,
            other => panic!("{other:?}"),
        };
        assert_eq!(kept_by(doc! {"$count": "n"}), Kept::Counter);
        assert_eq!(kept_by(doc! {"$group": {"_id": "$g"}}), Kept::Groups);
        assert_eq!(kept_by(doc! {"$sortByCount": "$g"}), Kept::Groups);
        assert_eq!(
            kept_by(doc! {"$bucket": {"groupBy": "$g", "boundaries": [0, 2]}}),
            Kept::Groups
        );
        assert_eq!(kept_by(doc! {"$sample": {"size": 7}}), Kept::Reservoir { size: 7 });
    }

    /// The shapes of a `$sort` that a `$limit` follows, as the parse leaves them: where the chain
    /// has a heap, what it keeps and what runs after it. `[$sort, $project, $limit]` is the
    /// commonest, and is a heap because the reordering moved the limit ahead of the `$project`.
    #[test]
    fn a_sort_and_the_window_after_it_are_laid_out_as_a_heap() {
        let sort = || doc! {"$sort": {"g": 1}};
        let project = || doc! {"$project": {"g": 1}};
        let heap = |at: std::ops::Range<usize>, skip, limit| Segment {
            stages: at,
            hold: Hold::State(Kept::Heap { skip, limit }),
        };
        let run = |at: std::ops::Range<usize>| Segment { stages: at, hold: Hold::Streams };
        let blocks = |at: usize| Segment { stages: at..at + 1, hold: Hold::Blocks };
        for (specs, expected) in [
            (vec![sort(), doc! {"$limit": 3}], vec![heap(0..2, 0, 3)]),
            (vec![sort(), doc! {"$skip": 2}, doc! {"$limit": 3}], vec![heap(0..3, 2, 3)]),
            (vec![sort(), project(), doc! {"$limit": 3}], vec![heap(0..2, 0, 3), run(2..3)]),
            (
                vec![sort(), project(), doc! {"$skip": 2}, doc! {"$limit": 3}],
                vec![heap(0..3, 2, 3), run(3..4)],
            ),
            (vec![sort()], vec![blocks(0)]),
            (vec![sort(), doc! {"$skip": 2}], vec![blocks(0), run(1..2)]),
            (
                vec![sort(), doc! {"$match": {"g": 1}}, doc! {"$limit": 3}],
                vec![blocks(0), run(1..3)],
            ),
            (
                vec![project(), sort(), doc! {"$limit": 3}, doc! {"$unwind": "$arr"}],
                vec![run(0..1), heap(1..3, 0, 3), run(3..4)],
            ),
            (
                vec![sort(), doc! {"$limit": 3}, doc! {"$count": "n"}],
                vec![heap(0..2, 0, 3), Segment { stages: 2..3, hold: Hold::State(Kept::Counter) }],
            ),
        ] {
            let stages = parsed(&specs);
            assert_eq!(segments(&stages), expected, "{specs:?}");
            // What is built is what is laid out: the runner is asked for the stages that block,
            // and a heap holds its answer to the end.
            let (ran, _) = observed(&stages);
            let blocking: Vec<usize> = expected
                .iter()
                .filter(|segment| segment.hold == Hold::Blocks)
                .map(|segment| segment.stages.start)
                .collect();
            assert_eq!(ran, blocking, "{specs:?}");
        }
    }

    #[test]
    fn a_chain_of_barriers_lays_every_stage_out_as_a_barrier() {
        let stages =
            parsed(&[doc! {"$match": {"g": 1}}, doc! {"$sort": {"g": 1}}, doc! {"$limit": 3}]);
        assert_eq!(segments_by(&stages, false).len(), stages.len());
        assert!(segments_by(&stages, false).iter().all(|segment| segment.hold == Hold::Blocks));
    }

    #[test]
    fn the_end_of_a_chain_hands_over_the_lease_that_charged_its_output() {
        let limits = Limits::default();
        let stages = parsed(&[doc! {"$project": {"g": 1}}]);
        // Documents that arrive one at a time are charged as they do, and the lease is the
        // caller's to keep: the output is not walked a second time to be charged again.
        let mut chain = build(&stages, &unused, &limits, &[]);
        for doc in laid_out(4) {
            let _ = chain.push(doc).unwrap();
        }
        let (docs, lease) = chain.finish_held().unwrap();
        let lease = lease.expect("charged as the documents arrived");
        assert_eq!(docs.len(), 4);
        assert_eq!(lease.bytes(), held_docs_bytes(&docs));
        assert_eq!(limits.buffer.used(), lease.bytes(), "the bytes are still charged");
        // Another holder's name, and the same bytes.
        let lease = lease.renamed("the answer");
        assert_eq!(limits.buffer.used(), held_docs_bytes(&docs));
        drop(lease);
        assert_eq!(limits.buffer.used(), 0);

        // `finish` is `finish_held` that gives the lease back.
        let mut chain = build(&stages, &unused, &limits, &[]);
        let _ = chain.push_all(laid_out(4)).unwrap();
        let docs = chain.finish().unwrap();
        assert_eq!((docs.len(), limits.buffer.used()), (4, 0));

        // A `Vec` that reaches the end whole (a stage that blocks answered it) was charged by
        // nothing here, and there is no lease to hand over.
        let stages = vec![sort()];
        let run = |_: &Stage, input: Vec<Document>, _: Option<Lease>| Ok::<_, Error>(input);
        let mut chain = build(&stages, &run, &limits, &[]);
        let _ = chain.push_all(laid_out(4)).unwrap();
        let (docs, lease) = chain.finish_held().unwrap();
        assert!(lease.is_none() && docs.len() == 4);
    }

    #[test]
    fn the_end_of_a_chain_is_named_for_the_pipeline_it_ends() {
        let stages = parsed(&[doc! {"$limit": 100}]);
        let one = held_docs_bytes(&laid_out(1));
        let refused = |output: Option<&'static str>| {
            let limits = Limits { buffer: BufferBudget::new(2 * one), ..Limits::default() };
            let mut chain = match output {
                Some(name) => build_into(&stages, &unused, &limits, &[], name),
                None => build(&stages, &unused, &limits, &[]),
            };
            laid_out(4).into_iter().find_map(|doc| chain.push(doc).err()).expect("refused")
        };
        let what = |err: Error| match err {
            Error::ValueTooLarge { what, .. } => what,
            other => panic!("{other:?}"),
        };
        assert_eq!(what(refused(None)), "the output of the pipeline");
        assert_eq!(what(refused(Some(OUTPUT))), "the output of the pipeline");
        assert_eq!(
            what(refused(Some("the output of a sub-pipeline"))),
            "the output of a sub-pipeline"
        );
        assert_eq!(
            what(refused(Some("the output of a $facet branch"))),
            "the output of a $facet branch"
        );
    }
}
