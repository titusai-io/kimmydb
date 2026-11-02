//! The push pipeline (ADR-270): a pipeline's stages as a chain of sinks, each taking one
//! document and pushing zero or more on to the next, and answering whether it still wants
//! input.
//!
//! The executor used to run a pipeline as a loop of `docs = stage(docs)`, each stage taking
//! the whole input and building the whole output. A chain is the same pipeline with the
//! stages' hands joined: a source pushes documents into the head, and the tail collects
//! what comes out. A stage that holds nothing between documents can then be a sink that
//! passes each one on, and a full `$limit` can tell the source to stop reading ([`Flow`]).
//!
//! # Every stage is a barrier, for now
//!
//! [`build`] makes every stage a [`Barrier`]: it collects its input, and when the input is
//! complete ([`Sink::finish`]) runs the stage as it always ran, through the runner the
//! caller supplies, and pushes the output on. The runner is where the stage's code lives,
//! so what a stage charges to the request's budget, the order in which stages fail and the
//! deadline checks are those of the loop this replaces, not a copy of them. A stage that
//! streams later takes the place of its barrier in [`build`] and in nothing else.
//!
//! The chain is not recursive: a sink does not finish the sink after it, [`Chain::finish`]
//! walks from the head to the tail, and a chain is taken apart the same way. A pipeline of
//! a hundred thousand stages (the body limit allows about that many of the shortest) must
//! not be a hundred thousand frames deep.
//!
//! # The request deadline (ADR-266)
//!
//! A sink that loops over documents ticks a `deadline::Ticker`, as every loop does (see
//! [`crate::aggregate`]): the default [`Sink::push_all`] does. A barrier does not loop; the
//! stage it runs checks and ticks for itself.

use std::cell::RefCell;
use std::rc::Rc;

use bson::Document;
use kimmy_core::Error;

use crate::aggregate::Stage;
use crate::deadline::Ticker;

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

/// How a stage is run: the stage and its whole input, to its whole output. The executor
/// that owns a storage handle supplies one that sends the stages that read storage to the
/// functions that do, and the rest to [`crate::aggregate::apply_with_vars`].
pub type Runner<'a, E> = &'a dyn Fn(&Stage, Vec<Document>) -> Result<Vec<Document>, E>;

/// A pipeline made into sinks, ready for documents. Push the source into it, then
/// [`Chain::finish`] it for the output.
pub struct Chain<'a, E: From<Error>> {
    head: Link<'a, E>,
    out: Rc<RefCell<Vec<Document>>>,
}

/// Make `stages` into a chain: one sink per stage, in order, ending in a collector.
pub fn build<'a, E: From<Error> + 'a>(stages: &'a [Stage], run: Runner<'a, E>) -> Chain<'a, E> {
    let out = Rc::new(RefCell::new(Vec::new()));
    let mut head: Link<'a, E> = Box::new(Collector { out: Rc::clone(&out) });
    for stage in stages.iter().rev() {
        head = Box::new(Barrier { stage, run, held: Vec::new(), next: Some(head) });
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

    /// The source is complete: finish every sink in order, and answer what came out of the
    /// last. The first sink to fail ends it, and the ones after it have run nothing.
    pub fn finish(mut self) -> Result<Vec<Document>, E> {
        let mut at: Option<&mut dyn Sink<'a, E>> = Some(&mut *self.head);
        while let Some(sink) = at {
            sink.finish()?;
            at = sink.next_mut();
        }
        Ok(std::mem::take(&mut *self.out.borrow_mut()))
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

/// A stage that cannot answer before it has seen everything it is given: it collects its
/// input, and at [`Sink::finish`] runs the stage on all of it.
struct Barrier<'a, E: From<Error>> {
    stage: &'a Stage,
    run: Runner<'a, E>,
    held: Vec<Document>,
    /// `None` only once [`Sink::detach`] has taken it.
    next: Option<Link<'a, E>>,
}

impl<'a, E: From<Error>> Sink<'a, E> for Barrier<'a, E> {
    fn push(&mut self, doc: Document) -> Result<Flow, E> {
        self.held.push(doc);
        Ok(Flow::More)
    }

    fn push_all(&mut self, docs: Vec<Document>) -> Result<Flow, E> {
        take_or_extend(&mut self.held, docs);
        Ok(Flow::More)
    }

    fn finish(&mut self) -> Result<(), E> {
        let input = std::mem::take(&mut self.held);
        let output = (self.run)(self.stage, input)?;
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
struct Collector {
    out: Rc<RefCell<Vec<Document>>>,
}

impl<'a, E: From<Error>> Sink<'a, E> for Collector {
    fn push(&mut self, doc: Document) -> Result<Flow, E> {
        self.out.borrow_mut().push(doc);
        Ok(Flow::More)
    }

    fn push_all(&mut self, docs: Vec<Document>) -> Result<Flow, E> {
        take_or_extend(&mut self.out.borrow_mut(), docs);
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

    use bson::doc;

    use super::*;

    type Calls = RefCell<Vec<(&'static str, Vec<i32>)>>;

    fn docs(ids: &[i32]) -> Vec<Document> {
        ids.iter().map(|i| doc! {"_id": *i}).collect()
    }

    fn ids(docs: &[Document]) -> Vec<i32> {
        docs.iter().map(|d| d.get_i32("_id").unwrap()).collect()
    }

    /// A runner that records which stage ran on which input, and passes the input through
    /// but for `$limit`, which keeps `n` and `$skip`, which drops `n`.
    fn recording(calls: &Calls) -> impl Fn(&Stage, Vec<Document>) -> Result<Vec<Document>, Error> {
        move |stage, input| {
            calls.borrow_mut().push((stage.name(), ids(&input)));
            Ok(match stage {
                Stage::Limit(n) => input.into_iter().take(*n).collect(),
                Stage::Skip(n) => input.into_iter().skip(*n).collect(),
                _ => input,
            })
        }
    }

    fn pipeline() -> Vec<Stage> {
        vec![Stage::Skip(1), Stage::Limit(2), Stage::Count("n".into())]
    }

    #[test]
    fn no_stages_answer_the_source() {
        let run = |_: &Stage, _: Vec<Document>| -> Result<Vec<Document>, Error> {
            panic!("there is no stage to run")
        };
        let mut chain = build(&[], &run);
        assert_eq!(chain.push_all(docs(&[3, 1, 2])).unwrap(), Flow::More);
        assert_eq!(ids(&chain.finish().unwrap()), [3, 1, 2]);
    }

    #[test]
    fn each_stage_runs_once_on_everything_the_one_before_answered() {
        let calls = Calls::default();
        let run = recording(&calls);
        let stages = pipeline();
        let mut chain = build(&stages, &run);
        assert_eq!(chain.push_all(docs(&[1, 2, 3, 4])).unwrap(), Flow::More);
        let out = chain.finish().unwrap();
        assert_eq!(ids(&out), [2, 3]);
        assert_eq!(
            *calls.borrow(),
            [("$skip", vec![1, 2, 3, 4]), ("$limit", vec![2, 3, 4]), ("$count", vec![2, 3])]
        );
    }

    #[test]
    fn nothing_runs_until_the_chain_is_finished() {
        let calls = Calls::default();
        let run = recording(&calls);
        let stages = pipeline();
        let mut chain = build(&stages, &run);
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
        let stages = [Stage::Skip(0)];
        let mut chain = build(&stages, &run);
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
        let run = |_: &Stage, input: Vec<Document>| -> Result<Vec<Document>, Error> {
            assert_eq!(input.as_ptr(), at, "the stage was given a copy");
            Ok(input)
        };
        let stages = [Stage::Skip(0), Stage::Limit(10)];
        let mut chain = build(&stages, &run);
        let _ = chain.push_all(source).unwrap();
        let out = chain.finish().unwrap();
        assert_eq!(out.as_ptr(), at, "the collector was given a copy");
    }

    #[test]
    fn the_first_failure_ends_the_run_and_later_stages_run_nothing() {
        let calls = Calls::default();
        let run = |stage: &Stage, input: Vec<Document>| -> Result<Vec<Document>, Error> {
            calls.borrow_mut().push((stage.name(), ids(&input)));
            match stage {
                Stage::Limit(_) => Err(Error::InvalidQuery("the limit failed".into())),
                _ => Ok(input),
            }
        };
        let stages = pipeline();
        let mut chain = build(&stages, &run);
        let _ = chain.push_all(docs(&[1, 2])).unwrap();
        let err = chain.finish().unwrap_err();
        assert_eq!(err.to_string(), "invalid query: the limit failed");
        let names: Vec<_> = calls.borrow().iter().map(|(name, _)| *name).collect();
        assert_eq!(names, ["$skip", "$limit"]);
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
        let run = |_: &Stage, _: Vec<Document>| -> Result<Vec<Document>, Mine> {
            Err(Mine::Stage("mine"))
        };
        let stages = [Stage::Skip(0)];
        let mut chain = build(&stages, &run);
        let _ = chain.push_all(docs(&[1])).unwrap();
        assert_eq!(chain.finish().unwrap_err(), Mine::Stage("mine"));
    }

    #[test]
    fn a_pipeline_of_many_stages_is_neither_a_deep_call_nor_a_deep_drop() {
        let run = |_: &Stage, input: Vec<Document>| -> Result<Vec<Document>, Error> { Ok(input) };
        let stages = vec![Stage::Skip(0); 300_000];
        let mut chain = build(&stages, &run);
        let _ = chain.push_all(docs(&[1, 2])).unwrap();
        assert_eq!(ids(&chain.finish().unwrap()), [1, 2]);
        // And dropped unfinished.
        let chain = build(&stages, &run);
        drop(chain);
    }

    /// A sink that takes `room` documents and then says `Done`.
    struct Fills {
        room: usize,
        got: Vec<i32>,
    }

    impl<'a> Sink<'a, Error> for Fills {
        fn push(&mut self, doc: Document) -> Result<Flow, Error> {
            self.got.push(doc.get_i32("_id").unwrap());
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
        let mut sink = Fills { room: 2, got: Vec::new() };
        assert_eq!(sink.push_all(docs(&[7, 8, 9, 10])).unwrap(), Flow::Done);
        assert_eq!(sink.got, [7, 8]);
        // Never Done: More, with everything pushed.
        let mut roomy = Fills { room: 10, got: Vec::new() };
        assert_eq!(roomy.push_all(docs(&[7, 8, 9])).unwrap(), Flow::More);
        assert_eq!(roomy.got, [7, 8, 9]);
        // Nothing to push is More, too.
        assert_eq!(roomy.push_all(Vec::new()).unwrap(), Flow::More);
    }

    #[test]
    fn push_all_ticks_the_deadline() {
        use crate::deadline::Deadline;
        use std::time::Duration;

        let mut sink = Fills { room: usize::MAX, got: Vec::new() };
        let _guard = Deadline::scope(Some(Duration::ZERO));
        let err = sink.push_all(docs(&[1, 2, 3])).unwrap_err();
        assert!(matches!(err, Error::DeadlineExceeded), "{err:?}");
        assert!(sink.got.is_empty(), "a document was pushed after the deadline");
    }
}
