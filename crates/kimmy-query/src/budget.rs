//! The bytes a pipeline's blocking stages hold (ADR-264).
//!
//! `$setWindowFields`, `$densify` and a `$fill` with a method buffer their whole input
//! and what they add to it. Every such buffer in one request is charged to **one**
//! [`BufferBudget`], carried in [`crate::aggregate::Limits`], so the stages of a
//! pipeline and the branches of a `$facet` cannot each take the whole cap. A stage
//! charges as its buffer grows and is refused with an [`Error::ValueTooLarge`] (`413`)
//! at the first byte over, before the work that would have filled it.
//!
//! A stage gives its bytes back when it ends, except under a `$facet`: while one runs,
//! every stage's bytes are kept until the `$facet` is done, because its branches'
//! outputs are alive together.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use kimmy_core::{Error, Result};

/// The most bytes the blocking stages of one request hold together: 256 MiB.
pub const MAX_BUFFER_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug)]
struct Inner {
    cap: usize,
    used: usize,
    /// How many `$facet`s are running.
    holds: usize,
    /// Bytes stages gave back under a hold, released with the last hold.
    deferred: usize,
}

/// A byte total shared by the stages of one pipeline. Cloning shares it.
#[derive(Clone, Debug)]
pub struct BufferBudget(Arc<Mutex<Inner>>);

impl Default for BufferBudget {
    fn default() -> Self {
        BufferBudget::new(MAX_BUFFER_BYTES)
    }
}

impl BufferBudget {
    /// A budget of `cap` bytes, not yet charged.
    pub fn new(cap: usize) -> Self {
        BufferBudget(Arc::new(Mutex::new(Inner { cap, used: 0, holds: 0, deferred: 0 })))
    }

    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The cap in bytes.
    pub fn cap(&self) -> usize {
        self.inner().cap
    }

    /// The bytes charged now.
    pub fn used(&self) -> usize {
        self.inner().used
    }

    /// A stage's account on this budget, named `what` in a refusal.
    pub(crate) fn lease(&self, what: &'static str) -> Lease {
        Lease { budget: self.clone(), what, own: 0 }
    }

    /// Keep every stage's bytes charged until the returned guard is dropped: taken by a
    /// `$facet`, whose branches' outputs are alive together.
    pub fn hold(&self) -> Hold {
        self.inner().holds += 1;
        Hold(self.clone())
    }
}

/// See [`BufferBudget::hold`].
#[derive(Debug)]
pub struct Hold(BufferBudget);

impl Drop for Hold {
    fn drop(&mut self) {
        let mut inner = self.0.inner();
        inner.holds = inner.holds.saturating_sub(1);
        if inner.holds == 0 {
            inner.used = inner.used.saturating_sub(inner.deferred);
            inner.deferred = 0;
        }
    }
}

/// What one stage has charged; it gives the bytes back when dropped.
#[derive(Debug)]
pub(crate) struct Lease {
    budget: BufferBudget,
    what: &'static str,
    own: usize,
}

impl Lease {
    /// Charge `bytes`; refused, without charging more, once the budget is over its cap.
    pub(crate) fn charge(&mut self, bytes: usize) -> Result<()> {
        let mut inner = self.budget.inner();
        inner.used = inner.used.saturating_add(bytes);
        self.own = self.own.saturating_add(bytes);
        if inner.used > inner.cap {
            return Err(Error::ValueTooLarge {
                what: self.what,
                bytes: inner.used,
                max: inner.cap,
                limit: "256 MiB limit on the bytes the blocking stages of one request hold \
                        (`$setWindowFields`, `$densify` and a `$fill` method)",
                remedy: "narrow the pipeline with an earlier $match or $project, or run the \
                         stage on a smaller input",
            });
        }
        Ok(())
    }

    /// Charge the encoded size of `doc`.
    pub(crate) fn charge_doc(&mut self, doc: &bson::Document) -> Result<()> {
        let bytes = bson::serialize_to_vec(doc)
            .map_err(|e| Error::Internal(format!("could not size a document: {e}")))?
            .len();
        self.charge(bytes)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut inner = self.budget.inner();
        if inner.holds > 0 {
            inner.deferred = inner.deferred.saturating_add(self.own);
        } else {
            inner.used = inner.used.saturating_sub(self.own);
        }
    }
}
