//! The bytes one aggregation holds, and the node-wide pool they come from (ADR-269,
//! which widens ADR-264).
//!
//! Every aggregation request carries **one** [`BufferBudget`], in
//! [`crate::aggregate::Limits`]. What the request holds is charged to it as it grows,
//! in BSON bytes, by whoever holds it:
//!
//! - the source read, document by document, so a read that passes the limit stops at
//!   the document that passes it;
//! - each stage's input and its output while the stage runs (the input stays charged
//!   until the stage returns, so the peak of `$addFields` is input plus output);
//! - a `$group`'s buckets and the values its accumulators keep, a `$lookup`'s joined
//!   arrays, and what `$setWindowFields`, `$densify` and `$fill` buffer;
//! - the answer, as it is built.
//!
//! A charge that takes the request over its limit is refused with an
//! [`Error::ValueTooLarge`] (`413`) naming what held the memory, before the work that
//! would have filled it. A budget made with [`BufferBudget::pooled`] also reserves what
//! it holds from a [`MemoryPool`] shared by every aggregation on the node, and a charge
//! the pool cannot cover is refused with [`Error::MemoryBusy`] (`503`). A budget gives
//! its bytes back to the pool as they are released, and all of them when it is dropped,
//! so a request that fails, is cut by its deadline or panics returns them as well.
//!
//! The unit is the BSON size of a document. A document of many small fields costs more
//! than that in memory, so the limit is not a ceiling on resident memory, only a bound
//! proportional to it.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bson::Document;
use kimmy_core::{Error, Result};

/// The default bytes one aggregation may hold at once: 1 GiB (`aggregate.memory_limit_bytes`).
pub const DEFAULT_MEMORY_LIMIT_BYTES: usize = 1024 * 1024 * 1024;

/// The default bytes all concurrent aggregations on a node may hold together: 2 GiB
/// (`aggregate.memory_pool_bytes`).
pub const DEFAULT_MEMORY_POOL_BYTES: usize = 2 * DEFAULT_MEMORY_LIMIT_BYTES;

/// What a refusal tells the caller to do about a request over its limit.
const REMEDY: &str = "add $match, $project or $limit early, or raise aggregate.memory_limit_bytes";

/// The BSON size of `doc`, worked out without encoding it.
pub fn doc_bytes(doc: &Document) -> usize {
    crate::expr::sizes::document_bytes(doc)
}

/// The bytes an array of documents takes as a field's value, given each document's size:
/// the array's framing and, for each element, its type byte, its decimal index and a
/// terminator (as `expr::sizes` counts an array).
pub fn array_of_docs_bytes(sizes: impl IntoIterator<Item = usize>) -> usize {
    sizes.into_iter().enumerate().fold(5usize, |sum, (index, size)| {
        let digits = index.checked_ilog10().map_or(1, |d| d as usize + 1);
        sum.saturating_add(2 + digits).saturating_add(size)
    })
}

/// The BSON size of a list of documents.
pub fn docs_bytes(docs: &[Document]) -> usize {
    docs.iter().fold(0usize, |sum, doc| sum.saturating_add(doc_bytes(doc)))
}

#[derive(Debug)]
struct PoolInner {
    cap: usize,
    used: AtomicUsize,
    /// Charges refused for taking one request over its own limit, and charges refused
    /// because the pool was empty: what `/metrics` counts.
    limit_refusals: AtomicU64,
    pool_refusals: AtomicU64,
}

/// A byte total shared by every aggregation on a node. Cloning shares it.
#[derive(Clone, Debug)]
pub struct MemoryPool(Arc<PoolInner>);

impl MemoryPool {
    /// A pool of `cap` bytes, none reserved.
    pub fn new(cap: usize) -> Self {
        MemoryPool(Arc::new(PoolInner {
            cap,
            used: AtomicUsize::new(0),
            limit_refusals: AtomicU64::new(0),
            pool_refusals: AtomicU64::new(0),
        }))
    }

    /// The pool's size in bytes.
    pub fn cap(&self) -> usize {
        self.0.cap
    }

    /// The bytes reserved now.
    pub fn used(&self) -> usize {
        self.0.used.load(Ordering::Acquire)
    }

    /// Requests refused for passing their own limit, since the pool was made.
    pub fn limit_refusals(&self) -> u64 {
        self.0.limit_refusals.load(Ordering::Relaxed)
    }

    /// Requests refused because the pool had not the bytes, since the pool was made.
    pub fn pool_refusals(&self) -> u64 {
        self.0.pool_refusals.load(Ordering::Relaxed)
    }

    /// Reserve `bytes` if the pool has them; whether it did. Atomic, so two requests
    /// cannot both take the last of it.
    fn try_reserve(&self, bytes: usize) -> bool {
        let mut used = self.0.used.load(Ordering::Acquire);
        loop {
            let Some(total) = used.checked_add(bytes).filter(|total| *total <= self.0.cap) else {
                return false;
            };
            match self.0.used.compare_exchange_weak(
                used,
                total,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(now) => used = now,
            }
        }
    }

    fn release(&self, bytes: usize) {
        // Saturating, so a pool that is somehow told to return more than it gave stops at
        // zero rather than wrapping to a number no request could ever reserve against.
        let mut used = self.0.used.load(Ordering::Acquire);
        loop {
            match self.0.used.compare_exchange_weak(
                used,
                used.saturating_sub(bytes),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(now) => used = now,
            }
        }
    }
}

#[derive(Debug)]
struct Inner {
    cap: usize,
    used: usize,
    pool: Option<MemoryPool>,
    /// How much of `used` the pool has been asked for and gave. Never over `used`: a
    /// charge the pool refused is in `used` (its lease gives it back) and not here.
    reserved: usize,
}

impl Inner {
    /// Give the pool back what is no longer used.
    fn settle(&mut self) {
        if self.reserved > self.used {
            if let Some(pool) = &self.pool {
                pool.release(self.reserved - self.used);
            }
            self.reserved = self.used;
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(pool) = &self.pool {
            pool.release(self.reserved);
        }
    }
}

/// A byte total shared by everything one aggregation holds. Cloning shares it.
#[derive(Clone, Debug)]
pub struct BufferBudget(Arc<Mutex<Inner>>);

impl Default for BufferBudget {
    fn default() -> Self {
        BufferBudget::new(DEFAULT_MEMORY_LIMIT_BYTES)
    }
}

impl BufferBudget {
    /// A budget of `cap` bytes, not yet charged, drawing on no pool.
    pub fn new(cap: usize) -> Self {
        BufferBudget(Arc::new(Mutex::new(Inner { cap, used: 0, pool: None, reserved: 0 })))
    }

    /// A budget of `cap` bytes that reserves what it holds from `pool`.
    pub fn pooled(cap: usize, pool: &MemoryPool) -> Self {
        BufferBudget(Arc::new(Mutex::new(Inner {
            cap,
            used: 0,
            pool: Some(pool.clone()),
            reserved: 0,
        })))
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

    /// The bytes this budget has reserved from its pool now (zero with no pool).
    pub fn reserved(&self) -> usize {
        self.inner().reserved
    }

    /// An account on this budget, named `what` in a refusal.
    pub fn lease(&self, what: &'static str) -> Lease {
        Lease { budget: self.clone(), what, own: 0 }
    }
}

/// What one holder has charged; it gives the bytes back when dropped.
#[derive(Debug)]
pub struct Lease {
    budget: BufferBudget,
    what: &'static str,
    own: usize,
}

impl Lease {
    /// Charge `bytes`; refused, without charging more, once the budget is over its cap
    /// (`413`) or the pool cannot cover it (`503`). The refused bytes stay on the lease
    /// and go back when it is dropped.
    pub fn charge(&mut self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let mut inner = self.budget.inner();
        inner.used = inner.used.saturating_add(bytes);
        self.own = self.own.saturating_add(bytes);
        if inner.used > inner.cap {
            if let Some(pool) = &inner.pool {
                pool.0.limit_refusals.fetch_add(1, Ordering::Relaxed);
            }
            return Err(Error::ValueTooLarge {
                what: self.what,
                bytes: inner.used,
                max: inner.cap,
                limit: format!(
                    "limit of {} bytes one aggregation may hold at once \
                     (aggregate.memory_limit_bytes)",
                    inner.cap
                ),
                remedy: REMEDY,
            });
        }
        let want = inner.used - inner.reserved;
        if let Some(pool) = inner.pool.clone() {
            if want > 0 {
                if !pool.try_reserve(want) {
                    pool.0.pool_refusals.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::MemoryBusy {
                        what: self.what,
                        bytes: want,
                        pool: pool.cap(),
                        in_use: pool.used(),
                    });
                }
                inner.reserved += want;
            }
        }
        Ok(())
    }

    /// Charge the encoded size of `doc`.
    pub fn charge_doc(&mut self, doc: &Document) -> Result<()> {
        self.charge(doc_bytes(doc))
    }

    /// Charge the encoded size of each document in `docs`, stopping at the first
    /// refusal: the walk is as far as the budget goes, not the whole list.
    pub fn charge_docs(&mut self, docs: &[Document]) -> Result<()> {
        docs.iter().try_for_each(|doc| self.charge_doc(doc))
    }

    /// Give back `bytes` of what this lease charged (at most what it holds).
    pub fn release(&mut self, bytes: usize) {
        let bytes = bytes.min(self.own);
        if bytes == 0 {
            return;
        }
        let mut inner = self.budget.inner();
        inner.used = inner.used.saturating_sub(bytes);
        self.own -= bytes;
        inner.settle();
    }

    /// The bytes this lease holds.
    pub fn bytes(&self) -> usize {
        self.own
    }

    /// Change what the lease holds from `before` to `after` bytes: charges a growth,
    /// releases a shrink.
    pub fn resize(&mut self, before: usize, after: usize) -> Result<()> {
        if after >= before {
            self.charge(after - before)
        } else {
            self.release(before - after);
            Ok(())
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut inner = self.budget.inner();
        inner.used = inner.used.saturating_sub(self.own);
        inner.settle();
    }
}

/// What a stage's output is named in a refusal for the bytes it holds.
pub(crate) fn output_of(stage: &str) -> &'static str {
    match stage {
        "$project" => "the output of $project (with its input, still held)",
        "$addFields" | "$set" => "the output of $addFields (with its input, still held)",
        "$replaceRoot" | "$replaceWith" => {
            "the output of $replaceRoot (with its input, still held)"
        }
        "$unwind" => "the output of $unwind (with its input, still held)",
        "$group" => "the buckets of $group (with its input, still held)",
        "$bucket" => "the buckets of $bucket (with its input, still held)",
        "$bucketAuto" => "the buckets of $bucketAuto (with its input, still held)",
        "$sortByCount" => "the buckets of $sortByCount (with its input, still held)",
        "$redact" => "the output of $redact (with its input, still held)",
        "$lookup" => "the output of $lookup (with its input, still held)",
        "$graphLookup" => "the output of $graphLookup (with its input, still held)",
        "$unionWith" => "the output of $unionWith (with its input, still held)",
        "$facet" => "the output of $facet (with its input, still held)",
        _ => "the output of a stage (with its input, still held)",
    }
}

/// What a stage's input is named in a refusal for the bytes it holds.
pub(crate) fn input_of(stage: &str) -> &'static str {
    match stage {
        "$match" => "the input of $match",
        "$project" => "the input of $project",
        "$addFields" | "$set" => "the input of $addFields",
        "$replaceRoot" | "$replaceWith" => "the input of $replaceRoot",
        "$sort" => "the input of $sort",
        "$limit" => "the input of $limit",
        "$skip" => "the input of $skip",
        "$unwind" => "the input of $unwind",
        "$group" => "the input of $group",
        "$count" => "the input of $count",
        "$lookup" => "the input of $lookup",
        "$facet" => "the input of $facet",
        "$bucket" => "the input of $bucket",
        "$bucketAuto" => "the input of $bucketAuto",
        "$graphLookup" => "the input of $graphLookup",
        "$sortByCount" => "the input of $sortByCount",
        "$sample" => "the input of $sample",
        "$redact" => "the input of $redact",
        "$unionWith" => "the input of $unionWith",
        _ => "the input of a stage",
    }
}
