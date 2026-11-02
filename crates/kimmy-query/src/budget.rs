//! The bytes one aggregation holds, and the node-wide pool they come from (ADR-269,
//! which widens ADR-264).
//!
//! Every aggregation request carries **one** [`BufferBudget`], in
//! [`crate::aggregate::Limits`]. What the request holds is charged to it as it grows,
//! in an estimate of the bytes it takes parsed ([`held_bytes`]), by whoever holds it:
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
//! The unit is the estimate of a parsed document's memory ([`held_bytes`]), not its BSON
//! size: a long string is charged about its BSON size, and a document of many small fields
//! what it costs (400 integer fields: 23 times their BSON). It is not a ceiling on resident
//! memory (the transient copies of an answer being built are beyond it), only a bound
//! proportional to it.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bson::{Bson, Document};
use kimmy_core::{Error, Result};

/// What a `Default` budget may hold at once: 1 GiB. A fallback for tests and for code that
/// embeds the library; **a member does not run on it**: `kimmyd` derives
/// `aggregate.memory_limit_bytes` and `aggregate.memory_pool_bytes` from the memory it runs
/// under (a pool of a tenth of it, a request half of the pool) and sets them at startup.
pub const DEFAULT_MEMORY_LIMIT_BYTES: usize = 1024 * 1024 * 1024;

/// What a `Default` node-wide pool holds: 2 GiB, twice [`DEFAULT_MEMORY_LIMIT_BYTES`]. The
/// same fallback, not what a member runs on.
pub const DEFAULT_MEMORY_POOL_BYTES: usize = 2 * DEFAULT_MEMORY_LIMIT_BYTES;

/// What a refusal tells the caller to do about a request over its limit.
///
/// Only a leading `$match` narrows what the source reads (the executor reads the whole
/// collection otherwise, and charges it before any stage runs); a `$project`, `$limit` or
/// `$match` after it reduces what the later stages hold.
const REMEDY: &str = "a leading $match narrows what is read; $project, $limit or $match after it \
                      reduce what later stages hold; or raise aggregate.memory_limit_bytes";

/// The BSON size of `doc`, worked out without encoding it. What the 16 MiB per-value bounds
/// are held in; the budget charges [`held_bytes`].
pub fn doc_bytes(doc: &Document) -> usize {
    crate::expr::sizes::document_bytes(doc)
}

// ---------------------------------------------------------------------------------------
// The estimate of a parsed document's memory (ADR-269, the review's B2)
// ---------------------------------------------------------------------------------------
//
// The budget charges an estimate of what a document costs **parsed**, not its BSON size. A
// parsed `Document` is an `IndexMap<String, Bson>`: each field costs a bucket (hash, key
// `String`, a `Bson` slot as large as the largest variant), a share of the hash table, the
// key's heap block and whatever the value holds on the heap, and the tables grow by
// doubling. For a long string that is about its BSON size; for 400 integer fields it is 19
// times it. The estimate mirrors that layout from the types' own sizes (`size_of`), so it
// follows the `bson` crate if its layout changes, and `tests/held_calibration.rs` measures
// real documents with a counting allocator and fails when the estimate is below what they
// hold or more than 1.5 times it.

/// What the allocator really takes for a request of `n` bytes (glibc's rule: an 8-byte
/// header and 16-byte rounding, 32 bytes at least; a large block is mapped in whole pages).
fn heap_block(n: usize) -> usize {
    match n {
        0 => 0,
        1..=131_071 => ((n + 8 + 15) & !15).max(32),
        _ => n.saturating_add(16 + 4095) & !4095,
    }
}

/// The buckets a hash table of `n` entries is made of, as `hashbrown` sizes it.
fn table_buckets(n: usize) -> usize {
    match n {
        0 => 0,
        1..=3 => 4,
        4..=7 => 8,
        _ => (n.saturating_mul(8) / 7 + 1).next_power_of_two(),
    }
}

/// The entries a table of `buckets` buckets holds before it grows (7/8 of them).
fn table_capacity(buckets: usize) -> usize {
    if buckets <= 8 { buckets.saturating_sub(1) } else { buckets / 8 * 7 }
}

/// The heap a document of `n` fields takes for its table and its entries, whatever the
/// fields hold: `indices` (a `usize` and a control byte for each bucket, and a group of
/// control bytes) and the entries, reserved up to the table's capacity.
fn doc_frame(n: usize) -> usize {
    let buckets = table_buckets(n);
    if buckets == 0 {
        return 0;
    }
    let entry = 8 + std::mem::size_of::<String>() + std::mem::size_of::<Bson>();
    heap_block(buckets * 9 + 16) + heap_block(table_capacity(buckets) * entry)
}

/// The heap a value holds, beyond its slot (a `Bson` is as large as its largest variant,
/// and every value sits in one: a field's, an array element's).
fn value_heap(value: &Bson) -> usize {
    match value {
        Bson::String(s) | Bson::JavaScriptCode(s) | Bson::Symbol(s) => heap_block(s.len()),
        Bson::Binary(b) => heap_block(b.bytes.len()),
        Bson::Document(d) => doc_heap(d),
        Bson::Array(items) => array_heap(items),
        Bson::RegularExpression(r) => {
            heap_block(r.pattern.as_str().len()) + heap_block(r.options.as_str().len())
        }
        Bson::JavaScriptCodeWithScope(c) => heap_block(c.code.len()) + doc_heap(&c.scope),
        _ => 0,
    }
}

/// The heap an array of `items` takes: its elements' slots (a `Vec` grown by doubling, four
/// at the least) and what they hold.
fn array_heap(items: &[Bson]) -> usize {
    let slots = match items.len() {
        0 => 0,
        n => n.next_power_of_two().max(4),
    };
    heap_block(slots * std::mem::size_of::<Bson>())
        + items.iter().fold(0usize, |sum, v| sum.saturating_add(value_heap(v)))
}

/// The heap a document takes, its own struct excluded: the frame, and for each field its
/// key and its value.
fn doc_heap(doc: &Document) -> usize {
    doc.iter().fold(doc_frame(doc.len()), |sum, (key, value)| {
        sum.saturating_add(heap_block(key.len())).saturating_add(value_heap(value))
    })
}

/// A safety margin over the layout above, in sixteenths: the allocator's fragmentation and
/// the slack of the tables of documents that were built field by field.
const HELD_MARGIN_SIXTEENTHS: usize = 2;

fn with_margin(bytes: usize) -> usize {
    bytes.saturating_add(bytes / 16 * HELD_MARGIN_SIXTEENTHS)
}

/// The estimate of the memory a parsed document holds: the document itself and everything
/// it points to, in one walk over its fields and their values, recursively over arrays and
/// nested documents. No serialisation. This is what the budget charges for a document.
pub fn held_bytes(doc: &Document) -> usize {
    with_margin(std::mem::size_of::<Document>().saturating_add(doc_heap(doc)))
}

/// The estimate for a value held on its own (an array element, a bucket key, a kept
/// accumulator value): its slot and what it holds.
pub fn held_bytes_bson(value: &Bson) -> usize {
    with_margin(std::mem::size_of::<Bson>().saturating_add(value_heap(value)))
}

/// The estimate of the heap a field `name` holding `value` adds to a document: its key and
/// what its value holds. Its bucket is in the document's table, which is
/// [`held_frame_growth`]'s.
pub fn held_element(name: &str, value: &Bson) -> usize {
    with_margin(heap_block(name.len()).saturating_add(value_heap(value)))
}

/// The estimate of what the table of a document of `len` fields grows by when `added` fields
/// are put into it: nothing while the capacity it already has holds them, and a doubling
/// when it does not (which is as much as the table already holds). Worked out from the
/// document's length alone.
pub fn held_frame_growth(len: usize, added: usize) -> usize {
    with_margin(doc_frame(len.saturating_add(added)).saturating_sub(doc_frame(len)))
}

/// The estimate for a key kept in a hash set of byte keys (a `$group` index, an
/// `$addToSet` member's identity): the `Vec` header, its table share and its block.
pub fn held_set_key(len: usize) -> usize {
    with_margin(std::mem::size_of::<Vec<u8>>() + 2 * 9 + heap_block(len))
}

/// The estimate of a list of documents.
pub fn held_docs_bytes(docs: &[Document]) -> usize {
    docs.iter().fold(0usize, |sum, doc| sum.saturating_add(held_bytes(doc)))
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

/// How long a request that is not at the youngest end waits for the pool when nothing gives
/// it a deadline (a request served over HTTP always has one: `BufferBudget::wait_until`).
pub const POOL_WAIT_DEFAULT: Duration = Duration::from_secs(5);

/// The longest a waiting request sleeps before it looks at the pool again; a release wakes
/// it sooner.
const POOL_WAIT_SLICE: Duration = Duration::from_millis(5);

/// The requests that hold a reservation, in the order they first reserved (ADR-269).
#[derive(Debug, Default)]
struct Order {
    next: u64,
    holders: BTreeSet<u64>,
}

#[derive(Debug)]
struct PoolInner {
    cap: usize,
    used: AtomicUsize,
    /// Who holds a reservation, oldest first, and the wake-up for a request that waits.
    order: Mutex<Order>,
    freed: Condvar,
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
            order: Mutex::new(Order::default()),
            freed: Condvar::new(),
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
                Ok(_) => break,
                Err(now) => used = now,
            }
        }
        self.0.freed.notify_all();
    }

    fn order(&self) -> MutexGuard<'_, Order> {
        self.0.order.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A place in the order for a request that is about to reserve for the first time.
    fn register(&self) -> u64 {
        let mut order = self.order();
        order.next += 1;
        order.next
    }

    /// `seq` holds a reservation now.
    fn hold(&self, seq: u64) {
        self.order().holders.insert(seq);
    }

    /// `seq` holds nothing now.
    fn unhold(&self, seq: u64) {
        self.order().holders.remove(&seq);
        self.0.freed.notify_all();
    }

    /// Whether no request that holds a reservation is younger than `seq`: the one that is
    /// refused when the pool is full, where an older one waits.
    fn is_youngest(&self, seq: u64) -> bool {
        self.order().holders.last().is_none_or(|youngest| *youngest <= seq)
    }

    /// The requests that hold a reservation now.
    pub fn holders(&self) -> usize {
        self.order().holders.len()
    }

    /// Sleep until a release, or `slice` at the most.
    fn wait_for_release(&self, slice: Duration) {
        let order = self.order();
        let _ = self.0.freed.wait_timeout(order, slice);
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
    /// This request's place in the pool's order, taken when it first asks the pool for bytes.
    seq: Option<u64>,
    /// When a request that waits for the pool gives up.
    wait_until: Option<Instant>,
}

impl Inner {
    /// Give the pool back what is no longer used.
    fn settle(&mut self) {
        if self.reserved > self.used {
            if let Some(pool) = &self.pool {
                pool.release(self.reserved - self.used);
            }
            self.reserved = self.used;
            self.leave_if_empty();
        }
    }

    /// A request that holds nothing is no holder.
    fn leave_if_empty(&self) {
        if self.reserved == 0
            && let (Some(pool), Some(seq)) = (&self.pool, self.seq)
        {
            pool.unhold(seq);
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(pool) = &self.pool {
            pool.release(self.reserved);
            if let Some(seq) = self.seq {
                pool.unhold(seq);
            }
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
        BufferBudget(Arc::new(Mutex::new(Inner {
            cap,
            used: 0,
            pool: None,
            reserved: 0,
            seq: None,
            wait_until: None,
        })))
    }

    /// A budget of `cap` bytes that reserves what it holds from `pool`.
    pub fn pooled(cap: usize, pool: &MemoryPool) -> Self {
        BufferBudget(Arc::new(Mutex::new(Inner {
            cap,
            used: 0,
            pool: Some(pool.clone()),
            reserved: 0,
            seq: None,
            wait_until: None,
        })))
    }

    /// Where a request of this budget that finds the pool full and is not the youngest holder
    /// gives up waiting for it, and is refused (`503`). A request served over HTTP sets its
    /// own deadline here; without one the wait is [`POOL_WAIT_DEFAULT`].
    pub fn wait_until(&self, at: Instant) {
        self.inner().wait_until = Some(at);
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
        if let Some(pool) = inner.pool.clone()
            && want > 0
        {
            let seq = *inner.seq.get_or_insert_with(|| pool.register());
            // Oldest first (ADR-269): when the pool is full, the youngest holder is refused,
            // and an older one waits for bytes to be freed, until its deadline. A request that
            // alone would pass the whole pool is refused at once: waiting cannot help it.
            let give_up = inner.wait_until.unwrap_or_else(|| Instant::now() + POOL_WAIT_DEFAULT);
            while !pool.try_reserve(want) {
                let now = Instant::now();
                if inner.used > pool.cap() || pool.is_youngest(seq) || now >= give_up {
                    // A holder that was younger may have been refused, and given its bytes
                    // back, since the pool was looked at: it is looked at once more (a
                    // release is made before its holder leaves the order, so one that made
                    // this request the youngest is seen here).
                    if inner.used <= pool.cap() && pool.try_reserve(want) {
                        break;
                    }
                    pool.0.pool_refusals.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::MemoryBusy {
                        what: self.what,
                        bytes: want,
                        pool: pool.cap(),
                        in_use: pool.used(),
                    });
                }
                pool.wait_for_release(POOL_WAIT_SLICE.min(give_up - now));
            }
            if inner.reserved == 0 {
                pool.hold(seq);
            }
            inner.reserved += want;
        }
        Ok(())
    }

    /// Charge the estimate of the memory `doc` holds parsed ([`held_bytes`]).
    pub fn charge_doc(&mut self, doc: &Document) -> Result<()> {
        self.charge(held_bytes(doc))
    }

    /// Charge the estimate of each document in `docs`, stopping at the first
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
