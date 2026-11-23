//! What a request resolved (ADR-272, decision 11; Q6.4 item 4). An aggregation does not run on one read snapshot: each stage reads
//! storage when it runs, so a pipeline that reads one name twice (the source and a `$lookup` of it, a `$facet` branch, a
//! sub-pipeline) could read two different physicals across a swap. The request therefore resolves each name it reads ONCE, at its
//! first use, and every later use of the name answers from the memo; and a sink that is about to write checks, after the last read
//! and before its first write, that no physical the request read has been buried since: if one has, nothing is written and the
//! request answers the retryable `503`. Burial is monotone, so unburied at the end means unburied throughout.
//!
//! The memo is per thread and per request, like the request's deadline: the synchronous part of an aggregation runs on one thread
//! under one [`Scope`], and a scope inside another (the pipeline is run from a scan and from a finish) joins the outer one.

use std::cell::RefCell;

use kimmy_storage::CollectionMeta;

use super::{ApiError, SharedState};

struct Entry {
    db: String,
    name: String,
    meta: CollectionMeta,
}

thread_local! {
    /// The names this thread's request has resolved, in order, with what each resolved to. `None` outside a request's scope.
    static MEMO: RefCell<Option<Vec<Entry>>> = const { RefCell::new(None) };
}

/// A request's resolution memo, for as long as it lives.
///
/// **Not `Send`, on purpose.** The memo is the thread's, and it is correct only while no await lies between [`Scope::begin`] and the
/// sink's end check: a future that moved threads at an await would carry a guard whose memo stayed behind on the first thread and
/// leave a stale one for the next request that runs there. A guard that is not `Send` held across an await makes the handler's
/// future not `Send`, which the server's router refuses to compile, so introducing an await inside that section is a compile
/// error and not a latent bug; `the_scope_cannot_be_held_across_an_await` pins that the guard is not `Send`. The drop also asserts
/// (in a debug build) that it runs on the thread that began it.
pub(super) struct Scope {
    /// Whether this scope made the memo (and clears it), or joined one already begun.
    owner: bool,
    /// The thread that began it.
    thread: std::thread::ThreadId,
    /// `*const ()` is not `Send`, so neither is the guard.
    _not_send: std::marker::PhantomData<*const ()>,
}

impl Scope {
    /// Begin the request's memo with its source, which the request resolved when it authorized the read. Joins a memo already begun
    /// (the pipeline is run from a scan and from a finish, one inside the other).
    pub(super) fn begin(db: &str, name: &str, meta: &CollectionMeta) -> Scope {
        MEMO.with(|memo| {
            let mut memo = memo.borrow_mut();
            let thread = std::thread::current().id();
            if memo.is_some() {
                return Scope { owner: false, thread, _not_send: std::marker::PhantomData };
            }
            *memo = Some(vec![Entry { db: db.into(), name: name.into(), meta: meta.clone() }]);
            Scope { owner: true, thread, _not_send: std::marker::PhantomData }
        })
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        debug_assert_eq!(
            std::thread::current().id(),
            self.thread,
            "a request's resolution memo was dropped on another thread than the one that began it: an await lies inside its section"
        );
        if self.owner {
            MEMO.with(|memo| *memo.borrow_mut() = None);
        }
    }
}

/// The physical this request already resolved `name` to, if it has.
pub(super) fn recall(db: &str, name: &str) -> Option<CollectionMeta> {
    MEMO.with(|memo| {
        memo.borrow()
            .as_ref()?
            .iter()
            .find(|entry| entry.db == db && entry.name == name)
            .map(|entry| entry.meta.clone())
    })
}

/// Remember what this request resolved `name` to. Outside a request's scope, nothing.
pub(super) fn remember(db: &str, name: &str, meta: &CollectionMeta) {
    MEMO.with(|memo| {
        if let Some(entries) = memo.borrow_mut().as_mut()
            && !entries.iter().any(|entry| entry.db == db && entry.name == name)
        {
            entries.push(Entry { db: db.into(), name: name.into(), meta: meta.clone() });
        }
    });
}

/// After the last read and before the first write: `503` if a physical this request read has been buried since it was resolved.
/// Burial is monotone, so a physical that is alive now was alive throughout. Nothing is checked outside a request's scope.
pub(super) fn end_check(state: &SharedState) -> Result<(), ApiError> {
    let read: Vec<(String, String, CollectionMeta)> = MEMO.with(|memo| {
        memo.borrow()
            .as_ref()
            .map(|entries| {
                entries.iter().map(|e| (e.db.clone(), e.name.clone(), e.meta.clone())).collect()
            })
            .unwrap_or_default()
    });
    for (db, name, meta) in read {
        let alive = match state.engine.get_collection(&db, &meta.name) {
            Ok(held) => held.id == meta.id && held.created == meta.created,
            Err(kimmy_storage::StorageError::Core(kimmy_core::Error::CollectionNotFound {
                ..
            })) => false,
            Err(e) => return Err(e.into()),
        };
        if !alive {
            return Err(ApiError::swap_in_progress(format!(
                "{db}.{name} was replaced while this request was running: the collection it read ({}) is gone. Nothing was \
                 written; retry",
                meta.name
            )));
        }
    }
    Ok(())
}
