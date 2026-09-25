//! Confirming a schema change on each live member, with at most one push in
//! flight to each (ADR-140, ADR-191).
//!
//! A confirmation used to be one push per change per member, all at once, each
//! carrying the whole window the member lacked (ADR-143). A burst of N index
//! creates on one member therefore sent every peer N windows that overlapped,
//! about N²/2 schema changes applied on each peer's single writer, and a
//! request's deadline dropped its push mid-apply, which the peer met as a
//! broken pipe.
//!
//! Here a member has one queue of changes waiting to be confirmed on it and
//! one driver that pushes for them, one window at a time. A change is resolved
//! by the first push whose *answered* window covers it; everything queued
//! while a push is in flight rides the next one. The driver belongs to the
//! [`Confirmer`], not to any request, so a request that stops waiting leaves
//! the push to finish and be read.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use kimmy_core::{NodeId, OplogEntry, Stamp, VersionVector};
use kimmy_storage::Engine;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tracing::error;

use crate::membership::Members;
use crate::protocol::{MAX_BATCH, Message, ProtocolError, read_frame, write_frame};
use crate::transport::{Fits, REQUEST_TIMEOUT, dial, how_many_fit};

/// The longest a member's back-off runs after pushes it did not answer
/// (ADR-191).
pub const MAX_CONFIRM_BACKOFF: Duration = Duration::from_secs(60);

/// How long a driver waits before dialling a member again after a push that
/// failed before its window was sent: a guard against hot redials to an
/// address that is not answering, and no back-off, since nothing reached the
/// member's writer (ADR-191).
pub const REDIAL_GUARD: Duration = Duration::from_secs(1);

/// What a confirmation became on one member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// The member took the change and did not refuse it: it applied it, or
    /// already held it (ADR-140's "holds").
    Confirmed,
    /// The member took the change and could not apply it (ADR-123), or
    /// declined a drop older than the index it holds (ADR-141).
    Refused,
    /// No answer that says either; anti-entropy carries the change.
    Pending { outcome: ConfirmOutcome, reason: String },
}

impl Resolution {
    fn pending(outcome: ConfirmOutcome, reason: impl Into<String>) -> Self {
        Self::Pending { outcome, reason: reason.into() }
    }

    /// The outcome a metric counts it under.
    pub fn outcome(&self) -> ConfirmOutcome {
        match self {
            Self::Confirmed => ConfirmOutcome::Confirmed,
            Self::Refused => ConfirmOutcome::Refused,
            Self::Pending { outcome, .. } => *outcome,
        }
    }
}

/// Every way a confirmation ends on one member, for
/// `kimmy_ddl_confirmations_total{outcome}` (ADR-191). A fixed set, so the
/// series is always all of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConfirmOutcome {
    Confirmed,
    Refused,
    /// The request's own deadline passed first.
    Timeout,
    /// The push failed or timed out.
    Failed,
    /// The window could not reach the change: the member is more than a
    /// batch behind, or below the retention horizon.
    Unreached,
    /// The member is still purging a drop of the name (ADR-189).
    Purging,
    /// The member's batch stopped before the change, at an entry for a
    /// collection it lacks (ADR-148).
    StoppedUnknown,
    /// A different member answered at the address.
    OtherMember,
    /// The push driver panicked or was aborted.
    TaskEnded,
    /// The member's queue is backing off after a push it did not answer.
    Backoff,
    /// The member's answer cannot be attributed per change (a version before
    /// ADR-191's fields).
    Unattributable,
    /// The request stopped waiting before any of the above: its client went
    /// away, and the request with it.
    Cancelled,
}

impl ConfirmOutcome {
    pub const COUNT: usize = 12;
    pub const ALL: [Self; Self::COUNT] = [
        Self::Confirmed,
        Self::Refused,
        Self::Timeout,
        Self::Failed,
        Self::Unreached,
        Self::Purging,
        Self::StoppedUnknown,
        Self::OtherMember,
        Self::TaskEnded,
        Self::Backoff,
        Self::Unattributable,
        Self::Cancelled,
    ];

    pub const fn slot(self) -> usize {
        self as usize
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Refused => "refused",
            Self::Timeout => "timeout",
            Self::Failed => "failed",
            Self::Unreached => "unreached",
            Self::Purging => "purging",
            Self::StoppedUnknown => "stopped_unknown",
            Self::OtherMember => "other_member",
            Self::TaskEnded => "task_ended",
            Self::Backoff => "backoff",
            Self::Unattributable => "unattributable",
            Self::Cancelled => "cancelled",
        }
    }
}

/// The timings a [`Confirmer`] runs by.
#[derive(Clone, Debug)]
pub struct ConfirmConfig {
    /// The first back-off after a push the member did not answer; each
    /// consecutive one doubles it, to `max_backoff`. The node sets it to
    /// `cluster.sync_interval_secs`: by then the member's own pull has had its
    /// turn at the same entries.
    pub first_backoff: Duration,
    pub max_backoff: Duration,
    pub redial_guard: Duration,
    /// The whole push, dial included.
    pub request_timeout: Duration,
}

impl ConfirmConfig {
    pub fn new(sync_interval: Duration) -> Self {
        Self {
            first_backoff: sync_interval,
            max_backoff: MAX_CONFIRM_BACKOFF,
            redial_guard: REDIAL_GUARD,
            request_timeout: REQUEST_TIMEOUT,
        }
    }
}

/// Called once per confirmation, with how it ended.
pub type ConfirmHook = Arc<dyn Fn(ConfirmOutcome) + Send + Sync>;
/// Called once per window pushed.
pub type PushSentHook = Arc<dyn Fn() + Send + Sync>;

/// Confirms schema changes on members, one push in flight per member
/// (ADR-191). One per node, for the node's life.
pub struct Confirmer {
    engine: Arc<Engine>,
    secret: String,
    members: Members,
    config: ConfirmConfig,
    peers: Mutex<HashMap<SocketAddr, PeerQueue>>,
    /// Every driver, so shutdown can abort them: a driver outlives any
    /// request and holds the engine and a connection (ADR-191).
    drivers: Mutex<tokio::task::JoinSet<()>>,
    on_resolved: Option<ConfirmHook>,
    on_push: Option<PushSentHook>,
    #[cfg(test)]
    hooks: test_hooks::Hooks,
    /// Pushes a member answered, so a test can tell a push read to its answer
    /// from one abandoned when a request stopped waiting.
    #[cfg(test)]
    answered: AtomicUsize,
}

#[derive(Default)]
struct PeerQueue {
    waiters: Vec<Waiter>,
    driving: bool,
    backoff: Option<Backoff>,
}

/// A member's back-off after pushes it did not answer. `step` outlives
/// `until`, so consecutive failures double it; an answered push, or SWIM
/// seeing the member come up again, ends it (ADR-191).
struct Backoff {
    until: Instant,
    step: Duration,
    generation: Option<u64>,
}

struct Waiter {
    entry: OplogEntry,
    node: NodeId,
    /// Queued before the snapshot of the push in flight, which is taken just
    /// before that push reads what this node can serve: that push resolves
    /// it, whatever its answer (ADR-191).
    pre_queued: bool,
    answer: oneshot::Sender<Resolution>,
}

impl Waiter {
    fn resolve(self, resolution: Resolution) {
        // A request that stopped waiting has nothing to be told.
        let _ = self.answer.send(resolution);
    }
}

impl PeerQueue {
    /// Nothing waiting, no driver, and no back-off that still matters: a
    /// back-off whose pause ended longer ago than the longest step has
    /// nothing left to double.
    fn idle(&self, now: Instant, max_backoff: Duration) -> bool {
        self.waiters.is_empty()
            && !self.driving
            && self.backoff.as_ref().is_none_or(|b| now >= b.until + max_backoff)
    }
}

/// How far a push got, for deciding whether a failure sent anything.
#[derive(Default)]
struct Progress {
    /// Set as the `Push` frame's write begins: from then on the member's
    /// writer may be carrying the window (ADR-191).
    sending: AtomicBool,
    /// Set once the batch snapshot has flagged the waiters present.
    snapshotted: AtomicBool,
    their_node: Mutex<Option<NodeId>>,
}

/// A window a member answered.
struct Answer {
    their_node: NodeId,
    /// The stamps in the window, in its order.
    sent: Vec<Stamp>,
    /// The schema changes among them.
    ddl_in_window: usize,
    /// What this node could serve, read before the window.
    versions: VersionVector,
    reply: Reply,
}

/// The fields of a `Pushed` a confirmation reads.
#[derive(Clone, Debug, Default)]
struct Reply {
    ddl_refused: usize,
    ddl_declined: usize,
    unknown_collection: usize,
    purge_pending: usize,
    refused: Vec<Stamp>,
    stopped_at: Option<Stamp>,
}

enum PushResult {
    /// A window was sent and answered.
    Answered(Answer),
    /// Nothing was sent: every waiter resolved early, or the window could
    /// not reach any of them (the reason says why, for those flagged).
    NothingSent {
        their_node: NodeId,
        unreached: Option<String>,
    },
    Failed(Failure),
}

struct Failure {
    /// Whether the `Push` frame's write had begun.
    sent: bool,
    /// Whether the batch snapshot had been taken. A push that fails before
    /// it (the dial, the handshake, `AskWitnessed`) flagged no one, and
    /// resolves everyone queued: none of them can be covered by a push that
    /// reached no member (ADR-191).
    snapshotted: bool,
    their_node: Option<NodeId>,
    reason: String,
}

impl Failure {
    fn before_send(their_node: Option<NodeId>, snapshotted: bool, reason: impl ToString) -> Self {
        Self { sent: false, snapshotted, their_node, reason: reason.to_string() }
    }
}

impl Confirmer {
    pub fn new(
        engine: Arc<Engine>,
        secret: String,
        members: Members,
        config: ConfirmConfig,
    ) -> Arc<Self> {
        Self::with_hooks(engine, secret, members, config, None, None)
    }

    pub fn with_hooks(
        engine: Arc<Engine>,
        secret: String,
        members: Members,
        config: ConfirmConfig,
        on_resolved: Option<ConfirmHook>,
        on_push: Option<PushSentHook>,
    ) -> Arc<Self> {
        Arc::new(Self {
            engine,
            secret,
            members,
            config,
            peers: Mutex::new(HashMap::new()),
            drivers: Mutex::new(tokio::task::JoinSet::new()),
            on_resolved,
            on_push,
            #[cfg(test)]
            hooks: test_hooks::Hooks::default(),
            #[cfg(test)]
            answered: AtomicUsize::new(0),
        })
    }

    /// Confirm `entry` on the member at `addr`, which SWIM knows as `node`,
    /// waiting at most `deadline` (ADR-140). The push this waits on is not
    /// cancelled when the deadline passes; it belongs to the member's driver.
    ///
    /// Counted however the wait ends, and from the moment this is called, not
    /// from the first poll: the future owns the count, so one dropped before it
    /// ever runs (a request gone before its task started) is counted
    /// `cancelled`, as is one dropped mid-wait; one that panics is counted
    /// `task_ended`.
    pub fn confirm(
        self: &Arc<Self>,
        addr: SocketAddr,
        node: NodeId,
        entry: OplogEntry,
        deadline: Duration,
    ) -> impl std::future::Future<Output = Resolution> + Send + 'static {
        let confirmer = Arc::clone(self);
        let mut counted = Counted { hook: self.on_resolved.clone(), done: false };
        async move {
            let resolution = match confirmer.enqueue(addr, node, entry) {
                Err(now) => now,
                Ok(answer) => match tokio::time::timeout(deadline, answer).await {
                    Ok(Ok(resolution)) => resolution,
                    Ok(Err(_)) => {
                        Resolution::pending(ConfirmOutcome::TaskEnded, "the push task ended")
                    }
                    Err(_) => Resolution::pending(
                        ConfirmOutcome::Timeout,
                        format!("no answer within {deadline:?}"),
                    ),
                },
            };
            counted.record(resolution.outcome());
            resolution
        }
    }

    /// Report the member at `addr` pending without pushing to it or waiting:
    /// for a request whose deadline leaves no time to wait. `Timeout`, counted
    /// like any other confirmation; anti-entropy carries the change, as it does
    /// for every pending member.
    ///
    /// Synchronous, deliberately: a schema change that has committed is
    /// answered in the same poll as its confirmation, so a request deadline
    /// that has passed cannot fire first and answer it "abandoned" (ADR-057).
    /// No push, because a push nothing waits for is dropped from its queue
    /// unsent.
    pub fn pending_without_waiting(&self) -> Resolution {
        let mut counted = Counted { hook: self.on_resolved.clone(), done: false };
        let resolution = Resolution::pending(
            ConfirmOutcome::Timeout,
            "the request's deadline left no time to wait for an answer",
        );
        counted.record(resolution.outcome());
        resolution
    }

    /// Queue `entry` for `addr`, starting its driver if none runs; or answer
    /// at once, during a back-off.
    fn enqueue(
        self: &Arc<Self>,
        addr: SocketAddr,
        node: NodeId,
        entry: OplogEntry,
    ) -> Result<oneshot::Receiver<Resolution>, Resolution> {
        #[cfg(test)]
        assert!(!self.hooks.panic_on_enqueue.load(Ordering::SeqCst), "a panic made for a test");
        let now = Instant::now();
        let generation = self.members.generation(&addr);
        let mut peers = self.peers.lock();
        // Lazily, so an address that churned does not keep an entry.
        let max = self.config.max_backoff;
        peers.retain(|a, queue| *a == addr || !queue.idle(now, max));
        self.reap();
        let queue = peers.entry(addr).or_default();
        // SWIM brought the member up again since the back-off began: a
        // restart, or a new incarnation. Its writer is not the one that did
        // not answer.
        if queue.backoff.as_ref().is_some_and(|b| b.generation != generation) {
            queue.backoff = None;
        }
        if queue.backoff.as_ref().is_some_and(|b| now < b.until) {
            return Err(Resolution::pending(
                ConfirmOutcome::Backoff,
                "backing off: the last push to this member did not answer",
            ));
        }
        let (answer, waiting) = oneshot::channel();
        queue.waiters.push(Waiter { entry, node, pre_queued: false, answer });
        if !queue.driving {
            queue.driving = true;
            let confirmer = Arc::clone(self);
            // UNSUPERVISED: request work, owned by this confirmer and aborted with the cluster tasks at shutdown; a panic resolves its waiters pending and must not stop the node.
            self.drivers.lock().spawn(async move { confirmer.drive(addr).await });
        }
        Ok(waiting)
    }

    /// Join the drivers that have finished, so the set holds only live ones
    /// and those that finished after the last driver started (ADR-191).
    fn reap(&self) {
        let mut drivers = self.drivers.lock();
        while let Some(done) = drivers.try_join_next() {
            if let Err(e) = done
                && e.is_panic()
            {
                error!(error = %e, "a schema-change push driver panicked; its waiters are pending");
            }
        }
    }

    /// Abort every driver. The node's shutdown calls this beside the cluster
    /// tasks: a driver holds the engine and a connection, and nothing else
    /// would stop it (ADR-191). Each aborted driver's waiters read "the push
    /// task ended".
    pub fn abort_all(&self) {
        self.drivers.lock().abort_all();
    }

    /// Members with a queue entry: something waiting, a push in flight, or a
    /// back-off that still counts. Empty on an idle node once back-offs have
    /// run out and a confirmation has run since.
    pub fn members_waiting(&self) -> Vec<SocketAddr> {
        self.peers.lock().keys().copied().collect()
    }

    /// The number of drivers the set holds, finished but unjoined included.
    pub fn drivers_held(&self) -> usize {
        self.drivers.lock().len()
    }

    /// Whether `addr`'s queue is in a back-off pause now.
    #[cfg(test)]
    fn backing_off(&self, addr: SocketAddr) -> bool {
        let now = Instant::now();
        self.peers.lock().get(&addr).and_then(|q| q.backoff.as_ref()).is_some_and(|b| now < b.until)
    }

    /// The step of `addr`'s back-off, paused or not.
    #[cfg(test)]
    fn backoff_step(&self, addr: SocketAddr) -> Option<Duration> {
        self.peers.lock().get(&addr).and_then(|q| q.backoff.as_ref()).map(|b| b.step)
    }

    async fn drive(self: Arc<Self>, addr: SocketAddr) {
        let mut guard = DriverGuard { confirmer: Arc::clone(&self), addr, armed: true };
        loop {
            {
                let mut peers = self.peers.lock();
                let Some(queue) = peers.get_mut(&addr) else {
                    guard.armed = false;
                    return;
                };
                queue.waiters.retain(|waiter| !waiter.answer.is_closed());
                if queue.waiters.is_empty() {
                    queue.driving = false;
                    if queue.backoff.is_none() {
                        peers.remove(&addr);
                    }
                    guard.armed = false;
                    return;
                }
            }
            let progress = Progress::default();
            let result = match tokio::time::timeout(
                self.config.request_timeout,
                self.push_window(addr, &progress),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => PushResult::Failed(Failure {
                    sent: progress.sending.load(Ordering::SeqCst),
                    snapshotted: progress.snapshotted.load(Ordering::SeqCst),
                    their_node: *progress.their_node.lock(),
                    reason: format!("no answer within {:?}", self.config.request_timeout),
                }),
            };
            if let Some(pause) = self.settle(addr, result) {
                tokio::time::sleep(pause).await;
            }
        }
    }

    /// One push to `addr`, for the waiters queued there.
    async fn push_window(&self, addr: SocketAddr, progress: &Progress) -> PushResult {
        let (mut stream, their_node) = match dial(&self.engine, addr, &self.secret).await {
            Ok(dialled) => dialled,
            Err(e) => return PushResult::Failed(Failure::before_send(None, false, e)),
        };
        *progress.their_node.lock() = Some(their_node);
        let failed = |e: ProtocolError| {
            PushResult::Failed(Failure::before_send(
                Some(their_node),
                progress.snapshotted.load(Ordering::SeqCst),
                e,
            ))
        };

        if let Err(e) = write_frame(&mut stream, &Message::AskWitnessed {}).await {
            return failed(e);
        }
        let held = match read_frame(&mut stream).await {
            Ok(Message::Witnessed(held)) => held,
            Ok(Message::Fault(reason)) => return failed(ProtocolError::Fault(reason)),
            Ok(other) => {
                return failed(ProtocolError::Malformed(format!(
                    "expected Witnessed, got {other:?}"
                )));
            }
            Err(e) => return failed(e),
        };

        // The batch snapshot: under the lock, immediately before `mine` is
        // read. Every waiter present is this push's to resolve; one arriving
        // after it is carried if this push does not cover it (ADR-191).
        let mut flagged = 0usize;
        {
            let mut peers = self.peers.lock();
            let Some(queue) = peers.get_mut(&addr) else {
                return PushResult::NothingSent { their_node, unreached: None };
            };
            let waiting = std::mem::take(&mut queue.waiters);
            for mut waiter in waiting {
                if waiter.answer.is_closed() {
                    continue;
                }
                if waiter.node != their_node {
                    waiter.resolve(other_member(addr));
                } else if held.get(waiter.entry.stamp.node) >= waiter.entry.stamp.hlc {
                    // Processed already, by a pull or an earlier push: today's
                    // early exit (ADR-143).
                    waiter.resolve(Resolution::Confirmed);
                } else {
                    waiter.pre_queued = true;
                    flagged += 1;
                    queue.waiters.push(waiter);
                }
            }
            progress.snapshotted.store(true, Ordering::SeqCst);
        }
        #[cfg(test)]
        self.hooks.after_snapshot();
        if flagged == 0 {
            return PushResult::NothingSent { their_node, unreached: None };
        }

        let mine = match self.engine.version_vector() {
            Ok(mine) => mine,
            Err(e) => return failed(ProtocolError::Malformed(e.to_string())),
        };
        #[cfg(test)]
        self.hooks.after_mine();
        let Some(from) = held.behind(&mine) else {
            return PushResult::NothingSent { their_node, unreached: None };
        };
        match self.engine.can_serve_peer_holding(&held) {
            Ok(true) => {}
            Ok(false) => {
                return PushResult::NothingSent {
                    their_node,
                    unreached: Some(
                        "the member is below this node's retention horizon; anti-entropy will \
                         hand it a snapshot"
                            .into(),
                    ),
                };
            }
            Err(e) => return failed(ProtocolError::Malformed(e.to_string())),
        }
        let engine = &self.engine;
        let mut window = match kimmy_storage::blocking(|| {
            engine.entries_for_peer_holding(from, MAX_BATCH, Some(&held))
        }) {
            Ok(window) => window,
            Err(e) => return failed(ProtocolError::Malformed(e.to_string())),
        };
        if let Fits::Only(fits) = how_many_fit(&window.entries) {
            if fits == 0 {
                return failed(ProtocolError::Malformed(format!(
                    "a single oplog entry at or after {from:?} exceeds the frame limit and \
                     cannot replicate"
                )));
            }
            window = match kimmy_storage::blocking(|| {
                engine.entries_for_peer_holding(from, fits, Some(&held))
            }) {
                Ok(window) => window,
                Err(e) => return failed(ProtocolError::Malformed(e.to_string())),
            };
        }
        let sent: Vec<Stamp> = window.entries.iter().map(|entry| entry.stamp).collect();
        let reaches_one = {
            let peers = self.peers.lock();
            peers.get(&addr).is_some_and(|queue| {
                queue
                    .waiters
                    .iter()
                    .any(|waiter| waiter.pre_queued && sent.contains(&waiter.entry.stamp))
            })
        };
        if !reaches_one {
            return PushResult::NothingSent {
                their_node,
                unreached: Some(format!(
                    "the member is more than {} entries behind this node; anti-entropy will \
                     carry the change",
                    sent.len()
                )),
            };
        }
        let ddl_in_window = window.entries.iter().filter(|entry| entry.kind.is_ddl()).count();

        progress.sending.store(true, Ordering::SeqCst);
        if let Some(hook) = &self.on_push {
            hook();
        }
        let sent_failure = |reason: String| {
            PushResult::Failed(Failure {
                sent: true,
                snapshotted: true,
                their_node: Some(their_node),
                reason,
            })
        };
        let push = Message::Push {
            entries: window.entries,
            scanned_to: window.scanned_to,
            exhausted: window.exhausted,
            versions: mine.clone(),
        };
        if let Err(e) = write_frame(&mut stream, &push).await {
            return sent_failure(e.to_string());
        }
        match read_frame(&mut stream).await {
            Ok(Message::Pushed {
                ddl_refused,
                unknown_collection,
                ddl_declined,
                purge_pending,
                refused,
                stopped_at,
                ..
            }) => PushResult::Answered(Answer {
                their_node,
                sent,
                ddl_in_window,
                versions: mine,
                reply: Reply {
                    ddl_refused,
                    ddl_declined,
                    unknown_collection,
                    purge_pending,
                    refused,
                    stopped_at,
                },
            }),
            Ok(Message::Fault(reason)) => sent_failure(ProtocolError::Fault(reason).to_string()),
            Ok(other) => sent_failure(format!("expected Pushed, got {other:?}")),
            Err(e) => sent_failure(e.to_string()),
        }
    }

    /// Resolve what `result` decides for `addr`'s waiters, and say how long
    /// to wait before the next push, if at all.
    fn settle(&self, addr: SocketAddr, result: PushResult) -> Option<Duration> {
        let generation = self.members.generation(&addr);
        let mut peers = self.peers.lock();
        let queue = peers.get_mut(&addr)?;
        let waiting = std::mem::take(&mut queue.waiters);
        let mut pause = None;
        match result {
            PushResult::Answered(answer) => {
                #[cfg(test)]
                self.answered.fetch_add(1, Ordering::SeqCst);
                queue.backoff = None;
                for mut waiter in waiting {
                    if waiter.node != answer.their_node {
                        waiter.resolve(other_member(addr));
                        continue;
                    }
                    match resolve_answered(&waiter, &answer) {
                        Some(resolution) => waiter.resolve(resolution),
                        None => {
                            waiter.pre_queued = false;
                            queue.waiters.push(waiter);
                        }
                    }
                }
            }
            PushResult::NothingSent { their_node, unreached } => {
                for mut waiter in waiting {
                    if waiter.node != their_node {
                        waiter.resolve(other_member(addr));
                    } else if waiter.pre_queued {
                        waiter.resolve(Resolution::pending(
                            ConfirmOutcome::Unreached,
                            unreached.clone().unwrap_or_else(|| {
                                "the window could not reach the change".to_string()
                            }),
                        ));
                    } else {
                        waiter.pre_queued = false;
                        queue.waiters.push(waiter);
                    }
                }
            }
            PushResult::Failed(failure) => {
                for waiter in waiting {
                    if failure.their_node.is_some_and(|node| node != waiter.node) {
                        waiter.resolve(other_member(addr));
                    } else if waiter.pre_queued || !failure.snapshotted {
                        waiter.resolve(Resolution::pending(
                            ConfirmOutcome::Failed,
                            failure.reason.clone(),
                        ));
                    } else {
                        queue.waiters.push(waiter);
                    }
                }
                if failure.sent {
                    // The window may be on the member's writer still: back off,
                    // doubling while the member keeps not answering (ADR-191).
                    let step = match &queue.backoff {
                        Some(previous) => (previous.step * 2).min(self.config.max_backoff),
                        // Never above the longest step, or the next doubling
                        // would shrink it.
                        None => self.config.first_backoff.min(self.config.max_backoff),
                    };
                    queue.backoff =
                        Some(Backoff { until: Instant::now() + step, step, generation });
                    for waiter in std::mem::take(&mut queue.waiters) {
                        waiter.resolve(Resolution::pending(
                            ConfirmOutcome::Backoff,
                            "backing off: the last push to this member did not answer",
                        ));
                    }
                } else {
                    pause = Some(self.config.redial_guard);
                }
            }
        }
        pause
    }
}

/// Records one confirmation's outcome exactly once: as it ended, or as
/// `cancelled` if the future is dropped first.
struct Counted {
    hook: Option<ConfirmHook>,
    done: bool,
}

impl Counted {
    fn record(&mut self, outcome: ConfirmOutcome) {
        self.done = true;
        if let Some(hook) = &self.hook {
            hook(outcome);
        }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        if !self.done
            && let Some(hook) = &self.hook
        {
            // A panic inside the confirmation is the task ending, not the
            // request going away.
            hook(if std::thread::panicking() {
                ConfirmOutcome::TaskEnded
            } else {
                ConfirmOutcome::Cancelled
            });
        }
    }
}

fn other_member(addr: SocketAddr) -> Resolution {
    Resolution::pending(
        ConfirmOutcome::OtherMember,
        format!("a different member answered at {addr}"),
    )
}

/// What an answered window decides for one waiter: `None` to carry it to the
/// next push (ADR-191, §4.2 of its design).
fn resolve_answered(waiter: &Waiter, answer: &Answer) -> Option<Resolution> {
    let stamp = waiter.entry.stamp;
    let reply = &answer.reply;
    let Some(index) = answer.sent.iter().position(|sent| *sent == stamp) else {
        return waiter.pre_queued.then(|| {
            Resolution::pending(ConfirmOutcome::Unreached, "the window did not carry the change")
        });
    };
    if stamp.hlc > answer.versions.get(stamp.node) {
        // In the window, above what it was introduced with: the member left it
        // for its next pull (ADR-148). Only an arrival can be here.
        return waiter.pre_queued.then(|| {
            Resolution::pending(ConfirmOutcome::Unreached, "the window did not carry the change")
        });
    }
    let attributable = reply.refused.len() == reply.ddl_refused + reply.ddl_declined
        && reply.stopped_at.is_some() == (reply.unknown_collection + reply.purge_pending > 0);
    if !attributable {
        return Some(if answer.ddl_in_window > 1 || reply.unknown_collection > 0 {
            Resolution::pending(
                ConfirmOutcome::Unattributable,
                "the member's answer cannot be attributed per change (older version)",
            )
        } else {
            per_window(reply)
        });
    }
    let stop = reply.stopped_at.map(|at| answer.sent.iter().position(|sent| *sent == at));
    match stop {
        Some(stop) if stop.is_none_or(|stop| stop <= index) => {
            if !waiter.pre_queued {
                return None;
            }
            Some(if reply.purge_pending > 0 {
                Resolution::pending(ConfirmOutcome::Purging, "still purging a drop of this name")
            } else if stop == Some(index) {
                Resolution::Refused
            } else {
                Resolution::pending(
                    ConfirmOutcome::StoppedUnknown,
                    "the window stopped before it, at an entry for a collection this member lacks",
                )
            })
        }
        _ if reply.refused.contains(&stamp) => Some(Resolution::Refused),
        _ => Some(Resolution::Confirmed),
    }
}

/// The per-window rule a confirmation used before ADR-191, kept for the one
/// window it is exact for: one schema change, from a member whose answer does
/// not name changes.
fn per_window(reply: &Reply) -> Resolution {
    if reply.purge_pending > 0 {
        Resolution::pending(ConfirmOutcome::Purging, "still purging a drop of this name")
    } else if reply.ddl_refused > 0 || reply.ddl_declined > 0 {
        Resolution::Refused
    } else {
        Resolution::Confirmed
    }
}

/// Resolves a driver's waiters and clears its queue's `driving` if the driver
/// ends without doing so: a panic, or an abort at shutdown.
struct DriverGuard {
    confirmer: Arc<Confirmer>,
    addr: SocketAddr,
    armed: bool,
}

impl Drop for DriverGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut peers = self.confirmer.peers.lock();
        if let Some(queue) = peers.get_mut(&self.addr) {
            queue.driving = false;
            // Dropping the senders answers each waiter "the push task ended".
            queue.waiters.clear();
            if queue.backoff.is_none() {
                peers.remove(&self.addr);
            }
        }
    }
}

#[cfg(test)]
mod test_hooks {
    use parking_lot::Mutex;

    type Hook = Box<dyn FnOnce() + Send>;

    /// Points inside a push where a test places a mint or a queue, so the
    /// order it asserts on is the order that ran.
    #[derive(Default)]
    pub struct Hooks {
        pub after_snapshot: Mutex<Option<Hook>>,
        pub after_mine: Mutex<Option<Hook>>,
        pub panic_on_enqueue: std::sync::atomic::AtomicBool,
    }

    impl Hooks {
        pub fn after_snapshot(&self) {
            if let Some(hook) = self.after_snapshot.lock().take() {
                hook();
            }
        }

        pub fn after_mine(&self) {
            if let Some(hook) = self.after_mine.lock().take() {
                hook();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use kimmy_core::{CollectionId, Hlc, IndexField, OpKind};
    use tokio::net::TcpListener;

    use super::*;
    use crate::transport::PushHook;

    const SECRET: &str = "a-confirm-test-secret";

    /// What a serving member's push hook saw: each window's schema changes
    /// applied and entries deferred, in order; and levers to hold its answer
    /// or break it.
    #[derive(Default)]
    struct Served {
        windows: Mutex<Vec<(usize, usize)>>,
        hold: Mutex<Duration>,
        break_next: AtomicUsize,
    }

    impl Served {
        fn windows(&self) -> Vec<(usize, usize)> {
            self.windows.lock().clone()
        }
        fn ddl(&self) -> usize {
            self.windows().iter().map(|(ddl, _)| ddl).sum()
        }
    }

    struct Member {
        engine: Arc<Engine>,
        addr: SocketAddr,
        served: Arc<Served>,
        serving: tokio::task::JoinHandle<()>,
        _dir: tempfile::TempDir,
    }

    fn engine() -> (Arc<Engine>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap()), dir)
    }

    /// A member serving on `listener`, whose push hook records every window,
    /// holds its answer for `served.hold`, and panics (dropping the
    /// connection after the window was taken) while `break_next` is above 0.
    fn member_on(listener: TcpListener) -> Member {
        let (engine, dir) = engine();
        let addr = listener.local_addr().unwrap();
        let served = Arc::new(Served::default());
        let hook: PushHook = Arc::new({
            let served = Arc::clone(&served);
            move |outcome: &kimmy_storage::SyncOutcome| {
                served.windows.lock().push((outcome.ddl, outcome.deferred));
                let hold = *served.hold.lock();
                if !hold.is_zero() {
                    std::thread::sleep(hold);
                }
                if served.break_next.load(Ordering::SeqCst) > 0 {
                    served.break_next.fetch_sub(1, Ordering::SeqCst);
                    panic!("a push answer broken for a test");
                }
            }
        });
        let serving = tokio::spawn(crate::transport::serve_with(
            Arc::clone(&engine),
            listener,
            SECRET.into(),
            Some(hook),
            None,
            Arc::new(crate::tls::ClusterTls::new().unwrap()),
        ));
        Member { engine, addr, served, serving, _dir: dir }
    }

    async fn member() -> Member {
        member_on(TcpListener::bind("127.0.0.1:0").await.unwrap())
    }

    /// The pushing node, a collection its member already holds, and a
    /// confirmer that records every outcome and every window it sends.
    struct Pusher {
        engine: Arc<Engine>,
        members: Members,
        confirmer: Arc<Confirmer>,
        outcomes: Arc<Mutex<Vec<ConfirmOutcome>>>,
        pushes: Arc<AtomicUsize>,
        _dir: tempfile::TempDir,
    }

    fn quick() -> ConfirmConfig {
        ConfirmConfig {
            first_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_millis(400),
            redial_guard: Duration::from_millis(100),
            request_timeout: Duration::from_secs(10),
        }
    }

    fn pusher_for(member: &Member, config: ConfirmConfig) -> Pusher {
        let (engine, dir) = engine();
        engine.create_collection("shop", "orders").unwrap();
        sync_into(&member.engine, &engine);
        let members = Members::default();
        members.insert_for_test(member.addr, member.engine.node_id());
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let pushes = Arc::new(AtomicUsize::new(0));
        let confirmer = Confirmer::with_hooks(
            Arc::clone(&engine),
            SECRET.into(),
            members.clone(),
            config,
            Some(Arc::new({
                let outcomes = Arc::clone(&outcomes);
                move |outcome| outcomes.lock().push(outcome)
            })),
            Some(Arc::new({
                let pushes = Arc::clone(&pushes);
                move || {
                    pushes.fetch_add(1, Ordering::SeqCst);
                }
            })),
        );
        Pusher { engine, members, confirmer, outcomes, pushes, _dir: dir }
    }

    /// Everything `from` holds, applied on `into` as a pulled window.
    fn sync_into(into: &Engine, from: &Engine) {
        let window = from.entries_for_peer(Hlc::ZERO, MAX_BATCH).unwrap();
        into.apply_peer_batch(
            &from.version_vector().unwrap(),
            &window.entries,
            window.scanned_to,
            window.exhausted,
        )
        .unwrap();
    }

    /// Create index `name` on `engine` and hand back the entry it minted.
    fn create(engine: &Engine, name: &str) -> OplogEntry {
        engine
            .create_index(
                "shop",
                "orders",
                vec![IndexField::ascending(name)],
                false,
                Some(name.into()),
            )
            .unwrap();
        engine.entries_for_peer(Hlc::ZERO, MAX_BATCH * 4).unwrap().entries.pop().unwrap()
    }

    const DEADLINE: Duration = Duration::from_secs(10);

    async fn eventually(what: &str, mut ok: impl FnMut() -> bool) {
        let until = Instant::now() + Duration::from_secs(10);
        while !ok() {
            assert!(Instant::now() < until, "{what}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // --- T1 ---

    /// A burst of 32 creates confirmed concurrently is applied once each on
    /// the member: the ~N²/2 of one push per change is gone (ADR-191).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_burst_of_32_creates_is_applied_once_each_on_a_member() {
        let b = member().await;
        *b.served.hold.lock() = Duration::from_millis(200);
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let mut asked = tokio::task::JoinSet::new();
        for i in 0..32 {
            let entry = create(&a.engine, &format!("f{i}"));
            let confirmer = Arc::clone(&a.confirmer);
            asked.spawn(async move { confirmer.confirm(b.addr, node, entry, DEADLINE).await });
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
        while let Some(resolution) = asked.join_next().await {
            assert_eq!(resolution.unwrap(), Resolution::Confirmed);
        }
        assert_eq!(b.served.ddl(), 32, "each create applied once: {:?}", b.served.windows());
        assert!(a.pushes.load(Ordering::SeqCst) <= 4, "{:?}", b.served.windows());
    }

    // --- T2 ---

    /// A change minted while a push is in flight is not resolved by it, but
    /// by the next push, which carries it (ADR-191).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_change_minted_after_the_in_flight_window_is_confirmed_by_the_next_push() {
        let b = member().await;
        *b.served.hold.lock() = Duration::from_millis(400);
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let first = create(&a.engine, "e1");
        let confirmer = Arc::clone(&a.confirmer);
        let one =
            tokio::spawn(async move { confirmer.confirm(b.addr, node, first, DEADLINE).await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        let second = create(&a.engine, "e2");
        let two = a.confirmer.confirm(b.addr, node, second, DEADLINE).await;
        assert_eq!(one.await.unwrap(), Resolution::Confirmed);
        assert_eq!(two, Resolution::Confirmed);
        assert_eq!(b.served.windows(), vec![(1, 0), (1, 0)], "e2 rode the second window");
    }

    // --- pure resolution: T3, T4 (fallback), T5, T16, T25, and T17 ---

    fn stamp(hlc: u64, node: u8) -> Stamp {
        Stamp::new(Hlc::new(hlc, 0), NodeId::from_bytes([node; 16]))
    }

    fn entry(at: Stamp, kind: OpKind) -> OplogEntry {
        OplogEntry { stamp: at, kind, collection: CollectionId(1), doc_id: None, body: None }
    }

    fn waiter(at: Stamp, pre_queued: bool) -> Waiter {
        let (answer, _) = oneshot::channel();
        Waiter {
            entry: entry(at, OpKind::CreateIndex),
            node: NodeId::from_bytes([9; 16]),
            pre_queued,
            answer,
        }
    }

    fn answered(sent: &[Stamp], ddl_in_window: usize, versions: &[Stamp], reply: Reply) -> Answer {
        let mut vector = VersionVector::new();
        for at in versions {
            vector.observe(*at);
        }
        Answer {
            their_node: NodeId::from_bytes([9; 16]),
            sent: sent.to_vec(),
            ddl_in_window,
            versions: vector,
            reply,
        }
    }

    /// T3: an entry in the window but above the vector it was introduced with
    /// is deferred by the member, so the push does not resolve it; and a stop
    /// is judged by position in the window, not by stamp.
    #[test]
    fn an_entry_in_the_window_but_above_its_versions_is_not_resolved_by_it() {
        let (e1, e2) = (stamp(1, 1), stamp(2, 1));
        let answer = answered(&[e1, e2], 2, &[e1], Reply::default());
        assert_eq!(resolve_answered(&waiter(e1, true), &answer), Some(Resolution::Confirmed));
        assert_eq!(resolve_answered(&waiter(e2, false), &answer), None, "carried");

        // The contract case: a window whose order at the stop is not stamp
        // order. Real windows are in stamp order (T17); this pins what
        // "before the stop" means.
        let (late, early) = (stamp(9, 1), stamp(3, 2));
        let stopped = Reply { unknown_collection: 1, stopped_at: Some(early), ..Reply::default() };
        let answer = answered(&[late, early], 2, &[late, early], stopped);
        assert_eq!(
            resolve_answered(&waiter(late, true), &answer),
            Some(Resolution::Confirmed),
            "before the stop in the window, though its stamp is higher"
        );
    }

    /// T4's fallback half and T16: an answer whose per-change fields do not
    /// account for its counts (an older member) is never read as refusing a
    /// change it cannot name (ADR-191).
    #[test]
    fn an_old_style_answer_refuses_nothing_it_cannot_attribute() {
        let (e1, e2) = (stamp(1, 1), stamp(2, 1));
        // Zero counts: attributable by empty fields, so covered is confirmed.
        let quiet = answered(&[e1, e2], 2, &[e1, e2], Reply::default());
        assert_eq!(resolve_answered(&waiter(e2, true), &quiet), Some(Resolution::Confirmed));

        // A refusal the answer does not name, in a window of two changes.
        let unnamed = Reply { ddl_refused: 1, ..Reply::default() };
        let two = answered(&[e1, e2], 2, &[e1, e2], unnamed.clone());
        for at in [e1, e2] {
            assert_eq!(
                resolve_answered(&waiter(at, true), &two).map(|r| r.outcome()),
                Some(ConfirmOutcome::Unattributable)
            );
        }
        // One change: the old per-window rule is exact there.
        let one = answered(&[e1], 1, &[e1], unnamed);
        assert_eq!(resolve_answered(&waiter(e1, true), &one), Some(Resolution::Refused));
        // A purge stop the answer does not place: pending, never confirmed.
        let purging = Reply { purge_pending: 1, ..Reply::default() };
        let two = answered(&[e1, e2], 2, &[e1, e2], purging);
        assert_eq!(
            resolve_answered(&waiter(e1, true), &two).map(|r| r.outcome()),
            Some(ConfirmOutcome::Unattributable)
        );
    }

    /// T4's named half: a refusal in a shared window refuses only its own
    /// change.
    #[test]
    fn a_named_refusal_refuses_only_its_own_change() {
        let (e1, e2) = (stamp(1, 1), stamp(2, 1));
        let reply = Reply { ddl_refused: 1, refused: vec![e1], ..Reply::default() };
        let answer = answered(&[e1, e2], 2, &[e1, e2], reply);
        assert_eq!(resolve_answered(&waiter(e1, true), &answer), Some(Resolution::Refused));
        assert_eq!(resolve_answered(&waiter(e2, true), &answer), Some(Resolution::Confirmed));
    }

    /// T5: a stop in a shared window leaves the stop and what follows it
    /// pending, and confirms what came before it.
    #[test]
    fn a_stop_in_a_shared_window_leaves_what_follows_it_pending() {
        let (e1, e2, e3) = (stamp(1, 1), stamp(2, 1), stamp(3, 1));
        let purge = Reply { purge_pending: 1, stopped_at: Some(e2), ..Reply::default() };
        let answer = answered(&[e1, e2, e3], 3, &[e1, e2, e3], purge);
        assert_eq!(resolve_answered(&waiter(e1, true), &answer), Some(Resolution::Confirmed));
        for at in [e2, e3] {
            assert_eq!(
                resolve_answered(&waiter(at, true), &answer).map(|r| r.outcome()),
                Some(ConfirmOutcome::Purging)
            );
        }
        let unknown = Reply { unknown_collection: 1, stopped_at: Some(e2), ..Reply::default() };
        let answer = answered(&[e1, e2, e3], 3, &[e1, e2, e3], unknown);
        assert_eq!(resolve_answered(&waiter(e2, true), &answer), Some(Resolution::Refused));
        assert_eq!(
            resolve_answered(&waiter(e3, true), &answer).map(|r| r.outcome()),
            Some(ConfirmOutcome::StoppedUnknown)
        );
    }

    /// T25: an old-style answer that stopped, in a window of one document
    /// then one change, does not call the unreached change refused.
    #[test]
    fn an_old_style_answer_stopped_at_a_document_does_not_refuse_the_one_ddl() {
        let (doc, ddl) = (stamp(1, 1), stamp(2, 1));
        let stopped = Reply { unknown_collection: 1, ..Reply::default() };
        let answer = answered(&[doc, ddl], 1, &[doc, ddl], stopped);
        assert_eq!(
            resolve_answered(&waiter(ddl, true), &answer).map(|r| r.outcome()),
            Some(ConfirmOutcome::Unattributable)
        );
    }

    proptest::proptest! {
        /// T17: a modelled member, with the sync.rs deferral and stop rule,
        /// never has a change confirmed that it did not take, or refused
        /// (ADR-191's claim). Real windows are in stamp order (the oplog key
        /// order matches `Stamp`'s), so the model builds them that way.
        #[test]
        fn the_confirmer_never_confirms_what_a_modelled_receiver_did_not_take(
            kinds in proptest::collection::vec(0u8..3, 1..16),
            origins in proptest::collection::vec(0u8..2, 16),
            introduced in 0usize..17,
            stop in proptest::option::of((0usize..16, proptest::bool::ANY)),
            refuse in proptest::collection::vec(proptest::bool::ANY, 16),
            old_style in proptest::bool::ANY,
            flagged in proptest::collection::vec(proptest::bool::ANY, 16),
        ) {
            let n = kinds.len();
            let stamps: Vec<Stamp> =
                (0..n).map(|i| stamp(i as u64 + 1, origins[i] + 1)).collect();
            let is_ddl = |i: usize| kinds[i] > 0;
            // The vector introduced with the window: the first `introduced`
            // entries; the rest the member defers.
            let introduced = introduced.min(n);
            let versions: Vec<Stamp> = stamps[..introduced].to_vec();
            let covered_by_versions = |i: usize| {
                let mut vector = VersionVector::new();
                for at in &versions { vector.observe(*at); }
                stamps[i].hlc <= vector.get(stamps[i].node)
            };
            // The member's walk: deferred entries skipped, a stop ends it.
            let stop = stop.filter(|(at, _)| *at < n && covered_by_versions(*at));
            let mut taken = vec![false; n];
            let mut refused = Vec::new();
            let (mut ddl_refused, mut unknown, mut purge) = (0, 0, 0);
            let mut stopped_at = None;
            for i in 0..n {
                if !covered_by_versions(i) { continue; }
                if let Some((at, purging)) = stop && at == i {
                    stopped_at = Some(stamps[i]);
                    if purging { purge += 1 } else { unknown += 1 }
                    break;
                }
                taken[i] = true;
                if is_ddl(i) && refuse[i] {
                    refused.push(stamps[i]);
                    ddl_refused += 1;
                }
            }
            let reply = if old_style {
                Reply { ddl_refused, unknown_collection: unknown, purge_pending: purge, ..Reply::default() }
            } else {
                Reply { ddl_refused, unknown_collection: unknown, purge_pending: purge, refused: refused.clone(), stopped_at, ..Reply::default() }
            };
            let answer = answered(&stamps, (0..n).filter(|i| is_ddl(*i)).count(), &versions, reply);
            for i in (0..n).filter(|i| is_ddl(*i)) {
                // A flagged waiter's entry is always within the versions.
                let pre_queued = flagged[i] && covered_by_versions(i);
                if resolve_answered(&waiter(stamps[i], pre_queued), &answer) == Some(Resolution::Confirmed) {
                    proptest::prop_assert!(taken[i], "confirmed but not taken: {i}");
                    proptest::prop_assert!(!refused.contains(&stamps[i]), "confirmed but refused: {i}");
                }
                if pre_queued {
                    proptest::prop_assert!(resolve_answered(&waiter(stamps[i], true), &answer).is_some(), "a flagged waiter is resolved");
                }
            }
        }
    }

    // --- T6 ---

    /// A request whose deadline passes answers pending, and the push it was
    /// waiting on is still answered and read: the next change rides the
    /// next window, and nothing failed (ADR-191).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_request_deadline_does_not_cancel_the_push() {
        let b = member().await;
        *b.served.hold.lock() = Duration::from_millis(1200);
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let first = create(&a.engine, "e1");
        let one = a.confirmer.confirm(b.addr, node, first, Duration::from_millis(300)).await;
        assert_eq!(one.outcome(), ConfirmOutcome::Timeout);
        *b.served.hold.lock() = Duration::ZERO;
        let second = create(&a.engine, "e2");
        let two = a.confirmer.confirm(b.addr, node, second, DEADLINE).await;
        assert_eq!(two, Resolution::Confirmed);
        assert_eq!(
            *a.outcomes.lock(),
            vec![ConfirmOutcome::Timeout, ConfirmOutcome::Confirmed],
            "the first push was answered, not failed"
        );
        assert_eq!(b.served.windows().len(), 2, "{:?}", b.served.windows());
        assert_eq!(
            a.confirmer.answered.load(Ordering::SeqCst),
            2,
            "both pushes were read to their answers"
        );
    }

    // --- T7, T8 ---

    /// A push whose window was taken and not answered leaves its changes
    /// pending, and the member backs off: a change during the back-off is
    /// pending at once, with nothing dialled; after it, the next confirms.
    /// And once the back-off has expired and a confirmation has run since,
    /// nothing of the member is held (T8).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_push_leaves_its_window_pending_and_the_member_backs_off() {
        let b = member().await;
        b.served.break_next.store(1, Ordering::SeqCst);
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let first = create(&a.engine, "e1");
        let one = a.confirmer.confirm(b.addr, node, first, DEADLINE).await;
        assert_eq!(one.outcome(), ConfirmOutcome::Failed, "{one:?}");
        assert!(a.confirmer.backing_off(b.addr));

        let pushed = a.pushes.load(Ordering::SeqCst);
        let during = a.confirmer.confirm(b.addr, node, create(&a.engine, "e2"), DEADLINE).await;
        assert_eq!(during.outcome(), ConfirmOutcome::Backoff);
        assert_eq!(a.pushes.load(Ordering::SeqCst), pushed, "nothing sent during the back-off");

        tokio::time::sleep(Duration::from_millis(250)).await;
        let after = a.confirmer.confirm(b.addr, node, create(&a.engine, "e3"), DEADLINE).await;
        assert_eq!(after, Resolution::Confirmed);
        eventually("the member's queue emptied", || a.confirmer.members_waiting().is_empty()).await;
    }

    // --- T9, T15 ---

    /// A different member at the address answers neither for the old one.
    /// Waiters for two ids at one address are each resolved by their own id,
    /// by the same push (T15).
    #[tokio::test]
    async fn waiters_for_two_node_ids_at_one_address_are_resolved_each_by_its_own_id() {
        let b = member().await;
        let a = pusher_for(&b, quick());
        let old = b.engine.node_id();
        let addr = b.addr;
        b.serving.abort();
        let _ = b.serving.await;
        let replacement = member_on(TcpListener::bind(addr).await.unwrap());
        sync_into(&replacement.engine, &a.engine);
        let new = replacement.engine.node_id();

        let for_old = a.confirmer.enqueue(addr, old, create(&a.engine, "x")).unwrap();
        let for_new = a.confirmer.enqueue(addr, new, create(&a.engine, "y")).unwrap();
        let (for_old, for_new) = (for_old.await.unwrap(), for_new.await.unwrap());
        assert_eq!(for_old.outcome(), ConfirmOutcome::OtherMember, "{for_old:?}");
        assert_eq!(for_new, Resolution::Confirmed);
        assert_eq!(replacement.served.windows().len(), 1, "one push for both");
    }

    // --- T11, T12 ---

    /// T11, the safety property end to end: an entry minted between the read
    /// of what this node can serve and the window is in the window, deferred
    /// by the member, not resolved by that push (neither confirmed nor
    /// pending), and confirmed by the next.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_change_minted_between_mine_and_the_window_is_deferred_and_confirmed_by_the_next_push()
     {
        let b = member().await;
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let late = Arc::new(Mutex::new(None));
        {
            let (engine, confirmer, late) =
                (Arc::clone(&a.engine), Arc::clone(&a.confirmer), Arc::clone(&late));
            *a.confirmer.hooks.after_mine.lock() = Some(Box::new(move || {
                let entry = create(&engine, "e2");
                *late.lock() = Some(confirmer.enqueue(b.addr, node, entry).unwrap());
            }));
        }
        let one = a.confirmer.confirm(b.addr, node, create(&a.engine, "e1"), DEADLINE).await;
        assert_eq!(one, Resolution::Confirmed);
        let queued = late.lock().take().unwrap();
        let two = queued.await.unwrap();
        assert_eq!(two, Resolution::Confirmed);
        assert_eq!(
            b.served.windows(),
            vec![(1, 1), (1, 0)],
            "e2 deferred by the first window, applied from the second"
        );
    }

    /// T12: an entry minted and queued between the snapshot and the read of
    /// what this node can serve is within that read, so the push covers it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_waiter_arriving_between_the_snapshot_and_mine_is_covered_by_that_push() {
        let b = member().await;
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let late = Arc::new(Mutex::new(None));
        {
            let (engine, confirmer, late) =
                (Arc::clone(&a.engine), Arc::clone(&a.confirmer), Arc::clone(&late));
            *a.confirmer.hooks.after_snapshot.lock() = Some(Box::new(move || {
                let entry = create(&engine, "e2");
                *late.lock() = Some(confirmer.enqueue(b.addr, node, entry).unwrap());
            }));
        }
        let one = a.confirmer.confirm(b.addr, node, create(&a.engine, "e1"), DEADLINE).await;
        assert_eq!(one, Resolution::Confirmed);
        let queued = late.lock().take().unwrap();
        let two = queued.await.unwrap();
        assert_eq!(two, Resolution::Confirmed);
        assert_eq!(b.served.windows(), vec![(2, 0)], "both in the one window");
    }

    // --- T13, T19, T20 ---

    /// T13: a push that times out after its window was sent backs off, and
    /// the driver does not wedge; a push that times out before sending (a
    /// listener that never speaks TLS) fails within the push's bound and
    /// backs off nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_push_that_times_out_backs_off_and_the_driver_does_not_wedge() {
        let b = member().await;
        *b.served.hold.lock() = Duration::from_millis(1500);
        let config = ConfirmConfig { request_timeout: Duration::from_millis(500), ..quick() };
        let a = pusher_for(&b, config);
        let node = b.engine.node_id();
        let one = a.confirmer.confirm(b.addr, node, create(&a.engine, "e1"), DEADLINE).await;
        assert_eq!(one.outcome(), ConfirmOutcome::Failed, "{one:?}");
        assert!(a.confirmer.backing_off(b.addr), "the window was sent");
        let during = a.confirmer.confirm(b.addr, node, create(&a.engine, "e2"), DEADLINE).await;
        assert_eq!(during.outcome(), ConfirmOutcome::Backoff);
        *b.served.hold.lock() = Duration::ZERO;
        tokio::time::sleep(Duration::from_millis(1300)).await;
        let after = a.confirmer.confirm(b.addr, node, create(&a.engine, "e3"), DEADLINE).await;
        assert_eq!(after, Resolution::Confirmed);

        // A listener that accepts and never speaks: the dial is inside the
        // push's timeout, and nothing was sent.
        let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let silent_addr = silent.local_addr().unwrap();
        let _held = tokio::spawn(async move {
            let mut kept = Vec::new();
            while let Ok((socket, _)) = silent.accept().await {
                kept.push(socket);
            }
        });
        a.members.insert_for_test(silent_addr, node);
        let started = Instant::now();
        let failed =
            a.confirmer.confirm(silent_addr, node, create(&a.engine, "e4"), DEADLINE).await;
        assert_eq!(failed.outcome(), ConfirmOutcome::Failed, "{failed:?}");
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
        assert!(!a.confirmer.backing_off(silent_addr), "nothing was sent");
    }

    /// T19: a back-off outlives its driver, and doubles on the next push the
    /// member does not answer; an idle entry is purged once its back-off has
    /// long run out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_back_off_outlives_its_driver_and_doubles() {
        let b = member().await;
        b.served.break_next.store(2, Ordering::SeqCst);
        let config = ConfirmConfig {
            first_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(400),
            ..quick()
        };
        let a = pusher_for(&b, config);
        let node = b.engine.node_id();
        let one = a.confirmer.confirm(b.addr, node, create(&a.engine, "e1"), DEADLINE).await;
        assert_eq!(one.outcome(), ConfirmOutcome::Failed);
        assert_eq!(a.confirmer.backoff_step(b.addr), Some(Duration::from_millis(50)));
        eventually("the driver exited", || {
            a.confirmer.drivers_held() == 0 || {
                a.confirmer.reap();
                a.confirmer.drivers_held() == 0
            }
        })
        .await;
        assert_eq!(a.confirmer.members_waiting(), vec![b.addr], "the back-off outlives it");
        tokio::time::sleep(Duration::from_millis(80)).await;
        let two = a.confirmer.confirm(b.addr, node, create(&a.engine, "e2"), DEADLINE).await;
        assert_eq!(two.outcome(), ConfirmOutcome::Failed);
        assert_eq!(a.confirmer.backoff_step(b.addr), Some(Duration::from_millis(100)), "doubled");

        // Long after the pause, a confirmation elsewhere purges the entry.
        tokio::time::sleep(Duration::from_millis(600)).await;
        let elsewhere: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let _ = a.confirmer.confirm(elsewhere, node, create(&a.engine, "e3"), DEADLINE).await;
        assert!(!a.confirmer.members_waiting().contains(&b.addr));
    }

    /// T20: a failure before the window is sent backs off nothing; the next
    /// confirmation dials again and is confirmed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failure_before_the_window_is_sent_does_not_back_off() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let (probe, _dir) = engine();
        let (pushing, _pdir) = engine();
        pushing.create_collection("shop", "orders").unwrap();
        let members = Members::default();
        members.insert_for_test(addr, probe.node_id());
        let confirmer =
            Confirmer::new(Arc::clone(&pushing), SECRET.into(), members.clone(), quick());
        let refused =
            confirmer.confirm(addr, probe.node_id(), create(&pushing, "e1"), DEADLINE).await;
        assert_eq!(refused.outcome(), ConfirmOutcome::Failed, "{refused:?}");
        assert!(!confirmer.backing_off(addr));

        let b = member_on(TcpListener::bind(addr).await.unwrap());
        sync_into(&b.engine, &pushing);
        members.insert_for_test(addr, b.engine.node_id());
        let answered =
            confirmer.confirm(addr, b.engine.node_id(), create(&pushing, "e2"), DEADLINE).await;
        assert_eq!(answered, Resolution::Confirmed);
    }

    // --- T21 ---

    /// SWIM seeing the member come up again ends its back-off; a pull does
    /// not; and a removal alone keeps the generation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_member_seen_up_again_by_swim_ends_the_back_off() {
        let b = member().await;
        b.served.break_next.store(1, Ordering::SeqCst);
        let config = ConfirmConfig {
            first_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_secs(60),
            ..quick()
        };
        let a = pusher_for(&b, config);
        let node = b.engine.node_id();
        let one = a.confirmer.confirm(b.addr, node, create(&a.engine, "e1"), DEADLINE).await;
        assert_eq!(one.outcome(), ConfirmOutcome::Failed);
        assert!(a.confirmer.backing_off(b.addr));

        // A pull between: B serves reads, which says nothing of its writer.
        let pull_dir = tempfile::tempdir().unwrap();
        let puller = Engine::open(&pull_dir.path().join("kimmy.redb")).unwrap();
        let _ = crate::transport::sync_once(&puller, b.addr, SECRET, None).await;
        let during = a.confirmer.confirm(b.addr, node, create(&a.engine, "e2"), DEADLINE).await;
        assert_eq!(during.outcome(), ConfirmOutcome::Backoff, "a pull does not end it");

        let before = a.members.generation(&b.addr);
        a.members.remove_for_test(&b.addr);
        assert_eq!(a.members.generation(&b.addr), before, "a removal keeps the generation");
        a.members.insert_for_test(b.addr, node);
        let after = a.confirmer.confirm(b.addr, node, create(&a.engine, "e3"), DEADLINE).await;
        assert_eq!(after, Resolution::Confirmed, "up again: the back-off is over");
    }

    // --- T23 ---

    /// The driver set holds live drivers, not every driver that ever ran.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_driver_set_stays_bounded() {
        let b = member().await;
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        for i in 0..200 {
            let resolution = a
                .confirmer
                .confirm(b.addr, node, create(&a.engine, &format!("f{i}")), DEADLINE)
                .await;
            assert_eq!(resolution, Resolution::Confirmed);
            assert!(a.confirmer.drivers_held() <= 2, "{} after {i}", a.confirmer.drivers_held());
        }
    }

    // --- T24 ---

    /// An arrival for the old id at an address, whose entry is inside the
    /// window the new member answered, is not confirmed for the old id.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_arrival_for_the_old_node_id_is_not_confirmed_by_the_new_members_answer() {
        let b = member().await;
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let old = NodeId::from_bytes([42; 16]);
        let first = create(&a.engine, "e1");
        let arrival = Arc::new(Mutex::new(None));
        {
            let (confirmer, arrival, entry) =
                (Arc::clone(&a.confirmer), Arc::clone(&arrival), first.clone());
            *a.confirmer.hooks.after_snapshot.lock() = Some(Box::new(move || {
                *arrival.lock() = Some(confirmer.enqueue(b.addr, old, entry).unwrap());
            }));
        }
        let one = a.confirmer.confirm(b.addr, node, first, DEADLINE).await;
        assert_eq!(one, Resolution::Confirmed);
        let queued = arrival.lock().take().unwrap();
        let for_old = queued.await.unwrap();
        assert_eq!(for_old.outcome(), ConfirmOutcome::OtherMember, "{for_old:?}");
    }

    // --- T14 ---

    /// A change past the batch cap is unreached, and so is one that arrived
    /// during the push and was carried: neither is sent a window that stops
    /// short of it (ADR-143).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_window_cut_by_the_batch_cap_pends_the_pre_queued_and_carries_the_arrivals() {
        let b = member().await;
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let orders = a.engine.get_collection("shop", "orders").unwrap();
        for i in 0..(MAX_BATCH + 50) {
            a.engine.insert(&orders, bson::doc! { "_id": i as i64 }).unwrap();
        }
        let late = Arc::new(Mutex::new(None));
        {
            let (engine, confirmer, late) =
                (Arc::clone(&a.engine), Arc::clone(&a.confirmer), Arc::clone(&late));
            *a.confirmer.hooks.after_snapshot.lock() = Some(Box::new(move || {
                let entry = create(&engine, "e2");
                *late.lock() = Some(confirmer.enqueue(b.addr, node, entry).unwrap());
            }));
        }
        let one = a.confirmer.confirm(b.addr, node, create(&a.engine, "e1"), DEADLINE).await;
        assert_eq!(one.outcome(), ConfirmOutcome::Unreached, "{one:?}");
        let queued = late.lock().take().unwrap();
        let two = queued.await.unwrap();
        assert_eq!(two.outcome(), ConfirmOutcome::Unreached, "{two:?}");
        assert_eq!(a.pushes.load(Ordering::SeqCst), 0, "no window stops short of a change");
    }

    /// A window cut by the batch cap between two changes queued before it:
    /// the one inside it is confirmed, and the one past the cut is unreached,
    /// resolved by that push rather than carried to another (ADR-191).
    #[tokio::test]
    async fn a_window_cut_between_two_queued_changes_confirms_one_and_pends_the_other() {
        let b = member().await;
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let inside = create(&a.engine, "e1");
        let orders = a.engine.get_collection("shop", "orders").unwrap();
        for i in 0..MAX_BATCH {
            a.engine.insert(&orders, bson::doc! { "_id": i as i64 }).unwrap();
        }
        let past = create(&a.engine, "e2");
        let inside = a.confirmer.enqueue(b.addr, node, inside).unwrap();
        let past = a.confirmer.enqueue(b.addr, node, past).unwrap();
        assert_eq!(inside.await.unwrap(), Resolution::Confirmed);
        let past = past.await.unwrap();
        assert_eq!(past.outcome(), ConfirmOutcome::Unreached, "{past:?}");
        assert_eq!(b.served.windows().len(), 1, "one window, cut at the cap");
    }

    /// A window cut by the frame limit rather than the batch cap: large
    /// documents between two queued changes, re-read at the count that fits.
    /// The change inside is confirmed, the one past the cut is unreached.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_window_cut_by_the_frame_limit_confirms_what_fits() {
        let b = member().await;
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let inside = create(&a.engine, "e1");
        let orders = a.engine.get_collection("shop", "orders").unwrap();
        let body = "x".repeat(4 * 1024 * 1024);
        for i in 0..20i64 {
            a.engine.insert(&orders, bson::doc! { "_id": i, "body": &body }).unwrap();
        }
        let past = create(&a.engine, "e2");
        let inside = a.confirmer.enqueue(b.addr, node, inside).unwrap();
        let past = a.confirmer.enqueue(b.addr, node, past).unwrap();
        assert_eq!(inside.await.unwrap(), Resolution::Confirmed);
        let past = past.await.unwrap();
        assert_eq!(past.outcome(), ConfirmOutcome::Unreached, "{past:?}");
    }

    /// A first step configured above the longest is clamped to it, so the
    /// next failure does not halve it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_first_back_off_step_is_never_above_the_longest() {
        let b = member().await;
        b.served.break_next.store(2, Ordering::SeqCst);
        let config = ConfirmConfig {
            first_backoff: Duration::from_millis(300),
            max_backoff: Duration::from_millis(100),
            ..quick()
        };
        let a = pusher_for(&b, config);
        let node = b.engine.node_id();
        let one = a.confirmer.confirm(b.addr, node, create(&a.engine, "e1"), DEADLINE).await;
        assert_eq!(one.outcome(), ConfirmOutcome::Failed);
        assert_eq!(a.confirmer.backoff_step(b.addr), Some(Duration::from_millis(100)));
        tokio::time::sleep(Duration::from_millis(150)).await;
        let two = a.confirmer.confirm(b.addr, node, create(&a.engine, "e2"), DEADLINE).await;
        assert_eq!(two.outcome(), ConfirmOutcome::Failed);
        assert_eq!(a.confirmer.backoff_step(b.addr), Some(Duration::from_millis(100)));
    }

    /// A request dropped mid-wait is counted `cancelled`, once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cancelled_request_is_counted() {
        let b = member().await;
        *b.served.hold.lock() = Duration::from_millis(1000);
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let confirmer = Arc::clone(&a.confirmer);
        let entry = create(&a.engine, "e1");
        let waiting =
            tokio::spawn(async move { confirmer.confirm(b.addr, node, entry, DEADLINE).await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(*a.outcomes.lock(), vec![ConfirmOutcome::Cancelled]);
    }

    /// A confirmation dropped before it was ever polled is counted
    /// `cancelled`: the count belongs to the future from the call.
    #[tokio::test]
    async fn a_confirmation_dropped_before_it_runs_is_counted() {
        let b = member().await;
        let a = pusher_for(&b, quick());
        let waiting =
            a.confirmer.confirm(b.addr, b.engine.node_id(), create(&a.engine, "e1"), DEADLINE);
        drop(waiting);
        assert_eq!(*a.outcomes.lock(), vec![ConfirmOutcome::Cancelled]);
        assert!(a.confirmer.members_waiting().is_empty(), "and nothing was queued");
    }

    /// A panic inside a confirmation is counted `task_ended`, not `cancelled`.
    #[tokio::test]
    async fn a_panic_inside_a_confirmation_is_counted_as_the_task_ending() {
        let b = member().await;
        let a = pusher_for(&b, quick());
        a.confirmer.hooks.panic_on_enqueue.store(true, Ordering::SeqCst);
        let waiting =
            a.confirmer.confirm(b.addr, b.engine.node_id(), create(&a.engine, "e1"), DEADLINE);
        let ended = tokio::spawn(waiting).await;
        assert!(ended.is_err_and(|e| e.is_panic()));
        assert_eq!(*a.outcomes.lock(), vec![ConfirmOutcome::TaskEnded]);
    }

    // --- T18 ---

    /// Shutdown aborts the drivers: their waiters read "the push task ended",
    /// the confirmer is held by nothing but its owner, and dropping it
    /// releases the engine.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_aborts_the_drivers_and_releases_the_engine() {
        let b = member().await;
        *b.served.hold.lock() = Duration::from_millis(2000);
        let a = pusher_for(&b, quick());
        let node = b.engine.node_id();
        let before = Arc::strong_count(&a.confirmer);
        let entry = create(&a.engine, "e1");
        let confirmer = Arc::clone(&a.confirmer);
        let waiting =
            tokio::spawn(async move { confirmer.confirm(b.addr, node, entry, DEADLINE).await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        a.confirmer.abort_all();
        let resolution = waiting.await.unwrap();
        assert_eq!(resolution.outcome(), ConfirmOutcome::TaskEnded, "{resolution:?}");
        eventually("the drivers let go of the confirmer", || {
            a.confirmer.reap();
            Arc::strong_count(&a.confirmer) == before
        })
        .await;
        let engine_before = Arc::strong_count(&a.engine);
        let Pusher { confirmer, engine, .. } = a;
        drop(confirmer);
        assert_eq!(
            Arc::strong_count(&engine),
            engine_before - 1,
            "the confirmer's engine is released"
        );
    }

    // --- T4 end to end ---

    /// A refusal in a shared window, end to end: the member holds one name
    /// under a definition it cannot arbitrate against, and refuses only that
    /// change; the other in the same window is confirmed.
    #[tokio::test]
    async fn a_refusal_in_a_shared_window_refuses_only_its_own_change() {
        let b = member().await;
        let (source, _sdir) = engine();
        source.create_collection("shop", "orders").unwrap();
        source
            .create_index(
                "shop",
                "orders",
                vec![IndexField::ascending("email")],
                false,
                Some("by_email".into()),
            )
            .unwrap();
        let mut page = source.snapshot_page(None, None).unwrap();
        for state in &mut page.collections {
            for index in &mut state.indexes {
                index.created = None;
            }
        }
        page.documents.clear();
        page.versions = VersionVector::default();
        let a = pusher_for(&b, quick());
        b.engine
            .apply_snapshot_page(
                a.engine.node_id(),
                &mut kimmy_storage::SnapshotProgress::whole_database(),
                &page,
            )
            .unwrap();
        let node = b.engine.node_id();
        a.engine
            .create_index(
                "shop",
                "orders",
                vec![IndexField::ascending("email")],
                true,
                Some("by_email".into()),
            )
            .unwrap();
        let clash = a.engine.entries_for_peer(Hlc::ZERO, MAX_BATCH).unwrap().entries.pop().unwrap();
        let fine = create(&a.engine, "by_name");
        let clash = a.confirmer.enqueue(b.addr, node, clash).unwrap();
        let fine = a.confirmer.enqueue(b.addr, node, fine).unwrap();
        assert_eq!(clash.await.unwrap(), Resolution::Refused);
        assert_eq!(fine.await.unwrap(), Resolution::Confirmed);
        assert_eq!(b.served.windows().len(), 1, "one window for both");
    }
}
