//! The replication loop: find peers, sync with them, repeat.

use std::collections::{BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kimmy_core::NodeId;
use kimmy_storage::Engine;
use tracing::{Instrument, debug, info, warn};

use crate::discovery::SeedSource;
use crate::health::{DEFAULT_FANOUT, PeerHealth};
use crate::membership::Members;
use crate::transport::{DivergenceProbe, PeerStalls, Repair, sync_once_with};

/// How often to run a round against every known peer.
pub const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(5);

/// How often to re-resolve the seed sources.
///
/// Separate from the sync interval and deliberately slower: DNS is the
/// expensive half, and a Kubernetes Service does not change pod set every few
/// seconds. But it must happen *repeatedly* — a node that resolved only at
/// startup would never see a peer that joined after it.
pub const DEFAULT_DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);

/// What the loop reports about a peer's staleness after each successful round:
/// the peer, and how far it trails this node when that exceeds tombstone
/// retention (`None` when within the window).
pub type PeerStalenessHook = Arc<dyn Fn(NodeId, Option<u64>) + Send + Sync>;

/// What one sync tick did, for the caller to count (ADR-123).
///
/// Everything here is a fact only the loop can see: which rounds failed,
/// which peers it is leaving alone, and what the rounds that succeeded had
/// to skip. Each is a number that says "something is wrong" when
/// `kimmy_replication_lag_seconds` says nothing — a failed round reports no
/// lag at all, by design (ADR-122), so a cluster wedged on a round that
/// fails every time read 0 lag and every member live for as long as it was
/// wedged.
///
/// A tick makes one **contact** per peer and may make several **pulls**
/// within it, draining a backlog while the batch cap keeps truncating it
/// (ADR-157). The counters of what the entries did — `ddl_refused`,
/// `ddl_declined`, the two `entries_skipped_*` and `repair_rounds` — sum
/// over the pulls, which is the work the tick actually did. The divergence
/// pair is per contact, one `divergence_checks` or one `divergence_skips`
/// per peer, decided by the tick's last pull at it; and `failed` is per
/// contact too, since a failure ends one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RoundReport {
    /// Rounds in this tick that failed, whatever the cause: a peer that could
    /// not be reached, a handshake that was refused, a batch that could not
    /// be applied. One per peer attempted, so at most the fanout. A caller
    /// adds this to a counter; a counter that keeps rising while the lag
    /// gauge sits at 0 is the shape of the silent wedge.
    pub failed: usize,
    /// Peers this node is currently backing off from after failures, as of
    /// the end of the tick: 0 when every known peer answered its last
    /// round, or when there are no peers. A level, for a gauge.
    pub backing_off: usize,
    /// Replicated schema changes the rounds in this tick could not apply to
    /// this node's state and skipped — `SyncOutcome::ddl_refused`, summed
    /// over the peers reached. Each one is an index this node now lacks and
    /// its peers hold, and nothing will retry it; the `warn!` at the time
    /// names it.
    pub ddl_refused: usize,
    /// Replicated index drops the rounds in this tick declined because the
    /// index here was created after the drop — `SyncOutcome::ddl_declined`,
    /// summed over the peers reached (ADR-141).
    pub ddl_declined: usize,
    /// Collections the cross-member divergence check currently has confirmed
    /// against some peer (ADR-133): held there and not here, or held by
    /// both with disagreeing document counts. A level, like `backing_off` —
    /// but keyed per peer, and for the count half per collection *and* peer,
    /// not per tick: a finding clears only when a later contact with the
    /// specific peer that reported it (and, for a count finding, the same
    /// probe of the same collection against that peer) no longer sees it,
    /// never merely because some tick's union of every peer reached that
    /// tick came up empty. See `DivergenceTracker::observe` for why a
    /// per-tick reading would have left this permanently unable to confirm
    /// past a handful of peers, or past one collection. It moves for exactly
    /// the condition that left every other field in this report at its
    /// healthiest value while the cluster silently lost data.
    pub divergent_collections: usize,
    /// Contacts in this tick whose round actually ran the cross-member
    /// check — the pull reached the peer's true tail, so `sync_once`
    /// returned a `divergent` set rather than `None` (ADR-135). A counter,
    /// summed over the peers reached.
    ///
    /// Without it `divergent_collections` reading 0 means two different
    /// things — *checked, and the peers agree* and *not checked at all* —
    /// and an operator cannot tell them apart from the metric alone. This
    /// is the half that says the reading is worth something.
    ///
    /// Incremented in the same branch that folds the finding into
    /// `DivergenceTracker`, not beside it, so this can never report a check
    /// the tracker was not told about.
    pub divergence_checks: usize,
    /// Contacts in this tick whose round did **not** run the check: the
    /// round completed but the pull was truncated by the batch cap
    /// (ADR-133's skip, ADR-135's counter), or the round failed (ADR-145).
    /// A counter, on the same terms.
    ///
    /// A failed round is in `failed` *and* here, since ADR-145: whatever
    /// failed and however far it got, the gauge was not re-examined on
    /// that round, which is the one thing this counter says. So
    /// `divergence_checks + divergence_skips` is every round this tick
    /// attempted, and `failed` is the part of the skips that failed. Before
    /// ADR-145 a failed round was in neither, and a member whose every
    /// round failed read as *both flat* — the same shape as a member with
    /// no peers — while its gauge went on serving a number half an hour
    /// old.
    pub divergence_skips: usize,
    /// Checked contacts in this tick in which the count half of the check
    /// compared the probed collection's count against the peer's
    /// (ADR-145). A counter. `divergence_checks` says the check ran;
    /// this says the half that catches a lost run of documents did.
    pub divergence_count_compared: usize,
    /// Checked contacts in this tick in which the count half was deferred:
    /// the rotation named a collection and the probe was dropped because
    /// the peer is behind this node and still advancing (ADR-133 defect 2,
    /// as amended by ADR-145). A counter. Rising on a busy cluster is
    /// ordinary; `divergence_count_compared` flat while this and
    /// `divergence_checks` rise is a count half that has not looked at
    /// anything.
    pub divergence_count_deferred: usize,
    /// When the check last ran against any peer — the instant of the last
    /// contact whose round ran it; `None` before the first such contact
    /// (ADR-145, ADR-154). Carried on every tick, checked or not, so it is
    /// a level the receiver replaces. The receiver computes the age from it
    /// *when the age is read*, not when this report was made: a report
    /// that carried the age itself, as ADR-145 first had it, froze the age
    /// with everything else on a member whose tick did not end — a loop
    /// waiting on the single writer pushed nothing for an hour, and the one
    /// series meant to say its gauge was old read the same number on every
    /// scrape (ADR-154). An instant cannot go stale: seconds since it is a
    /// subtraction the reader does against its own clock, whether or not
    /// this loop ever ticks again.
    pub divergence_last_check: Option<Instant>,
    /// Batches the rounds in this tick stopped short at an entry for a
    /// collection this node does not hold — `SyncOutcome::unknown_collection`,
    /// summed over the peers reached (ADR-148). A counter. Each one is a
    /// window this node re-serves from the same place next round until the
    /// collection is here; the round plans a snapshot from the peer to
    /// bring it, and `repair_rounds` counts that happening.
    pub entries_skipped_unknown_collection: usize,
    /// Entries the rounds in this tick left for a later window because
    /// they sat above the vector the peer had advertised for their origin
    /// — `SyncOutcome::deferred`, summed (ADR-148). A counter. Ordinary and
    /// rare on a busy cluster: the peer appended them between advertising
    /// and serving, and the next round takes them from the right position.
    pub entries_skipped_beyond_advertised: usize,
    /// Pulls in this tick spent repairing against a peer (ADR-148):
    /// re-serving its oplog from below this node's position, or pulling its
    /// snapshot, because the check confirmed a divergence against it or a
    /// batch stopped at a collection this node lacks. A counter. Rising is
    /// a repair under way; it stops when the repair is done.
    pub repair_rounds: usize,
    /// Where the tick's pulls spent their time, how long what they carried
    /// had waited, and how each contact ended (ADR-175). Counters and
    /// histograms, summed over the tick.
    pub pulls: PullReport,
}

/// Upper bounds of `kimmy_sync_pull_seconds`, in microseconds (ADR-175).
///
/// Dense where an ordinary pull lands, and reaching as far as a phase can
/// really go:
/// - **Bottom:** a converged window of a few entries on localhost takes about
///   a millisecond.
/// - **Ordinary pulls:** a full 1,024-entry batch applied in 25–30 ms on a
///   local benchmark (0.30.1's notes), and round 0310's drain fitted 22–23
///   full windows into a five-second tick, about 220 ms a pull.
/// - **Top:** not the tick. The first pull of a contact always runs, and
///   `cluster.sync_interval_secs` has no upper bound. `serve` is bounded by
///   the 30 s request timeout. `wait` is bounded by nothing: a replicated
///   batch takes the writer without a budget, behind whatever holds it, and a
///   retention pass has held it for ten to twelve minutes. So the bounds
///   run past 30 s to fifteen minutes.
pub const PULL_BUCKETS_US: [u64; 16] = [
    1_000,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    30_000_000,
    60_000_000,
    300_000_000,
    900_000_000,
];

/// Upper bounds of `kimmy_sync_entry_wait_seconds`, in microseconds
/// (ADR-175).
///
/// An entry written just before a tick waits for the pull alone, a few
/// milliseconds; one written just after waits the whole interval, five
/// seconds by default; and one behind a backlog waits as long as the drain.
/// So the bounds are dense below the interval, where one tick's cadence is
/// read, and wide above it, where a backlog's age is, up to the hour.
pub const ENTRY_WAIT_BUCKETS_US: [u64; 11] = [
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    30_000_000,
    60_000_000,
    300_000_000,
    3_600_000_000,
];

/// A histogram as the loop gathers it: observations in each bucket, **not**
/// cumulative, with an implicit `+Inf` the count covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Histogram<const N: usize> {
    pub buckets: [u64; N],
    pub count: u64,
    pub sum_us: u64,
}

impl<const N: usize> Default for Histogram<N> {
    fn default() -> Self {
        Self { buckets: [0; N], count: 0, sum_us: 0 }
    }
}

impl<const N: usize> Histogram<N> {
    /// One observation against `bounds`, in microseconds.
    pub fn observe(&mut self, value: Duration, bounds: &[u64; N]) {
        let us = u64::try_from(value.as_micros()).unwrap_or(u64::MAX);
        if let Some(slot) = bounds.iter().position(|upper| us <= *upper) {
            self.buckets[slot] += 1;
        }
        self.count += 1;
        self.sum_us = self.sum_us.saturating_add(us);
    }

    /// Fold in another histogram over the same bounds.
    pub fn add(&mut self, other: &Self) {
        for (into, from) in self.buckets.iter_mut().zip(other.buckets) {
            *into += from;
        }
        self.count += other.count;
        self.sum_us = self.sum_us.saturating_add(other.sum_us);
    }
}

/// How a tick's contact with a peer ended (ADR-175): whether it left a
/// backlog behind for the next tick, and if so what stopped the drain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContactEnd {
    /// The last pull did not come back truncated: nothing more could be
    /// pulled from this peer at once. The ordinary end. Not proof there is
    /// nothing left — a batch stopped at a collection this node lacks ends
    /// here too, and `kimmy_sync_entries_skipped_total` says so — and not
    /// proof nothing is waiting: what the peer appends after its window was
    /// served waits for the next tick.
    CaughtUp,
    /// Truncated, and the next pull would not have fitted in what was left
    /// of the tick (ADR-157): a backlog carried into the next tick because
    /// the tick's time ran out.
    Budget,
    /// Truncated with time left, at [`MAX_PULLS_PER_CONTACT`]: a backlog
    /// carried because the contact made as many pulls as one may.
    Ceiling,
    /// A pull failed, which ends the contact however far it got.
    Failed,
}

impl ContactEnd {
    pub const COUNT: usize = 4;
    pub const ALL: [ContactEnd; Self::COUNT] =
        [Self::CaughtUp, Self::Budget, Self::Ceiling, Self::Failed];

    pub fn label(self) -> &'static str {
        match self {
            Self::CaughtUp => "caught_up",
            Self::Budget => "budget",
            Self::Ceiling => "ceiling",
            Self::Failed => "failed",
        }
    }

    pub fn slot(self) -> usize {
        self as usize
    }
}

/// What the tick's pulls measured (ADR-175). See [`PullTiming`](kimmy_storage::PullTiming).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PullReport {
    /// The peer's walk and the wire, per pull.
    pub serve: Histogram<{ PULL_BUCKETS_US.len() }>,
    /// Waiting for this node's single writer while applying, per pull.
    pub wait: Histogram<{ PULL_BUCKETS_US.len() }>,
    /// Applying, less the wait, per pull.
    pub apply: Histogram<{ PULL_BUCKETS_US.len() }>,
    /// Entries the pulls carried. Against `apply`'s sum, the cost of one.
    pub entries: u64,
    /// How long the oldest entry this node lacked had waited when its pull
    /// arrived, per pull that carried one.
    pub entry_wait: Histogram<{ ENTRY_WAIT_BUCKETS_US.len() }>,
    /// Pulls whose oldest lacked entry was stamped ahead of this node's
    /// clock, and so could not be observed in `entry_wait`.
    pub entry_wait_ahead: u64,
    /// Contacts, by how they ended, in [`ContactEnd::ALL`] order.
    pub contacts: [u64; ContactEnd::COUNT],
}

impl PullReport {
    /// Fold in one pull.
    pub fn pulled(&mut self, pull: &kimmy_storage::PullTiming) {
        self.serve.observe(pull.serve, &PULL_BUCKETS_US);
        self.wait.observe(pull.wait, &PULL_BUCKETS_US);
        self.apply.observe(pull.apply, &PULL_BUCKETS_US);
        self.entries += pull.entries as u64;
        match pull.oldest_lacked {
            Some(kimmy_storage::EntryWait::Waited(waited)) => {
                self.entry_wait.observe(waited, &ENTRY_WAIT_BUCKETS_US)
            }
            Some(kimmy_storage::EntryWait::Ahead) => self.entry_wait_ahead += 1,
            None => {}
        }
    }

    /// Count one contact's end.
    pub fn ended(&mut self, end: ContactEnd) {
        self.contacts[end.slot()] += 1;
    }

    /// Fold in another report: a tick's, into a running total.
    pub fn add(&mut self, other: &PullReport) {
        for (into, from) in [
            (&mut self.serve, &other.serve),
            (&mut self.wait, &other.wait),
            (&mut self.apply, &other.apply),
        ] {
            into.add(from);
        }
        self.entries += other.entries;
        self.entry_wait.add(&other.entry_wait);
        self.entry_wait_ahead += other.entry_wait_ahead;
        for (into, from) in self.contacts.iter_mut().zip(other.contacts) {
            *into += from;
        }
    }
}

/// What a contact does after a pull that succeeded (ADR-157, ADR-175).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AfterPull {
    /// Pull from this peer again this tick.
    Again,
    /// The contact is over, and this is why.
    End(ContactEnd),
}

/// Decide [`AfterPull`] from whether the pull came back `truncated`, whether
/// another would `fit` in the tick, and how many `pulls` the contact has
/// made. Out of the loop so the decision the `ended` label reports is one
/// with a test of its own, and so the label cannot be decided apart from what
/// the loop does.
fn after_pull(truncated: bool, fits: bool, pulls: usize) -> AfterPull {
    match (truncated, fits) {
        (false, _) => AfterPull::End(ContactEnd::CaughtUp),
        (true, false) => AfterPull::End(ContactEnd::Budget),
        (true, true) if pulls < MAX_PULLS_PER_CONTACT => AfterPull::Again,
        (true, true) => AfterPull::End(ContactEnd::Ceiling),
    }
}

/// What the loop reports after every sync tick. See [`RoundReport`].
pub type RoundHook = Arc<dyn Fn(RoundReport) + Send + Sync>;

pub struct ReplicationConfig {
    pub seeds: Vec<SeedSource>,
    pub secret: String,
    /// This node's own listener, so it does not sync with itself.
    pub local: SocketAddr,
    pub sync_interval: Duration,
    pub discovery_interval: Duration,
    /// Peers contacted per round.
    ///
    /// A cap rather than a quota: a cluster smaller than this contacts
    /// everyone. Keeping it constant is what makes the per-round cost
    /// independent of cluster size.
    pub fanout: usize,
    /// Where to send discovered addresses so membership can announce to them.
    ///
    /// Membership finds the rest of the cluster by gossip once it has *one*
    /// contact, but a node that starts alone has none — so discovery keeps
    /// feeding it, not only at startup.
    pub announce: Option<tokio::sync::mpsc::Sender<SocketAddr>>,
    /// Live members according to SWIM, when membership is running.
    ///
    /// Preferred over discovery once it knows anyone: discovery reports who was
    /// *configured*, membership reports who is *up*, and only the second can
    /// tell a node that was removed from a Service from one that never
    /// answered. Discovery remains the bootstrap and the fallback.
    pub members: Option<Members>,
    /// Called after each sync round with the round's worst replication lag,
    /// in milliseconds: how far behind in time this node is against the peers
    /// it reached (ADR-122). Milliseconds since ADR-175: whole seconds,
    /// truncated, could not read an effect of a few seconds.
    ///
    /// A callback rather than a metrics handle: the peer's version vector —
    /// the only thing lag can honestly be computed from — exists nowhere but
    /// this loop, and this crate has no business knowing what the caller does
    /// with the number (ADR-043 called this shape out when deferring the
    /// metric). Not called when no peer was reached: an unreachable cluster
    /// has *unknown* lag, and overwriting the last known value with zero
    /// would report the outage as perfect health.
    pub on_lag: Option<std::sync::Arc<dyn Fn(u64) + Send + Sync>>,
    /// This node's `storage.tombstone_retention_secs`: the window past which a
    /// peer that has not caught up may resurrect a delete (ADR-085).
    pub tombstone_retention: Duration,
    /// Called after every successful round with the peer's id and, when it
    /// trails this node by more than tombstone retention, by how much;
    /// `None` when it is within the window. Same shape as `on_lag`, for the
    /// same reason: the peer's vector exists nowhere but this loop.
    pub on_peer_staleness: Option<PeerStalenessHook>,
    /// Called once after every sync tick, reached peers or not, with what
    /// the tick did that `on_lag` cannot say: rounds that failed, peers
    /// being backed off, schema changes refused (ADR-123). Same shape as
    /// `on_lag`, for the same reason — this crate knows nothing about
    /// metrics — and unlike `on_lag` it *is* called when no peer was
    /// reached, because "every round failed" is precisely the report.
    pub on_round: Option<RoundHook>,
}

/// The most pulls one contact makes in a tick, however much of the tick's
/// budget is left (ADR-157's addendum). A constant, not a setting.
///
/// The budget ends an ordinary drain, and it cannot end a bad one early: a
/// peer that answers every pull with a window that moves nothing and says it
/// is not exhausted is pulled from again until the tick's deadline. What such
/// a pull costs is its connection: a fresh TCP and TLS handshake and the HMAC
/// exchange, about 14 ms on localhost even from a peer doing no work, which
/// is 145 pulls in a two-second tick and 326 in a five-second one. A pull a
/// real drain makes costs more — round 0310's re-serve drain fitted 22–23
/// full windows into each five-second tick — so the two cannot be told apart
/// by rate, only bounded.
///
/// 128 is above every tick of real draining measured, with room to spare, and
/// at the default interval it holds a peer like that to under two seconds of
/// a five-second tick. What it costs a real drain is a spill: a tick that
/// could have pulled more than 128 full windows (about 131,000 entries) from
/// one peer leaves the rest to the next tick, one tick later per 131,000
/// entries at most.
pub const MAX_PULLS_PER_CONTACT: usize = 128;

impl ReplicationConfig {
    pub fn new(seeds: Vec<SeedSource>, secret: String, local: SocketAddr) -> Self {
        Self {
            seeds,
            secret,
            local,
            sync_interval: DEFAULT_SYNC_INTERVAL,
            discovery_interval: DEFAULT_DISCOVERY_INTERVAL,
            fanout: DEFAULT_FANOUT,
            announce: None,
            members: None,
            on_lag: None,
            // The storage default; a caller with a configured window passes
            // its own, and zero disables the check.
            tombstone_retention: Duration::from_secs(24 * 60 * 60),
            on_peer_staleness: None,
            on_round: None,
        }
    }
}

/// Run anti-entropy against known peers, forever.
pub async fn replicate(engine: Arc<Engine>, config: ReplicationConfig) {
    let mut discovered: BTreeSet<SocketAddr> = BTreeSet::new();
    let mut health = PeerHealth::new(config.fanout, config.sync_interval);
    let mut discovery = ticker(config.discovery_interval);
    let mut sync = ticker(config.sync_interval);

    // The cross-member divergence check (ADR-133): which collection this
    // tick probes for a document count, and which findings have recurred
    // often enough to confirm. Owned by the loop, not the engine — like
    // `health` and `stale_peers` below, it is a fact about this process's
    // ticks, not about the data.
    let mut divergence = kimmy_storage::DivergenceTracker::new();
    // Where each peer last stood behind this node, and for how long
    // (ADR-145): what lets the count half tell a peer that is catching up
    // from one that has stopped. Owned here for the same reason as the
    // tracker, and read and written inside the round, which is where the
    // peer's vector exists.
    let mut stalls = PeerStalls::new();
    // A snapshot this node was part-way through when it stopped resumes where
    // it left off rather than transferring everything again (ADR-161). Never
    // fatal: the record is an optimisation, and a node that will not start
    // because it cannot read one is a worse failure than the transfer it
    // avoids.
    match engine.snapshots_to_resume() {
        Ok(recorded) => stalls.resume_snapshots(recorded),
        Err(e) => {
            warn!(error = %e, "could not read the recorded snapshot pulls; any that were in flight start again")
        }
    }
    // When the check last ran against anyone, so the report can say how old
    // the tracker's reading is (ADR-145). A tick in which every round fails
    // moves nothing else about the check. The report carries the instant,
    // not the age (ADR-154): the age is the reader's subtraction, so it
    // keeps rising while a tick of this loop is stuck and nothing here runs.
    let mut last_check = LastCheck::default();

    // Peers currently flagged as stale rejoiners, so the warning fires on the
    // transition and not on every round they stay that way.
    let mut stale_peers: BTreeSet<NodeId> = BTreeSet::new();
    let retention_ms = config.tombstone_retention.as_millis() as u64;

    loop {
        tokio::select! {
            _ = discovery.tick() => {
                discovered = resolve(&config.seeds, config.local).await;
                debug!(count = discovered.len(), "resolved peers");

                // Offer every resolved address to membership. Announcing to one
                // we already know is harmless — foca ignores it — and doing it
                // every interval is what lets a node that started alone join
                // the cluster whenever it appears.
                if let Some(announce) = &config.announce {
                    for peer in &discovered {
                        let _ = announce.try_send(*peer);
                    }
                }
            }
            _ = sync.tick() => {
                // When this tick began, so the tick that overran the
                // interval is named once it ends (ADR-154). The loop cannot
                // say anything *while* a tick is stuck — the stuck tick is
                // this arm, and a tick that starts while the previous one
                // has not finished does not exist in a `select!` loop, whose
                // arms run to completion — so the live signal for a stuck
                // tick is the age series computed on read, and this is the
                // line that says afterwards how long it was and that it was
                // this loop.
                let tick_started = Instant::now();
                // A subset, not everyone: anti-entropy is transitive, so a
                // write reaches the cluster through intermediate peers without
                // every node contacting every other one every interval.
                // Membership when it knows anyone, discovery otherwise. A node
                // that has just started has resolved seeds but not yet gossiped
                // with them, so discovery is what gets the first round out.
                let peers = match &config.members {
                    Some(members) if !members.is_empty() => {
                        let mut live = members.snapshot();
                        live.remove(&config.local);
                        live
                    }
                    _ => discovered.clone(),
                };

                // The round's worst lag across the peers actually reached.
                // `None` when nothing answered, and then nothing is reported:
                // an unreachable cluster has unknown lag, not zero lag.
                let mut round_lag: Option<u64> = None;
                let mut report = RoundReport::default();

                // This tick's turn in the divergence check's rotation
                // (ADR-133): a fresh, cheap read of what this node holds —
                // metadata only — and this node's own count of whichever
                // collection is up next, computed once and reused against
                // every peer this tick rather than once per peer.
                //
                // A failure to read it must not reach `advance_probe` at
                // all, empty set or otherwise: `advance_probe` sweeps its
                // count-side state against whatever it is handed, on the
                // reasoning that a collection absent from that set is
                // *provably* gone (ADR-133, defect 6) — true of a fresh
                // read, and false of a transient error standing in for one.
                // Passing an empty set on error used to be harmless, before
                // that sweep existed; now it would read a storage hiccup as
                // "this node holds nothing" and silently discard every live
                // count finding, the exact silence this check exists to
                // rule out. `advance_probe_on` is where that decision lives,
                // pulled out on its own so it is a function with a test
                // rather than a branch inside this loop.
                let mine_collections = engine.all_collection_ids();
                if let Err(e) = &mine_collections {
                    warn!(error = %e, "divergence check: could not list this node's own \
                          collections; skipping this tick's probe rotation");
                }
                let probe = advance_probe_on(&mut divergence, mine_collections).map(|id| {
                    // This node's count and the vector it is judged against,
                    // from one snapshot so the vector cannot name an entry the
                    // count missed (ADR-168). A read of the kept count since
                    // ADR-174, not a walk; still off the worker, as every
                    // storage read from the loop is (ADR-153).
                    match kimmy_storage::blocking(|| engine.count_probe_reading(id)) {
                        Ok((vector, mine_count)) => {
                            DivergenceProbe { id, mine_count, mine_at: Some(vector) }
                        }
                        Err(e) => {
                            warn!(error = %e, collection = %id, "divergence check: could not \
                                  read this node's side of the count probe; comparing existence \
                                  only this tick");
                            DivergenceProbe { id, mine_count: None, mine_at: None }
                        }
                    }
                });
                // The tick's wall-clock budget for draining (ADR-157). A
                // peer whose pull the batch cap truncated is pulled from
                // again inside this tick rather than left for the next one,
                // so a member behind by more than one batch is not held to
                // one batch per interval — 1,024 entries every five seconds
                // whatever the wire or the writer could do. The budget is
                // the interval itself, and it is spent against a margin —
                // see `Contact::fits_before` — so a tick that drains stops
                // short of the period it owns rather than one pull past it,
                // and ADR-154's overrun warning goes on meaning a tick that
                // was stuck rather than a tick that was busy.
                let deadline = tick_started + config.sync_interval;
                // A new tick, so the next pull at each peer opens that
                // peer's contact: what the repair machinery counts in
                // rounds is counted once per contact, not once per pull
                // (ADR-148's cooldown is stated in wall-clock terms).
                stalls.tick_opened();
                // Chosen once: `select` advances its own rotation, so
                // asking it again mid-tick would move on to other peers
                // rather than hand back the ones this tick is draining.
                // A peer still truncated goes to the back of the queue, so
                // the tick round-robins its peers rather than draining one
                // to exhaustion: a member behind on two origins advances on
                // both, and one deep backlog does not spend the whole
                // budget.
                let mut draining: VecDeque<Contact> =
                    health.select(&peers, Instant::now()).into_iter().map(Contact::new).collect();
                while let Some(mut contact) = draining.pop_front() {
                    let peer = contact.peer;
                    // Sequential rather than concurrent: a round is cheap when
                    // converged, and syncing with every peer at once would make
                    // a large cluster stampede one node that fell behind.
                    let span = contact.span();
                    let started = Instant::now();
                    let pulled =
                        sync_once_with(&engine, peer, &config.secret, probe.clone(), &mut stalls)
                            .instrument(span)
                            .await;
                    let took = started.elapsed();
                    // Taken whether the round succeeded or not: a window
                    // applied before a failure later in the round is work
                    // done, and the pull series must not lose it in exactly
                    // the conditions they exist to diagnose (ADR-175).
                    if let Some(pull) = stalls.take_pull() {
                        report.pulls.pulled(&pull);
                    }
                    // Likewise what those applies refused, declined and
                    // skipped: counted as they committed, not only when the
                    // round went on to succeed (ADR-177).
                    let applied = stalls.take_applied();
                    report.ddl_refused += applied.ddl_refused;
                    report.ddl_declined += applied.ddl_declined;
                    report.entries_skipped_unknown_collection += applied.unknown_collection;
                    report.entries_skipped_beyond_advertised += applied.deferred;
                    match pulled {
                        Ok(mut outcome) => {
                            contact.pulled(&outcome, took);
                            health.succeeded(peer);
                            report.repair_rounds += usize::from(outcome.repairing);
                            // More of the peer's oplog behind the cap, and
                            // budget left to go and get it: this contact is
                            // not over, so nothing below runs for it yet.
                            // Everything below is decided by the tick's
                            // *last* pull from the peer — the lag it left,
                            // where the peer stands, and above all the
                            // divergence accounting, which stays exactly one
                            // check or one skip per peer per tick (ADR-133,
                            // ADR-135, ADR-145). A truncated pull has no
                            // check to fold in and would otherwise be
                            // counted as a skip on a tick that goes on to
                            // check.
                            let fits = outcome.truncated && contact.fits_before(deadline);
                            let ended = match after_pull(outcome.truncated, fits, contact.pulls) {
                                AfterPull::Again => {
                                    draining.push_back(contact);
                                    continue;
                                }
                                AfterPull::End(ended) => ended,
                            };
                            // How the contact ended, counted once per contact
                            // on the pull that ended it (ADR-175).
                            report.pulls.ended(ended);
                            if ended == ContactEnd::Ceiling {
                                // Budget left and still truncated at the
                                // ceiling (ADR-157's addendum). The contact
                                // ends here as though the budget had run out,
                                // and the next tick resumes from wherever
                                // this one stood. A real drain deeper than the
                                // ceiling reaches it too, and says so at info
                                // with what it applied; only a contact that
                                // applied nothing in all of its pulls is the
                                // shape of a peer serving windows that cannot
                                // advance, and that is the one worth a warning.
                                if contact.applied > 0 {
                                    info!(
                                        peer = %peer,
                                        pulls = contact.pulls,
                                        applied = contact.applied,
                                        "pull ceiling reached for this peer this tick; the next \
                                         tick resumes"
                                    );
                                } else {
                                    warn!(
                                        peer = %peer,
                                        pulls = contact.pulls,
                                        applied = 0,
                                        "pull ceiling reached for this peer this tick with nothing \
                                         applied in any pull; the next tick resumes"
                                    );
                                }
                            }
                            // `i64` throughout: `tracing-opentelemetry` has
                            // no `record_u64`, so an unsigned value is
                            // formatted with `Debug` and reaches a collector
                            // as a string nothing can graph.
                            contact.record("lag_ms", outcome.lag_ms as i64);
                            // The last pull's reading, not the worst of the
                            // tick's: lag is a level, and a drain that
                            // closed a backlog has left the peer where the
                            // final pull says it did.
                            round_lag = Some(round_lag.unwrap_or(0).max(outcome.lag_ms));
                            if let Some(node) = outcome.peer {
                                // Folded in only when the check actually ran
                                // against this peer this contact
                                // (`exhausted`, see `sync_once`) — a peer
                                // whose round had a backlog too deep to
                                // reach its tail must not be read as having
                                // reconciled, and `observe` treats "not
                                // called" and "called with nothing found" as
                                // the two different facts they are
                                // (ADR-133). `divergent` and `count_probe`
                                // are always set together by `sync_once`, so
                                // `divergent`'s presence alone gates both.
                                //
                                // The two counters are incremented in these
                                // same two arms rather than beside them
                                // (ADR-135), so what they report and what the
                                // tracker was told are decided by one branch
                                // and cannot drift: a "check" cannot be
                                // counted for a contact the fold below never
                                // received. `Some` — including `Some` of an
                                // empty set — is the check having run and
                                // found nothing, which is exactly what the
                                // gauge's 0 is supposed to mean; `None` is
                                // ADR-133's cap-truncation skip, which the
                                // gauge alone cannot distinguish from it. A
                                // round that *failed* reaches neither arm; it
                                // is counted in `failed` and as a skip in the
                                // `Err` arm below (ADR-145).
                                //
                                // The count half's own pair and the check's
                                // clock sit in this same arm for the same
                                // reason (ADR-145): a comparison the tracker
                                // was not handed cannot be counted, and the
                                // age cannot reset on a contact whose finding
                                // was not folded in.
                                if let Some(existence) = outcome.divergent.take() {
                                    report.divergence_checks += 1;
                                    last_check.ran(Instant::now());
                                    let count = outcome.count_probe.take();
                                    if count.is_some() {
                                        report.divergence_count_compared += 1;
                                    } else if outcome.count_probe_deferred {
                                        report.divergence_count_deferred += 1;
                                    }
                                    let findings =
                                        kimmy_storage::DivergenceFindings { existence, count };
                                    divergence.observe(node, findings);
                                    // Detection to repair (ADR-148): what
                                    // the check has confirmed against this
                                    // peer is asked for on the next round
                                    // with it, from below this node's
                                    // position — the collection's whole
                                    // history from its creation, or the
                                    // peer's snapshot for a collection this
                                    // node does not hold at all.
                                    let confirmed = divergence.confirmed_against(node);
                                    for collection in &confirmed {
                                        let repair = match engine.collection_by_id(*collection) {
                                            Ok(Some(meta)) => Repair::Replay { from: meta.created },
                                            _ => Repair::Snapshot,
                                        };
                                        if stalls.plan_repair(node, *collection, repair) {
                                            warn!(
                                                %peer,
                                                node = %node,
                                                collection = %collection,
                                                ?repair,
                                                "divergence confirmed; planned a repair from \
                                                 this peer on the next round with it"
                                            );
                                        }
                                    }
                                    stalls.retain_repaired(node, &confirmed);
                                } else {
                                    report.divergence_skips += 1;
                                }
                                let stale = retention_ms > 0 && outcome.behind_ms > retention_ms;
                                let was = stale_peers.contains(&node);
                                if stale && !was {
                                    stale_peers.insert(node);
                                    warn!(
                                        %peer,
                                        node = %node,
                                        behind_secs = outcome.behind_ms / 1_000,
                                        retention_secs = retention_ms / 1_000,
                                        "peer trails this node by more than tombstone \
                                         retention; deletes it missed may already be \
                                         collected here, so merging it can resurrect them \
                                         — a stale rejoiner should be reset, not merged"
                                    );
                                } else if !stale && was {
                                    stale_peers.remove(&node);
                                    info!(%peer, node = %node, "peer is back within tombstone retention");
                                }
                                if let Some(report) = &config.on_peer_staleness {
                                    report(node, stale.then_some(outcome.behind_ms));
                                }
                            }
                            contact.finish();
                        }
                        // A peer being unreachable is the normal state of a
                        // cluster, not an error worth stopping for — but it is
                        // worth backing off, so a node that is not coming back
                        // stops costing a connection every interval.
                        //
                        // A failure ends the tick's contact with this peer,
                        // however much budget is left and however truncated
                        // the pull before it was: a failed round goes
                        // through the health backoff, and retrying it inside
                        // the tick would spend the budget on a peer that has
                        // just said it cannot answer (ADR-157).
                        Err(e) => {
                            let now = Instant::now();
                            // Reported on the first failure and then at a
                            // bounded cadence, not once and never again: a peer
                            // that never recovers has to stay visible, or a
                            // half-converged cluster looks like a healthy one.
                            let due = health.failed(peer, now);
                            let failures = health.failures(peer);
                            report.failed += 1;
                            // A failed round did not run the check, whatever
                            // failed and however far it got — so it is a
                            // skip too (ADR-145). Not a check: running the
                            // check on the strength of a round that did not
                            // complete would reopen ADR-133's hole. Before
                            // this a member whose every round failed read
                            // as "both counters flat", indistinguishable
                            // from a member with no peers, while its gauge
                            // served a value nothing had re-examined.
                            report.divergence_skips += 1;
                            report.pulls.ended(ContactEnd::Failed);
                            if due {
                                warn!(%peer, error = %e, failures, "sync round failed; backing off");
                            } else {
                                debug!(%peer, error = %e, failures, "sync round failed");
                            }
                            // Whatever earlier pulls of this contact merged
                            // is still merged, and still worth one line.
                            contact.finish();
                        }
                    }
                }
                if let (Some(on_lag), Some(lag_ms)) = (&config.on_lag, round_lag) {
                    on_lag(lag_ms);
                }
                // Read every tick regardless of whether anything reports it,
                // for the same reason the line above computes `report`
                // unconditionally: the tracker's own state does not depend
                // on whether a caller wired up `on_round`, only `observe`
                // above does, and that already ran per peer contacted.
                report.divergent_collections = divergence.confirmed_count();
                // And when that reading was last re-examined (ADR-145): the
                // tracker keeps its last value through any number of ticks
                // in which no check ran, and this is what lets the reader
                // say how old it is. The instant rather than the age
                // (ADR-154), so that the reader's number goes on rising
                // through a tick of this loop that never ends.
                report.divergence_last_check = last_check.at();
                // Reported whether or not anything was reached: the tick in
                // which every round failed is the one an operator most needs
                // to hear about, and it is the one `on_lag` says nothing for.
                if let Some(on_round) = &config.on_round {
                    report.backing_off = health.backing_off(Instant::now());
                    on_round(report);
                }
                // A tick that took longer than the interval is the shape of
                // a loop that was stuck — behind the single writer, in the
                // round that found this — and it is logged once it is back,
                // with how long it was gone (ADR-154). Not per round: the
                // rounds run one after another inside the tick, and it is
                // the tick's length, not any one round's, that the age
                // series and the operator's threshold are written against.
                // The ticker does not catch up on missed ticks (`ticker`
                // below): one tick follows this one at once, the rest a full
                // interval apart, so a stall of any length is one line.
                let tick_took = tick_started.elapsed();
                if tick_took >= config.sync_interval {
                    warn!(
                        elapsed_secs = tick_took.as_secs(),
                        interval_secs = config.sync_interval.as_secs(),
                        peers = peers.len(),
                        "a sync tick took longer than cluster.sync_interval_secs; \
                         kimmy_sync_divergent_collections was not re-examined while it ran"
                    );
                }
            }
        }
    }
}

/// One tick's contact with one peer: the pulls it made, what they merged,
/// and how long the slowest of them took (ADR-157).
///
/// A tick makes as many pulls from a peer as the batch cap and its budget
/// call for, and they are one conversation, not one per pull. So the span
/// and the merged line belong to the contact rather than to a pull: a drain
/// reads as one line naming twelve pulls and twelve thousand entries, where
/// a line per pull would bury the tick that made them in twelve identical
/// ones. `pulls` is the number an operator reads a drain by — a steady 1 is
/// a cluster keeping up, a number that keeps rising is one that is not.
struct Contact {
    peer: SocketAddr,
    /// One span per peer per contact, not one per tick: an anti-entropy
    /// tick against three peers is three conversations with three different
    /// outcomes, and folding them into one span would lose which peer was
    /// the slow one — the only thing anybody opens this trace to find out.
    /// `applied`, `ddl` and `lag_ms` are declared empty and filled when the
    /// contact ends, so a contact whose pull failed still leaves a span with
    /// the peer on it rather than nothing at all.
    ///
    /// Built on the contact's first pull rather than when the tick queues
    /// it, because a subscriber stamps a span's start when it is *created*:
    /// building every peer's span at the top of the tick would start them
    /// all at the same instant and give a peer served last a duration
    /// inflated by however long the peers before it took, which is the one
    /// reading this span exists for.
    span: Option<tracing::Span>,
    pulls: usize,
    applied: usize,
    ddl: usize,
    /// Everything the pulls accounted for, [`kimmy_storage::SyncOutcome::total`]
    /// summed: what decides whether this contact merged anything worth a
    /// line, on the same terms as before a contact could make more than one
    /// pull.
    total: usize,
    /// The longest any of this contact's pulls took, and the estimate of
    /// what the next one would cost.
    slowest: Duration,
}

impl Contact {
    fn new(peer: SocketAddr) -> Self {
        Self { peer, span: None, pulls: 0, applied: 0, ddl: 0, total: 0, slowest: Duration::ZERO }
    }

    /// The span this contact's pulls run under, created on the first of
    /// them. See the field.
    fn span(&mut self) -> tracing::Span {
        let peer = self.peer;
        self.span
            .get_or_insert_with(|| {
                tracing::info_span!(
                    "cluster.sync",
                    otel.kind = "client",
                    peer = %peer,
                    applied = tracing::field::Empty,
                    ddl = tracing::field::Empty,
                    lag_ms = tracing::field::Empty,
                )
            })
            .clone()
    }

    /// Record a field on the contact's span, if it has one — it has, by the
    /// time anything is recorded, since only a pull produces a value.
    fn record(&self, field: &str, value: i64) {
        if let Some(span) = &self.span {
            span.record(field, value);
        }
    }

    /// Fold in what one pull of this contact brought back, and what it cost.
    fn pulled(&mut self, outcome: &kimmy_storage::SyncOutcome, took: Duration) {
        self.pulls += 1;
        self.applied += outcome.applied;
        self.ddl += outcome.ddl;
        self.total += outcome.total();
        self.slowest = self.slowest.max(took);
    }

    /// Whether another pull of this contact can be expected to finish before
    /// `deadline` — the tick's budget — judged on the slowest pull it has
    /// already made (ADR-157).
    ///
    /// The estimate, rather than "is there any time left at all": a tick
    /// that starts a pull with a millisecond to spare runs a whole pull past
    /// its own period, so *every* tick that saturated its budget would
    /// overrun it and fire ADR-154's warning — one line per tick for the
    /// whole of the backlog this drain exists to clear, in place of the one
    /// line per stall that warning was written to be.
    ///
    /// The slowest rather than the last, because a drain's pulls are the
    /// same shape — a full batch each — so the longest one is the honest
    /// estimate of the next, and one quick pull cannot talk the tick into a
    /// slow one. It is still an estimate: a pull slower than every pull
    /// before it, by more than the slack left over, can cross the line
    /// anyway. That makes the overrun rare and worth reading rather than
    /// impossible, which is what ADR-154's warning needs of it.
    fn fits_before(&self, deadline: Instant) -> bool {
        deadline.checked_duration_since(Instant::now()).is_some_and(|left| left > self.slowest)
    }

    /// The tick is done with this peer, having pulled from it or failed
    /// against it: record what the contact merged on its span, and say so
    /// once.
    fn finish(self) {
        self.record("applied", self.applied as i64);
        self.record("ddl", self.ddl as i64);
        if self.total > 0 {
            info!(
                peer = %self.peer,
                pulls = self.pulls,
                applied = self.applied,
                ddl = self.ddl,
                "merged from peer"
            );
        }
    }
}

/// A ticker for one of the loop's two arms, which does not catch up on
/// ticks it missed (ADR-154).
///
/// Tokio's default fires every missed tick at once, back to back, after a
/// tick that overran. A sync tick that waited on the single writer for an
/// hour at the default five-second interval would be followed by some 720
/// rounds against every peer the fanout selects, fired as fast as they
/// complete — a stampede on a cluster that has just come out of a stall,
/// to make up for ticks whose work the next one does anyway. `Delay` fires
/// the one tick that was due and schedules the rest a full interval apart
/// from there, as ADR-151 set on the retention collector for the same
/// reason. Discovery
/// gets the same for the same shape: a resolve that stalled on DNS should
/// not be followed by a burst of resolves.
fn ticker(period: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker
}

/// When the cross-member divergence check last ran against any peer
/// (ADR-145).
///
/// `DivergenceTracker::confirmed_count` is a level the loop re-reads every
/// tick, and it keeps its last value through any number of ticks in which
/// no check ran — a member whose every round fails re-reads the same number
/// for as long as the wedge lasts, and nothing on the report said how old it
/// was. This is the clock the report reads that from. It advances only in
/// the arm that folds a finding into the tracker, so the age can never
/// reset on a contact the tracker was not told about, and it advances for
/// a contact with *any* peer: the gauge is a union over peers, so its
/// reading is as fresh as the last check against anyone.
///
/// `None` before the first check, rather than a value: a node that has
/// never checked has no reading to be old. The caller renders that as `0`
/// beside a `ran` counter that also reads `0`, which is what ADR-135's
/// objection to an age gauge — that the never-checked case has no honest
/// number — comes down to once the counter is there to carry it.
///
/// The loop hands the reader the instant, not an age it computed
/// (ADR-154): an age computed here is as of the moment this loop last got
/// to the end of a tick, and on the member whose tick was stuck behind the
/// writer for an hour that moment was an hour old, so the age froze with
/// the gauge it was meant to qualify. The instant is what the reader
/// subtracts from its own clock, and that subtraction needs nothing from
/// this loop.
#[derive(Debug, Default)]
struct LastCheck(Option<Instant>);

impl LastCheck {
    /// A check ran, at `now`.
    fn ran(&mut self, now: Instant) {
        self.0 = Some(now);
    }

    /// When the last check ran; `None` if none has.
    fn at(&self) -> Option<Instant> {
        self.0
    }

    /// How long ago the last check ran, as of `now`; `None` if none has.
    /// The reader's subtraction (`Metrics` in `kimmy-api` does it against
    /// its own clock, ADR-154); here so the loop's half of the contract has
    /// a test of its own.
    #[cfg(test)]
    fn age(&self, now: Instant) -> Option<Duration> {
        self.at().map(|at| now.saturating_duration_since(at))
    }
}

/// The collection id this tick probes for a count, given the result of
/// reading this node's own collection set — or `None`, without touching
/// `tracker` at all, when that read failed.
///
/// A read failure is *not* an empty set standing in for one:
/// `DivergenceTracker::advance_probe` sweeps its count-side confirmation
/// state against whatever it is handed, on the premise that a collection
/// absent from that set is provably gone: this node no longer holds it
/// (ADR-133, defect 6). That premise holds for a genuine read and fails for
/// a read that merely errored — a transient storage hiccup is not evidence
/// this node suddenly holds zero collections, and reading it that way would
/// silently discard every live count finding for as long as the error
/// lasts. So `advance_probe` is not called at all on that path: the
/// rotation's cursor stays exactly where it was, and every finding survives
/// untouched into the next tick, which may read cleanly.
fn advance_probe_on<E>(
    tracker: &mut kimmy_storage::DivergenceTracker,
    mine: Result<BTreeSet<kimmy_core::CollectionId>, E>,
) -> Option<kimmy_core::CollectionId> {
    match mine {
        Ok(ids) => tracker.advance_probe(&ids),
        Err(_) => None,
    }
}

/// Resolve every seed source, dropping this node's own address.
async fn resolve(seeds: &[SeedSource], local: SocketAddr) -> BTreeSet<SocketAddr> {
    let mut out = BTreeSet::new();
    for seed in seeds {
        match seed.resolve().await {
            Ok(addrs) => out.extend(addrs),
            // A name that does not resolve yet is what a cluster looks like
            // while it is starting. Logged, not fatal.
            Err(e) => warn!(seed = %seed.describe(), error = %e, "could not resolve seed"),
        }
    }

    // A headless Service resolves to *every* pod including this one, and a node
    // syncing with itself would do work to learn nothing.
    out.remove(&local);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_contact_ends_on_what_stopped_it() {
        // ADR-175's `ended` label is this decision, so each arm is checked
        // on its own: an untruncated pull ends the contact whatever else is
        // true, a truncated one that would not fit is the budget, and one
        // that would fit pulls again until the ceiling.
        assert_eq!(after_pull(false, true, 1), AfterPull::End(ContactEnd::CaughtUp));
        assert_eq!(
            after_pull(false, false, MAX_PULLS_PER_CONTACT),
            AfterPull::End(ContactEnd::CaughtUp)
        );
        assert_eq!(after_pull(true, false, 1), AfterPull::End(ContactEnd::Budget));
        assert_eq!(
            after_pull(true, false, MAX_PULLS_PER_CONTACT),
            AfterPull::End(ContactEnd::Budget),
            "out of time at the ceiling is still out of time"
        );
        assert_eq!(after_pull(true, true, MAX_PULLS_PER_CONTACT - 1), AfterPull::Again);
        assert_eq!(
            after_pull(true, true, MAX_PULLS_PER_CONTACT),
            AfterPull::End(ContactEnd::Ceiling)
        );
    }

    #[test]
    fn an_entry_stamped_ahead_is_counted_and_not_observed_as_a_wait() {
        // ADR-175: a wait that would be negative is clock skew, and folding
        // it into the histogram as zero would read as an entry that arrived
        // at once. It is counted apart instead, and a pull that carried
        // nothing this node lacked is neither.
        let pull = |oldest_lacked| kimmy_storage::PullTiming {
            entries: 3,
            oldest_lacked,
            ..Default::default()
        };
        let mut report = PullReport::default();
        report.pulled(&pull(Some(kimmy_storage::EntryWait::at(1_000, 900))));
        report.pulled(&pull(None));
        report.pulled(&pull(Some(kimmy_storage::EntryWait::at(1_000, 3_500))));

        assert_eq!(report.entry_wait_ahead, 1, "{report:?}");
        assert_eq!(report.entry_wait.count, 1, "only the real wait is observed: {report:?}");
        assert_eq!(report.entry_wait.sum_us, 2_500_000, "{report:?}");
        assert_eq!(report.serve.count, 3, "every pull is a pull, whatever it carried: {report:?}");
        assert_eq!(report.entries, 9, "{report:?}");
    }

    #[tokio::test]
    async fn resolution_drops_this_node() {
        // Kubernetes headless DNS returns every pod, including the one asking.
        let local: SocketAddr = "127.0.0.1:7900".parse().unwrap();
        let other: SocketAddr = "127.0.0.1:7901".parse().unwrap();
        let seeds = vec![SeedSource::Static(vec![local, other])];

        let peers = resolve(&seeds, local).await;

        assert_eq!(peers, BTreeSet::from([other]), "a node must not sync with itself");
    }

    #[tokio::test]
    async fn an_unresolvable_seed_does_not_lose_the_others() {
        // One bad DNS name must not blind a node to every peer it can reach.
        let good: SocketAddr = "127.0.0.1:7901".parse().unwrap();
        let seeds = vec![
            SeedSource::Dns { name: "no-such-host.invalid".into(), port: 7900 },
            SeedSource::Static(vec![good]),
        ];

        let peers = resolve(&seeds, "127.0.0.1:7900".parse().unwrap()).await;

        assert!(peers.contains(&good));
    }

    #[tokio::test]
    async fn duplicate_addresses_are_collapsed() {
        // Overlapping seed sources are normal; syncing twice per round is not.
        let a: SocketAddr = "127.0.0.1:7901".parse().unwrap();
        let seeds = vec![SeedSource::Static(vec![a]), SeedSource::Static(vec![a])];

        let peers = resolve(&seeds, "127.0.0.1:7900".parse().unwrap()).await;

        assert_eq!(peers.len(), 1);
    }

    /// The fanout scaling defect, composed the way the real loop composes
    /// it: `PeerHealth::select`'s own rotation feeding `DivergenceTracker`,
    /// not a hand-written stand-in for either. `DivergenceTracker`'s unit
    /// tests already pin that it confirms across non-adjacent contacts in
    /// isolation; this is the piece that pins the *other* half actually
    /// produces contacts shaped that way at a cluster size the default
    /// fanout cannot cover in one pass, so a future change to either
    /// `select`'s rotation policy or the tracker's keying cannot silently
    /// re-break the composition while each component's own tests stay
    /// green.
    #[test]
    fn a_peer_confirms_despite_a_fanout_smaller_than_the_cluster() {
        use std::collections::HashMap;

        let peers: BTreeSet<SocketAddr> =
            (0..8).map(|i| format!("127.0.0.1:{}", 7900 + i).parse().unwrap()).collect();
        // Stands in for the handshake's introduction in the real protocol,
        // which is where a `SocketAddr` and a `NodeId` are actually paired.
        let ids: HashMap<SocketAddr, NodeId> = peers
            .iter()
            .enumerate()
            .map(|(i, &addr)| (addr, NodeId::from_bytes((i as u128).to_be_bytes())))
            .collect();
        // Past the sixth of eight peers at the default fanout of 3 -- two
        // ticks in a row cannot possibly both reach it by round-robin alone.
        let divergent_peer = *peers.iter().nth(5).unwrap();
        let divergent_collection = kimmy_core::CollectionId(1);

        let mut health = PeerHealth::new(DEFAULT_FANOUT, Duration::from_secs(5));
        let mut tracker = kimmy_storage::DivergenceTracker::new();
        let now = Instant::now();

        for _ in 0..40 {
            for addr in health.select(&peers, now) {
                health.succeeded(addr);
                let existence = if addr == divergent_peer {
                    BTreeSet::from([divergent_collection])
                } else {
                    BTreeSet::new()
                };
                let findings = kimmy_storage::DivergenceFindings { existence, count: None };
                tracker.observe(ids[&addr], findings);
            }
            if tracker.confirmed_count() > 0 {
                break;
            }
        }
        assert_eq!(tracker.confirmed_count(), 1, "confirms at 8 peers despite fanout 3");
    }

    /// Defect 6's sweep introduced a regression of its own: a transient
    /// failure to read this node's own collections must not be handed to
    /// `advance_probe` as an empty set, or it silently discards every live
    /// count finding on the strength of an error rather than a fact.
    #[test]
    fn a_read_failure_leaves_confirmed_count_findings_untouched() {
        let mut tracker = kimmy_storage::DivergenceTracker::new();
        let p = NodeId::from_bytes(1u128.to_be_bytes());
        let mismatched = kimmy_storage::DivergenceFindings {
            existence: BTreeSet::new(),
            count: Some((kimmy_core::CollectionId(7), true)),
        };
        tracker.observe(p, mismatched.clone());
        tracker.observe(p, mismatched);
        assert_eq!(tracker.confirmed_count(), 1, "confirmed before the read failure");

        // Stands in for `engine.all_collection_ids()` failing this tick —
        // not for it succeeding with nothing in it.
        let failed: Result<BTreeSet<kimmy_core::CollectionId>, &str> = Err("transient");
        let probe = advance_probe_on(&mut tracker, failed);

        assert_eq!(probe, None, "nothing to probe when the read itself failed");
        assert_eq!(
            tracker.confirmed_count(),
            1,
            "a read failure must not be read as \"this node holds nothing\""
        );
    }

    /// The margin the drain's budget is spent against (ADR-157): a tick
    /// starts another pull only when the pull before it would have fitted
    /// in the time left, so a tick that saturates its budget stops short of
    /// its own period rather than one whole pull past it.
    ///
    /// Deciding on "is there any time left at all" instead makes every tick
    /// that saturates its budget end *after* `tick_started + sync_interval`,
    /// which fires ADR-154's overrun warning on every one of them — one line
    /// per tick for the whole of a backlog, in place of the one line per
    /// stall that warning was written to be, and in exactly the condition
    /// draining exists to clear. Measured at 0.07 s to 2.33 s past a
    /// two-second interval, on every saturated tick.
    #[test]
    fn a_pull_is_started_only_when_the_pull_before_it_would_have_fitted() {
        let outcome = kimmy_storage::SyncOutcome::default();
        let mut contact = Contact::new("127.0.0.1:7900".parse().unwrap());
        let now = Instant::now();

        // Nothing measured yet. The first pull of a contact is not decided
        // here — the tick always makes it — so this only says a contact
        // with no measurement is not held back by one.
        assert!(contact.fits_before(now + Duration::from_millis(50)));
        assert!(!contact.fits_before(now), "no budget left, no pull");

        contact.pulled(&outcome, Duration::from_millis(400));
        assert!(
            contact.fits_before(now + Duration::from_millis(900)),
            "a pull's worth of budget and more to spare"
        );
        assert!(
            !contact.fits_before(now + Duration::from_millis(300)),
            "less budget than the pull before it took"
        );

        // The slowest pull of the contact, not the last: a drain's pulls are
        // a full batch each, so one quick one must not talk the tick into a
        // slow one.
        contact.pulled(&outcome, Duration::from_millis(10));
        assert!(!contact.fits_before(now + Duration::from_millis(300)));
    }

    /// A tick that overran is followed by *one* tick at once — the one that
    /// was already due — and then by the next a full interval later, not by
    /// a burst of every tick it missed (ADR-154). Under paused time the
    /// runtime advances the clock to the next due tick when nothing else
    /// can run, so "how long until the next tick" is the elapsed time around
    /// the await: nothing for the due one, one interval for the one after
    /// it, where tokio's default would give nothing 720 times over.
    #[tokio::test(start_paused = true)]
    async fn a_tick_that_overran_is_followed_by_the_next_a_full_interval_later() {
        let period = Duration::from_secs(5);
        let mut sync = ticker(period);
        sync.tick().await; // the first tick fires at once

        // The tick's work took an hour: 720 ticks fell due while it ran.
        tokio::time::advance(Duration::from_secs(3_600)).await;

        let before = tokio::time::Instant::now();
        sync.tick().await;
        assert_eq!(before.elapsed(), Duration::ZERO, "the tick that was due fires at once");
        let before = tokio::time::Instant::now();
        sync.tick().await;
        assert_eq!(
            before.elapsed(),
            period,
            "the one after it is a full interval later: no burst of the 719 others"
        );
        let before = tokio::time::Instant::now();
        sync.tick().await;
        assert_eq!(before.elapsed(), period, "and the spacing holds from there");
    }

    /// The age the reader computes from what the report carries (ADR-145,
    /// ADR-154): absent before any check has run, rising through ticks in
    /// which every round failed — nothing touches the clock on a failed
    /// round — and back to zero the moment a check runs. The report carries
    /// the instant; the age is a subtraction against whatever clock reads
    /// it, which is why it also rises through a tick that never ends.
    #[test]
    fn the_check_age_is_absent_then_rises_through_failed_rounds_and_resets_on_a_check() {
        let mut last = LastCheck::default();
        let t0 = Instant::now();
        assert_eq!(last.at(), None, "nothing has run, so there is no instant to carry");
        assert_eq!(last.age(t0), None, "nothing has run, so nothing is old");

        last.ran(t0);
        assert_eq!(last.at(), Some(t0), "the report carries the instant itself");
        assert_eq!(last.age(t0), Some(Duration::ZERO));

        // Three ticks of failed rounds: the `Err` arm never calls `ran`, so
        // the age is simply the clock.
        let t1 = t0 + Duration::from_secs(5);
        let t2 = t0 + Duration::from_secs(10);
        let t3 = t0 + Duration::from_secs(15);
        assert_eq!(last.age(t1), Some(Duration::from_secs(5)));
        assert_eq!(last.age(t2), Some(Duration::from_secs(10)));
        assert_eq!(last.age(t3), Some(Duration::from_secs(15)), "rises with the wedge");

        last.ran(t3);
        assert_eq!(last.age(t3), Some(Duration::ZERO), "a check resets it");
        assert_eq!(last.age(t3 + Duration::from_secs(2)), Some(Duration::from_secs(2)));
    }

    /// ADR-177 at the loop: a round that fails after its apply committed
    /// still reports what the apply refused. The receiver holds a definition
    /// it cannot arbitrate against the sender's, so the sender's create is
    /// refused; the round against the sender then fails right after its
    /// apply. The refusal must reach the round report once, and the failure
    /// must too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_round_that_fails_after_its_apply_still_reports_what_the_apply_refused() {
        const SECRET: &str = "a-loop-test-secret";
        async fn node() -> (Arc<Engine>, std::net::SocketAddr, tempfile::TempDir) {
            let dir = tempfile::tempdir().unwrap();
            let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(crate::transport::serve(Arc::clone(&engine), listener, SECRET.into()));
            (engine, addr, dir)
        }
        let (a, a_addr, _a_dir) = node().await;
        let (b, b_addr, _b_dir) = node().await;
        let source_dir = tempfile::tempdir().unwrap();
        let source = Engine::open(&source_dir.path().join("kimmy.redb")).unwrap();

        // B holds `by_email` with no creation stamp to arbitrate by; A creates
        // a different `by_email`, which B must refuse.
        source.create_collection("shop", "orders").unwrap();
        source
            .create_index(
                "shop",
                "orders",
                vec![kimmy_core::IndexField::ascending("email")],
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
        page.versions = kimmy_core::VersionVector::default();
        b.apply_snapshot_page(
            a.node_id(),
            &mut kimmy_storage::SnapshotProgress::whole_database(),
            &page,
        )
        .unwrap();
        a.create_collection("shop", "orders").unwrap();
        a.create_index(
            "shop",
            "orders",
            vec![kimmy_core::IndexField::ascending("email")],
            true,
            Some("by_email".into()),
        )
        .unwrap();

        *crate::transport::test_hooks::FAIL_AFTER_APPLY.lock().unwrap() = Some(a_addr);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
        let mut config =
            ReplicationConfig::new(vec![SeedSource::Static(vec![a_addr])], SECRET.into(), b_addr);
        config.sync_interval = Duration::from_millis(100);
        config.discovery_interval = Duration::from_millis(100);
        config.on_round = Some(Arc::new(move |report| {
            let _ = tx.send(report);
        }));
        let looping = tokio::spawn(replicate(Arc::clone(&b), config));

        let mut seen = RoundReport::default();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while seen.failed == 0 || seen.ddl_refused == 0 {
            let report = tokio::time::timeout_at(deadline, rx.recv())
                .await
                .unwrap_or_else(|_| panic!("no report carried both signals in time; saw {seen:?}"))
                .expect("the loop keeps reporting");
            seen.failed += report.failed;
            seen.ddl_refused += report.ddl_refused;
        }
        // A few more ticks: the refusal is not counted again.
        for _ in 0..3 {
            if let Ok(Some(report)) = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await
            {
                seen.ddl_refused += report.ddl_refused;
            }
        }
        looping.abort();
        *crate::transport::test_hooks::FAIL_AFTER_APPLY.lock().unwrap() = None;

        assert!(seen.failed >= 1, "the round failed after its apply: {seen:?}");
        assert_eq!(seen.ddl_refused, 1, "and its refusal is reported, once: {seen:?}");
    }
}
