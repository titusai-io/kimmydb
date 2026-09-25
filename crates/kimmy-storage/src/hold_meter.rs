//! What the single writer was held *for*, and what serving a peer's window
//! read (ADR-176).
//!
//! `kimmy_write_lock_held_seconds` says how long the writer was held and by
//! what kind of work. Under load it is the bottleneck for every write shape —
//! throughput has matched one over the mean hold — and nothing said what a hold
//! was made of. This module splits every hold two ways.
//!
//! **By what the holding thread was doing** ([`Component`]): inside the storage
//! backend reading a page, writing one, or asking for an fsync; on the CPU
//! outside those calls; or off the CPU outside them — descheduled, or waiting on
//! a lock inside redb. The split is sound because of a property of redb 4 rather
//! than an assumption: redb runs no threads of its own, so every page read, page
//! write and fsync a transaction causes happens on the thread that holds it,
//! through [`redb::StorageBackend`]. A write transaction never leaves the thread
//! that opened it — it holds the writer gate's guard, which is not `Send` — so
//! what the backend does on that thread between the gate being taken and let go
//! is what the hold contained, and nothing another thread does can land in it.
//!
//! **By where in the transaction** ([`Phase`]): the path's own work, the
//! live-count flush every commit makes (ADR-174's addendum), and redb's commit.
//!
//! The meter is a thread-local, installed for the length of a hold (or of a
//! served window's walk) and absent otherwise, so a backend call on a thread
//! with nothing installed costs one thread-local read and no clock.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use crate::engine::WriterHolder;

/// What a hold's time was spent on (ADR-176). Disjoint, and together the hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Component {
    /// Wall time inside the backend's `read`: a page redb's cache did not hold.
    /// Wall time here, and for `Write` and `Sync`, includes the meter's own two
    /// CPU clock reads around a call whose CPU is read, a few hundred
    /// nanoseconds, so the instrument's cost is attributed rather than left in
    /// `OffCpu`.
    /// Also its `len`, the file's size, which redb reads from inside a hold.
    Read,
    /// Wall time inside the backend's `write`: redb writing a page out. Also
    /// its `set_len`, which grows the file from inside a hold.
    Write,
    /// Wall time inside the backend's `sync_data`: the fsync.
    Sync,
    /// The thread's CPU time over the hold, less its CPU time inside the three
    /// calls above: B-tree work over cached pages, encoding, index keys, the
    /// live-count bookkeeping.
    Cpu,
    /// What is left: off the CPU, outside any backend call — scheduler delay,
    /// or a wait on a lock inside redb. **A residual**, so anything the other
    /// four fail to capture lands here too. **Read it only from a release
    /// build**: an unoptimised build spends real time off the CPU with nothing
    /// contending (430–580 ms of a 1.3–1.5 s uncontended bulk on macOS, against
    /// 0–6 ms in release), which reads as contention that is not there.
    OffCpu,
}

impl Component {
    pub const COUNT: usize = 5;
    pub const ALL: [Self; Self::COUNT] =
        [Self::Read, Self::Write, Self::Sync, Self::Cpu, Self::OffCpu];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Sync => "sync",
            Self::Cpu => "cpu",
            Self::OffCpu => "off_cpu",
        }
    }

    pub const fn slot(self) -> usize {
        self as usize
    }
}

/// Where in the transaction a hold's time was spent (ADR-176). Consecutive
/// spans of one timeline, so they add up to the hold exactly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// From taking the writer to asking to commit: the path's own reads and
    /// writes. The whole hold of a transaction that aborted.
    Work,
    /// The live counts and their mark, written once per transaction before
    /// the commit (ADR-174's addendum).
    Counts,
    /// redb's commit, page writes and fsync included, to letting go.
    Commit,
}

impl Phase {
    pub const COUNT: usize = 3;
    pub const ALL: [Self; Self::COUNT] = [Self::Work, Self::Counts, Self::Commit];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Work => "work",
            Self::Counts => "counts",
            Self::Commit => "commit",
        }
    }

    pub const fn slot(self) -> usize {
        self as usize
    }
}

/// Every write call in a hold up to this many has the thread's CPU time read
/// around it (ADR-176). Past it, one in [`WRITE_SAMPLE_EVERY`] does.
///
/// Reading a thread's CPU clock is a system call on Linux — the vDSO does not
/// serve it — at about 290 ns against 20 ns for the monotonic clock. A
/// single-document insert makes about 31 page writes, so it is measured exactly;
/// a bulk of 1,000 makes over 2,000, and a clock pair around each would have
/// cost more than 1% of its hold.
pub const WRITES_MEASURED: u64 = 32;

/// Past [`WRITES_MEASURED`], the share of write calls whose CPU time is read.
pub const WRITE_SAMPLE_EVERY: u64 = 32;

/// How far the four measured components may add up past the hold before the
/// hold is counted as over-counted: a fixed part for the clocks' granularity
/// and a proportional part for their per-call rounding.
pub const OVERCOUNT_TOLERANCE: Duration = Duration::from_millis(1);

/// The coarsest a thread CPU clock reading is rounded to on the platforms
/// that have one: macOS reports microseconds, Linux nanoseconds. Each call
/// whose CPU is read contributes two readings, so a sum of them can be off by
/// twice this per call.
pub const CPU_CLOCK_GRAIN: Duration = Duration::from_micros(1);

/// This thread's CPU time, or `None` where the platform has no per-thread CPU
/// clock (ADR-176).
///
/// `CLOCK_THREAD_CPUTIME_ID` on Linux and on macOS. Anywhere else a hold's
/// `cpu` and `off_cpu` are not recorded at all — never recorded as zero — and
/// [`HoldCounters::cpu_unmeasured`] counts the hold instead.
pub fn thread_cpu() -> Option<Duration> {
    #[cfg(test)]
    if test_hooks::CPU_CLOCK_FAILS.with(|f| f.get()) {
        return None;
    }
    #[cfg(test)]
    if let Some(stuck) = test_hooks::CPU_CLOCK_STUCK_AT.with(|s| s.get()) {
        return Some(stuck);
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        // SAFETY: `ts` is a valid, writable timespec for the call's duration.
        let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
        if rc == 0 {
            return Some(Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32));
        }
        None
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// Which backend call is being metered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Io {
    Read,
    Write,
    Sync,
    /// The file's size read. Metered with `read`.
    Len,
    /// The file grown or shrunk. Metered with `write`, but never sampled:
    /// its CPU is read exactly, and it counts neither as a page write nor
    /// towards the sampling of page writes.
    SetLen,
}

/// What the backend did on this thread while a meter was installed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Meter {
    /// Whether to read the CPU clock around calls at all: a hold does, a
    /// served window's walk, which needs only its reads' wall time, does not.
    cpu: bool,
    pub read: Duration,
    pub read_bytes: u64,
    pub read_cpu: Duration,
    pub write: Duration,
    pub write_bytes: u64,
    pub write_calls: u64,
    /// Page writes whose CPU was read, and their wall and CPU time.
    pub write_sampled_calls: u64,
    pub write_sampled: Duration,
    pub write_sampled_cpu: Duration,
    pub sync: Duration,
    pub sync_cpu: Duration,
    /// `len` and `set_len`: wall and CPU time, read exactly.
    pub len: Duration,
    pub len_cpu: Duration,
    pub set_len: Duration,
    pub set_len_cpu: Duration,
    /// Calls whose CPU time was read, of every kind: what the CPU
    /// comparison's tolerance scales with.
    pub cpu_reads: u64,
    /// Whether a CPU clock read the meter needed failed.
    pub cpu_failed: bool,
}

impl Meter {
    fn add(&mut self, inner: &Meter) {
        self.read += inner.read;
        self.read_bytes += inner.read_bytes;
        self.read_cpu += inner.read_cpu;
        self.write += inner.write;
        self.write_bytes += inner.write_bytes;
        self.write_calls += inner.write_calls;
        self.write_sampled_calls += inner.write_sampled_calls;
        self.write_sampled += inner.write_sampled;
        self.write_sampled_cpu += inner.write_sampled_cpu;
        self.sync += inner.sync;
        self.sync_cpu += inner.sync_cpu;
        self.len += inner.len;
        self.len_cpu += inner.len_cpu;
        self.set_len += inner.set_len;
        self.set_len_cpu += inner.set_len_cpu;
        self.cpu_reads += inner.cpu_reads;
        self.cpu_failed |= inner.cpu_failed;
    }
}

thread_local! {
    static METER: RefCell<Option<Meter>> = const { RefCell::new(None) };
}

/// Run one backend call, metering it if this thread has a meter installed.
pub(crate) fn io<T>(
    kind: Io,
    bytes: usize,
    call: impl FnOnce() -> std::io::Result<T>,
) -> std::io::Result<T> {
    let Some((cpu, write_calls)) =
        METER.with(|m| m.borrow().as_ref().map(|m| (m.cpu, m.write_calls)))
    else {
        return call();
    };
    let with_cpu = cpu
        && match kind {
            // The calls that can block on the disk, and there are few of them:
            // measured exactly, so time off the CPU inside them is never
            // mistaken for work.
            Io::Read | Io::Sync | Io::Len | Io::SetLen => true,
            Io::Write => write_calls < WRITES_MEASURED || write_calls % WRITE_SAMPLE_EVERY == 0,
        };
    // The CPU reads nest inside the wall-clock reads, so a call's CPU interval
    // lies within its wall interval and its CPU time cannot exceed its wall
    // time except by the two clocks' rounding. Read the other way round, the
    // CPU interval was the wider one and nearly every sampled write read more
    // CPU than wall time.
    let from = Instant::now();
    let cpu_from = if with_cpu { thread_cpu() } else { None };
    #[cfg(test)]
    let true_cpu_from = test_hooks::true_cpu_from(cpu, kind);
    #[cfg(test)]
    test_hooks::metered_call(kind);
    #[cfg(test)]
    test_hooks::inside_io(kind, write_calls);
    let result = call();
    #[cfg(test)]
    test_hooks::true_cpu_to(true_cpu_from);
    let cpu_spent = match cpu_from {
        Some(start) => thread_cpu().map(|end| end.saturating_sub(start)),
        None => None,
    };
    let wall = from.elapsed();
    METER.with(|m| {
        let mut m = m.borrow_mut();
        let Some(m) = m.as_mut() else { return };
        if with_cpu && cpu_spent.is_none() {
            m.cpu_failed = true;
        }
        if with_cpu {
            m.cpu_reads += 1;
        }
        let cpu_spent = cpu_spent.unwrap_or_default();
        match kind {
            Io::Read => {
                m.read += wall;
                m.read_bytes += bytes as u64;
                m.read_cpu += cpu_spent;
            }
            Io::Write => {
                m.write += wall;
                m.write_bytes += bytes as u64;
                m.write_calls += 1;
                if with_cpu {
                    m.write_sampled_calls += 1;
                    m.write_sampled += wall;
                    m.write_sampled_cpu += cpu_spent;
                }
            }
            Io::Sync => {
                m.sync += wall;
                m.sync_cpu += cpu_spent;
            }
            Io::Len => {
                m.len += wall;
                m.len_cpu += cpu_spent;
            }
            Io::SetLen => {
                m.set_len += wall;
                m.set_len_cpu += cpu_spent;
            }
        }
    });
    result
}

/// A meter installed on this thread until [`Scope::finish`].
///
/// Installing one where another is already installed puts the outer one aside;
/// finishing puts it back with the inner one's calls added, so an outer scope
/// sees everything that happened inside it.
pub(crate) struct Scope {
    outer: Option<Meter>,
    cpu_from: Option<Duration>,
    finished: bool,
}

impl Scope {
    /// Start metering a hold: the backend's calls and the thread's CPU time.
    pub(crate) fn hold() -> Self {
        Self::install(true)
    }

    /// Start metering a walk: the backend's calls' wall time and bytes only.
    pub(crate) fn walk() -> Self {
        Self::install(false)
    }

    fn install(cpu: bool) -> Self {
        // A walk's meter reads no CPU clock, so a hold it nested inside would
        // lose the CPU of the calls made under it and read that time as `cpu`.
        debug_assert!(
            cpu || METER.with(|m| m.borrow().as_ref().is_none_or(|outer| !outer.cpu)),
            "a scope that reads no CPU clock is nested inside a hold"
        );
        let outer = METER.with(|m| m.replace(Some(Meter { cpu, ..Meter::default() })));
        Self { outer, cpu_from: if cpu { thread_cpu() } else { None }, finished: false }
    }

    /// Stop metering, and return what was metered and the thread's CPU time
    /// over the scope (`None` when it could not be read).
    pub(crate) fn finish(mut self) -> (Meter, Option<Duration>) {
        let cpu = match self.cpu_from {
            Some(start) => thread_cpu().map(|end| end.saturating_sub(start)),
            None => None,
        };
        (self.uninstall(), cpu)
    }

    fn uninstall(&mut self) -> Meter {
        self.finished = true;
        let inner = METER.with(|m| m.take()).unwrap_or_default();
        let outer = self.outer.take().map(|mut outer| {
            outer.add(&inner);
            outer
        });
        METER.with(|m| *m.borrow_mut() = outer);
        inner
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        // A scope a panic unwinds through must not leave this thread metering
        // into it.
        if !self.finished {
            self.uninstall();
        }
    }
}

/// One hold, decomposed (ADR-176).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Decomposed {
    /// Indexed by [`Component::slot`]. `cpu` and `off_cpu` are zero when
    /// `cpu_measured` is false and are then not recorded.
    pub components: [Duration; Component::COUNT],
    pub cpu_measured: bool,
    /// The four measured components came to more than the hold, past
    /// [`OVERCOUNT_TOLERANCE`]: something was counted twice.
    pub overcounted: bool,
    /// Wall time of the writes whose CPU was estimated rather than read: the
    /// most the split between `cpu` and `off_cpu` can be wrong by.
    pub write_estimated: Duration,
    /// The CPU the estimate credited to those writes: `write_estimated` times
    /// the sampled writes' share of CPU. Kept for the tests only.
    #[cfg(test)]
    pub estimated_write_cpu: Duration,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

/// Split a hold of `hold` with `cpu_over_hold` of thread CPU time, given what
/// the backend did during it (ADR-176).
///
/// The CPU inside write calls whose CPU was not read is estimated from the
/// sampled writes' ratio of CPU to wall time. That estimate can be wrong in
/// either direction, and `write_estimated` bounds by how much.
pub(crate) fn decompose(
    meter: &Meter,
    hold: Duration,
    cpu_over_hold: Option<Duration>,
) -> Decomposed {
    let mut out = Decomposed {
        read_bytes: meter.read_bytes,
        write_bytes: meter.write_bytes,
        write_estimated: meter.write.saturating_sub(meter.write_sampled),
        ..Decomposed::default()
    };
    out.components[Component::Read.slot()] = meter.read + meter.len;
    out.components[Component::Write.slot()] = meter.write + meter.set_len;
    out.components[Component::Sync.slot()] = meter.sync;
    let io = meter.read + meter.len + meter.write + meter.set_len + meter.sync;
    let tolerance = OVERCOUNT_TOLERANCE + hold / 100;

    let cpu_over_hold = if meter.cpu_failed { None } else { cpu_over_hold };
    let Some(cpu_over_hold) = cpu_over_hold else {
        out.overcounted = io > hold + tolerance;
        return out;
    };
    out.cpu_measured = true;

    let estimated_write_cpu = if meter.write_sampled.is_zero() {
        Duration::ZERO
    } else {
        out.write_estimated.mul_f64(cpu_share(meter.write_sampled_cpu, meter.write_sampled))
    };
    #[cfg(test)]
    {
        out.estimated_write_cpu = estimated_write_cpu;
    }
    let cpu_in_io = meter.read_cpu
        + meter.len_cpu
        + meter.set_len_cpu
        + meter.sync_cpu
        + meter.write_sampled_cpu
        + estimated_write_cpu;
    // CPU inside the calls cannot exceed the CPU over the hold except by an
    // estimate or the clock's rounding; past that it is a double count. Both
    // sides come from one clock, so the tolerance is that clock's grain per
    // read, not the wall-clock tolerance above, which on a hold of mostly
    // off-CPU time would pass several times the CPU there was.
    let cpu_tolerance = CPU_CLOCK_GRAIN * (2 * (meter.cpu_reads as u32 + 1)) + cpu_over_hold / 100;
    if cpu_in_io > cpu_over_hold + cpu_tolerance {
        out.overcounted = true;
    }
    let mut cpu = cpu_over_hold.saturating_sub(cpu_in_io);
    let measured = io + cpu;
    if measured > hold + tolerance {
        out.overcounted = true;
    }
    // Within the tolerance, a measured sum past the hold is the clocks'
    // rounding, and it comes off `cpu`, the one component read from a
    // different clock: so the five add up to the hold exactly, unless the
    // backend calls alone outlast it.
    cpu = cpu.saturating_sub(measured.saturating_sub(hold));
    out.components[Component::Cpu.slot()] = cpu;
    out.components[Component::OffCpu.slot()] = hold.saturating_sub(io + cpu);
    out
}

/// The share of `wall` that was CPU, capped at one (ADR-176): what the
/// sampled writes say the unsampled ones spent on the CPU.
///
/// `f64::min` is load-bearing: it returns 1.0 for an infinite ratio and for
/// NaN, which is what keeps an estimate inside `write_estimated`. Written as
/// `if ratio > 1.0 { 1.0 } else { ratio }` it would pass NaN through, since
/// `NaN > 1.0` is false, and `Duration::mul_f64` panics on NaN — inside a
/// hold's release. `the_cpu_share_is_a_number_in_zero_to_one_whatever_the_clocks_read`
/// fails if it is.
fn cpu_share(cpu: Duration, wall: Duration) -> f64 {
    (cpu.as_secs_f64() / wall.as_secs_f64()).min(1.0)
}

/// The per-holder totals of every decomposed hold, since start (ADR-176).
pub(crate) struct HoldCounters {
    component_ns: [[AtomicU64; Component::COUNT]; WriterHolder::COUNT],
    phase_ns: [[AtomicU64; Phase::COUNT]; WriterHolder::COUNT],
    read_bytes: [AtomicU64; WriterHolder::COUNT],
    write_bytes: [AtomicU64; WriterHolder::COUNT],
    write_estimated_ns: [AtomicU64; WriterHolder::COUNT],
    overcounted: [AtomicU64; WriterHolder::COUNT],
    cpu_unmeasured: AtomicU64,
}

impl Default for HoldCounters {
    fn default() -> Self {
        let zero = || AtomicU64::new(0);
        Self {
            component_ns: std::array::from_fn(|_| std::array::from_fn(|_| zero())),
            phase_ns: std::array::from_fn(|_| std::array::from_fn(|_| zero())),
            read_bytes: std::array::from_fn(|_| zero()),
            write_bytes: std::array::from_fn(|_| zero()),
            write_estimated_ns: std::array::from_fn(|_| zero()),
            overcounted: std::array::from_fn(|_| zero()),
            cpu_unmeasured: zero(),
        }
    }
}

fn ns(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

impl HoldCounters {
    pub(crate) fn record(
        &self,
        holder: WriterHolder,
        hold: &Decomposed,
        phases: [Duration; Phase::COUNT],
    ) {
        let row = holder.slot();
        // `decompose` leaves `cpu` and `off_cpu` at zero for a hold whose CPU
        // time could not be read, so adding them adds nothing.
        for component in Component::ALL {
            self.component_ns[row][component.slot()]
                .fetch_add(ns(hold.components[component.slot()]), Relaxed);
        }
        if !hold.cpu_measured {
            self.cpu_unmeasured.fetch_add(1, Relaxed);
        }
        for phase in Phase::ALL {
            self.phase_ns[row][phase.slot()].fetch_add(ns(phases[phase.slot()]), Relaxed);
        }
        self.read_bytes[row].fetch_add(hold.read_bytes, Relaxed);
        self.write_bytes[row].fetch_add(hold.write_bytes, Relaxed);
        self.write_estimated_ns[row].fetch_add(ns(hold.write_estimated), Relaxed);
        if hold.overcounted {
            self.overcounted[row].fetch_add(1, Relaxed);
        }
    }

    pub(crate) fn snapshot(&self) -> HoldDecomposition {
        HoldDecomposition {
            component_ns: std::array::from_fn(|h| {
                std::array::from_fn(|c| self.component_ns[h][c].load(Relaxed))
            }),
            phase_ns: std::array::from_fn(|h| {
                std::array::from_fn(|p| self.phase_ns[h][p].load(Relaxed))
            }),
            read_bytes: std::array::from_fn(|h| self.read_bytes[h].load(Relaxed)),
            write_bytes: std::array::from_fn(|h| self.write_bytes[h].load(Relaxed)),
            write_estimated_ns: std::array::from_fn(|h| self.write_estimated_ns[h].load(Relaxed)),
            overcounted: std::array::from_fn(|h| self.overcounted[h].load(Relaxed)),
            cpu_unmeasured: self.cpu_unmeasured.load(Relaxed),
        }
    }
}

/// What every hold since start was made of, per holder, as a scrape reads it
/// (ADR-176). Nanoseconds and bytes; rows in [`WriterHolder::ALL`] order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HoldDecomposition {
    /// Columns in [`Component::ALL`] order. `cpu` and `off_cpu` hold only
    /// the holds whose CPU time could be read; see `cpu_unmeasured`.
    pub component_ns: [[u64; Component::COUNT]; WriterHolder::COUNT],
    /// Columns in [`Phase::ALL`] order.
    pub phase_ns: [[u64; Phase::COUNT]; WriterHolder::COUNT],
    pub read_bytes: [u64; WriterHolder::COUNT],
    pub write_bytes: [u64; WriterHolder::COUNT],
    /// The bound on the `cpu`/`off_cpu` split's error: write time whose CPU
    /// was estimated.
    pub write_estimated_ns: [u64; WriterHolder::COUNT],
    pub overcounted: [u64; WriterHolder::COUNT],
    /// Holds, of any holder, whose `cpu` and `off_cpu` were not recorded
    /// because this thread's CPU time could not be read.
    pub cpu_unmeasured: u64,
}

/// Upper bounds of the serve-walk histogram, in microseconds (ADR-176).
///
/// A walk of a caught-up peer's window over a warm cache is well under a
/// millisecond; a full window of 1,024 entries off a cold cache, or one that
/// passes over a long run of what the peer already holds (ADR-171), is where
/// the upper buckets go.
pub const SERVE_WALK_BUCKETS_US: [u64; 12] = [
    100, 1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000,
    30_000_000,
];

/// What serving peers' windows has cost this node, since start (ADR-176).
#[derive(Default)]
pub(crate) struct ServeCounters {
    windows: AtomicU64,
    entries: AtomicU64,
    passed: AtomicU64,
    walk_buckets: [AtomicU64; SERVE_WALK_BUCKETS_US.len()],
    walk_sum_us: AtomicU64,
    read_ns: AtomicU64,
    read_bytes: AtomicU64,
}

impl ServeCounters {
    pub(crate) fn record(&self, entries: usize, passed: u64, walk: Duration, meter: &Meter) {
        let us = u64::try_from(walk.as_micros()).unwrap_or(u64::MAX);
        self.windows.fetch_add(1, Relaxed);
        self.entries.fetch_add(entries as u64, Relaxed);
        self.passed.fetch_add(passed, Relaxed);
        if let Some(slot) = SERVE_WALK_BUCKETS_US.iter().position(|upper| us <= *upper) {
            self.walk_buckets[slot].fetch_add(1, Relaxed);
        }
        self.walk_sum_us.fetch_add(us, Relaxed);
        self.read_ns.fetch_add(ns(meter.read), Relaxed);
        self.read_bytes.fetch_add(meter.read_bytes, Relaxed);
    }

    pub(crate) fn snapshot(&self) -> ServeSnapshot {
        ServeSnapshot {
            windows: self.windows.load(Relaxed),
            entries: self.entries.load(Relaxed),
            passed: self.passed.load(Relaxed),
            walk_buckets: std::array::from_fn(|slot| self.walk_buckets[slot].load(Relaxed)),
            walk_sum_us: self.walk_sum_us.load(Relaxed),
            read_ns: self.read_ns.load(Relaxed),
            read_bytes: self.read_bytes.load(Relaxed),
        }
    }
}

/// What serving peers' windows has cost this node, as a scrape reads it
/// (ADR-176).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ServeSnapshot {
    /// Windows walked for a peer; also the walk histogram's count.
    pub windows: u64,
    /// Entries those windows carried.
    pub entries: u64,
    /// Entries the walks examined and did not serve: passed over because the
    /// peer already holds them (ADR-171), or never served to a peer.
    pub passed: u64,
    /// Walks in each bucket of [`SERVE_WALK_BUCKETS_US`], **not** cumulative.
    pub walk_buckets: [u64; SERVE_WALK_BUCKETS_US.len()],
    pub walk_sum_us: u64,
    /// Wall time and bytes of the backend reads the walks made.
    pub read_ns: u64,
    pub read_bytes: u64,
}

/// A file backend that meters what it is asked to do (ADR-176). Every engine
/// opens its database through one. It also holds the store's lock, taken
/// before the backend is built and answered to redb from there (ADR-190,
/// [`crate::store_lock`]).
#[derive(Debug)]
pub(crate) struct MeteredBackend {
    inner: redb::backends::FileBackend,
    lock: crate::store_lock::StoreLock,
    /// Where the first I/O error is recorded (ADR-188). Here because every
    /// byte the engine reads or writes passes through this backend, and it
    /// sees each error at the call where redb latches it.
    health: std::sync::Arc<crate::health::StorageHealth>,
}

impl MeteredBackend {
    pub(crate) fn new(
        inner: redb::backends::FileBackend,
        lock: crate::store_lock::StoreLock,
        health: std::sync::Arc<crate::health::StorageHealth>,
    ) -> Self {
        Self { inner, lock, health }
    }

    /// `call`, with the test switch's injected error ahead of it and any error
    /// it returns recorded on the way out.
    fn checked<T>(
        &self,
        name: &'static str,
        call: impl FnOnce() -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        let result = match self.health.injected(name) {
            Some(error) => Err(error),
            None => call(),
        };
        if let Err(error) = &result {
            self.health.record(name, error);
        }
        result
    }
}

impl redb::StorageBackend for MeteredBackend {
    fn len(&self) -> std::result::Result<u64, std::io::Error> {
        #[cfg(test)]
        test_hooks::backend_call(Io::Len);
        self.checked("len", || io(Io::Len, 0, || self.inner.len()))
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> std::result::Result<(), std::io::Error> {
        let bytes = out.len();
        #[cfg(test)]
        test_hooks::backend_call(Io::Read);
        self.checked("read", || io(Io::Read, bytes, || self.inner.read(offset, out)))
    }

    fn set_len(&self, len: u64) -> std::result::Result<(), std::io::Error> {
        #[cfg(test)]
        test_hooks::backend_call(Io::SetLen);
        self.checked("set_len", || io(Io::SetLen, 0, || self.inner.set_len(len)))
    }

    fn sync_data(&self) -> std::result::Result<(), std::io::Error> {
        #[cfg(test)]
        test_hooks::backend_call(Io::Sync);
        self.checked("sync_data", || io(Io::Sync, 0, || self.inner.sync_data()))
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), std::io::Error> {
        #[cfg(test)]
        test_hooks::backend_call(Io::Write);
        self.checked("write", || io(Io::Write, data.len(), || self.inner.write(offset, data)))
    }

    fn close(&self) -> std::result::Result<(), std::io::Error> {
        let closed = self.inner.close();
        self.lock.release();
        closed
    }

    fn try_lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<bool, redb::BackendError> {
        self.lock.request(start, end, false)
    }

    fn try_lock_shared_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<bool, redb::BackendError> {
        self.lock.request(start, end, true)
    }

    fn lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<(), redb::BackendError> {
        Err(crate::store_lock::unexpected("a blocking lock", start, end))
    }

    fn lock_shared_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<(), redb::BackendError> {
        Err(crate::store_lock::unexpected("a blocking shared lock", start, end))
    }

    fn unlock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<(), redb::BackendError> {
        self.lock.unlock(start, end)
    }

    fn query_lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<bool, redb::BackendError> {
        Err(crate::store_lock::unexpected("a lock query", start, end))
    }
}

/// Test-only ways to put known work inside a hold, so each component and
/// phase can be shown to move when what it measures does. `cfg(test)`: none
/// of this exists in any other build.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::cell::Cell;
    use std::time::{Duration, Instant};

    use super::{Io, Phase};

    thread_local! {
        /// Slept, off the CPU, at the start of each phase of a hold.
        pub static SLEEP_IN: Cell<[Duration; Phase::COUNT]> =
            const { Cell::new([Duration::ZERO; Phase::COUNT]) };
        /// Spun, on the CPU, at the start of the work phase.
        pub static SPIN_IN_WORK: Cell<Duration> = const { Cell::new(Duration::ZERO) };
        /// Spun, on the CPU, at the start of the work phase, until this much
        /// of this thread's CPU time has passed rather than this much wall
        /// time: what a test needs when the claim is about CPU time and the
        /// machine may be lending the thread's core to someone else.
        pub static SPIN_CPU_IN_WORK: Cell<Duration> = const { Cell::new(Duration::ZERO) };
        /// Spun inside each read, write and sync call.
        pub static SPIN_IN_IO: Cell<Duration> = const { Cell::new(Duration::ZERO) };
        /// Slept inside every `set_len` and every `len`.
        pub static SLEEP_IN_FILE_SIZE: Cell<Duration> = const { Cell::new(Duration::ZERO) };
        /// `set_len` and `len` calls made while a meter was installed.
        pub static FILE_SIZE_CALLS: Cell<u64> = const { Cell::new(0) };
        /// `len` calls alone made while a meter was installed.
        pub static LEN_CALLS: Cell<u64> = const { Cell::new(0) };
        /// Backend calls this thread made, metered or not, by kind in
        /// `Io` declaration order; and those a meter recorded.
        pub static BACKEND_CALLS: Cell<[u64; 5]> = const { Cell::new([0; 5]) };
        pub static METERED_CALLS: Cell<[u64; 5]> = const { Cell::new([0; 5]) };
        /// Every thread CPU clock read fails while set.
        pub static CPU_CLOCK_FAILS: Cell<bool> = const { Cell::new(false) };
        /// Every thread CPU clock read returns this while set: a clock that
        /// has stopped advancing.
        pub static CPU_CLOCK_STUCK_AT: Cell<Option<Duration>> = const { Cell::new(None) };
        /// Slept inside every write call from this index on.
        pub static SLEEP_IN_WRITES_FROM: Cell<Option<(u64, Duration)>> = const { Cell::new(None) };
        /// What `SLEEP_IN_WRITES_FROM` asked to sleep, summed.
        pub static SLEPT_IN_WRITES: Cell<Duration> = const { Cell::new(Duration::ZERO) };
        /// The thread's CPU time inside every metered write call, read around
        /// each one whether the meter sampled it or not: the truth the
        /// meter's estimate for the unsampled writes stands in for.
        pub static TRUE_WRITE_CPU: Cell<Duration> = const { Cell::new(Duration::ZERO) };
        /// The last hold released on this thread.
        pub static LAST_HOLD: Cell<Option<LastHold>> = const { Cell::new(None) };
        /// The backend call the next engine open on this thread fails once,
        /// armed before redb's open runs.
        pub static ARM_AT_OPEN: Cell<Option<&'static str>> = const { Cell::new(None) };
    }

    /// A released hold: who held it, what was metered, the CPU over the hold,
    /// its length, and how it was decomposed.
    #[derive(Clone, Copy, Debug)]
    pub struct LastHold {
        pub holder: crate::engine::WriterHolder,
        pub meter: super::Meter,
        pub cpu_over_hold: Option<Duration>,
        pub held: Duration,
        pub decomposed: super::Decomposed,
    }

    pub fn spin(d: Duration) {
        if d.is_zero() {
            return;
        }
        let until = Instant::now() + d;
        let mut x = 0u64;
        while Instant::now() < until {
            x = std::hint::black_box(x.wrapping_add(1));
        }
    }

    /// [`spin`] to a CPU-time deadline: spins until this thread has spent `d`
    /// on the CPU, however long that takes on a busy machine, and fails if
    /// 30 s of wall time pass first, as the platform-clock test does: a clock
    /// that has stopped advancing must fail the test, not hang the job.
    pub fn spin_cpu(d: Duration) {
        spin_cpu_within(d, Duration::from_secs(30));
    }

    pub fn spin_cpu_within(d: Duration, limit: Duration) {
        if d.is_zero() {
            return;
        }
        let from =
            super::thread_cpu().expect("a test that spins to CPU time runs where it is read");
        let started = Instant::now();
        let mut x = 0u64;
        while super::thread_cpu().unwrap().saturating_sub(from) < d {
            x = std::hint::black_box(x.wrapping_add(1));
            assert!(
                started.elapsed() < limit,
                "{limit:?} of spinning and the CPU clock has not advanced {d:?}: it is stuck"
            );
        }
    }

    fn slot(kind: Io) -> usize {
        match kind {
            Io::Read => 0,
            Io::Write => 1,
            Io::Sync => 2,
            Io::Len => 3,
            Io::SetLen => 4,
        }
    }

    pub fn backend_call(kind: Io) {
        BACKEND_CALLS.with(|c| {
            let mut v = c.get();
            v[slot(kind)] += 1;
            c.set(v);
        });
    }

    pub fn metered_call(kind: Io) {
        METERED_CALLS.with(|c| {
            let mut v = c.get();
            v[slot(kind)] += 1;
            c.set(v);
        });
    }

    pub fn at_phase(phase: Phase) {
        std::thread::sleep(SLEEP_IN.with(|s| s.get())[phase.slot()]);
        if phase == Phase::Work {
            spin(SPIN_IN_WORK.with(|s| s.get()));
            spin_cpu(SPIN_CPU_IN_WORK.with(|s| s.get()));
        }
    }

    /// Where a metered write that reads CPU at all starts, for
    /// [`TRUE_WRITE_CPU`].
    pub fn true_cpu_from(cpu: bool, kind: Io) -> Option<Duration> {
        if cpu && kind == Io::Write { super::thread_cpu() } else { None }
    }

    pub fn true_cpu_to(from: Option<Duration>) {
        if let Some(from) = from
            && let Some(to) = super::thread_cpu()
        {
            TRUE_WRITE_CPU.with(|c| c.set(c.get() + to.saturating_sub(from)));
        }
    }

    pub fn inside_io(kind: Io, write_calls: u64) {
        if kind == Io::Len {
            LEN_CALLS.with(|c| c.set(c.get() + 1));
        }
        if matches!(kind, Io::Len | Io::SetLen) {
            FILE_SIZE_CALLS.with(|c| c.set(c.get() + 1));
            std::thread::sleep(SLEEP_IN_FILE_SIZE.with(|s| s.get()));
        }
        spin(SPIN_IN_IO.with(|s| s.get()));
        if kind == Io::Write
            && let Some((from, d)) = SLEEP_IN_WRITES_FROM.with(|s| s.get())
            && write_calls >= from
        {
            SLEPT_IN_WRITES.with(|s| s.set(s.get() + d));
            std::thread::sleep(d);
        }
    }

    /// Put every hook back to doing nothing.
    pub fn reset() {
        SLEEP_IN.with(|s| s.set([Duration::ZERO; Phase::COUNT]));
        SPIN_IN_WORK.with(|s| s.set(Duration::ZERO));
        SPIN_CPU_IN_WORK.with(|s| s.set(Duration::ZERO));
        SPIN_IN_IO.with(|s| s.set(Duration::ZERO));
        SLEEP_IN_WRITES_FROM.with(|s| s.set(None));
        SLEPT_IN_WRITES.with(|s| s.set(Duration::ZERO));
        TRUE_WRITE_CPU.with(|c| c.set(Duration::ZERO));
        LAST_HOLD.with(|h| h.set(None));
        SLEEP_IN_FILE_SIZE.with(|s| s.set(Duration::ZERO));
        FILE_SIZE_CALLS.with(|c| c.set(0));
        LEN_CALLS.with(|c| c.set(0));
        BACKEND_CALLS.with(|c| c.set([0; 5]));
        METERED_CALLS.with(|c| c.set([0; 5]));
        CPU_CLOCK_FAILS.with(|f| f.set(false));
        CPU_CLOCK_STUCK_AT.with(|s| s.set(None));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bson::doc;

    use super::*;
    use crate::engine::{DurabilityClass, Engine};

    fn fresh() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        (engine, dir)
    }

    /// One holder's row of what changed between two readings.
    #[derive(Debug, Default)]
    struct Row {
        holds: u64,
        held: Duration,
        components: [Duration; Component::COUNT],
        phases: [Duration; Phase::COUNT],
        read_bytes: u64,
        write_bytes: u64,
        write_estimated: Duration,
        overcounted: u64,
    }

    impl Row {
        fn get(&self, c: Component) -> Duration {
            self.components[c.slot()]
        }
        fn phase(&self, p: Phase) -> Duration {
            self.phases[p.slot()]
        }
    }

    /// What `f` added to `holder`'s row.
    fn during(engine: &Engine, holder: WriterHolder, f: impl FnOnce()) -> Row {
        let (h0, d0) = (engine.writer_hold(), engine.writer_hold_decomposition());
        f();
        let (h1, d1) = (engine.writer_hold(), engine.writer_hold_decomposition());
        let r = holder.slot();
        let ns = |n: u64| Duration::from_nanos(n);
        Row {
            holds: h1.count[r] - h0.count[r],
            held: Duration::from_micros(h1.sum_us[r] - h0.sum_us[r]),
            components: std::array::from_fn(|c| ns(d1.component_ns[r][c] - d0.component_ns[r][c])),
            phases: std::array::from_fn(|p| ns(d1.phase_ns[r][p] - d0.phase_ns[r][p])),
            read_bytes: d1.read_bytes[r] - d0.read_bytes[r],
            write_bytes: d1.write_bytes[r] - d0.write_bytes[r],
            write_estimated: ns(d1.write_estimated_ns[r] - d0.write_estimated_ns[r]),
            overcounted: d1.overcounted[r] - d0.overcounted[r],
        }
    }

    fn inserts(engine: &Engine, n: usize) {
        let coll = engine.create_collection("shop", "orders").unwrap();
        for i in 0..n {
            engine.insert(&coll, doc! { "n": i as i64, "body": "x".repeat(200) }).unwrap();
        }
    }

    /// Every component sums to the hold it came from, to within the rounding of
    /// the hold histogram's microseconds: one per hold.
    fn assert_adds_up(row: &Row) {
        let slack = Duration::from_micros(row.holds);
        let components: Duration = row.components.iter().sum();
        let phases: Duration = row.phases.iter().sum();
        for (what, sum) in [("components", components), ("phases", phases)] {
            let gap = sum.abs_diff(row.held);
            assert!(gap <= slack, "{what} sum to {sum:?} of a {:?} hold: {row:?}", row.held);
        }
    }

    #[test]
    fn this_platforms_thread_clock_reads_cpu_time_and_not_zero() {
        // Condition 2 of ADR-176: a clock stuck at zero would make every `cpu`
        // assertion below vacuous on the machine the gate runs on.
        if !cfg!(any(target_os = "linux", target_os = "macos")) {
            assert_eq!(thread_cpu(), None, "a platform without the clock says so");
            return;
        }
        // Spun to a CPU-time deadline rather than a wall-clock one, so a
        // machine busy with other tests slows it down rather than failing it:
        // the claim is that the clock advances, and only while the thread
        // runs, which holds however many cores the thread gets.
        // The CPU reads nest inside the wall reads, as in `io`, so the CPU
        // interval cannot start before the wall interval or end after it.
        let wall_from = std::time::Instant::now();
        let from = thread_cpu().expect("Linux and macOS have a per-thread CPU clock");
        let want = Duration::from_millis(30);
        let mut x = 0u64;
        while thread_cpu().unwrap() - from < want {
            x = std::hint::black_box(x.wrapping_add(1));
            assert!(
                wall_from.elapsed() < Duration::from_secs(30),
                "30 s of spinning and the CPU clock has not advanced 30 ms: it is stuck"
            );
        }
        let spent = thread_cpu().unwrap() - from;
        let wall = wall_from.elapsed();
        assert!(spent >= want, "{spent:?}");
        assert!(cpu_fits_in_wall(spent, wall), "{spent:?} of CPU in {wall:?} of wall time");
        let from = thread_cpu().unwrap();
        std::thread::sleep(Duration::from_millis(60));
        let slept = thread_cpu().unwrap() - from;
        assert!(slept < Duration::from_millis(20), "60 ms asleep read as {slept:?} of CPU");
    }

    /// Whether `spent` of thread CPU fits in `wall` of monotonic time, as a
    /// clock that reads CPU time must. Not within the grain alone: the two
    /// clocks do not run at one rate. `Instant` is `CLOCK_MONOTONIC`, which NTP
    /// slews by up to 500 ppm, and the thread CPU clock is the scheduler's
    /// accounting, which it does not; so the tolerance is the grain of the two
    /// CPU reads plus 1% of the interval, as `decompose`'s is.
    fn cpu_fits_in_wall(spent: Duration, wall: Duration) -> bool {
        spent <= wall + CPU_CLOCK_GRAIN * 2 + wall / 100
    }

    #[test]
    fn cpu_past_wall_by_a_clock_rate_fits_and_a_clock_running_fast_does_not() {
        // The reading CI took on Linux: 3.8 µs past 30 ms of wall time, about
        // 128 ppm, which the grain alone refused.
        assert!(cpu_fits_in_wall(
            Duration::from_nanos(30_001_006),
            Duration::from_nanos(29_997_178)
        ));
        // The bound of a slewed monotonic clock.
        assert!(cpu_fits_in_wall(Duration::from_micros(30_015), Duration::from_millis(30)));
        // What the leg is for, and what the sleep leg cannot see: a clock that
        // advances only while the thread runs, but too fast. Twice the rate,
        // or a unit mistaken by 1,000, reads zero asleep all the same.
        assert!(!cpu_fits_in_wall(Duration::from_millis(60), Duration::from_millis(30)));
        assert!(!cpu_fits_in_wall(Duration::from_micros(30_400), Duration::from_millis(30)));
    }

    #[test]
    fn every_hold_decomposes_into_exactly_its_length() {
        let (engine, _dir) = fresh();
        let row = during(&engine, WriterHolder::Write, || inserts(&engine, 100));
        assert_eq!(row.holds, 100);
        // A single-document insert makes fewer page writes than are measured
        // exactly, so none of its CPU is estimated.
        assert_eq!(row.write_estimated, Duration::ZERO, "{row:?}");
        assert_adds_up(&row);
        assert_eq!(row.overcounted, 0, "{row:?}");
        assert_eq!(engine.writer_hold_decomposition().cpu_unmeasured, 0);
    }

    #[test]
    fn every_backend_call_a_hold_makes_is_metered() {
        // The guard against under-measurement, against a baseline: the
        // backend's own tally of the calls this thread made, metered or not.
        // An unmetered call inside a hold lands in `off_cpu` whole, which is how
        // `set_len` went unseen; an absolute bound on `off_cpu` could not see
        // it, and cannot be trusted in an unoptimised build that inflates the
        // residual for reasons of its own. Page writes, fsyncs and file growth
        // happen only inside a write transaction, so every one must be
        // metered. Reads and size reads also happen outside holds, in read
        // transactions and at open, so the meter may see fewer of those — which
        // means unmetering `read` entirely would still pass here (0 is at most
        // N). `read` is guarded by `a_page_the_cache_does_not_hold_is_a_read`
        // and `len` by `redb_reads_the_files_size_only_when_it_opens_and_never_inside_a_hold`;
        // neither is redundant with this test.
        let (engine, _dir) = fresh();
        let coll = engine.create_collection("shop", "orders").unwrap();
        test_hooks::reset();
        for i in 0..50 {
            engine.insert(&coll, doc! { "n": i }).unwrap();
        }
        let docs = (0..2_000).map(|i| doc! { "n": i, "body": "x".repeat(4_000) }).collect();
        engine.insert_many(&coll, docs).unwrap();
        engine
            .create_index(
                "shop",
                "orders",
                vec![crate::meta::IndexField::ascending("n")],
                false,
                None,
            )
            .unwrap();
        engine.drop_collection("shop", "orders").unwrap();
        let backend = test_hooks::BACKEND_CALLS.with(|c| c.get());
        let metered = test_hooks::METERED_CALLS.with(|c| c.get());
        test_hooks::reset();
        for (slot, kind) in ["write", "sync", "set_len"].iter().zip([1, 2, 4]) {
            assert!(backend[kind] > 0, "the workload made no {slot}: {backend:?}");
            assert_eq!(metered[kind], backend[kind], "{slot} calls escaped the meter");
        }
        for (slot, kind) in ["read", "len"].iter().zip([0, 3]) {
            assert!(metered[kind] <= backend[kind], "{slot}: {metered:?} > {backend:?}");
        }
    }

    #[test]
    fn a_durable_commits_fsync_is_its_own_and_a_coalesced_one_is_the_barriers() {
        // The pair: under `durable` the fsync is inside the write's hold; under
        // `coalesced` the write's hold has none and the barrier's has it. A
        // `sync` that read the same under both would pass either half alone.
        let (engine, _dir) = fresh();
        let durable = during(&engine, WriterHolder::Write, || inserts(&engine, 10));
        assert!(durable.get(Component::Sync) > Duration::ZERO, "{durable:?}");
        assert!(durable.get(Component::Write) > Duration::ZERO, "{durable:?}");
        assert!(durable.write_bytes > 0, "{durable:?}");

        let (engine, _dir) = fresh();
        engine.set_durability(DurabilityClass::Coalesced, Duration::from_millis(2));
        let coll = engine.create_collection("shop", "orders").unwrap();
        let before = engine.writer_hold_decomposition();
        let write = during(&engine, WriterHolder::Write, || {
            for i in 0..10 {
                engine.insert(&coll, doc! { "n": i }).unwrap();
            }
        });
        let after = engine.writer_hold_decomposition();
        let barrier = WriterHolder::Durability.slot();
        let barrier_sync = after.component_ns[barrier][Component::Sync.slot()]
            - before.component_ns[barrier][Component::Sync.slot()];
        assert_eq!(write.get(Component::Sync), Duration::ZERO, "{write:?}");
        assert!(barrier_sync > 0, "the barrier's flush is where a coalesced fsync is");
    }

    #[test]
    fn a_page_the_cache_does_not_hold_is_a_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let filler = "x".repeat(1_000);
        {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("shop", "orders").unwrap();
            for _ in 0..24 {
                let docs = (0..1_000).map(|i| doc! { "n": i, "body": filler.clone() }).collect();
                engine.insert_many(&coll, docs).unwrap();
            }
        }
        // Reopened with a cache far smaller than the store, so an insert's
        // path to its leaf pages is not all in it.
        let engine = Engine::open_with_cache(&path, Some(1 << 20)).unwrap();
        let coll = engine.get_collection("shop", "orders").unwrap();
        let row = during(&engine, WriterHolder::Write, || {
            for i in 0..50 {
                engine.insert(&coll, doc! { "n": i, "body": filler.clone() }).unwrap();
            }
        });
        assert!(row.get(Component::Read) > Duration::ZERO, "{row:?}");
        assert!(row.read_bytes > 0, "{row:?}");
        assert_adds_up(&row);
    }

    #[test]
    #[should_panic(expected = "of spinning and the CPU clock has not advanced")]
    fn a_spin_to_cpu_time_on_a_stuck_clock_fails_rather_than_hangs() {
        test_hooks::CPU_CLOCK_STUCK_AT.with(|s| s.set(Some(Duration::from_secs(1))));
        test_hooks::spin_cpu_within(Duration::from_millis(10), Duration::from_millis(200));
    }

    #[test]
    fn work_on_the_cpu_inside_a_hold_is_cpu() {
        // Spun to 80 ms of this thread's CPU time, not of wall time. Spun to a
        // wall-clock deadline on a machine lending the core elsewhere, the
        // thread got 55 ms of CPU in its 80 ms and the test read that as the
        // meter losing CPU. It is the claim that CPU time spent in a hold is
        // `cpu`, so it is held to CPU time: all of it, less the clock's
        // rounding, whatever else the machine is doing. How much *off* the
        // CPU the spin also took is the machine's, not the meter's, and is
        // not asserted; the components still add up to the hold.
        let (engine, _dir) = fresh();
        let coll = engine.create_collection("shop", "orders").unwrap();
        let spun = Duration::from_millis(80);
        test_hooks::SPIN_CPU_IN_WORK.with(|s| s.set(spun));
        let row = during(&engine, WriterHolder::Write, || {
            engine.insert(&coll, doc! { "n": 1 }).unwrap();
        });
        test_hooks::reset();
        assert!(row.get(Component::Cpu) >= spun - Duration::from_millis(1), "{row:?}");
        assert!(row.phase(Phase::Work) >= spun - Duration::from_millis(1), "{row:?}");
        assert_adds_up(&row);
    }

    #[test]
    fn time_asleep_inside_a_hold_is_off_cpu() {
        let (engine, _dir) = fresh();
        let coll = engine.create_collection("shop", "orders").unwrap();
        test_hooks::SLEEP_IN
            .with(|s| s.set([Duration::from_millis(80), Duration::ZERO, Duration::ZERO]));
        let row = during(&engine, WriterHolder::Write, || {
            engine.insert(&coll, doc! { "n": 1 }).unwrap();
        });
        test_hooks::reset();
        assert!(row.get(Component::OffCpu) >= Duration::from_millis(80), "{row:?}");
        assert!(row.get(Component::Cpu) < Duration::from_millis(40), "{row:?}");
        assert_adds_up(&row);
    }

    #[test]
    fn a_holder_starved_of_the_cpu_reads_off_cpu() {
        // Real scheduler delay rather than an injected sleep: the hold wants
        // 300 ms of CPU while four times as many threads as there are cores
        // spin beside it, so the holder is descheduled for much of it. `spin`
        // runs to a wall-clock deadline, so the CPU the holder was denied is
        // time off the CPU inside the hold.
        let (engine, _dir) = fresh();
        let coll = engine.create_collection("shop", "orders").unwrap();
        let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
        let stop = std::sync::atomic::AtomicBool::new(false);
        let row = std::thread::scope(|scope| {
            for _ in 0..cores * 4 {
                scope.spawn(|| {
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        std::hint::black_box(0u64);
                    }
                });
            }
            std::thread::sleep(Duration::from_millis(50));
            test_hooks::SPIN_IN_WORK.with(|s| s.set(Duration::from_millis(300)));
            let row = during(&engine, WriterHolder::Write, || {
                engine.insert(&coll, doc! { "n": 1 }).unwrap();
            });
            test_hooks::reset();
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            row
        });
        let off = row.get(Component::OffCpu).as_secs_f64() / row.held.as_secs_f64();
        assert!(off >= 0.3, "a starved holder read {:.0}% off the CPU: {row:?}", off * 100.0);
        assert_adds_up(&row);
    }

    #[test]
    fn each_phase_holds_what_happens_in_it() {
        // A test of which phase a sleep lands in, not of how fast a disk is.
        // Under `durable` the commit phase holds a real fsync, and a slow
        // runner's 139 ms of it read as the work phase's sleep leaking into
        // the commit. Under `coalesced` the hold writes without its own fsync
        // and the shared one runs in the barrier's hold, outside this one, so
        // a phase holds only the sleep the test puts there and the engine's
        // own work; the first assertion below pins that no fsync is in it.
        for phase in Phase::ALL {
            let (engine, _dir) = fresh();
            engine.set_durability(DurabilityClass::Coalesced, Duration::from_millis(2));
            let coll = engine.create_collection("shop", "orders").unwrap();
            let mut sleeps = [Duration::ZERO; Phase::COUNT];
            sleeps[phase.slot()] = Duration::from_millis(60);
            test_hooks::SLEEP_IN.with(|s| s.set(sleeps));
            let row = during(&engine, WriterHolder::Write, || {
                engine.insert(&coll, doc! { "n": 1 }).unwrap();
            });
            test_hooks::reset();
            assert_eq!(row.get(Component::Sync), Duration::ZERO, "no fsync in the hold: {row:?}");
            for other in Phase::ALL {
                if other == phase {
                    assert!(row.phase(other) >= Duration::from_millis(60), "{phase:?}: {row:?}");
                } else {
                    assert!(
                        row.phase(other) < Duration::from_millis(40),
                        "{phase:?} leaked into {other:?}: {row:?}"
                    );
                }
            }
            assert_adds_up(&row);
        }
    }

    #[test]
    fn cpu_spent_inside_a_backend_call_is_not_counted_as_cpu_as_well() {
        // The subtraction that keeps `cpu` and the three call components
        // disjoint. Without it, every call's own CPU is in both, the measured
        // sum passes the hold, and the over-count guard fires.
        let (engine, _dir) = fresh();
        let coll = engine.create_collection("shop", "orders").unwrap();
        test_hooks::SPIN_IN_IO.with(|s| s.set(Duration::from_millis(2)));
        let row = during(&engine, WriterHolder::Write, || {
            for i in 0..3 {
                engine.insert(&coll, doc! { "n": i }).unwrap();
            }
        });
        test_hooks::reset();
        let io = row.get(Component::Read) + row.get(Component::Write) + row.get(Component::Sync);
        assert!(io >= Duration::from_millis(60), "{row:?}");
        assert!(row.get(Component::Cpu) < io / 4, "{row:?}");
        assert_eq!(row.overcounted, 0, "{row:?}");
        assert_adds_up(&row);
    }

    #[test]
    fn a_write_that_blocks_past_the_sample_lands_in_write_and_the_split_stays_in_its_bound() {
        // Dirty-page throttling cannot be reproduced here, so the blocking is
        // injected: every write call from the one after those measured exactly
        // sleeps. The sleep is off the CPU inside a call whose CPU is estimated
        // for most of them, from a sample of calls that did not all sleep. The
        // time must land in `write` and not in `cpu`, and however wrong the
        // estimate, `cpu` and `off_cpu` can be off by no more than
        // `write_estimated`.
        //
        // Every claim is checked against this one hold's own clocks, never
        // against a second bulk: under load, one bulk's time off the CPU
        // outside the calls differs from the next one's by more than
        // `write_estimated`, and a bound read across two runs failed on that
        // noise. The hooks read the true CPU of every write call, which is
        // what the estimate stands in for.
        if !cfg!(any(target_os = "linux", target_os = "macos")) {
            return;
        }
        let (engine, _dir) = fresh();
        let coll = engine.create_collection("shop", "orders").unwrap();
        // The open and the creation wrote too; only the bulk's writes count.
        test_hooks::reset();
        let sleep = Duration::from_millis(1);
        test_hooks::SLEEP_IN_WRITES_FROM.with(|s| s.set(Some((WRITES_MEASURED, sleep))));
        let row = during(&engine, WriterHolder::Bulk, || {
            let docs = (0..400).map(|i| doc! { "n": i, "body": "x".repeat(300) }).collect();
            engine.insert_many(&coll, docs).unwrap();
        });
        let slept = test_hooks::SLEPT_IN_WRITES.with(|s| s.get());
        let true_write_cpu = test_hooks::TRUE_WRITE_CPU.with(|c| c.get());
        let test_hooks::LastHold { holder, meter, cpu_over_hold, held, decomposed: d } =
            test_hooks::LAST_HOLD.with(|h| h.take()).expect("the bulk's hold was recorded");
        test_hooks::reset();
        let cpu_over_hold = cpu_over_hold.expect("this platform reads a thread's CPU time");
        let grain = CPU_CLOCK_GRAIN * (2 * (meter.cpu_reads as u32 + meter.write_calls as u32 + 1));
        let context = format!(
            "slept {slept:?}, true write CPU {true_write_cpu:?}, CPU over the hold \
             {cpu_over_hold:?}, held {held:?}: {meter:?} {d:?}"
        );

        assert_eq!(row.holds, 1, "one bulk, one hold: {row:?}");
        assert_eq!(holder, WriterHolder::Bulk, "the hold checked is the bulk's: {context}");
        assert!(d.write_estimated > Duration::ZERO, "the bulk passed the sample: {context}");
        assert!(slept >= Duration::from_millis(20), "the writes slept: {context}");

        // The sleeps were off the CPU, inside the write calls: their CPU and
        // the sleeps fit in the calls' wall time. A sleep overruns what it
        // asked for and costs microseconds of CPU, so this holds with room;
        // a hook that spun instead would put its CPU on both sides.
        assert!(
            true_write_cpu + slept <= meter.write + grain,
            "the write calls' CPU and their sleeps outran the calls: {context}"
        );
        assert!(row.get(Component::Write) >= slept, "the sleeps are write time: {row:?}");

        // The estimate is the sampled writes' share of CPU applied to the
        // unsampled writes' time, and the sample saw the sleeps, so that
        // share is well under one: a whole unsampled write credited as CPU
        // would read here.
        let share = cpu_share(meter.write_sampled_cpu, meter.write_sampled);
        assert!(share < 0.5, "the sample included sleeping writes: share {share}, {context}");
        let expected = d.write_estimated.mul_f64(share);
        // Large enough that an estimate forced to nothing reads here.
        assert!(
            expected >= Duration::from_millis(1),
            "the sampled share of the unsampled writes is {expected:?}, too little for a \
             missing estimate to show: {context}"
        );
        assert!(
            d.estimated_write_cpu.abs_diff(expected) <= Duration::from_micros(1),
            "the estimate is {:?}, the sampled share of {:?} is {expected:?}: {context}",
            d.estimated_write_cpu,
            d.write_estimated
        );

        // And the bound, exactly: the CPU outside the calls, as the true
        // clocks say, against what the hold reported. The reported `cpu` also
        // gives up any rounding past the hold, which the tolerance covers.
        let true_cpu_in_calls =
            meter.read_cpu + meter.len_cpu + meter.set_len_cpu + meter.sync_cpu + true_write_cpu;
        let true_cpu = cpu_over_hold.saturating_sub(true_cpu_in_calls);
        let error = d.components[Component::Cpu.slot()].abs_diff(true_cpu);
        assert!(
            error <= d.write_estimated + OVERCOUNT_TOLERANCE + held / 100 + grain,
            "cpu is {error:?} from the truth, past the {:?} bound: {context}",
            d.write_estimated
        );
        assert_adds_up(&row);
    }

    #[test]
    fn the_estimate_is_bounded_by_the_write_time_it_covers() {
        // Pure: a sample of writes that was all CPU, and unsampled writes that
        // were all asleep. The estimate credits the sleep as CPU, so `cpu`
        // reads low and `off_cpu` high by the same amount — the direction that
        // manufactures apparent contention. The over-count guard sees it only
        // once the error exceeds the CPU spent outside the calls; below that
        // it is invisible, and bounded only by `write_estimated`.
        let meter = Meter {
            cpu: true,
            write: Duration::from_millis(30),
            write_calls: 64,
            write_sampled: Duration::from_millis(10),
            write_sampled_cpu: Duration::from_millis(10),
            ..Meter::default()
        };
        // Truth: 10 ms of CPU in the sampled writes, 20 ms asleep in the rest,
        // 30 ms of CPU outside the calls, and 40 ms off the CPU outside them.
        let hold = Duration::from_millis(100);
        let d = decompose(&meter, hold, Some(Duration::from_millis(40)));
        assert_eq!(d.write_estimated, Duration::from_millis(20));
        let (true_cpu, true_off_cpu) = (Duration::from_millis(30), Duration::from_millis(40));
        let (cpu, off) =
            (d.components[Component::Cpu.slot()], d.components[Component::OffCpu.slot()]);
        assert!(
            off > true_off_cpu && cpu < true_cpu,
            "the direction that invents contention: {d:?}"
        );
        assert!(off - true_off_cpu <= d.write_estimated, "and within the bound: {d:?}");
        assert!(!d.overcounted, "and the guard cannot see it: {d:?}");
    }

    #[test]
    fn a_measured_sum_past_the_hold_is_an_overcount() {
        let meter = Meter { cpu: true, write: Duration::from_millis(60), ..Meter::default() };
        let d = decompose(&meter, Duration::from_millis(50), Some(Duration::ZERO));
        assert!(d.overcounted, "{d:?}");
        let meter = Meter {
            cpu: true,
            read: Duration::from_millis(5),
            read_cpu: Duration::from_millis(30),
            ..Meter::default()
        };
        let d = decompose(&meter, Duration::from_millis(50), Some(Duration::from_millis(10)));
        assert!(d.overcounted, "CPU inside the calls cannot exceed the hold's: {d:?}");
        let meter = Meter {
            cpu: true,
            write: Duration::from_millis(20),
            write_sampled: Duration::from_millis(20),
            write_sampled_cpu: Duration::from_millis(15),
            ..Meter::default()
        };
        let d = decompose(&meter, Duration::from_millis(50), Some(Duration::from_millis(25)));
        assert!(!d.overcounted, "{d:?}");
        assert_eq!(d.components.iter().sum::<Duration>(), Duration::from_millis(50));
    }

    #[test]
    fn a_sum_past_the_hold_within_the_tolerance_is_rounding_and_still_adds_up() {
        // Two clocks, each rounding: the CPU clock can read a few hundred
        // microseconds more than the wall clock allows. That is not a double
        // count, and the five components still add up to the hold.
        let meter = Meter {
            cpu: true,
            write: Duration::from_millis(30),
            write_sampled: Duration::from_millis(30),
            write_sampled_cpu: Duration::from_millis(10),
            ..Meter::default()
        };
        let hold = Duration::from_millis(50);
        let d = decompose(&meter, hold, Some(Duration::from_micros(30_400)));
        assert!(!d.overcounted, "{d:?}");
        assert_eq!(d.components.iter().sum::<Duration>(), hold, "{d:?}");
        assert_eq!(d.components[Component::OffCpu.slot()], Duration::ZERO);
    }

    #[test]
    fn a_hold_whose_cpu_cannot_be_read_records_no_cpu_and_says_so() {
        let meter = Meter { cpu: true, write: Duration::from_millis(5), ..Meter::default() };
        let d = decompose(&meter, Duration::from_millis(50), None);
        assert!(!d.cpu_measured);
        let counters = HoldCounters::default();
        counters.record(
            WriterHolder::Write,
            &d,
            [Duration::from_millis(50), Duration::ZERO, Duration::ZERO],
        );
        let s = counters.snapshot();
        let row = WriterHolder::Write.slot();
        assert_eq!(s.cpu_unmeasured, 1);
        assert_eq!(s.component_ns[row][Component::Cpu.slot()], 0);
        assert_eq!(
            s.component_ns[row][Component::OffCpu.slot()],
            0,
            "not 45 ms of apparent contention"
        );
        assert_eq!(s.component_ns[row][Component::Write.slot()], 5_000_000);
    }

    /// A hold of exactly `writes` write calls through the meter, each call
    /// sleeping `sleep_from.1` from call index `sleep_from.0` on (off the CPU,
    /// so its true CPU is near zero), and otherwise spinning `spin` (on it).
    fn hold_of_writes(writes: u64, spin: Duration, sleep_from: Option<(u64, Duration)>) -> Meter {
        let scope = Scope::hold();
        for i in 0..writes {
            let _ = io(Io::Write, 4_096, || {
                match sleep_from {
                    Some((from, d)) if i >= from => std::thread::sleep(d),
                    _ => test_hooks::spin(spin),
                }
                Ok(())
            });
        }
        scope.finish().0
    }

    #[test]
    fn holds_of_32_and_33_writes_are_measured_exactly_and_34_is_the_first_estimate() {
        // The exactness claim at its boundary: every write up to the 32nd has
        // its CPU read, the 33rd is the first of one-in-32, and the 34th is
        // the first whose CPU is estimated. And the one-in-32 half, by count:
        // write 33 (index 32), 65 and 97 are sampled, so a hold that sampled
        // nothing after the first 32 reads 32 at every size past it.
        for (writes, estimated_calls, sampled_calls) in [
            (31, 0, 31),
            (32, 0, 32),
            (33, 0, 33),
            (34, 1, 33),
            (64, 31, 33),
            (65, 31, 34),
            (66, 32, 34),
            (96, 62, 34),
            (97, 62, 35),
        ] {
            let meter = hold_of_writes(writes, Duration::ZERO, None);
            assert_eq!(meter.write_calls, writes);
            assert_eq!(meter.write_sampled_calls, sampled_calls, "a hold of {writes} writes");
            assert_eq!(meter.write_calls - meter.write_sampled_calls, estimated_calls);
            let sampled = meter.write != meter.write_sampled;
            assert_eq!(
                sampled,
                estimated_calls > 0,
                "a hold of {writes} writes: {estimated_calls} estimated calls expected, {meter:?}"
            );
            let d = decompose(&meter, meter.write + Duration::from_millis(5), thread_cpu());
            assert_eq!(d.write_estimated, meter.write - meter.write_sampled, "{meter:?}");
            if estimated_calls == 0 {
                assert_eq!(d.write_estimated, Duration::ZERO, "{writes} writes: {d:?}");
            } else {
                assert!(d.write_estimated > Duration::ZERO, "{writes} writes: {d:?}");
            }
        }
    }

    #[test]
    fn at_34_writes_an_unrepresentative_sample_stays_inside_the_bound() {
        // The worst case for the estimate: the 33 writes whose CPU is read are
        // all on the CPU, and the one estimated write is all asleep. The
        // estimate credits it as CPU; the error is at most its wall time,
        // which is exactly what `write_estimated` publishes.
        let spin = Duration::from_millis(2);
        let asleep = Duration::from_millis(20);
        let wall_from = std::time::Instant::now();
        let cpu_from = thread_cpu().unwrap();
        let meter = hold_of_writes(34, spin, Some((33, asleep)));
        let cpu = thread_cpu().unwrap() - cpu_from;
        let hold = wall_from.elapsed();
        let d = decompose(&meter, hold, Some(cpu));
        assert!(d.write_estimated >= asleep, "{d:?}");
        assert_eq!(meter.write_calls - meter.write_sampled_calls, 1, "only the 34th: {meter:?}");
        // Truth: almost no CPU outside the calls, and almost no time off it
        // outside them either — the sleep is inside a call. What a busy
        // machine adds by descheduling the thread between calls is real time
        // off the CPU, not error, and the slack allows for it.
        let off = d.components[Component::OffCpu.slot()];
        let slack = Duration::from_millis(3) + hold / 10;
        assert!(off <= d.write_estimated + slack, "off_cpu {off:?} past its bound: {d:?}");
        // With next to no CPU outside the calls to absorb it, an error this
        // large exceeds it, and this is the case ADR-176 says the over-count
        // guard does see.
        assert!(d.overcounted, "{d:?}");
    }

    #[test]
    fn a_near_zero_sampled_wall_time_cannot_inflate_the_estimate_past_its_bound() {
        // Sub-microsecond page-cache writes can read more CPU than wall time
        // on two clocks that round differently. The ratio is capped at one, so
        // the estimate never exceeds the unsampled wall time; a zero sampled
        // wall time estimates nothing rather than dividing by it.
        let hold = Duration::from_millis(100);
        for (sampled, sampled_cpu) in [
            (Duration::from_nanos(1), Duration::from_millis(5)),
            (Duration::from_nanos(900), Duration::from_micros(2)),
            (Duration::ZERO, Duration::from_millis(5)),
            (Duration::ZERO, Duration::ZERO),
        ] {
            let meter = Meter {
                cpu: true,
                write: sampled + Duration::from_millis(40),
                write_calls: 64,
                write_sampled: sampled,
                write_sampled_cpu: sampled_cpu,
                ..Meter::default()
            };
            // 60 ms of CPU over the hold, so an estimate of more than the
            // 40 ms unsampled would show as `cpu` short by more than 40 ms.
            let d = decompose(&meter, hold, Some(Duration::from_millis(60) + sampled_cpu));
            assert_eq!(d.write_estimated, Duration::from_millis(40), "{d:?}");
            let cpu = d.components[Component::Cpu.slot()];
            assert!(
                cpu >= Duration::from_millis(20),
                "estimate past the 40 ms bound for sample {sampled:?}/{sampled_cpu:?}: {d:?}"
            );
            assert_eq!(d.components.iter().sum::<Duration>(), hold, "{d:?}");
            if sampled.is_zero() {
                // Nothing sampled is nothing to estimate from: no CPU is
                // credited to the unsampled writes, rather than a ratio of
                // 0/0 or x/0 whose value depends on how `f64::min` treats NaN.
                assert_eq!(cpu, Duration::from_millis(60), "{d:?}");
            }
        }
    }

    #[test]
    fn the_cpu_share_is_a_number_in_zero_to_one_whatever_the_clocks_read() {
        // Called directly, past the zero-sample check that keeps these cases
        // from reaching it through the meter: 0/0 is NaN and x/0 infinite, and
        // either must come out as a share `Duration::mul_f64` accepts.
        let ms = Duration::from_millis;
        for (cpu, wall, expected) in [
            (Duration::ZERO, Duration::ZERO, 1.0),
            (ms(5), Duration::ZERO, 1.0),
            (ms(5), Duration::from_nanos(1), 1.0),
            (ms(3), ms(2), 1.0),
            (ms(1), ms(4), 0.25),
            (Duration::ZERO, ms(4), 0.0),
        ] {
            let share = cpu_share(cpu, wall);
            assert!(
                share.is_finite() && (0.0..=1.0).contains(&share),
                "{cpu:?}/{wall:?} gave {share}"
            );
            assert_eq!(share, expected, "{cpu:?}/{wall:?}");
            let _ = ms(10).mul_f64(share);
        }
    }

    #[test]
    fn growing_the_file_inside_a_hold_is_write_time_not_off_cpu() {
        // redb grows the file with `set_len` from inside a transaction, and
        // reads its size with `len`. Neither is a page read, write or fsync,
        // and an unmetered one lands in `off_cpu`, the residual: time asleep
        // inside either must land in `read` or `write` instead. In practice
        // the calls counted here are all `set_len`: `len` is metered as a
        // precaution and never happens in a hold (see
        // `redb_reads_the_files_size_only_when_it_opens_and_never_inside_a_hold`),
        // so a non-zero count here proves nothing about `len`. Against the
        // same bulk without the sleeps, because an unoptimised build spends
        // real time off the CPU in a large bulk with nothing contending.
        let (engine, _dir) = fresh();
        let coll = engine.create_collection("shop", "orders").unwrap();
        let bulk = || {
            let docs = (0..200).map(|i| doc! { "n": i, "body": "x".repeat(4_000) }).collect();
            engine.insert_many(&coll, docs).unwrap();
        };
        let base = during(&engine, WriterHolder::Bulk, bulk);
        test_hooks::reset();
        // Long against what a busy machine adds to `off_cpu` by itself: a
        // bulk here has read 50 ms more of it than the same bulk before it,
        // with nothing wrong, and the bound below is half of this.
        let asleep = Duration::from_secs(1);
        test_hooks::SLEEP_IN_FILE_SIZE.with(|s| s.set(asleep));
        let row = during(&engine, WriterHolder::Bulk, bulk);
        let calls = test_hooks::FILE_SIZE_CALLS.with(|c| c.get());
        test_hooks::reset();
        assert!(calls > 0, "the bulk grew the file inside its hold: {row:?}");
        let slept = asleep * calls as u32;
        let io = row.get(Component::Read) + row.get(Component::Write);
        assert!(
            io >= slept,
            "{calls} file-size calls asleep {slept:?}, in the calls {io:?}: {row:?}"
        );
        let added_off = row.get(Component::OffCpu).saturating_sub(base.get(Component::OffCpu));
        assert!(
            added_off < slept / 2,
            "{slept:?} asleep in file-size calls added {added_off:?} of off_cpu: {row:?} vs {base:?}"
        );
        assert_adds_up(&row);
    }

    /// Closing the backend lets the store go even while another descriptor
    /// shares its open file description, which keeps a `flock` alive past the
    /// backend's own descriptor: only the explicit release frees it.
    #[test]
    fn closing_the_backend_releases_the_store_while_its_description_is_shared() {
        use redb::StorageBackend as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        let shared = file.try_clone().unwrap();
        let inner = redb::backends::FileBackend::new(file).unwrap();
        let lock = crate::store_lock::StoreLock::take(&shared, &inner).unwrap();
        let backend = MeteredBackend::new(inner, lock, Default::default());
        let other = std::fs::File::open(&path).unwrap();
        assert!(matches!(other.try_lock(), Err(std::fs::TryLockError::WouldBlock)), "held");
        backend.close().unwrap();
        other.try_lock().expect("closed, with the description still shared");
        drop((shared, backend));
    }

    #[test]
    fn inside_a_hold_redb_reads_the_files_size_only_to_resize_it() {
        // redb 4.1 read the backend's `len` only when a database was opened.
        // redb 4.3 also reads it inside a transaction, exactly once before
        // each `set_len` (`PagedCachedFile::resize`, the only caller of
        // `set_len`), and the metering is what keeps that time out of
        // `off_cpu`. Every kind of hold here, the file growing under them,
        // reads the size once per resize; a redb that reads it anywhere else,
        // or resizes without reading it, fails this.
        let (engine, _dir) = fresh();
        test_hooks::reset();
        let coll = engine.create_collection("shop", "orders").unwrap();
        for i in 0..50 {
            engine.insert(&coll, doc! { "n": i }).unwrap();
        }
        let docs = (0..2_000).map(|i| doc! { "n": i, "body": "x".repeat(4_000) }).collect();
        engine.insert_many(&coll, docs).unwrap();
        engine
            .create_index(
                "shop",
                "orders",
                vec![crate::meta::IndexField::ascending("n")],
                false,
                None,
            )
            .unwrap();
        engine.drop_collection("shop", "orders").unwrap();
        let (len, file_size) = (
            test_hooks::LEN_CALLS.with(|c| c.get()),
            test_hooks::FILE_SIZE_CALLS.with(|c| c.get()),
        );
        test_hooks::reset();
        let set_len = file_size - len;
        assert!(set_len > 0, "the file grew inside a hold, so the hook was reachable");
        assert_eq!(
            len, set_len,
            "redb reads the file's size once per resize inside a hold, and nowhere else"
        );

        // And the hook sees a `len` when one happens: opening a database reads
        // the file's size, here under a meter installed for the purpose.
        let dir = tempfile::tempdir().unwrap();
        let scope = Scope::hold();
        let _opened = Engine::open(&dir.path().join("other.redb")).unwrap();
        drop(scope);
        let opened_len = test_hooks::LEN_CALLS.with(|c| c.get());
        test_hooks::reset();
        assert!(opened_len > 0, "an open reads the file's size, and the meter saw none");
    }

    #[test]
    fn a_clamped_sample_credits_the_unsampled_writes_no_more_than_their_wall_time() {
        // A sample whose CPU reads more than its wall time — two clocks
        // rounding over a sub-microsecond call — caps the share at one. The
        // CPU credited to the unsampled writes is then their wall time and no
        // more, and a hold with the CPU to cover it is not an over-count.
        let meter = Meter {
            cpu: true,
            write: Duration::from_millis(101),
            write_calls: 64,
            write_sampled_calls: 33,
            write_sampled: Duration::from_micros(1),
            write_sampled_cpu: Duration::from_millis(1),
            cpu_reads: 33,
            ..Meter::default()
        };
        let hold = Duration::from_millis(120);
        let cpu_over_hold = Duration::from_millis(110);
        let d = decompose(&meter, hold, Some(cpu_over_hold));
        let credited =
            cpu_over_hold - meter.write_sampled_cpu - d.components[Component::Cpu.slot()];
        assert!(credited <= d.write_estimated, "credited {credited:?}: {d:?}");
        assert!(!d.overcounted, "{d:?}");
        assert_eq!(d.components.iter().sum::<Duration>(), hold, "{d:?}");
    }

    #[test]
    fn cpu_inside_the_calls_past_the_holds_cpu_is_an_overcount_at_the_clocks_grain() {
        // Both sides of this comparison come from one clock, so it is held to
        // that clock's grain rather than the wall-clock tolerance: a 43 ms
        // hold with 1 ms of CPU cannot claim 2.4 ms of CPU inside its calls.
        let meter = Meter {
            cpu: true,
            read: Duration::from_millis(3),
            read_cpu: Duration::from_micros(2_400),
            cpu_reads: 10,
            ..Meter::default()
        };
        let d = decompose(&meter, Duration::from_millis(43), Some(Duration::from_millis(1)));
        assert!(d.overcounted, "{d:?}");
        // And the grain itself is not an over-count.
        let meter = Meter { read_cpu: Duration::from_micros(1_010), ..meter };
        let d = decompose(&meter, Duration::from_millis(43), Some(Duration::from_millis(1)));
        assert!(!d.overcounted, "{d:?}");
    }

    #[test]
    fn a_cpu_clock_that_fails_mid_hold_records_no_cpu() {
        let scope = Scope::hold();
        let _ = io(Io::Write, 4_096, || Ok(()));
        test_hooks::CPU_CLOCK_FAILS.with(|f| f.set(true));
        let _ = io(Io::Write, 4_096, || Ok(()));
        test_hooks::CPU_CLOCK_FAILS.with(|f| f.set(false));
        let (meter, cpu) = scope.finish();
        test_hooks::reset();
        assert!(meter.cpu_failed, "the failed read is remembered: {meter:?}");
        assert!(cpu.is_some(), "the hold's own clock read did not fail");
        let d = decompose(&meter, Duration::from_millis(10), cpu);
        assert!(!d.cpu_measured, "a hold with a failed read reports no cpu: {d:?}");
        assert_eq!(d.components[Component::OffCpu.slot()], Duration::ZERO, "{d:?}");
    }

    #[test]
    fn a_served_window_counts_a_unique_violation_entry_as_passed() {
        // Unique-violation entries are in the oplog and are never served to a
        // peer: the walk examines them, so they are passed, not served.
        let (engine, _dir) = fresh();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "n": 1 }).unwrap();
        let violation = kimmy_core::OplogEntry {
            stamp: engine.next_stamp(),
            kind: kimmy_core::OpKind::UniqueViolation,
            collection: coll.id,
            doc_id: None,
            body: None,
        };
        let txn = engine.begin_write(WriterHolder::Write).unwrap();
        crate::engine::append_oplog(&txn, &violation).unwrap();
        txn.commit().unwrap();
        engine.insert(&coll, doc! { "n": 2 }).unwrap();

        let all = engine
            .read_oplog_from_where(kimmy_core::Hlc::ZERO, usize::MAX, |_| true)
            .unwrap()
            .entries;
        let before = engine.serve_cost();
        let window = engine.serve_entries_to_peer(kimmy_core::Hlc::ZERO, 1_024, None, &[]).unwrap();
        let after = engine.serve_cost();
        assert!(window.entries.iter().all(|e| e.kind != kimmy_core::OpKind::UniqueViolation));
        assert_eq!(after.passed - before.passed, 1, "the one violation entry");
        assert_eq!(after.entries - before.entries, window.entries.len() as u64);
        assert_eq!(all.len() as u64, window.entries.len() as u64 + 1, "the walk saw it");
    }

    #[test]
    #[should_panic(expected = "nested inside a hold")]
    #[cfg(debug_assertions)]
    fn a_walk_scope_cannot_nest_inside_a_hold() {
        let _hold = Scope::hold();
        let _walk = Scope::walk();
    }

    #[test]
    fn a_scope_a_panic_unwinds_through_stops_metering() {
        let outcome = std::panic::catch_unwind(|| {
            let _scope = Scope::hold();
            panic!("inside the scope");
        });
        assert!(outcome.is_err());
        assert_eq!(METER.with(|m| *m.borrow()), None);
    }

    #[test]
    fn serving_a_window_counts_what_it_walked_and_what_it_served() {
        let (engine, _dir) = fresh();
        let coll = engine.create_collection("shop", "orders").unwrap();
        let mut stamps = Vec::new();
        for i in 0..10 {
            stamps.push(engine.insert_stamped(&coll, doc! { "n": i }).unwrap().1);
        }
        // The peer holds everything up to the fourth insert of this node's.
        let mut held = kimmy_core::VersionVector::new();
        held.observe(stamps[3]);
        let all = engine.entries_for_peer(kimmy_core::Hlc::ZERO, usize::MAX).unwrap().entries;
        let passed = all.iter().filter(|e| e.stamp.hlc <= stamps[3].hlc).count() as u64;

        let before = engine.serve_cost();
        let window =
            engine.serve_entries_to_peer(kimmy_core::Hlc::ZERO, 1_024, Some(&held), &[]).unwrap();
        let after = engine.serve_cost();
        assert_eq!(after.windows - before.windows, 1);
        assert_eq!(after.entries - before.entries, window.entries.len() as u64);
        assert_eq!(window.entries.len() as u64 + passed, all.len() as u64);
        assert_eq!(after.passed - before.passed, passed, "the walk's passes are counted");
        let walks: u64 =
            after.walk_buckets.iter().sum::<u64>() - before.walk_buckets.iter().sum::<u64>();
        assert_eq!(walks, 1);
        // A local read of the same range is not a window served.
        let _ = engine.entries_for_peer(kimmy_core::Hlc::ZERO, 10).unwrap();
        let _ =
            engine.entries_for_peer_marked(kimmy_core::Hlc::ZERO, 10, Some(&held), &[]).unwrap();
        assert_eq!(engine.serve_cost().windows, after.windows);
    }

    #[test]
    fn a_served_walk_off_a_cold_cache_reads_the_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let filler = "x".repeat(1_000);
        {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("shop", "orders").unwrap();
            for _ in 0..8 {
                let docs = (0..1_000).map(|i| doc! { "n": i, "body": filler.clone() }).collect();
                engine.insert_many(&coll, docs).unwrap();
            }
        }
        let engine = Engine::open_with_cache(&path, Some(1 << 20)).unwrap();
        let window = engine.serve_entries_to_peer(kimmy_core::Hlc::ZERO, 1_024, None, &[]).unwrap();
        assert_eq!(window.entries.len(), 1_024);
        let cost = engine.serve_cost();
        assert!(cost.read_bytes > 0 && cost.read_ns > 0, "{cost:?}");
        // The walk's reads are the walk's, not a hold's.
        let holds = engine.writer_hold_decomposition();
        assert!(holds.read_bytes.iter().sum::<u64>() < cost.read_bytes, "{holds:?}");
    }
}
