//! What the daemon's yield evaluator (ADR-213) exposes to the rest of the API:
//! one handle that `/metrics`, the OTLP bridge and `/v1/topology` read.
//!
//! The evaluator and its heartbeat cells live below this crate, and the daemon
//! builds and starts them. This holds the read side: the evaluator's published
//! decision, the three class cells for the owned counts, the fault counters and
//! the ttl progress age, and the facts about this start (the off switch and
//! probation) that the series report.

use std::sync::Arc;
use std::time::Duration;

use kimmy_cluster::yielding::driver::{Clock, RealClock};
use kimmy_cluster::yielding::{Direction, Published, Suppression, Verdict};
use kimmy_cluster::{ClassState, OwnerClass, StallCause};
use kimmy_storage::class_step::ClassCell;

use crate::metrics::YieldReading;

/// The evaluator's read side. Built once by the daemon and handed to the state.
pub struct YieldHandle {
    pub published: Arc<Published>,
    /// The classes' cells, in `OwnerClass::ALL` order.
    pub cells: [&'static ClassCell; 3],
    pub clock: Arc<RealClock>,
    /// The tick, which the staleness rule is a multiple of.
    pub e: Duration,
    /// `KIMMY_OWNERSHIP_YIELD` is not `off`.
    pub enabled: bool,
    /// This start began in probation.
    pub probation: bool,
}

/// The index of a [`ClassState`] in the one-hot series: ok, idle, suspect,
/// stalled, then unknown.
pub fn state_slot(state: ClassState) -> usize {
    match state {
        ClassState::Ok => 0,
        ClassState::Idle => 1,
        ClassState::Suspect => 2,
        ClassState::Stalled => 3,
        ClassState::Unknown => 4,
    }
}

/// The index of a stall cause: 0 none, then local, runtime, probation.
pub fn cause_slot(cause: Option<StallCause>) -> usize {
    match cause {
        None => 0,
        Some(StallCause::Local) => 1,
        Some(StallCause::Runtime) => 2,
        Some(StallCause::Probation) => 3,
        Some(StallCause::Unknown) => 0,
    }
}

impl YieldHandle {
    fn now_ms(&self) -> u64 {
        self.clock.now().as_millis() as u64
    }

    /// How long since the evaluator last ticked (or started).
    pub fn evaluator_age(&self) -> Duration {
        let now = self.now_ms();
        Duration::from_millis(now.saturating_sub(self.published.last_tick_ms().unwrap_or(0)))
    }

    /// Seconds since the ttl class last beat.
    pub fn ttl_beat_age(&self) -> Duration {
        self.cells[0].reading().since_beat
    }

    /// What was last decided, for the series and the topology.
    pub fn reading(&self) -> YieldReading {
        let current = self.published.current();
        let mut reading = YieldReading {
            ticks: self.published.ticks(),
            responsive: current.responsive,
            enabled: self.enabled,
            probation: self.probation,
            ..YieldReading::default()
        };
        for (i, class) in OwnerClass::ALL.into_iter().enumerate() {
            reading.state[i] = state_slot(current.state.of(class));
            reading.cause[i] = cause_slot(current.cause.of(class));
            reading.yielding[i] = current.yielding.of(class);
            let cell = self.cells[i].reading();
            reading.owned[i] = cell.owned;
            reading.faults[i] = [cell.local_fault, cell.remote_fault, cell.config_fault];
            for direction in Direction::ALL {
                reading.transitions[i][direction as usize] =
                    self.published.transitions(class, direction);
            }
            for reason in Suppression::ALL {
                reading.suppressed[i][reason.slot()] =
                    self.published.suppressed(class, reason) != 0;
            }
            for verdict in Verdict::ALL {
                reading.observations[i][verdict.slot()] =
                    self.published.observations(class, verdict);
            }
        }
        reading
    }
}
