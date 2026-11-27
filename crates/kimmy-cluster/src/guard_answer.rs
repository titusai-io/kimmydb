//! What a member says of itself where a restore asks before it joins (D13 containment, ADR-274):
//! the answer in the `Welcome` to a probe and in the `Welcome` of a member that is undecided, and
//! the report a dialler hands an undecided member back over the connection it opened.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use kimmy_core::NodeId;
use kimmy_storage::Engine;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::warn;

use crate::membership::Members;
use crate::protocol::{GuardAnswer, Standing};

/// The most listings, and the most members, one answer names.
pub const MAX_NAMED: usize = 1_024;

/// The most diallers' reports kept at once, one for each dialling process.
const MAX_REPORTS: usize = 1_024;

/// What a member set keeps for its member's answers.
#[derive(Default)]
pub(crate) struct GuardState {
    /// This process's token, made at the first answer.
    token: OnceLock<Vec<u8>>,
    gossiping_since: OnceLock<Instant>,
    /// Where this member tells others it listens.
    address: OnceLock<SocketAddr>,
    /// The reports handed to this member while it was undecided and not yet taken, by the
    /// dialler's token.
    reports: Mutex<BTreeMap<Vec<u8>, (NodeId, GuardAnswer)>>,
    report_arrived: Notify,
    cut_said: AtomicBool,
}

impl Members {
    /// This member's membership has begun to gossip: the age an answer gives counts from here.
    /// Later calls do nothing.
    pub(crate) fn note_gossiping(&self) {
        let _ = self.guard_state().gossiping_since.set(Instant::now());
    }

    /// Where this member tells others it listens, for the report it hands an undecided member.
    /// Later calls do nothing.
    pub(crate) fn note_address(&self, address: SocketAddr) {
        let _ = self.guard_state().address.set(address);
    }

    /// Whether this member is undecided: its mistaken-restore guard holds it.
    pub(crate) fn guard_standing(&self) -> Standing {
        if self.catch_up().is_some_and(|catch_up| catch_up.held_by_guard()) {
            Standing::Undecided
        } else {
            Standing::Open
        }
    }

    /// This process's token: how it knows its own answers.
    pub(crate) fn guard_token(&self) -> Vec<u8> {
        // A version 4 UUID's bytes: random, and made once for this member set, of which a
        // process has one.
        self.guard_state().token.get_or_init(|| NodeId::generate().to_bytes().to_vec()).clone()
    }

    /// This member's answer to a member of `asker`.
    pub(crate) fn guard_answer(&self, engine: &Engine, asker: NodeId) -> GuardAnswer {
        let state = self.guard_state();
        let entries = self.entries();
        let mut listings: Vec<String> = entries
            .iter()
            .filter(|(_, node)| *node == asker)
            .map(|(addr, _)| addr.to_string())
            .collect();
        let mut members: Vec<(String, NodeId)> =
            entries.iter().map(|(addr, node)| (addr.to_string(), *node)).collect();
        if members.len() > MAX_NAMED && !state.cut_said.swap(true, Ordering::Relaxed) {
            warn!(
                cap = MAX_NAMED,
                members = members.len(),
                "this member lists more members than an answer names at most: the rest are left \
                 out of what it tells a restore that asks before it joins (said once)"
            );
        }
        listings.truncate(MAX_NAMED);
        members.truncate(MAX_NAMED);
        GuardAnswer {
            token: self.guard_token(),
            standing: self.guard_standing(),
            membership_age_ms: state
                .gossiping_since
                .get()
                .map_or(0, |since| u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)),
            founded_unmet: engine.is_founding_epoch() && !engine.has_met_a_peer(),
            listings,
            members,
            address: state.address.get().map(ToString::to_string),
        }
    }

    /// Keep `report`, which a dialler that proved the cluster secret as `from` handed this member
    /// while it was undecided: the latest of each dialling process, its lists cut as an answer's
    /// are.
    pub(crate) fn note_guard_report(&self, from: NodeId, mut report: GuardAnswer) {
        report.listings.truncate(MAX_NAMED);
        report.members.truncate(MAX_NAMED);
        let mut reports = self.guard_state().reports.lock();
        if reports.len() >= MAX_REPORTS && !reports.contains_key(&report.token) {
            reports.pop_first();
        }
        reports.insert(report.token.clone(), (from, report));
        drop(reports);
        self.guard_state().report_arrived.notify_one();
    }

    /// Take the reports that have arrived: each is one answer, of one round of the guard's.
    pub(crate) fn take_guard_reports(&self) -> Vec<(NodeId, GuardAnswer)> {
        std::mem::take(&mut *self.guard_state().reports.lock()).into_values().collect()
    }

    /// Wait for a report to arrive since the last wait, or at once if one has.
    pub(crate) async fn guard_report_arrived(&self) {
        self.guard_state().report_arrived.notified().await;
    }

    /// The reports diallers have handed this member while it was undecided that its guard has not
    /// taken, the latest of each dialling process, each with the node id its dialler proved the
    /// cluster secret under.
    pub fn guard_reports(&self) -> Vec<(NodeId, GuardAnswer)> {
        self.guard_state().reports.lock().values().cloned().collect()
    }
}
