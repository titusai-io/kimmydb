//! The question a restore asks before it joins its cluster, asked in rounds, and the verdict on
//! each round (D13 containment, ADR-274).
//!
//! A round asks every address the seeds resolve to, then every address an open member's answer
//! names, until no answer names a new one. A report that a dialler hands the asker is one more
//! answer of the round it arrives in. A running host of the asking node halts it as soon as it is
//! heard. A round passes the asker when some answer counts and no member still lists, for the
//! asker's node, an address that stayed silent. Any other round decides nothing, and another
//! follows: **no passage of time passes anyone.**

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use kimmy_core::NodeId;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::membership::Members;
use crate::protocol::{GuardAnswer, Standing};
use crate::transport::probe_answer;

/// How long an address that nobody names for the asking node has to answer in a round.
pub const UNNAMED_WITHIN: Duration = Duration::from_millis(1_500);

/// The pause between one round that decided nothing and the next.
pub const BETWEEN_ROUNDS: Duration = Duration::from_secs(1);

/// The most addresses one round asks, which keeps lists that name ever more from making a round
/// endless.
const MAX_ASKED: usize = 4_096;

/// Where a running host is said to be when its report does not say where it listens.
const NOWHERE: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);

/// What asks, and how long it waits.
#[derive(Clone)]
pub struct Asking {
    /// The node id of the restore that asks.
    pub own_node: NodeId,
    pub secret: String,
    /// How old a member's membership must be for its answer to count, and how long an address
    /// named for the asking node has to answer: twice foca's suspicion-to-down time.
    pub window: Duration,
    pub unnamed_within: Duration,
    pub between_rounds: Duration,
    /// `--replace` was given for this epoch's restore and is honoured.
    pub replace: bool,
}

impl Asking {
    pub fn new(own_node: NodeId, secret: &str, window: Duration, replace: bool) -> Self {
        Self {
            own_node,
            secret: secret.to_string(),
            window,
            unnamed_within: UNNAMED_WITHIN,
            between_rounds: BETWEEN_ROUNDS,
            replace,
        }
    }
}

/// What the rounds came to. A round that comes to neither is followed by another: no passage of
/// time decides.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// A running member answered and no running host of the asking node did, or `--replace` was
    /// given. With every address the deciding round was named, for the member to be offered.
    Pass { named: Vec<SocketAddr> },
    /// A running host of the asking node answered, from this address.
    LiveHost(SocketAddr),
}

/// Let a passed restore in, in rule 5's order: first its membership, which is offered `offer`,
/// and only then `open`, which lets go of what the guard held.
///
/// **Scaffold of the tests commit**: opens first. The daemon does not call it yet.
pub async fn let_in<M, F, O>(offer: Vec<SocketAddr>, membership: M, open: O)
where
    M: FnOnce(Vec<SocketAddr>) -> F,
    F: Future<Output = ()>,
    O: FnOnce(),
{
    open();
    membership(offer).await;
}

/// Ask, round after round, until one decides. `seeds` is resolved again for each round;
/// `members` holds the asker's token and the reports that diallers hand it.
pub async fn ask_until_decided<F, Fut>(asking: &Asking, members: &Members, seeds: F) -> Decision
where
    F: Fn() -> Fut,
    Fut: Future<Output = Vec<SocketAddr>>,
{
    let own_token = members.guard_token();
    // The addresses that have answered with this process's token: itself, and asked no more.
    let mut own = BTreeSet::new();
    // A report handed over before the asking began arrived in none of its rounds.
    drop(members.take_guard_reports());
    let mut carried = Vec::new();
    // What the last round that decided nothing was waiting on, so that it is said when it changes.
    let mut said = None;
    loop {
        let resolved = seeds().await;
        let mut round = Round::new(asking, &own_token, &mut own);
        if let Some(live) = round.run(members, resolved, carried).await {
            return Decision::LiveHost(live);
        }
        if let Some(decision) = round.verdict(members).await {
            return decision;
        }
        let waiting = round.waiting_on();
        if said.as_ref() != Some(&waiting) {
            info!(
                asked = round.asked.len(),
                undecided = waiting.undecided,
                uncounted = waiting.uncounted,
                silent_named = ?waiting.silent_named,
                "the mistaken-restore guard waits: nothing a member has answered lets this host \
                 join, so it asks again, and no passage of time passes it. `undecided` members \
                 are waiting too; `uncounted` ones have met nobody or have only just joined; \
                 `silent_named` are addresses listed for this node that keep it waiting"
            );
            said = Some(waiting);
        }
        carried = match pause(asking, &own_token, members).await {
            Ok(reports) => reports,
            Err(live) => return Decision::LiveHost(live),
        };
    }
}

/// The pause between rounds. A report that arrives in it is the next round's, but one from a
/// running host of the asking node halts at once.
async fn pause(
    asking: &Asking,
    own_token: &[u8],
    members: &Members,
) -> Result<Vec<(NodeId, GuardAnswer)>, SocketAddr> {
    let until = Instant::now() + asking.between_rounds;
    let mut carried = Vec::new();
    loop {
        for (from, report) in members.take_guard_reports() {
            if let Some(live) = running_host(asking, own_token, from, Some(&report)) {
                return Err(live.unwrap_or(NOWHERE));
            }
            carried.push((from, report));
        }
        if Instant::now() >= until {
            return Ok(carried);
        }
        tokio::select! {
            () = members.guard_report_arrived() => {}
            () = tokio::time::sleep_until(until) => {}
        }
    }
}

/// Whether an answer from `node` is a running host of the asking node, which halts the asker: its
/// gate open, the asker's node id, and not the asker's own token; never under `--replace`. With
/// where it says it listens, if it says.
fn running_host(
    asking: &Asking,
    own_token: &[u8],
    node: NodeId,
    answer: Option<&GuardAnswer>,
) -> Option<Option<SocketAddr>> {
    let open = answer.is_none_or(|answer| answer.standing == Standing::Open);
    let own = answer.is_some_and(|answer| answer.token == own_token);
    if asking.replace || node != asking.own_node || !open || own {
        return None;
    }
    Some(answer.and_then(|answer| answer.address.as_ref()?.parse().ok()))
}

/// Where `answer` has the node `node`: its listings, which are for the asker, and the entries of
/// its member list.
fn listed_for(node: NodeId, answer: &GuardAnswer) -> BTreeSet<SocketAddr> {
    let entries = answer.members.iter().filter(|(_, listed)| *listed == node);
    answer
        .listings
        .iter()
        .chain(entries.map(|(addr, _)| addr))
        .filter_map(|addr| addr.parse().ok())
        .collect()
}

/// An answer from an open member, for the last question of a round.
struct Heard {
    /// Where it was asked; `None` for a report, which cannot be asked again.
    at: Option<SocketAddr>,
    /// Where it lists the asking node.
    lists: BTreeSet<SocketAddr>,
}

/// What a round that decided nothing was waiting on.
#[derive(PartialEq, Eq)]
struct Waiting {
    /// The answers from members that are undecided themselves.
    undecided: usize,
    /// The open answers that do not count.
    uncounted: usize,
    silent_named: BTreeSet<SocketAddr>,
}

type Probed = (SocketAddr, Option<(NodeId, Option<Box<GuardAnswer>>)>);

/// One round: what it has asked, and what has answered.
struct Round<'a> {
    asking: &'a Asking,
    own_token: &'a [u8],
    own: &'a mut BTreeSet<SocketAddr>,
    asked: BTreeSet<SocketAddr>,
    /// The probes not yet ended, with when each began.
    waiting: BTreeMap<SocketAddr, Instant>,
    probes: JoinSet<Probed>,
    named: BTreeSet<SocketAddr>,
    named_for_me: BTreeSet<SocketAddr>,
    answered: BTreeSet<SocketAddr>,
    open: Vec<Heard>,
    counted: bool,
    undecided: usize,
    uncounted: usize,
}

impl<'a> Round<'a> {
    fn new(asking: &'a Asking, own_token: &'a [u8], own: &'a mut BTreeSet<SocketAddr>) -> Self {
        Self {
            asking,
            own_token,
            own,
            asked: BTreeSet::new(),
            waiting: BTreeMap::new(),
            probes: JoinSet::new(),
            named: BTreeSet::new(),
            named_for_me: BTreeSet::new(),
            answered: BTreeSet::new(),
            open: Vec::new(),
            counted: false,
            undecided: 0,
            uncounted: 0,
        }
    }

    /// Ask `addr`, once in a round. Every probe may take the window: one for an address that
    /// nobody names for this node is given up sooner by the round's own end.
    fn ask(&mut self, addr: SocketAddr) {
        if self.own.contains(&addr) || self.asked.len() >= MAX_ASKED || !self.asked.insert(addr) {
            return;
        }
        self.waiting.insert(addr, Instant::now());
        spawn_probe(&mut self.probes, self.asking, addr, self.asking.window);
    }

    /// Take one answer, of the member asked at `at` or of a report, and ask whatever it newly
    /// names. Returns where a running host of the asking node is, if this is one.
    fn hear(
        &mut self,
        at: Option<SocketAddr>,
        node: NodeId,
        answer: Option<&GuardAnswer>,
    ) -> Option<SocketAddr> {
        self.answered.extend(at);
        if let Some(says) = running_host(self.asking, self.own_token, node, answer) {
            return Some(at.or(says).unwrap_or(NOWHERE));
        }
        let answer = answer?;
        if answer.token == self.own_token {
            self.own.extend(at);
            return None;
        }
        // An undecided member is not running anything: it is no answer that counts, and what it
        // names is not asked.
        if answer.standing == Standing::Undecided {
            self.undecided += 1;
            return None;
        }
        if node == self.asking.own_node {
            warn!(
                ?at,
                "a running host of this node answered the guard; `--replace` was given for this \
                 epoch, so this host goes on and the ordinary containment tells that one"
            );
        }
        let lists = listed_for(self.asking.own_node, answer);
        self.named_for_me.extend(&lists);
        let members = answer.members.iter().filter_map(|(addr, _)| addr.parse().ok());
        for addr in members.chain(lists.iter().copied()).collect::<Vec<_>>() {
            self.named.insert(addr);
            self.ask(addr);
        }
        let counts = !answer.founded_unmet
            && Duration::from_millis(answer.membership_age_ms) >= self.asking.window;
        self.counted |= counts;
        self.uncounted += usize::from(!counts);
        self.open.push(Heard { at, lists });
        None
    }

    /// Ask `seeds` and everything the open answers name, until every probe has ended, or the only
    /// ones still waiting are for addresses nobody names for this node and have had their time.
    /// Returns early, with where it is, on a running host of this node.
    async fn run(
        &mut self,
        members: &Members,
        seeds: Vec<SocketAddr>,
        carried: Vec<(NodeId, GuardAnswer)>,
    ) -> Option<SocketAddr> {
        for addr in seeds {
            self.ask(addr);
        }
        let mut reports = carried;
        loop {
            // Heard before the round's end is judged, so that what a report names is asked.
            reports.extend(members.take_guard_reports());
            for (from, report) in reports.drain(..) {
                if let Some(live) = self.hear(None, from, Some(&report)) {
                    return Some(live);
                }
            }
            if self.probes.is_empty() {
                return None;
            }
            let named_waits = self.waiting.keys().any(|addr| self.named_for_me.contains(addr));
            let give_up = (!named_waits)
                .then(|| self.waiting.values().max().copied())
                .flatten()
                .map(|latest| latest + self.asking.unnamed_within);
            if give_up.is_some_and(|at| Instant::now() >= at) {
                self.probes.abort_all();
                return None;
            }
            tokio::select! {
                Some(done) = self.probes.join_next() => {
                    // A probe that did not return is no answer.
                    if let Ok((addr, got)) = done {
                        self.waiting.remove(&addr);
                        if let Some((node, answer)) = got
                            && let Some(live) = self.hear(Some(addr), node, answer.as_deref())
                        {
                            return Some(live);
                        }
                    }
                }
                () = members.guard_report_arrived() => {}
                () = sleep_until_if(give_up) => {}
            }
        }
    }

    /// The addresses named for this node that gave no answer and are not this host.
    fn silent_named(&self) -> BTreeSet<SocketAddr> {
        self.named_for_me
            .iter()
            .copied()
            .filter(|addr| !self.answered.contains(addr) && !self.own.contains(addr))
            .collect()
    }

    fn waiting_on(&self) -> Waiting {
        Waiting {
            undecided: self.undecided,
            uncounted: self.uncounted,
            silent_named: self.silent_named(),
        }
    }

    /// The verdict on a round that has ended, or `None`: nothing is decided.
    async fn verdict(&mut self, members: &Members) -> Option<Decision> {
        let pass = Decision::Pass { named: self.named.iter().copied().collect() };
        if self.asking.replace {
            return Some(pass);
        }
        if !self.counted {
            return None;
        }
        let silent = self.silent_named();
        if silent.is_empty() {
            return Some(pass);
        }
        match self.still_listed(&silent).await {
            Err(live) => Some(Decision::LiveHost(live)),
            Ok(true) => None,
            Ok(false) => {
                // A running host of this node may have dialled while the last question was out.
                let live = members.take_guard_reports().into_iter().find_map(|(from, report)| {
                    running_host(self.asking, self.own_token, from, Some(&report))
                });
                Some(live.map_or(pass, |says| Decision::LiveHost(says.unwrap_or(NOWHERE))))
            }
        }
    }

    /// The last question of a round that left an address named for this node silent: does any
    /// member that answered still list one of those? Each is asked again. One that listed such an
    /// address and does not answer now still lists it, and so does a report, which cannot be asked.
    async fn still_listed(&mut self, silent: &BTreeSet<SocketAddr>) -> Result<bool, SocketAddr> {
        let mut again = JoinSet::new();
        let mut unanswered = BTreeSet::new();
        for heard in &self.open {
            let listed = !heard.lists.is_disjoint(silent);
            match heard.at {
                None if listed => return Ok(true),
                None => {}
                Some(at) => {
                    if listed {
                        unanswered.insert(at);
                    }
                    spawn_probe(&mut again, self.asking, at, self.asking.unnamed_within);
                }
            }
        }
        let mut still = false;
        while let Some(done) = again.join_next().await {
            let Ok((at, Some((node, answer)))) = done else { continue };
            let answer = answer.as_deref();
            if running_host(self.asking, self.own_token, node, answer).is_some() {
                return Err(at);
            }
            let Some(answer) = answer.filter(|answer| answer.standing == Standing::Open) else {
                continue;
            };
            unanswered.remove(&at);
            still |= !listed_for(self.asking.own_node, answer).is_disjoint(silent);
        }
        Ok(still || !unanswered.is_empty())
    }
}

/// Start a probe of `addr`, given `within`.
fn spawn_probe(probes: &mut JoinSet<Probed>, asking: &Asking, addr: SocketAddr, within: Duration) {
    let (own, secret) = (asking.own_node, asking.secret.clone());
    // UNSUPERVISED: one probe, whose panic ends that probe only; the join reads it as no answer.
    probes.spawn(async move {
        let got = tokio::time::timeout(within, probe_answer(own, addr, &secret)).await;
        (addr, got.ok().flatten())
    });
}

/// Sleep until `at`, or for ever without one.
async fn sleep_until_if(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
