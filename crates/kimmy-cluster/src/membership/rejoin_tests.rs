//! A member the cluster declared down, and that rejoins, must go on failure
//! detecting: deterministic, in-memory runs of SWIM (foca, with this crate's
//! `Member` and its [`super::after_input`]) through the schedules that left a
//! member silent in the field. Datagrams to a frozen member wait in its socket
//! buffer, as the kernel's do, and its timers wait with it; a dead member takes
//! nothing; a cut link drops what crosses it. Every two seconds each member
//! announces itself to its seeds as the daemon's discovery offers them, through
//! [`super::announce_targets`]. A member can also be restarted: a new process,
//! whose identity starts one above the incarnation its data directory recorded
//! (ADR-242), as the daemon's does.
//!
//! Each process also holds a member set, fed the production notification
//! handling, whose immediate facts exchanges (ADR-247) run synchronously inside
//! the ask: the asker's block is recorded at the member asked, if its process
//! runs, and that member's block at the asker, as an exchange's two frames would.
//! No sync tick runs here, so a block held is a block an exchange brought.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use foca::{Config, Foca, Notification, PostcardCodec, Runtime, Timer};
use rand::SeedableRng;
use rand::rngs::StdRng;

use super::incarnation::first_after;
use super::{Member, Members, Swim, apply_notification};
use crate::facts::{Facts, next_decoded_seq};
use kimmy_core::NodeId;

/// How often each member announces itself to its seeds (the daemon's
/// discovery interval in the cluster harness).
const DISCOVERY_MS: u64 = 2_000;

/// The sync interval the processes' member sets debounce their exchanges by.
const SYNC_INTERVAL: Duration = Duration::from_secs(5);

/// A probe period, a probe's round trip and the suspicion time
/// (`Config::new_lan(16)`: 1 s, 0.5 s, 4.8 s), and a probe period's wait for
/// the probe that picks the member.
const DOWN_WITHIN_MS: u64 = 8_000;

/// What one call into foca asked for, and said.
#[derive(Default)]
struct Out {
    sends: Vec<(SocketAddr, Vec<u8>)>,
    timers: Vec<(Timer<Member>, Duration)>,
    rejoined: bool,
    down: Vec<SocketAddr>,
    renamed: Vec<SocketAddr>,
    /// The renewed incarnation the daemon's step recorded.
    recorded: Option<u64>,
    /// The process's member set and the input's instant, which every
    /// notification is applied to as the daemon applies it.
    members: Option<(Members, Instant)>,
    /// What each notification that drops blocks drops (`None`: every peer's,
    /// a rejoin's or a defunct's), with how many exchanges had been started
    /// when it came, in the order they came.
    drops: Vec<(usize, Option<SocketAddr>)>,
    exchanges: Option<Arc<parking_lot::Mutex<Exchanges>>>,
}

impl Runtime<Member> for Out {
    fn notify(&mut self, notification: Notification<'_, Member>) {
        match &notification {
            Notification::Rejoin(_) => self.rejoined = true,
            Notification::MemberDown(member) => self.down.push(member.addr),
            Notification::Rename(_, new) => self.renamed.push(new.addr),
            _ => {}
        }
        let drops = match &notification {
            Notification::Rename(_, member) | Notification::MemberDown(member) => {
                Some(Some(member.addr))
            }
            Notification::Rejoin(_) | Notification::Defunct => Some(None),
            _ => None,
        };
        if let (Some(dropped), Some(exchanges)) = (drops, &self.exchanges) {
            self.drops.push((exchanges.lock().log.len(), dropped));
        }
        if let Some((members, at)) = &self.members {
            apply_notification(members, notification, *at);
        }
    }
    fn send_to(&mut self, to: Member, data: &[u8]) {
        self.sends.push((to.addr, data.to_vec()));
    }
    fn submit_after(&mut self, event: Timer<Member>, after: Duration) {
        self.timers.push((event, after));
    }
}

/// One exchange a process started: by which member, with which, when, and the
/// boot of the process that answered (`None`: none did, dead or frozen).
#[derive(Clone, Debug)]
struct Exchange {
    from: usize,
    to: usize,
    at: u64,
    answered: Option<Vec<u8>>,
}

/// What the exchanges read and leave: each member's running process's set
/// (`None` while it is dead or frozen), the clock, and every exchange started.
#[derive(Default)]
struct Exchanges {
    running: Vec<Option<Members>>,
    now: u64,
    log: Vec<Exchange>,
}

fn node_id(member: usize) -> NodeId {
    NodeId::from_bytes([member as u8 + 1; 16])
}

/// The boot of process `epoch` of `member`.
fn boot(member: usize, epoch: u64) -> Vec<u8> {
    vec![member as u8 + 1, epoch as u8]
}

/// The member set of process `epoch` of `member`, whose exchanges run against
/// `shared` inside the ask: its block recorded at the member asked, and that
/// member's at it, when that member's process runs.
fn process_members(
    shared: &Arc<parking_lot::Mutex<Exchanges>>,
    member: usize,
    epoch: u64,
) -> Members {
    let members = Members::default();
    members.configure_lease(SYNC_INTERVAL, 3);
    let own = boot(member, epoch);
    members.set_facts_source(Arc::new(move || Facts {
        boot: own.clone(),
        started_ms: Some(epoch),
        ..Facts::default()
    }));
    let shared = Arc::clone(shared);
    members.set_exchange_sink_for_test(Arc::new(move |addr, node| {
        let to = usize::from(addr.port() - 7900);
        let (mine, theirs, at) = {
            let shared = shared.lock();
            (shared.running[member].clone(), shared.running[to].clone(), shared.now)
        };
        let answered = match (mine, theirs) {
            (Some(mine), Some(theirs)) => {
                let (block, generation) = mine.local_facts().expect("a source");
                let seq = next_decoded_seq();
                theirs.record_peer_facts(
                    node_id(member),
                    block,
                    Some(generation),
                    seq,
                    Instant::now(),
                );
                let (block, generation) = theirs.local_facts().expect("a source");
                let answered = block.boot.clone();
                let seq = next_decoded_seq();
                mine.record_peer_facts(node, block, Some(generation), seq, Instant::now());
                Some(answered)
            }
            _ => None,
        };
        shared.lock().log.push(Exchange { from: member, to, at, answered });
    }));
    members
}

/// The daemon's step after every input, as built.
fn daemon(swim: &mut Swim, out: &mut Out) {
    let rejoined = out.rejoined;
    let mut recorded = None;
    let _ = super::after_input(swim, rejoined, &mut *out, &mut recorded);
    out.recorded = recorded;
}

/// The simulation's record of a renewal: what the daemon's step recorded.
impl super::KeepIncarnation for Option<u64> {
    fn record(&mut self, incarnation: u64) {
        *self = Some(incarnation);
    }
}

/// foca alone, without that step.
fn foca_alone(_: &mut Swim, _: &mut Out) {}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Life {
    Running,
    Frozen,
    Dead,
}

/// How a member came to rejoin: by a datagram from a sender it held active, from
/// one it held down, or otherwise.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Via {
    ActiveSender,
    DownSender,
    Other,
}

struct Node {
    swim: Swim,
    addr: SocketAddr,
    seeds: Vec<usize>,
    life: Life,
    /// Datagrams that arrived while frozen, in order, with their sender.
    buffered: VecDeque<(usize, Vec<u8>)>,
    /// Timers that came due while frozen.
    due: Vec<Timer<Member>>,
    rejoins: Vec<(Via, u64)>,
    /// Every member this node declared down, and when.
    downs: Vec<(SocketAddr, u64)>,
    /// Every member this node saw renew its identity, and when.
    renames: Vec<(SocketAddr, u64)>,
    /// Which process of this member is running: a timer an earlier one asked
    /// for is not delivered to a later one.
    epoch: u64,
    /// What its data directory's incarnation file says, `None` with no file.
    kept: Option<u64>,
    /// Whether a recorded renewal reaches the file (`false`: a write that
    /// failed, or a crash before it).
    keeps: bool,
    /// The running process's member set.
    members: Members,
}

/// The simulated network: nodes, a clock in milliseconds, and the timers each
/// node has asked for.
struct Net {
    nodes: Vec<Node>,
    now: u64,
    /// `(fire at, sequence, node, timer's slot in `pending`)`.
    timers: BinaryHeap<Reverse<(u64, u64, usize, usize)>>,
    /// Each timer, with the epoch of the process that asked for it.
    pending: Vec<(Timer<Member>, u64)>,
    config: Config,
    seq: u64,
    next_discovery: u64,
    /// Directed links that drop what crosses them.
    cut: BTreeSet<(usize, usize)>,
    after_input: fn(&mut Swim, &mut Out),
    exchanges: Arc<parking_lot::Mutex<Exchanges>>,
    /// The instant the clock's 0 stands for, for the member sets.
    base: Instant,
}

impl Net {
    /// `n` members; member `i` is seeded with `seeds[i]`.
    fn new(seeds: &[&[usize]], after_input: fn(&mut Swim, &mut Out)) -> Net {
        let config = Config::new_lan(std::num::NonZeroU32::new(16).unwrap());
        let exchanges = Arc::new(parking_lot::Mutex::new(Exchanges::default()));
        let nodes: Vec<Node> = seeds
            .iter()
            .enumerate()
            .map(|(i, seeds)| {
                let addr: SocketAddr = format!("127.0.0.1:{}", 7900 + i).parse().unwrap();
                let first = first_after(None);
                let identity = Member::identified(addr, node_id(i), first);
                let rng = StdRng::seed_from_u64(i as u64);
                Node {
                    swim: Foca::new(identity, config.clone(), rng, PostcardCodec),
                    addr,
                    seeds: seeds.to_vec(),
                    life: Life::Running,
                    buffered: VecDeque::new(),
                    due: Vec::new(),
                    rejoins: Vec::new(),
                    downs: Vec::new(),
                    renames: Vec::new(),
                    epoch: 0,
                    kept: Some(first),
                    keeps: true,
                    members: process_members(&exchanges, i, 0),
                }
            })
            .collect();
        exchanges.lock().running = nodes.iter().map(|node| Some(node.members.clone())).collect();
        let mut net = Net {
            nodes,
            now: 0,
            timers: BinaryHeap::new(),
            pending: Vec::new(),
            config,
            seq: 0,
            next_discovery: 0,
            cut: BTreeSet::new(),
            after_input,
            exchanges,
            base: Instant::now(),
        };
        net.run_for(15_000);
        for (i, node) in net.nodes.iter().enumerate() {
            assert_eq!(node.swim.num_members(), net.nodes.len() - 1, "premise: {i} formed");
        }
        net
    }

    fn index(&self, addr: SocketAddr) -> Option<usize> {
        self.nodes.iter().position(|node| node.addr == addr)
    }

    /// Hand `node` one input, then the daemon's step, and route what came out.
    fn input(&mut self, node: usize, via: Via, input: impl FnOnce(&mut Swim, &mut Out)) {
        let since = {
            let mut shared = self.exchanges.lock();
            shared.now = self.now;
            shared.log.len()
        };
        let at = self.base + Duration::from_millis(self.now);
        let mut out = Out {
            members: Some((self.nodes[node].members.clone(), at)),
            exchanges: Some(Arc::clone(&self.exchanges)),
            ..Out::default()
        };
        input(&mut self.nodes[node].swim, &mut out);
        (self.after_input)(&mut self.nodes[node].swim, &mut out);
        self.exchanges_held(node, since, &out.drops);
        if out.rejoined {
            self.nodes[node].rejoins.push((via, self.now));
        }
        let now = self.now;
        self.nodes[node].downs.extend(out.down.iter().map(|addr| (*addr, now)));
        self.nodes[node].renames.extend(out.renamed.iter().map(|addr| (*addr, now)));
        if let Some(renewed) = out.recorded
            && self.nodes[node].keeps
        {
            self.nodes[node].kept = Some(renewed);
        }
        let epoch = self.nodes[node].epoch;
        for (event, after) in out.timers {
            self.pending.push((event, epoch));
            self.seq += 1;
            let at = self.now + after.as_millis() as u64;
            self.timers.push(Reverse((at, self.seq, node, self.pending.len() - 1)));
        }
        for (to, data) in out.sends {
            if let Some(to) = self.index(to) {
                self.deliver(node, to, data);
            }
        }
    }

    /// Every exchange `node` started in the input just handled (log entries from
    /// `since`) that a process answered left `node` holding that process's block:
    /// an exchange is asked after the change it is for, never before a drop that
    /// would remove what it brought. Unless a later notification of the same
    /// input dropped it (`drops`, recorded before each is applied), or `node` no
    /// longer holds the member live.
    fn exchanges_held(&self, node: usize, since: usize, drops: &[(usize, Option<SocketAddr>)]) {
        let shared = self.exchanges.lock();
        let view = self.nodes[node].members.view();
        for (index, started) in shared.log.iter().enumerate().skip(since) {
            if started.from != node {
                continue;
            }
            let Some(answered) = &started.answered else { continue };
            let addr = self.nodes[started.to].addr;
            let dropped_after =
                drops.iter().any(|(count, of)| *count > index && of.is_none_or(|of| of == addr));
            let to = node_id(started.to);
            if dropped_after || !view.live.contains(&to) {
                continue;
            }
            let held = view.blocks.get(&to).map(|held| &held.facts.boot);
            assert_eq!(
                held,
                Some(answered),
                "at {} ms member {node} started an exchange with {} in this input and does not \
                 hold the block it brought",
                self.now,
                started.to
            );
        }
    }

    fn deliver(&mut self, from: usize, to: usize, data: Vec<u8>) {
        if self.cut.contains(&(from, to)) {
            return;
        }
        match self.nodes[to].life {
            Life::Running => self.read(from, to, data),
            Life::Frozen => self.nodes[to].buffered.push_back((from, data)),
            Life::Dead => {}
        }
    }

    fn read(&mut self, from: usize, to: usize, data: Vec<u8>) {
        let from_addr = self.nodes[from].addr;
        let via =
            if self.live_at(to).contains(&from_addr) { Via::ActiveSender } else { Via::DownSender };
        self.input(to, via, |swim, out| {
            let _ = swim.handle_data(&data, out);
        });
    }

    /// Advance the clock by `ms`, firing every timer that comes due and
    /// announcing every member to its seeds each discovery interval.
    fn run_for(&mut self, ms: u64) {
        let until = self.now + ms;
        loop {
            let timer = self.timers.peek().map(|Reverse((at, ..))| *at);
            let next = match timer {
                Some(at) if at <= self.next_discovery => at,
                _ => self.next_discovery,
            };
            if next > until {
                break;
            }
            self.now = next;
            if timer == Some(next) {
                let Reverse((_, _, node, slot)) = self.timers.pop().unwrap();
                let (event, epoch) = self.pending[slot].clone();
                if epoch != self.nodes[node].epoch {
                    continue;
                }
                match self.nodes[node].life {
                    Life::Running => self.input(node, Via::Other, |swim, out| {
                        let _ = swim.handle_timer(event, out);
                    }),
                    Life::Frozen => self.nodes[node].due.push(event),
                    Life::Dead => {}
                }
            } else {
                self.next_discovery += DISCOVERY_MS;
                for node in 0..self.nodes.len() {
                    if self.nodes[node].life != Life::Running {
                        continue;
                    }
                    let mut targets: Vec<SocketAddr> = Vec::new();
                    for seed in self.nodes[node].seeds.clone() {
                        let offered = self.nodes[seed].addr;
                        for addr in super::announce_targets(&self.nodes[node].swim, offered) {
                            if !targets.contains(&addr) {
                                targets.push(addr);
                            }
                        }
                    }
                    for addr in targets {
                        let to = Member::announcing(addr);
                        self.input(node, Via::Other, |swim, out| {
                            let _ = swim.announce(to, out);
                        });
                    }
                }
            }
        }
        self.now = until;
    }

    fn freeze(&mut self, node: usize) {
        self.nodes[node].life = Life::Frozen;
        self.exchanges.lock().running[node] = None;
    }

    fn kill(&mut self, node: usize) {
        self.nodes[node].life = Life::Dead;
        self.exchanges.lock().running[node] = None;
    }

    /// Start a new process of `node`: a fresh SWIM instance that knows nothing,
    /// as a restarted daemon's does, whose identity is one above what the data
    /// directory recorded, recorded in turn before it starts
    /// ([`super::IncarnationFile::begin`]). What the dead one buffered, and its
    /// timers, are gone with it.
    fn restart(&mut self, node: usize) {
        let n = &mut self.nodes[node];
        let first = first_after(n.kept);
        n.kept = Some(first);
        let identity = Member::identified(n.addr, n.swim.identity().node, first);
        n.epoch += 1;
        let rng = StdRng::seed_from_u64(node as u64 + 1000 * n.epoch);
        n.swim = Foca::new(identity, self.config.clone(), rng, PostcardCodec);
        n.life = Life::Running;
        n.buffered.clear();
        n.due.clear();
        n.members = process_members(&self.exchanges, node, n.epoch);
        self.exchanges.lock().running[node] = Some(n.members.clone());
    }

    /// Resume a frozen node: the timers that came due fire, and the datagrams
    /// that waited in its socket are read, in order, as a resumed process does.
    fn resume(&mut self, node: usize) {
        self.nodes[node].life = Life::Running;
        self.exchanges.lock().running[node] = Some(self.nodes[node].members.clone());
        for event in std::mem::take(&mut self.nodes[node].due) {
            self.input(node, Via::Other, |swim, out| {
                let _ = swim.handle_timer(event, out);
            });
        }
        while let Some((from, data)) = self.nodes[node].buffered.pop_front() {
            self.read(from, node, data);
        }
    }

    /// Cut every link between `a` and the members `others`, both ways.
    fn partition(&mut self, a: usize, others: &[usize]) {
        for other in others {
            self.cut.insert((a, *other));
            self.cut.insert((*other, a));
        }
    }

    fn heal(&mut self) {
        self.cut.clear();
    }

    /// The members `node` holds as live.
    fn live_at(&self, node: usize) -> BTreeSet<SocketAddr> {
        self.nodes[node].swim.iter_members().map(|member| member.id().addr).collect()
    }

    fn addrs(&self, nodes: &[usize]) -> BTreeSet<SocketAddr> {
        nodes.iter().map(|node| self.nodes[*node].addr).collect()
    }

    /// When `node` declared `peer` down, after `since`.
    fn downed(&self, node: usize, peer: usize, since: u64) -> Option<u64> {
        let addr = self.nodes[peer].addr;
        self.nodes[node]
            .downs
            .iter()
            .find(|(at, when)| *at == addr && *when >= since)
            .map(|(_, t)| *t)
    }
}

// The field schedule's members, and their seeds as the cluster harness gives
// them: Y and Z seed each other, X and M seed Y.
const Y: usize = 0;
const Z: usize = 1;
const X: usize = 2;
const M: usize = 3;
const FIELD_SEEDS: &[&[usize]] = &[&[Z], &[Y], &[Y], &[Y]];

/// X and M frozen long enough to be declared down; then Y and Z frozen, and X
/// and M resumed, two seconds apart. Returns the net and when each resumed.
fn freeze_and_rejoin(after_input: fn(&mut Swim, &mut Out)) -> (Net, u64, u64) {
    let mut net = Net::new(FIELD_SEEDS, after_input);
    net.freeze(X);
    net.freeze(M);
    net.run_for(15_000);
    assert!(!net.live_at(Y).contains(&net.nodes[X].addr), "premise: Y declared X down");
    net.freeze(Y);
    net.freeze(Z);
    let x_back = net.now;
    net.resume(X);
    net.run_for(2_000);
    let m_back = net.now;
    net.resume(M);
    net.run_for(30_000);
    (net, x_back, m_back)
}

/// The same with Y and Z stopped for good (a clean `TERM`) while X and M are
/// frozen.
fn rejoin_after_peers_died(after_input: fn(&mut Swim, &mut Out)) -> (Net, u64) {
    let mut net = Net::new(FIELD_SEEDS, after_input);
    net.freeze(X);
    net.freeze(M);
    net.run_for(15_000);
    net.kill(Y);
    net.kill(Z);
    let back = net.now;
    net.resume(X);
    net.resume(M);
    net.run_for(30_000);
    (net, back)
}

/// Each of `rejoined` rejoined, declared every one of `gone` down within
/// [`DOWN_WITHIN_MS`] of `since`, and holds every one of `live` live, and they
/// it.
fn recovered(net: &Net, rejoined: &[usize], gone: &[usize], live: &[usize], since: u64) {
    for &member in rejoined {
        assert!(!net.nodes[member].rejoins.is_empty(), "premise: {member} rejoined");
        for &peer in gone {
            let at = net.downed(member, peer, since);
            assert!(
                at.is_some_and(|at| at - since <= DOWN_WITHIN_MS),
                "member {member} declared {peer} down at {at:?}, {since} + {DOWN_WITHIN_MS} ms \
                 expected; it holds {:?} live",
                net.live_at(member)
            );
        }
        let expected: BTreeSet<_> =
            net.addrs(live).into_iter().filter(|addr| *addr != net.nodes[member].addr).collect();
        assert_eq!(net.live_at(member), expected, "member {member}'s view");
        for &peer in live {
            if peer != member {
                assert!(
                    net.live_at(peer).contains(&net.nodes[member].addr),
                    "{peer} sees {member} live again"
                );
            }
        }
    }
}

/// The field schedule: rejoined by a `TurnUndead` from a peer each held
/// active, X and M declare the frozen Y and Z down in time and see each other
/// live.
#[test]
fn a_member_rejoined_after_a_freeze_declares_its_frozen_peers_down_and_is_seen_live() {
    let (net, x_back, m_back) = freeze_and_rejoin(daemon);
    for member in [X, M] {
        assert!(
            net.nodes[member].rejoins.iter().any(|(via, _)| *via == Via::ActiveSender),
            "premise: {member} rejoined by a datagram from a peer it held active: {:?}",
            net.nodes[member].rejoins
        );
    }
    recovered(&net, &[X], &[Y, Z], &[X, M], x_back);
    recovered(&net, &[M], &[Y, Z], &[X, M], m_back);
}

/// The same with Y and Z dead.
#[test]
fn a_member_rejoined_after_its_peers_died_declares_them_down() {
    let (net, back) = rejoin_after_peers_died(daemon);
    recovered(&net, &[X, M], &[Y, Z], &[X, M], back);
}

/// The other `TurnUndead` path: a member that rejoins on hearing it was
/// declared down from a peer it holds down itself, while it still holds other
/// members active. Every member was seeded with D, which is gone, so no
/// announce to a seed reaches anyone. A is cut off from B and C until each side
/// has declared the other down; when the cut heals, A (alone) announces to the
/// members it knows, and B and C, holding A down, answer with a `TurnUndead`
/// that A, holding them down, answers in kind: B or C rejoins on the word of a
/// member it holds down, with the other still active. Every member must be live
/// to every other within the time a probe and an announce take.
#[test]
fn a_member_that_rejoins_on_a_down_peers_word_resumes() {
    let (a, b, c, d) = (0, 1, 2, 3);
    let mut net = Net::new(&[&[d], &[d], &[d], &[a]], daemon);
    net.kill(d);
    net.partition(a, &[b, c]);
    net.run_for(20_000);
    assert!(net.live_at(a).is_empty(), "premise: A holds B and C down");
    assert_eq!(net.live_at(b), net.addrs(&[c]), "premise: B holds A down, C live");
    net.heal();
    let healed = net.now;
    net.run_for(20_000);
    assert!(
        [b, c].iter().any(|node| {
            net.nodes[*node]
                .rejoins
                .iter()
                .any(|(via, at)| *via == Via::DownSender && *at >= healed)
        }),
        "premise: B or C rejoined on the word of a member it held down: {:?} {:?}",
        net.nodes[b].rejoins,
        net.nodes[c].rejoins
    );
    for node in [a, b, c] {
        let others: BTreeSet<_> = net
            .addrs(&[a, b, c])
            .into_iter()
            .filter(|addr| *addr != net.nodes[node].addr)
            .collect();
        assert_eq!(net.live_at(node), others, "member {node}, 20 s after the heal at {healed}");
    }
}

/// A rejoin with no live peer in view: A, declared down while frozen, resumes
/// with B and C frozen. It declares both down and is alone; when they return it
/// is found again, by the announces to its seed, and every member is live to
/// every other.
#[test]
fn a_member_that_rejoins_with_no_live_peer_reconnects_when_they_return() {
    let (a, b, c) = (0, 1, 2);
    let mut net = Net::new(&[&[b], &[a], &[a]], daemon);
    net.freeze(a);
    net.run_for(15_000);
    net.freeze(b);
    net.freeze(c);
    let back = net.now;
    net.resume(a);
    net.run_for(15_000);
    recovered(&net, &[a], &[b, c], &[a], back);
    assert!(net.live_at(a).is_empty(), "alone");
    net.resume(b);
    net.resume(c);
    net.run_for(20_000);
    for node in 0..3 {
        let others: BTreeSet<_> = net
            .addrs(&[a, b, c])
            .into_iter()
            .filter(|addr| *addr != net.nodes[node].addr)
            .collect();
        assert_eq!(net.live_at(node), others, "member {node}, 20 s after B and C returned");
    }
}

/// foca 2.0.0 on its own leaves a member that rejoined on a `TurnUndead`
/// silent: in the field schedule X and M still hold the frozen Y and Z live 30 s
/// later. What [`super::after_input`] is for; when a foca release fails this
/// test, the call can go.
#[test]
fn foca_alone_leaves_a_member_that_rejoined_silent() {
    let (net, ..) = freeze_and_rejoin(foca_alone);
    for member in [X, M] {
        assert!(!net.nodes[member].rejoins.is_empty(), "premise: {member} rejoined");
        assert!(
            net.live_at(member).is_superset(&net.addrs(&[Y, Z])),
            "{member} holds Y and Z live: {:?}",
            net.live_at(member)
        );
    }
}

// Restarts (ADR-242): A, B and C, each seeded with the other two, and B
// restarted.
const A: usize = 0;
const B: usize = 1;
const C: usize = 2;

fn trio() -> Net {
    Net::new(&[&[B, C], &[A, C], &[A, B]], daemon)
}

/// Kill `node`, leave it dead for `down_ms`, start it again, and give it 12 s
/// (a probe, its round trip, the suspicion time and two discovery intervals):
/// answers its rejoins since the start and the renames of it its peers saw.
/// Every member sees every other live by then.
fn restart_once(net: &mut Net, node: usize, down_ms: u64) -> (usize, usize) {
    net.kill(node);
    net.run_for(down_ms);
    let since = net.now;
    net.restart(node);
    net.run_for(12_000);
    let rejoins = net.nodes[node].rejoins.iter().filter(|(_, at)| *at >= since).count();
    let addr = net.nodes[node].addr;
    let renames = (0..net.nodes.len())
        .filter(|peer| *peer != node)
        .map(|peer| {
            net.nodes[peer].renames.iter().filter(|(of, at)| *of == addr && *at >= since).count()
        })
        .sum();
    for member in 0..net.nodes.len() {
        assert_eq!(
            net.live_at(member).len(),
            net.nodes.len() - 1,
            "member {member} sees every other live 12 s after {node}'s start"
        );
    }
    (rejoins, renames)
}

fn incarnation(net: &Net, node: usize) -> u64 {
    net.nodes[node].swim.identity().incarnation
}

/// Freeze `node` until its peers declare it down, then resume it: it renews in
/// its process. Answers the incarnation it renewed to.
fn renew_in_process(net: &mut Net, node: usize) -> u64 {
    let before = incarnation(net, node);
    net.freeze(node);
    net.run_for(15_000);
    assert!(
        (0..net.nodes.len())
            .all(|peer| peer == node || !net.live_at(peer).contains(&net.nodes[node].addr)),
        "premise: its peers declared {node} down"
    );
    net.resume(node);
    net.run_for(15_000);
    let renewed = incarnation(net, node);
    assert_eq!(renewed, before + 1, "premise: {node} renewed once in its process");
    renewed
}

/// Every restart is taken by the peers at once, however long the member was
/// dead: no rejoin, one rename on each peer, one incarnation more. Dead for no
/// time its peers keep it live; for 1.5 s and 3 s they suspect the identity
/// they hold (which, before ADR-242, the restarted member walked back to and
/// could not refute, and was declared down 4.8 s into its run); for 10 s they
/// declare it down. Before ADR-242 every start was at incarnation 0 and the
/// rejoins of those three were 1, 2, 3, … up to 20, one more per restart.
#[test]
fn a_restarted_member_is_taken_at_once_however_long_it_was_down() {
    for down in [0, 1_500, 3_000, 10_000] {
        let mut net = trio();
        for restart in 1..=25 {
            let before = incarnation(&net, B);
            let (rejoins, renames) = restart_once(&mut net, B, down);
            assert_eq!(rejoins, 0, "dead {down} ms, restart {restart}: B rejoined {rejoins} times");
            assert_eq!(renames, 2, "dead {down} ms, restart {restart}: each peer renamed B once");
            assert_eq!(incarnation(&net, B), before + 1, "dead {down} ms, restart {restart}");
            assert_eq!(net.nodes[B].kept, Some(before + 1), "recorded before it was sent");
        }
    }
}

/// A renewal in a process is recorded before anything carries it, so the next
/// start is above it as well.
#[test]
fn a_renewal_in_a_process_is_recorded_for_the_next_start() {
    let mut net = trio();
    let renewed = renew_in_process(&mut net, B);
    assert_eq!(net.nodes[B].kept, Some(renewed), "the renewal is recorded");
    let (rejoins, renames) = restart_once(&mut net, B, 10_000);
    assert_eq!((rejoins, renames), (0, 2), "the next start is taken at once");
    assert_eq!(incarnation(&net, B), renewed + 1);
}

/// A renewal whose record was lost (its write failed, or the process died
/// before it) costs the next start one renewal, no more, and the start after
/// that none.
#[test]
fn a_renewal_whose_record_was_lost_costs_the_next_start_one_step() {
    let mut net = trio();
    net.nodes[B].keeps = false;
    let renewed = renew_in_process(&mut net, B);
    assert_eq!(net.nodes[B].kept, Some(renewed - 1), "premise: the renewal was not recorded");
    net.nodes[B].keeps = true;
    let (rejoins, _) = restart_once(&mut net, B, 10_000);
    assert_eq!(rejoins, 1, "one step up past the identity its peers hold");
    let (rejoins, renames) = restart_once(&mut net, B, 10_000);
    assert_eq!((rejoins, renames), (0, 2), "and the next start none");
}

/// A start with nothing recorded (the first start of this build on a data
/// directory an older one ran, a wiped one, a restore into a new one) starts at
/// 0, as every start did before ADR-242, and walks once past what its peers
/// hold. Every renewal of the walk is recorded, so the next start is one step.
#[test]
fn a_start_with_nothing_recorded_walks_once_and_the_next_is_taken_at_once() {
    let mut net = trio();
    for restart in 1..=20 {
        net.nodes[B].kept = None;
        let (rejoins, _) = restart_once(&mut net, B, 10_000);
        assert_eq!(rejoins, restart, "with nothing recorded, restart {restart} walks");
    }
    assert_eq!(net.nodes[B].kept, Some(20), "the walk is recorded");
    let (rejoins, renames) = restart_once(&mut net, B, 10_000);
    assert_eq!((rejoins, renames), (0, 2));
    assert_eq!(incarnation(&net, B), 21);
}

// Immediate facts exchanges (ADR-247).

/// How many exchanges have been started: a mark for [`started_since`].
fn mark(net: &Net) -> usize {
    net.exchanges.lock().log.len()
}

/// The exchanges started since `mark`, as `(from, to)` pairs, sorted.
fn started_since(net: &Net, mark: usize) -> Vec<(usize, usize)> {
    let mut pairs: Vec<(usize, usize)> =
        net.exchanges.lock().log[mark..].iter().map(|started| (started.from, started.to)).collect();
    pairs.sort_unstable();
    pairs
}

/// The boot of the block `holder` holds of `of`.
fn held_boot(net: &Net, holder: usize, of: usize) -> Option<Vec<u8>> {
    let view = net.nodes[holder].members.view();
    view.blocks.get(&node_id(of)).map(|held| held.facts.boot.clone())
}

/// Run the clock in 50 ms steps until `peer` has seen `of` renamed once more
/// than it had (within 10 s), and answer when.
fn run_until_renamed(net: &mut Net, peer: usize, of: usize) -> u64 {
    let addr = net.nodes[of].addr;
    let renamed = |net: &Net| -> Vec<u64> {
        net.nodes[peer]
            .renames
            .iter()
            .filter(|(at, _)| *at == addr)
            .map(|(_, when)| *when)
            .collect()
    };
    let before = renamed(net).len();
    let until = net.now + 10_000;
    loop {
        if let Some(when) = renamed(net).get(before) {
            return *when;
        }
        assert!(net.now < until, "{peer} never saw {of} renamed again");
        net.run_for(50);
    }
}

/// Every restart, dead for 0, 1.5, 3 or 10 s: each peer, which sees the member
/// renamed (and up as well, when it had declared it down), starts exactly one
/// exchange with it, and the restarted member, which sees each peer up, exactly
/// one with each. Each is answered and leaves its starter holding the other's
/// block at the end of the input that started it (`Net::exchanges_held`), and
/// with no sync tick in the simulation every member holds every other's current
/// block.
#[test]
fn every_restart_starts_one_exchange_each_way_and_every_block_is_held() {
    for down in [0, 1_500, 3_000, 10_000] {
        let mut net = trio();
        for restart in 1..=10 {
            let since = mark(&net);
            restart_once(&mut net, B, down);
            assert_eq!(
                started_since(&net, since),
                vec![(A, B), (B, A), (B, C), (C, B)],
                "dead {down} ms, restart {restart}"
            );
            let log = net.exchanges.lock().log[since..].to_vec();
            assert!(
                log.iter().all(|started| started.answered.is_some()),
                "dead {down} ms, restart {restart}: every exchange answered: {log:?}"
            );
            for holder in [A, B, C] {
                for of in [A, B, C] {
                    if holder != of {
                        assert_eq!(
                            held_boot(&net, holder, of),
                            Some(boot(of, net.nodes[of].epoch)),
                            "dead {down} ms, restart {restart}: {holder} holds {of}'s block"
                        );
                    }
                }
            }
        }
    }
}

/// A rename that lands within a sync interval of the last exchange with the
/// member starts none, and one that lands after it starts one (ADR-247).
#[test]
fn a_rename_within_the_interval_of_the_last_exchange_starts_none_and_one_after_it_does() {
    let mut net = trio();
    let first = mark(&net);
    net.kill(B);
    net.restart(B);
    let renamed = run_until_renamed(&mut net, A, B);
    assert_eq!(started_since(&net, first).iter().filter(|p| **p == (A, B)).count(), 1);

    let second = mark(&net);
    net.kill(B);
    net.restart(B);
    let again = run_until_renamed(&mut net, A, B);
    assert!(again - renamed < 5_000, "premise: within the interval ({renamed}, {again})");
    net.run_for((renamed + 4_900).saturating_sub(net.now));
    assert_eq!(
        started_since(&net, second).iter().filter(|p| **p == (A, B)).count(),
        0,
        "A started none for the rename at {again}, {} ms after the last",
        again - renamed
    );

    net.run_for(12_000);
    let third = mark(&net);
    net.kill(B);
    net.restart(B);
    let later = run_until_renamed(&mut net, A, B);
    assert!(later - renamed >= 5_000, "premise");
    net.run_for(1_000);
    assert_eq!(started_since(&net, third).iter().filter(|p| **p == (A, B)).count(), 1);
}

/// A member that rejoins (frozen until its peers declared it down, then resumed)
/// dropped every block it held, and starts an exchange with every peer it holds
/// live in the input that rejoined it; each peer, seeing it renamed, starts one
/// with it (ADR-247). Each then holds the other's block.
#[test]
fn a_member_that_rejoins_exchanges_with_every_peer_it_holds_live() {
    let mut net = trio();
    net.freeze(B);
    net.run_for(15_000);
    assert!(
        [A, C].iter().all(|peer| !net.live_at(*peer).contains(&net.nodes[B].addr)),
        "premise: its peers declared B down"
    );
    let (back, since) = (net.now, mark(&net));
    net.resume(B);
    net.run_for(15_000);
    let &(_, rejoined) = net.nodes[B].rejoins.last().expect("premise: B rejoined");
    assert!(rejoined >= back);
    let at_rejoin: BTreeSet<usize> = net
        .exchanges
        .lock()
        .log
        .iter()
        .filter(|started| started.from == B && started.at == rejoined)
        .map(|started| started.to)
        .collect();
    assert_eq!(at_rejoin, BTreeSet::from([A, C]), "B, in the input that rejoined it");
    let since = started_since(&net, since);
    for peer in [A, C] {
        assert_eq!(since.iter().filter(|p| **p == (peer, B)).count(), 1, "{peer} on the rename");
        assert_eq!(held_boot(&net, B, peer), Some(boot(peer, 0)));
        assert_eq!(held_boot(&net, peer, B), Some(boot(B, 0)));
    }
}
