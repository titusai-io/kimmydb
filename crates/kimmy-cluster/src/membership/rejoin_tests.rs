//! A member the cluster declared down, and that rejoins, must go on failure
//! detecting: deterministic, in-memory runs of SWIM (foca, with this crate's
//! `Member` and its [`super::after_input`]) through the schedules that left a
//! member silent in the field. Datagrams to a frozen member wait in its socket
//! buffer, as the kernel's do, and its timers wait with it; a dead member takes
//! nothing; a cut link drops what crosses it. Every two seconds each member
//! announces itself to its seeds as the daemon's discovery offers them, through
//! [`super::announce_targets`].

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;

use foca::{Config, Foca, Notification, PostcardCodec, Runtime, Timer};
use rand::SeedableRng;
use rand::rngs::StdRng;

use super::{Member, Swim};
use kimmy_core::NodeId;

/// How often each member announces itself to its seeds (the daemon's
/// discovery interval in the cluster harness).
const DISCOVERY_MS: u64 = 2_000;

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
}

impl Runtime<Member> for Out {
    fn notify(&mut self, notification: Notification<'_, Member>) {
        match notification {
            Notification::Rejoin(_) => self.rejoined = true,
            Notification::MemberDown(member) => self.down.push(member.addr),
            _ => {}
        }
    }
    fn send_to(&mut self, to: Member, data: &[u8]) {
        self.sends.push((to.addr, data.to_vec()));
    }
    fn submit_after(&mut self, event: Timer<Member>, after: Duration) {
        self.timers.push((event, after));
    }
}

/// The daemon's step after every input, as built.
fn daemon(swim: &mut Swim, out: &mut Out) {
    let rejoined = out.rejoined;
    super::after_input(swim, rejoined, &mut *out);
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
}

/// The simulated network: nodes, a clock in milliseconds, and the timers each
/// node has asked for.
struct Net {
    nodes: Vec<Node>,
    now: u64,
    /// `(fire at, sequence, node, timer's slot in `pending`)`.
    timers: BinaryHeap<Reverse<(u64, u64, usize, usize)>>,
    pending: Vec<Timer<Member>>,
    seq: u64,
    next_discovery: u64,
    /// Directed links that drop what crosses them.
    cut: BTreeSet<(usize, usize)>,
    after_input: fn(&mut Swim, &mut Out),
}

impl Net {
    /// `n` members; member `i` is seeded with `seeds[i]`.
    fn new(seeds: &[&[usize]], after_input: fn(&mut Swim, &mut Out)) -> Net {
        let config = Config::new_lan(std::num::NonZeroU32::new(16).unwrap());
        let nodes = seeds
            .iter()
            .enumerate()
            .map(|(i, seeds)| {
                let addr: SocketAddr = format!("127.0.0.1:{}", 7900 + i).parse().unwrap();
                let identity = Member::identified(addr, NodeId::from_bytes([i as u8 + 1; 16]));
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
                }
            })
            .collect();
        let mut net = Net {
            nodes,
            now: 0,
            timers: BinaryHeap::new(),
            pending: Vec::new(),
            seq: 0,
            next_discovery: 0,
            cut: BTreeSet::new(),
            after_input,
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
        let mut out = Out::default();
        input(&mut self.nodes[node].swim, &mut out);
        (self.after_input)(&mut self.nodes[node].swim, &mut out);
        if out.rejoined {
            self.nodes[node].rejoins.push((via, self.now));
        }
        let now = self.now;
        self.nodes[node].downs.extend(out.down.iter().map(|addr| (*addr, now)));
        for (event, after) in out.timers {
            self.pending.push(event);
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
                let event = self.pending[slot].clone();
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
    }

    fn kill(&mut self, node: usize) {
        self.nodes[node].life = Life::Dead;
    }

    /// Resume a frozen node: the timers that came due fire, and the datagrams
    /// that waited in its socket are read, in order, as a resumed process does.
    fn resume(&mut self, node: usize) {
        self.nodes[node].life = Life::Running;
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
/// declared down from a peer it holds down itself. A is cut off from B and C
/// until each side has declared the other down; when the cut heals, the
/// announces cross, and a member holding the sender down answers it with a
/// `TurnUndead`, which its receiver also holds down. Every member must be live
/// to every other within the time a probe and a discovery interval take.
#[test]
fn a_member_that_rejoins_on_a_down_peers_word_resumes() {
    let (a, b, c) = (0, 1, 2);
    let mut net = Net::new(&[&[b], &[a], &[a]], daemon);
    net.partition(a, &[b, c]);
    net.run_for(20_000);
    assert!(net.live_at(a).is_empty() && !net.live_at(b).contains(&net.nodes[a].addr), "premise");
    net.heal();
    let healed = net.now;
    net.run_for(20_000);
    let via_down: Vec<usize> = (0..3)
        .filter(|node| net.nodes[*node].rejoins.iter().any(|(via, _)| *via == Via::DownSender))
        .collect();
    assert!(!via_down.is_empty(), "premise: a member rejoined on a down sender's word");
    for node in 0..3 {
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
