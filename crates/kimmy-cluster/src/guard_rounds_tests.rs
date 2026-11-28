//! The restore's rounds of asking and their verdict (D13 containment, ADR-274), against a member
//! made up for the test, which can be wrong in one way at a time: undecided, young, founded and
//! unmet, slow, silent, or listing an address. A daemon cannot be made to be one of these alone.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use kimmy_core::{Epoch, NodeId};
use kimmy_storage::Engine;
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::guard_rounds::{Asking, AtPass, Decision, Seeds, ask_until_decided};
use crate::membership::Members;
use crate::protocol::{GuardAnswer, Message, Standing, nonce, prove, read_frame, write_frame};
use crate::transport::probe_answer;

const SECRET: &str = "a-guard-rounds-secret";
/// The rounds' own bounds, shorter than the daemon's. Where a row needs an answer to come inside
/// one of them, it is sent at half of it or at two thirds, which leaves at least half a second for
/// a connection on a loaded machine.
const WINDOW: Duration = Duration::from_millis(3_000);
const UNNAMED_WITHIN: Duration = Duration::from_millis(1_000);
const BETWEEN: Duration = Duration::from_millis(100);
/// Long enough for several rounds, whatever a round holds.
const ROUNDS: Duration = Duration::from_millis(2_500);

fn id(n: u8) -> NodeId {
    NodeId::from_bytes([n; 16])
}

/// The node every restore of these tests is of.
const ORIGINAL: u8 = 100;

/// What a made-up member says when it is asked.
#[derive(Clone)]
struct Says {
    node: NodeId,
    answer: GuardAnswer,
    after: Duration,
}

/// An open member of node `n` whose answer counts: it has met a peer, and has been a member for
/// a minute. It lists nothing and knows nobody.
fn member(n: u8) -> Says {
    Says {
        node: id(n),
        answer: GuardAnswer {
            token: vec![n; 16],
            standing: Standing::Open,
            membership_age_ms: 60_000,
            founded_unmet: false,
            listings: Vec::new(),
            members: Vec::new(),
            address: None,
        },
        after: Duration::ZERO,
    }
}

impl Says {
    fn undecided(mut self) -> Self {
        self.answer.standing = Standing::Undecided;
        self
    }

    /// A member for less than the window.
    fn young(mut self) -> Self {
        self.answer.membership_age_ms = 10;
        self
    }

    fn founded_unmet(mut self) -> Self {
        self.answer.founded_unmet = true;
        self
    }

    /// It lists the asker's node at `addr`.
    fn listing(mut self, addr: SocketAddr) -> Self {
        self.answer.listings.push(addr.to_string());
        self
    }

    /// Its member list has `node` at `addr`.
    fn knows(mut self, addr: SocketAddr, node: NodeId) -> Self {
        self.answer.members.push((addr.to_string(), node));
        self
    }

    fn after(mut self, delay: Duration) -> Self {
        self.after = delay;
        self
    }

    fn token(mut self, token: Vec<u8>) -> Self {
        self.answer.token = token;
        self
    }
}

/// A made-up member listening: it answers what it was last told to say, and records every `Hello`.
struct Fake {
    addr: SocketAddr,
    says: Arc<Mutex<Option<Says>>>,
    /// Whether each `Hello` was a probe.
    hellos: Arc<Mutex<Vec<bool>>>,
    listening: JoinHandle<()>,
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.listening.abort();
    }
}

impl Fake {
    /// Takes connections and never answers.
    async fn silent() -> Fake {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let says = Arc::new(Mutex::new(None::<Says>));
        let hellos = Arc::new(Mutex::new(Vec::new()));
        let (said, heard) = (Arc::clone(&says), Arc::clone(&hellos));
        let tls = Arc::new(crate::tls::ClusterTls::new().unwrap());
        let listening = tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else { return };
                let (said, heard) = (Arc::clone(&said), Arc::clone(&heard));
                let acceptor = tls.acceptor();
                tokio::spawn(async move {
                    let Ok(mut stream) = acceptor.accept(tcp).await else { return };
                    let binding = {
                        let (_, conn) = stream.get_ref();
                        crate::tls::binding(conn).unwrap()
                    };
                    let Ok(Message::Hello { nonce: asked, probe, .. }) =
                        read_frame(&mut stream).await
                    else {
                        return;
                    };
                    heard.lock().push(probe);
                    let says = said.lock().clone();
                    let Some(says) = says else {
                        return std::future::pending().await;
                    };
                    tokio::time::sleep(says.after).await;
                    let welcome = Message::Welcome {
                        node: says.node,
                        nonce: nonce(says.node),
                        proof: prove(SECRET, &asked, &binding),
                        protocol: Some(crate::protocol::PROTOCOL),
                        epoch: Some(Epoch::from_u64(1)),
                        guard: Some(Box::new(says.answer)),
                    };
                    let _ = write_frame(&mut stream, &welcome).await;
                    // Held until the asker ends the connection.
                    let _ = read_frame(&mut stream).await;
                });
            }
        });
        Fake { addr, says, hellos, listening }
    }

    async fn saying(says: Says) -> Fake {
        let fake = Fake::silent().await;
        fake.say(says);
        fake
    }

    fn say(&self, says: Says) {
        *self.says.lock() = Some(says);
    }

    fn probes(&self) -> usize {
        self.hellos.lock().iter().filter(|probe| **probe).count()
    }

    fn other_hellos(&self) -> usize {
        self.hellos.lock().iter().filter(|probe| !**probe).count()
    }
}

/// An address nothing listens on: a stopped host that is still listed.
async fn stopped() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// A restore of node [`ORIGINAL`] asking, with seeds the test can change.
struct Restore {
    members: Members,
    seeds: Arc<Mutex<Vec<SocketAddr>>>,
    /// How often the seeds have been resolved.
    resolved: Arc<AtomicUsize>,
    running: JoinHandle<Decision>,
}

impl Drop for Restore {
    fn drop(&mut self) {
        self.running.abort();
    }
}

/// Start asking, the seeds resolving to nothing for the first `empty` rounds.
fn asking_with(
    members: Members,
    own_node: NodeId,
    seeds: &[SocketAddr],
    replace: bool,
    empty: usize,
) -> Restore {
    asking_resolving(members, own_node, seeds, replace, empty, true)
}

/// [`asking_with`], every source of the seeds resolving or not.
fn asking_resolving(
    members: Members,
    own_node: NodeId,
    seeds: &[SocketAddr],
    replace: bool,
    empty: usize,
    every_source_resolved: bool,
) -> Restore {
    let asking = Asking {
        unnamed_within: UNNAMED_WITHIN,
        between_rounds: BETWEEN,
        ..Asking::new(own_node, SECRET, WINDOW, replace)
    };
    let seeds = Arc::new(Mutex::new(seeds.to_vec()));
    let resolved = Arc::new(AtomicUsize::new(0));
    let (asked, counted, set) = (Arc::clone(&seeds), Arc::clone(&resolved), members.clone());
    let running = tokio::spawn(async move {
        ask_until_decided(&asking, &set, move || {
            let nth = counted.fetch_add(1, Ordering::SeqCst);
            let addresses = if nth < empty { Vec::new() } else { asked.lock().clone() };
            std::future::ready(Seeds { addresses, every_source_resolved })
        })
        .await
    });
    Restore { members, seeds, resolved, running }
}

fn asking(seeds: &[SocketAddr]) -> Restore {
    asking_with(Members::default(), id(ORIGINAL), seeds, false, 0)
}

impl Restore {
    /// It comes to nothing for `rounds`: no passage of time decides.
    async fn undecided_for(&mut self, rounds: Duration, why: &str) {
        if let Ok(decided) = tokio::time::timeout(rounds, &mut self.running).await {
            panic!("decided {decided:?} while {why}");
        }
    }

    async fn decides(&mut self) -> Decision {
        tokio::time::timeout(Duration::from_secs(10), &mut self.running)
            .await
            .expect("a decision")
            .expect("the asking runs")
    }

    async fn passes(&mut self) {
        let decided = self.decides().await;
        assert!(matches!(decided, Decision::Pass { .. }), "a pass: {decided:?}");
    }

    /// A report a dialler hands this restore.
    fn report(&self, says: Says) {
        self.members.note_guard_report(says.node, says.answer);
    }
}

/// Row 3: a young member that lists nothing decides nothing, however often it is asked; once it
/// has been a member long enough and names the original, the original's own answer halts.
#[tokio::test]
async fn a_young_members_answer_does_not_count() {
    let original = Fake::saying(member(ORIGINAL)).await;
    let beside = Fake::saying(member(1).young()).await;
    let mut restore = asking(&[beside.addr]);
    restore.undecided_for(ROUNDS, "the only member is young").await;
    beside.say(member(1).listing(original.addr));
    assert_eq!(restore.decides().await, Decision::LiveHost(original.addr));
}

/// Row 4: an address that refuses, one that takes a connection and says nothing, one slower than
/// a round waits, and a member whose answer does not count: nothing is decided, round after round,
/// and the uncounted member is only ever asked. A member whose answer counts then decides, either
/// way.
#[tokio::test]
async fn nothing_but_a_counting_answer_decides_and_no_time_does() {
    let refused = stopped().await;
    let black_hole = Fake::silent().await;
    let slow = Fake::saying(member(2).after(UNNAMED_WITHIN * 3)).await;
    let uncounted = Fake::saying(member(3).founded_unmet()).await;
    let mut restore = asking(&[refused, black_hole.addr, slow.addr, uncounted.addr]);
    restore.undecided_for(ROUNDS * 2, "nobody who counts has answered").await;
    assert!(uncounted.probes() >= 3, "asked again each round: {}", uncounted.probes());
    assert_eq!(uncounted.other_hellos(), 0, "and told nothing but the question");
    let counted = Fake::saying(member(4)).await;
    restore.seeds.lock().push(counted.addr);
    restore.passes().await;

    let original = Fake::saying(member(ORIGINAL)).await;
    let mut other = asking(&[uncounted.addr]);
    other.undecided_for(ROUNDS, "the only answer does not count").await;
    let naming = Fake::saying(member(5).listing(original.addr)).await;
    other.seeds.lock().push(naming.addr);
    assert_eq!(other.decides().await, Decision::LiveHost(original.addr));
}

/// Row 5: no pass while a member lists an address of the asking node that is silent; a pass once
/// it no longer does. A restore in place, whose listed address answers with its own token, passes
/// while it is still listed.
#[tokio::test]
async fn a_listed_silent_address_holds_the_pass_until_it_is_no_longer_listed() {
    let old = stopped().await;
    let beside = Fake::saying(member(1).listing(old)).await;
    let mut restore = asking(&[beside.addr]);
    restore.undecided_for(ROUNDS, "the old address is listed").await;
    beside.say(member(1));
    restore.passes().await;

    let members = Members::default();
    let own = Fake::saying(member(ORIGINAL).token(members.guard_token())).await;
    let beside = Fake::saying(member(1).listing(own.addr)).await;
    let mut in_place = asking_with(members, id(ORIGINAL), &[beside.addr], false, 0);
    in_place.passes().await;
    assert!(own.probes() >= 1, "premise: its own address was asked");
}

/// Row 6: an address still listed for the asking node, which another member now answers at: no
/// halt, and the restore passes.
#[tokio::test]
async fn a_listed_address_that_another_node_answers_at_does_not_hold_the_pass() {
    let reused = Fake::saying(member(7)).await;
    let beside = Fake::saying(member(1).listing(reused.addr)).await;
    let mut restore = asking(&[beside.addr]);
    restore.passes().await;
    assert!(reused.probes() >= 1, "premise: the listed address was asked");
}

/// Row 13: the original runs where the members reach it and the restore cannot. While any member
/// lists it there is no pass, whatever the others say.
#[tokio::test]
async fn no_pass_while_any_member_lists_an_address_the_restore_cannot_reach() {
    let unreachable = stopped().await;
    let one = Fake::saying(member(1)).await;
    let other = Fake::saying(member(2).listing(unreachable)).await;
    let mut restore = asking(&[one.addr, other.addr]);
    restore.undecided_for(ROUNDS, "a member lists it").await;
    other.say(member(2));
    restore.passes().await;
}

/// Row 16: the asking process knows its own answers by their token and by no address. A listed
/// address that answers with its token is itself; one that answers as its node under another
/// token halts it, wherever it is.
#[tokio::test]
async fn its_own_answer_is_known_by_its_token_and_by_no_address() {
    let members = Members::default();
    let own = Fake::saying(member(ORIGINAL).token(members.guard_token())).await;
    let beside = Fake::saying(member(1).listing(own.addr)).await;
    asking_with(members, id(ORIGINAL), &[beside.addr, own.addr], false, 0).passes().await;

    let another = Fake::saying(member(ORIGINAL)).await;
    let beside = Fake::saying(member(1).listing(another.addr)).await;
    let decided = asking(&[beside.addr]).decides().await;
    assert_eq!(decided, Decision::LiveHost(another.addr));
}

/// Row 19: the round ends before anything is decided: a counting answer is in at once, and the
/// one that names the original comes later in the same round.
#[tokio::test]
async fn a_first_counting_answer_does_not_end_the_round() {
    let original = Fake::saying(member(ORIGINAL)).await;
    let quick = Fake::saying(member(1)).await;
    let late = Fake::saying(member(2).listing(original.addr).after(UNNAMED_WITHIN / 2)).await;
    let decided = asking(&[quick.addr, late.addr]).decides().await;
    assert_eq!(decided, Decision::LiveHost(original.addr));
}

/// Row 30: an undecided member's answer does not count, however long it has been a member.
#[tokio::test]
async fn an_undecided_members_answer_does_not_count() {
    let undecided = Fake::saying(member(1).undecided()).await;
    let mut restore = asking(&[undecided.addr]);
    restore.undecided_for(ROUNDS, "the only member is undecided").await;
    let counted = Fake::saying(member(2)).await;
    restore.seeds.lock().push(counted.addr);
    restore.passes().await;
}

/// Row 33: a member that founded its store and has met nobody does not count: two new nodes that
/// have never met say nothing about a cluster.
#[tokio::test]
async fn a_founded_and_unmet_members_answer_does_not_count() {
    let beside = Fake::saying(member(1).founded_unmet()).await;
    let mut restore = asking(&[beside.addr]);
    restore.undecided_for(ROUNDS, "the only member has met nobody").await;
    beside.say(member(1));
    restore.passes().await;
}

/// Row 34: an address named for the asking node has the whole window to answer, longer than an
/// address nobody names.
#[tokio::test]
async fn an_address_named_for_this_node_has_the_whole_window_to_answer() {
    let original = Fake::saying(member(ORIGINAL).after(UNNAMED_WITHIN * 2)).await;
    let beside = Fake::saying(member(1).listing(original.addr)).await;
    let decided = asking(&[beside.addr]).decides().await;
    assert_eq!(decided, Decision::LiveHost(original.addr));
}

/// Row 35: the members an answer names are asked too, until no answer names a new one: the
/// original is listed only by a member that the seed names.
#[tokio::test]
async fn the_members_an_answer_names_are_asked_too() {
    let original = Fake::saying(member(ORIGINAL)).await;
    let further = Fake::saying(member(2).listing(original.addr)).await;
    let seed = Fake::saying(member(1).knows(further.addr, id(2))).await;
    let decided = asking(&[seed.addr]).decides().await;
    assert_eq!(decided, Decision::LiveHost(original.addr));
}

/// Row 36: the seeds are resolved again for each round: a name that resolves to nothing at first
/// decides once it resolves to a member.
#[tokio::test]
async fn the_seeds_are_resolved_again_each_round() {
    let beside = Fake::saying(member(1)).await;
    let mut restore = asking_with(Members::default(), id(ORIGINAL), &[beside.addr], false, 3);
    restore.passes().await;
    assert!(restore.resolved.load(Ordering::SeqCst) >= 4, "resolved for each round");
}

/// Row 42: a named address that an undecided host of the asking node answers at does not halt the
/// asker, and does not hold its pass: that host is not running the node.
#[tokio::test]
async fn an_undecided_host_of_this_node_neither_halts_nor_holds_the_pass() {
    let twin = Fake::saying(member(ORIGINAL).undecided()).await;
    let beside = Fake::saying(member(1).listing(twin.addr)).await;
    let mut restore = asking(&[beside.addr]);
    restore.passes().await;
    assert!(twin.probes() >= 1, "premise: the listed address was asked");
}

/// Row 46: a report is one answer of the round it arrives in, and is then gone. A counting report
/// decides no later round, and one handed over before the asking began, by an original since
/// stopped, halts nothing.
#[tokio::test]
async fn a_report_is_an_answer_of_one_round_only() {
    let silent = stopped().await;
    let beside = Fake::saying(member(1).young().listing(silent)).await;
    let mut restore = asking(&[beside.addr]);
    restore.undecided_for(BETWEEN * 3, "the only member is young").await;
    restore.report(member(5));
    restore.undecided_for(ROUNDS, "the report's round has a listed silent address").await;
    beside.say(member(1).young());
    restore.undecided_for(ROUNDS, "the report was an earlier round's").await;
    restore.report(member(5));
    restore.passes().await;

    let members = Members::default();
    members.note_guard_report(id(ORIGINAL), member(ORIGINAL).answer);
    let beside = Fake::saying(member(1)).await;
    asking_with(members, id(ORIGINAL), &[beside.addr], false, 0).passes().await;
}

/// Row 47: a halted host answers nobody, as one that its guard halted does: it serves nothing, so
/// it is no host running its node, and it does not halt its replacement.
#[tokio::test]
async fn a_halted_original_answers_nobody_and_does_not_halt_its_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let original = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = tokio::spawn(crate::transport::serve_with(
        Arc::clone(&original),
        listener,
        SECRET.into(),
        None,
        None,
        None,
        Arc::new(crate::tls::ClusterTls::new().unwrap()),
        Some(Members::default()),
    ));
    assert!(probe_answer(id(1), addr, SECRET).await.is_some(), "premise: it answers before");
    original.halt_by_guard().unwrap();
    let answered =
        tokio::time::timeout(Duration::from_secs(5), probe_answer(id(1), addr, SECRET)).await;
    assert!(matches!(answered, Ok(None)), "a halted host answers nobody: {answered:?}");
    let beside = Fake::saying(member(1).knows(addr, original.node_id())).await;
    let mut replacement =
        asking_with(Members::default(), original.node_id(), &[beside.addr, addr], false, 0);
    replacement.undecided_for(ROUNDS, "the halted address is still listed").await;
    beside.say(member(1));
    replacement.passes().await;
    serving.abort();
}

/// Row 48: with no seed there is nobody to ask, and nothing passes. A dialler's report is an
/// answer: one that counts passes the restore, and one from a running host of its node halts it,
/// naming where that host says it listens.
#[tokio::test]
async fn a_restore_with_no_seeds_is_answered_by_its_dialler() {
    let mut restore = asking(&[]);
    restore.undecided_for(ROUNDS, "nobody has been asked").await;
    restore.report(member(5));
    restore.passes().await;

    let mut halted = asking(&[]);
    halted.undecided_for(ROUNDS, "nobody has been asked").await;
    let mut original = member(ORIGINAL);
    original.answer.address = Some("10.1.2.3:7000".to_string());
    halted.report(original);
    let decided = halted.decides().await;
    assert_eq!(decided, Decision::LiveHost("10.1.2.3:7000".parse().unwrap()));
}

/// Row 49: `--replace` passes at the end of one round, whatever the round held, a running host of
/// the node among it.
#[tokio::test]
async fn replace_passes_after_one_round_whatever_it_holds() {
    let original = Fake::saying(member(ORIGINAL)).await;
    let mut restore = asking_with(Members::default(), id(ORIGINAL), &[original.addr], true, 0);
    restore.passes().await;
    assert!(original.probes() >= 1, "premise: the original was asked");
}

/// Rule 5's order at a pass, over its three sinks (row 31): membership is offered every address
/// first; then what the pass says of the marker, with the names its answers gave, is recorded;
/// and only once that is done is anything the guard held let go.
#[tokio::test]
async fn a_pass_offers_membership_records_the_marker_and_only_then_opens() {
    let done = Mutex::new(Vec::new());
    let offered = vec![SocketAddr::from(([127, 0, 0, 1], 7001))];
    let said = AtPass { names: [id(3)].into(), alone: false };
    crate::guard_rounds::let_in(
        offered.clone(),
        said.clone(),
        |offer| {
            let done = &done;
            async move {
                tokio::task::yield_now().await;
                done.lock().push(format!("membership offered {offer:?}"));
            }
        },
        |at_pass| {
            let done = &done;
            async move {
                tokio::task::yield_now().await;
                done.lock().push(format!("recorded {at_pass:?}"));
            }
        },
        || done.lock().push("opened".to_string()),
    )
    .await;
    let expected = [
        format!("membership offered {offered:?}"),
        format!("recorded {said:?}"),
        "opened".to_string(),
    ];
    assert_eq!(*done.lock(), expected);
}

/// What a restore under `--replace` that asks `seeds` is judged to be at its pass.
async fn judged_alone(
    members: &Members,
    seeds: &[SocketAddr],
    every_source_resolved: bool,
) -> bool {
    let own_node = id(ORIGINAL);
    let mut restore =
        asking_resolving(members.clone(), own_node, seeds, true, 0, every_source_resolved);
    match restore.decides().await {
        Decision::Pass { at_pass, .. } => at_pass.alone,
        other => panic!("a pass: {other:?}"),
    }
}

/// Row 37 (M1): under `--replace`, with no answer from another node, a restore is alone only
/// where every seed source resolved and every seed address answered as this host or as its node.
#[tokio::test]
async fn a_restore_is_alone_only_where_every_seed_answered_as_it_or_as_its_node() {
    let members = Members::default();
    let own = Fake::saying(member(ORIGINAL).token(members.guard_token())).await;
    let original = Fake::saying(member(ORIGINAL)).await;
    let me = [own.addr, original.addr];
    assert!(judged_alone(&members, &me, true).await, "each answered as it or as its node");
    assert!(judged_alone(&members, &[], true).await, "no seed at all");
    assert!(!judged_alone(&members, &me, false).await, "an unresolved name may be a member");
    let silent = stopped().await;
    let with_silent = [own.addr, original.addr, silent];
    assert!(!judged_alone(&members, &with_silent, true).await, "a silent seed may be one");
    let undecided = Fake::saying(member(2).undecided()).await;
    let with_undecided = [own.addr, original.addr, undecided.addr];
    assert!(!judged_alone(&members, &with_undecided, true).await, "another node answered");
    let other = Fake::saying(member(7)).await;
    original.say(member(ORIGINAL).knows(other.addr, id(7)));
    assert!(!judged_alone(&members, &me, true).await, "another node answered where it was named");
}

/// Row 50 (M1): the original, taken over under `--replace`, lists another member that does not
/// answer and is no seed. The restore is told of a member it has not heard, so it is not alone:
/// its marker stays, and its owner work is off. What the original lists at the restore's own
/// address is nobody else.
#[tokio::test]
async fn a_member_the_original_lists_and_nobody_hears_leaves_the_restore_not_alone() {
    let members = Members::default();
    let own = Fake::saying(member(ORIGINAL).token(members.guard_token())).await;
    let replaced = member(ORIGINAL).knows(own.addr, id(2));
    let original = Fake::saying(replaced.clone()).await;
    let seeds = [original.addr];
    assert!(judged_alone(&members, &seeds, true).await, "what it lists is where this host is");
    original.say(replaced.knows(stopped().await, id(3)));
    let mut restore = asking_with(members, id(ORIGINAL), &seeds, true, 0);
    let Decision::Pass { at_pass, .. } = restore.decides().await else { panic!("a pass") };
    assert!(!at_pass.alone, "a member it was told of has not answered");

    let store = tempfile::tempdir().unwrap();
    let engine = Engine::open(&store.path().join("kimmy.redb")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    crate::catchup::write_marker(dir.path(), crate::CatchUpReason::Restored).unwrap();
    let catch_up = crate::catchup::CatchUp::open(dir.path(), Duration::from_secs(120));
    catch_up.at_guard_pass(&engine, &at_pass).unwrap();
    assert!(catch_up.is_set() && catch_up.holds_owner_work(), "kept, with its owner work off");
}

/// Rows 15 and 31 (rule 5): a pass carries the node ids its open answers and its reports named,
/// the answerers' own among them and the asker's left out; under `--replace` as at any pass.
/// What an undecided member names is not taken.
#[tokio::test]
async fn a_pass_carries_the_nodes_its_answers_named() {
    for replace in [false, true] {
        let members = Members::default();
        let own = Fake::saying(member(ORIGINAL).token(members.guard_token())).await;
        let beside = member(1)
            .knows(stopped().await, id(3))
            .knows(own.addr, id(ORIGINAL))
            .after(UNNAMED_WITHIN / 3);
        let beside = Fake::saying(beside).await;
        let undecided = Fake::saying(member(2).undecided().knows(stopped().await, id(4))).await;
        let seeds = [beside.addr, undecided.addr];
        let mut restore = asking_with(members, id(ORIGINAL), &seeds, replace, 0);
        // Handed over while the round is out, so that it is an answer of the round.
        let until = tokio::time::Instant::now() + Duration::from_secs(5);
        while beside.probes() == 0 {
            assert!(tokio::time::Instant::now() < until, "the member was never asked");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        restore.report(member(5).knows(stopped().await, id(6)));
        let Decision::Pass { at_pass, .. } = restore.decides().await else {
            panic!("a pass, replace: {replace}")
        };
        let named = BTreeSet::from([id(1), id(3), id(5), id(6)]);
        assert_eq!(at_pass.names, named, "replace: {replace}");
        assert!(!at_pass.alone, "others answered it, replace: {replace}");
    }
}

/// A restore that waits says what on, where its readiness is read from: how many it asked, who is
/// undecided, whose answer does not count, and which address listed for its node is silent. The
/// hold's end takes it away.
#[tokio::test]
async fn the_rounds_say_what_they_wait_on() {
    let silent = stopped().await;
    let undecided = Fake::saying(member(1).undecided()).await;
    let young = Fake::saying(member(2).young().listing(silent)).await;
    let dir = tempfile::tempdir().unwrap();
    let catch_up = crate::catchup::CatchUp::open(dir.path(), Duration::from_secs(120));
    catch_up.hold_for_guard();
    let members = Members::default();
    members.set_catch_up(catch_up.clone());
    let mut restore = asking_with(members, id(ORIGINAL), &[undecided.addr, young.addr], false, 0);
    restore.undecided_for(ROUNDS, "nobody who counts has answered").await;
    let waits = catch_up.guard_waits().expect("it says what it waits on");
    assert_eq!((waits.asked, waits.undecided, waits.uncounted), (3, 1, 1), "{waits:?}");
    assert_eq!(waits.silent_named, [silent]);
    catch_up.release_guard_hold();
    assert_eq!(catch_up.guard_waits(), None, "nothing is waited on once it is let go");
}
