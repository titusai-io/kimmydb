//! The question a restore's guard asks and the answer it gets (D13 containment, ADR-274): a member
//! whose guard holds it answers at once, saying it is undecided, and nothing is recorded of either
//! end; and what an answer holds is the answering member's own.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use kimmy_core::{Epoch, NodeId};
use kimmy_storage::Engine;
use tokio::net::TcpListener;

use super::containment_tests::{
    an_epoch_ahead, an_old_and_a_new_host_of_one_node, engine, raw_hello,
};
use super::*;
use crate::catchup::CatchUp;
use crate::guard_answer::MAX_NAMED;
use crate::protocol::GuardAnswer;
use crate::second_dispatcher::a_second_dispatcher;

const SECRET: &str = "a-guard-answer-secret";
const BINDING: &[u8] = b"a-guard-answer-binding";

/// A member set whose guard holds its member, and the directory its catching-up state is in.
fn held() -> (Members, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let catch_up = CatchUp::open(dir.path(), Duration::from_secs(120));
    catch_up.hold_for_guard();
    let members = Members::default();
    members.set_catch_up(catch_up);
    (members, dir)
}

/// A member serving the cluster listener, with its member set.
async fn serving(
    engine: &Arc<Engine>,
    members: &Members,
) -> (SocketAddr, tokio::task::AbortHandle) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_with(
        Arc::clone(engine),
        listener,
        SECRET.into(),
        None,
        None,
        None,
        Arc::new(crate::tls::ClusterTls::new().unwrap()),
        Some(members.clone()),
    ));
    (addr, task.abort_handle())
}

fn arc_engine() -> (Arc<Engine>, tempfile::TempDir) {
    let (engine, dir) = engine();
    (Arc::new(engine), dir)
}

fn at(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// What a dial ended in, without its stream.
async fn dialled(
    dialler: &Engine,
    addr: SocketAddr,
    members: &Members,
) -> Result<NodeId, ProtocolError> {
    tokio::time::timeout(Duration::from_secs(5), dial(dialler, addr, SECRET, Some(members)))
        .await
        .expect("the dial ends")
        .map(|(_, node)| node)
}

/// The reports `members` has been handed, once there is one, or after two seconds whatever there
/// is: the dialled side reads a report after its dialler has returned.
async fn reports_of(members: &Members) -> Vec<(NodeId, GuardAnswer)> {
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        let reports = members.guard_reports();
        if !reports.is_empty() || Instant::now() >= until {
            return reports;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// What a probe from `asker` is answered at `addr`.
async fn asked(asker: NodeId, addr: SocketAddr) -> (NodeId, GuardAnswer) {
    let (node, answer) =
        tokio::time::timeout(Duration::from_secs(5), probe_answer(asker, addr, SECRET))
            .await
            .expect("the probe ends")
            .expect("the probe is answered");
    (node, *answer.expect("with an answer for the guard"))
}

/// Rule 2: a member its guard holds does not leave a dialler waiting. It says at once that it is
/// undecided, and the dialler's round ends there.
#[tokio::test]
async fn a_dialler_of_an_undecided_member_is_told_so_at_once() {
    let (member, _m) = arc_engine();
    let (members, _held) = held();
    let (addr, task) = serving(&member, &members).await;
    let (dialler, _d) = engine();
    let started = Instant::now();
    let ended = dialled(&dialler, addr, &Members::default()).await;
    assert!(matches!(ended, Err(ProtocolError::Undecided)), "told it is undecided: {ended:?}");
    assert!(started.elapsed() < Duration::from_secs(1), "at once: {:?}", started.elapsed());
    task.abort();
}

/// An undecided member's `Welcome` carries no epoch, for a probe and for a `Hello` that is not
/// one, and says it is undecided. A dialler that did not know the standing would have nothing to
/// record.
#[tokio::test]
async fn an_undecided_members_welcome_carries_no_epoch() {
    let (member, _m) = engine();
    let (members, _held) = held();
    for probe in [true, false] {
        let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
        let accepting = accept_handshake(&member, &mut theirs, SECRET, BINDING, Some(&members));
        let asking = async {
            let epoch = (!probe).then(|| an_epoch_ahead(0));
            raw_hello(&mut ours, NodeId::generate(), Some(crate::protocol::PROTOCOL), epoch, probe)
                .await;
            let welcome = read_frame(&mut ours).await.unwrap();
            drop(ours);
            welcome
        };
        let (_, welcome) =
            tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(accepting, asking) })
                .await
                .expect("the handshake ends");
        let Message::Welcome { epoch, guard, .. } = welcome else {
            panic!("expected a Welcome, got {welcome:?}");
        };
        assert_eq!(guard.map(|answer| answer.standing), Some(Standing::Undecided), "probe {probe}");
        assert_eq!(epoch, None, "probe {probe}: no epoch");
    }
}

/// A dialler believes nothing of a member that says it is undecided, whatever its `Welcome`
/// carries. Here the answerer claims the dialler's own node with a newer epoch, which from a
/// member that has decided would halt the dialler.
#[tokio::test]
async fn a_dialler_records_nothing_of_an_answerer_that_says_it_is_undecided() {
    let (dialler, _d) = engine();
    let newest = dialler.newest_epoch(dialler.node_id()).unwrap();
    let newer = an_epoch_ahead(1_000);
    assert!(newer > dialler.epoch(), "premise: a newer epoch of the dialler's node");
    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    let opening = open_handshake_claimed(&dialler, &mut ours, SECRET, BINDING, None);
    let answering = async {
        let Ok(Message::Hello { nonce: asked, .. }) = read_frame(&mut theirs).await else {
            panic!("expected the dialler's Hello");
        };
        let answer = GuardAnswer {
            token: vec![7; 16],
            standing: Standing::Undecided,
            membership_age_ms: 0,
            founded_unmet: false,
            listings: Vec::new(),
            members: Vec::new(),
            address: None,
        };
        let welcome = Message::Welcome {
            node: dialler.node_id(),
            nonce: nonce(dialler.node_id()),
            proof: prove(SECRET, &asked, BINDING),
            protocol: Some(crate::protocol::PROTOCOL),
            epoch: Some(newer),
            guard: Some(Box::new(answer)),
        };
        write_frame(&mut theirs, &welcome).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), read_frame(&mut theirs)).await;
    };
    let (ended, ()) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(opening, answering) })
            .await
            .expect("the handshake ends");
    assert!(matches!(ended, Err(ProtocolError::Undecided)), "an undecided end: {ended:?}");
    assert!(dialler.halted().is_none(), "not halted by what an undecided member claims");
    assert_eq!(dialler.newest_epoch(dialler.node_id()).unwrap(), newest, "nothing recorded");
    assert!(!dialler.has_met_a_peer(), "and it has met nobody");
}

/// Row 28: the original dials an undecided restore of itself. The restore's epoch is the newer, so
/// a handshake that carried it would halt the original: the harm the guard exists to prevent, by
/// another road. A dialler that has met nobody has met nobody still.
#[tokio::test]
async fn dialling_an_undecided_restore_of_itself_halts_nobody() {
    let (original, restore, _dirs) = an_old_and_a_new_host_of_one_node();
    let restore = Arc::new(restore);
    let (members, _held) = held();
    let (addr, task) = serving(&restore, &members).await;
    let newest = original.newest_epoch(original.node_id()).unwrap();
    assert!(restore.epoch() > original.epoch(), "premise: the restore's epoch is the newer");
    assert!(
        crate::containment::claim_within_bound(
            kimmy_storage::physical_now_ms(),
            original.clock_bound().skew_ms,
            &restore.epoch()
        ),
        "premise: within the original's clock bound, so a claim of it would be believed"
    );
    assert!(!original.has_met_a_peer(), "premise: this dialler has met nobody");

    let ended = dialled(&original, addr, &Members::default()).await;
    assert!(matches!(ended, Err(ProtocolError::Undecided)), "an undecided end: {ended:?}");
    assert!(original.halted().is_none(), "the original is not halted");
    assert_eq!(original.newest_epoch(original.node_id()).unwrap(), newest, "nor told");
    assert!(!original.has_met_a_peer(), "an undecided member is not one it has met");
    task.abort();
}

/// Row 29: an undecided member records nothing of who dials or probes it: no epoch, and no peer
/// met, which would let a restart of it pass unguarded. It is handed the dialler's report, with
/// where the dialler lists its node.
#[tokio::test]
async fn an_undecided_member_records_nothing_of_who_dials_it() {
    let (_original, restore, _dirs) = an_old_and_a_new_host_of_one_node();
    let restore = Arc::new(restore);
    let (members, _held) = held();
    let (addr, task) = serving(&restore, &members).await;
    let (dialler, _d) = engine();
    let diallers = Members::default();
    diallers.insert_for_test(at(7001), restore.node_id());
    diallers.insert_for_test(at(7002), NodeId::from_bytes([2; 16]));

    let ended = dialled(&dialler, addr, &diallers).await;
    assert!(matches!(ended, Err(ProtocolError::Undecided)), "an undecided end: {ended:?}");
    let _ = probe_answer(NodeId::generate(), addr, SECRET).await;
    let reports = reports_of(&members).await;
    assert_eq!(reports.len(), 1, "the dialler's report, and none from a probe: {reports:?}");
    let (from, report) = &reports[0];
    assert_eq!(*from, dialler.node_id());
    assert_eq!(report.standing, Standing::Open);
    assert_eq!(report.token, diallers.guard_answer(&dialler, restore.node_id()).token);
    assert_eq!(report.listings, [at(7001).to_string()], "where the dialler lists this node");
    assert_eq!(report.members.len(), 2);
    assert!(!restore.has_met_a_peer(), "no peer is met");
    assert_eq!(restore.newest_epoch(dialler.node_id()).unwrap(), Epoch::ZERO, "no epoch recorded");
    task.abort();
}

/// Row 12's second half: a member that says it is undecided has not failed. The replication loop
/// that dials it counts no failed round and backs off nothing, tick after tick, and each dial hands
/// it a report.
#[tokio::test]
async fn a_replication_loop_does_not_back_off_an_undecided_peer() {
    let (member, _m) = arc_engine();
    let (members, _held) = held();
    let (addr, task) = serving(&member, &members).await;
    let (dialler, _d) = arc_engine();
    let (sender, mut rounds) = tokio::sync::mpsc::unbounded_channel();
    let mut config = crate::peers::ReplicationConfig::new(
        vec![crate::SeedSource::Static(vec![addr])],
        SECRET.into(),
        at(1),
    );
    config.members = Some(Members::default());
    config.sync_interval = Duration::from_millis(100);
    config.discovery_interval = Duration::from_millis(100);
    config.on_round = Some(Arc::new(move |report| {
        let _ = sender.send(report);
    }));
    let looping = tokio::spawn(crate::peers::replicate(dialler, config));
    let mut seen = Vec::new();
    while seen.len() < 10 {
        let report = tokio::time::timeout(Duration::from_secs(10), rounds.recv())
            .await
            .expect("a round is reported")
            .expect("the loop runs");
        seen.push(report);
    }
    looping.abort();
    task.abort();
    assert!(!members.guard_reports().is_empty(), "premise: the loop dialled it, and reported");
    for report in &seen {
        assert_eq!((report.failed, report.backing_off), (0, 0), "nobody failed: {report:?}");
    }
}

/// A seed that membership does not list is dialled all the same, so that a member whose guard
/// holds it, which has joined no membership, is dialled and handed a report; a seed that nothing
/// answers on is not counted as a failed round, and backs nothing off. The loop's rounds go to the
/// members it lists, as before.
#[tokio::test]
async fn a_seed_membership_does_not_list_is_dialled_and_no_failure_of_it_counted() {
    let (listed, _l) = arc_engine();
    let (listed_addr, listed_task) = serving(&listed, &Members::default()).await;
    let (member, _m) = arc_engine();
    let (members, _held) = held();
    let (held_addr, held_task) = serving(&member, &members).await;
    let dead = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let (dialler, _d) = arc_engine();
    let known = Members::default();
    known.insert_for_test(listed_addr, listed.node_id());
    let (sender, mut rounds) = tokio::sync::mpsc::unbounded_channel();
    let mut config = crate::peers::ReplicationConfig::new(
        vec![crate::SeedSource::Static(vec![held_addr, dead])],
        SECRET.into(),
        at(1),
    );
    config.members = Some(known);
    config.sync_interval = Duration::from_millis(100);
    config.discovery_interval = Duration::from_millis(100);
    config.on_round = Some(Arc::new(move |report| {
        let _ = sender.send(report);
    }));
    let looping = tokio::spawn(crate::peers::replicate(dialler, config));
    let mut seen = Vec::new();
    while seen.len() < 10 {
        let report = tokio::time::timeout(Duration::from_secs(10), rounds.recv())
            .await
            .expect("a round is reported")
            .expect("the loop runs");
        seen.push(report);
    }
    let reports = reports_of(&members).await;
    looping.abort();
    listed_task.abort();
    held_task.abort();
    assert!(!reports.is_empty(), "the unlisted held seed was dialled and handed a report");
    for report in &seen {
        assert_eq!((report.failed, report.backing_off), (0, 0), "nobody failed: {report:?}");
    }
}

/// Row 43: what an answer says of the member is the member's own: its standing, how long it has
/// been a member, whether it founded its store and has met nobody, and a token that is its
/// process's.
#[tokio::test]
async fn an_answer_is_the_answering_members_own() {
    let (member, _m) = arc_engine();
    let members = Members::default();
    let (addr, task) = serving(&member, &members).await;
    let asker = NodeId::generate();

    let (node, first) = asked(asker, addr).await;
    assert_eq!(node, member.node_id());
    assert_eq!(first.standing, Standing::Open);
    assert!(first.founded_unmet, "a new store that has met nobody");
    assert_eq!(first.membership_age_ms, 0, "it has not begun to gossip");
    assert_eq!(first.token.len(), 16);

    members.note_gossiping();
    tokio::time::sleep(Duration::from_millis(60)).await;
    members.note_gossiping();
    let (_, later) = asked(asker, addr).await;
    assert!(later.membership_age_ms >= 60, "from the first: {}", later.membership_age_ms);
    assert_eq!(later.token, first.token, "one token for the process");

    member.note_met_a_peer();
    let (_, met) = asked(asker, addr).await;
    assert!(!met.founded_unmet, "it has met a peer");

    let (_original, restore, _dirs) = an_old_and_a_new_host_of_one_node();
    let restore = Arc::new(restore);
    let (restored, held_dir) = held();
    let (restore_addr, restore_task) = serving(&restore, &restored).await;
    let (_, other) = asked(asker, restore_addr).await;
    assert_eq!(other.standing, Standing::Undecided);
    assert!(!other.founded_unmet, "a restore founded nothing");
    assert_ne!(other.token, first.token, "another process, another token");
    drop(held_dir);
    task.abort();
    restore_task.abort();
}

/// Row 44: an answer's listings are where the asker's node id is, and no other's; its member list
/// is the whole of it.
#[tokio::test]
async fn an_answer_lists_where_the_askers_node_is_and_no_other() {
    let (member, _m) = arc_engine();
    let members = Members::default();
    let (asker, other) = (NodeId::from_bytes([1; 16]), NodeId::from_bytes([2; 16]));
    members.insert_for_test(at(7001), asker);
    members.insert_for_test(at(7002), other);
    members.insert_for_test(at(7003), asker);
    let (addr, task) = serving(&member, &members).await;
    let (_, answer) = asked(asker, addr).await;
    assert_eq!(answer.listings, [at(7001).to_string(), at(7003).to_string()]);
    assert_eq!(
        answer.members,
        [
            (at(7001).to_string(), asker),
            (at(7002).to_string(), other),
            (at(7003).to_string(), asker)
        ]
    );
    task.abort();
}

/// The messages of the events a thread records.
#[derive(Clone, Default)]
struct Said(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for Said {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::Id {
        tracing::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Message(String);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        self.0.lock().unwrap().push(message.0);
    }
    fn enter(&self, _: &tracing::Id) {}
    fn exit(&self, _: &tracing::Id) {}
}

/// Row 45: an answer names at most 1,024 listings and 1,024 members, and the cut is said once.
#[test]
fn an_answers_lists_are_cut_and_the_cut_is_said_once() {
    let (member, _m) = engine();
    let members = Members::default();
    let asker = NodeId::from_bytes([1; 16]);
    for port in 0..(MAX_NAMED as u16 + 6) {
        members.insert_for_test(at(10_000 + port), asker);
    }
    let said = Said::default();
    let _not_alone = a_second_dispatcher();
    let _recording = tracing::subscriber::set_default(said.clone());
    for _ in 0..3 {
        let answer = members.guard_answer(&member, asker);
        assert_eq!((answer.listings.len(), answer.members.len()), (MAX_NAMED, MAX_NAMED));
    }
    let cut =
        said.0.lock().unwrap().iter().filter(|m| m.contains("an answer names at most")).count();
    assert_eq!(cut, 1, "said once");
}
