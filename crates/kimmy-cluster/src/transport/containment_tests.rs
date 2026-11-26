//! D13 containment at the handshake (ADR-274): which epochs a member records and when, what it
//! tells, what it refuses by name, and what a probe is. The tests commit of unit 14a: each test is
//! red on its designed assertion against the scaffold (the frames carry the fields and the
//! handshake does nothing with them), and the code commits turn it green. Test numbers are those of
//! `D13-prebuild.md`.

use std::time::Duration;

use kimmy_core::{Epoch, Hlc, NodeId};
use kimmy_storage::Engine;
use tokio::io::DuplexStream;

use super::*;

const SECRET: &str = "a-containment-secret";
const BINDING: &[u8] = b"a-containment-binding";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    (engine, dir)
}

/// Two hosts of one node: the original, and a restore of its backup (the same node and a newer
/// epoch).
fn an_old_and_a_new_host_of_one_node() -> (Engine, Engine, Vec<tempfile::TempDir>) {
    let (old, old_dir) = engine();
    old.create_collection("shop", "orders").unwrap();
    let mut backup = Vec::new();
    old.backup_to(&mut backup, kimmy_storage::WalkScope::Request).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    let new_dir = tempfile::tempdir().unwrap();
    let path = new_dir.path().join("kimmy.redb");
    kimmy_storage::backup::restore(&path, &mut backup.as_slice()).unwrap();
    let new = Engine::open(&path).unwrap();
    assert_eq!(new.node_id(), old.node_id(), "premise: one node");
    assert!(new.epoch() > old.epoch(), "premise: the restore's epoch is the newer");
    (old, new, vec![old_dir, new_dir])
}

/// An epoch another host could have minted `ahead_ms` after this clock.
fn an_epoch_ahead(ahead_ms: u64) -> Epoch {
    Epoch::new(Hlc::new(kimmy_storage::physical_now_ms() + ahead_ms, 0), [1; 8])
}

/// A whole handshake over in-memory streams: `dialler` opens, `responder` answers. The streams come
/// back open, so that a test can read what either side wrote after the handshake.
async fn handshake(
    dialler: &Engine,
    responder: &Engine,
) -> (Result<NodeId, ProtocolError>, Result<NodeId, ProtocolError>, DuplexStream, DuplexStream) {
    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    let (opened, accepted) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            open_handshake(dialler, &mut ours, SECRET, BINDING),
            accept_handshake(responder, &mut theirs, SECRET, BINDING)
        )
    })
    .await
    .expect("the handshake ends");
    (opened, accepted, ours, theirs)
}

/// The next frame within `within`, or `None`: nothing came, or the stream ended.
async fn next_frame(stream: &mut DuplexStream, within: Duration) -> Option<Message> {
    tokio::time::timeout(within, read_frame(stream)).await.ok().and_then(Result::ok)
}

/// A `Hello` written by hand, as a socket that is not a member would write it. Returns its
/// challenge.
async fn raw_hello(
    ours: &mut DuplexStream,
    peer: NodeId,
    protocol: Option<u32>,
    epoch: Option<Epoch>,
    probe: bool,
) -> Vec<u8> {
    let challenge = nonce(peer);
    let hello = Message::Hello { node: peer, nonce: challenge.clone(), protocol, epoch, probe };
    write_frame(ours, &hello).await.unwrap();
    challenge
}

/// A dialler written by hand that proves itself with the cluster secret and claims `epoch`, against
/// `responder`: what the responder's handshake returned.
async fn verified_claim(
    responder: &Engine,
    peer: NodeId,
    epoch: Epoch,
) -> Result<NodeId, ProtocolError> {
    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    let accepting = accept_handshake(responder, &mut theirs, SECRET, BINDING);
    let claiming = async {
        raw_hello(&mut ours, peer, Some(crate::protocol::PROTOCOL), Some(epoch), false).await;
        if let Ok(Message::Welcome { nonce: theirs, .. }) = read_frame(&mut ours).await {
            let proof = prove(SECRET, &theirs, BINDING);
            write_frame(&mut ours, &Message::Confirm { proof }).await.unwrap();
        }
    };
    let (accepted, ()) =
        tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(accepting, claiming) })
            .await
            .expect("the handshake ends");
    accepted
}

// -- a claim is believed only when it is verified (test 5; findings 6, E)
// -----------------------------------------------

/// The responder records the dialler's epoch once the dialler's proof has verified.
#[tokio::test]
async fn a_responder_records_the_epoch_of_a_dialler_whose_proof_verified() {
    let ((dialler, _d), (responder, _r)) = (engine(), engine());
    let (opened, accepted, _ours, _theirs) = handshake(&dialler, &responder).await;
    opened.unwrap();
    accepted.unwrap();
    assert_eq!(
        responder.newest_epoch(dialler.node_id()).unwrap(),
        dialler.epoch(),
        "the dialler's epoch, claimed in its Hello and proved by its Confirm, is recorded"
    );
}

/// The dialler records the responder's epoch once the responder's proof has verified.
#[tokio::test]
async fn a_dialler_records_the_epoch_of_a_responder_whose_proof_verified() {
    let ((dialler, _d), (responder, _r)) = (engine(), engine());
    let (opened, accepted, _ours, _theirs) = handshake(&dialler, &responder).await;
    opened.unwrap();
    accepted.unwrap();
    assert_eq!(
        dialler.newest_epoch(responder.node_id()).unwrap(),
        responder.epoch(),
        "the responder's epoch, claimed in its Welcome and proved by it, is recorded"
    );
}

/// A claim whose proof fails is recorded nowhere, and a verified claim of the same epoch is (the
/// control).
#[tokio::test]
async fn a_claim_whose_proof_fails_records_nothing_and_a_verified_one_is_recorded() {
    let (responder, _r) = engine();
    let peer = NodeId::generate();
    let claim = an_epoch_ahead(60_000);

    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    let accepting = accept_handshake(&responder, &mut theirs, SECRET, BINDING);
    let forging = async {
        raw_hello(&mut ours, peer, Some(crate::protocol::PROTOCOL), Some(claim), false).await;
        assert!(matches!(read_frame(&mut ours).await.unwrap(), Message::Welcome { .. }));
        write_frame(&mut ours, &Message::Confirm { proof: vec![0; 32] }).await.unwrap();
        let _ = read_frame(&mut ours).await;
    };
    let (accepted, ()) =
        tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(accepting, forging) })
            .await
            .expect("the handshake ends");
    assert!(matches!(accepted, Err(ProtocolError::Unauthenticated)), "{accepted:?}");
    assert_eq!(
        responder.newest_epoch(peer).unwrap(),
        Epoch::ZERO,
        "a claim that was never proved is recorded nowhere (no record on Hello)"
    );
    assert!(responder.halted().is_none(), "and halts nobody");

    verified_claim(&responder, peer, claim).await.unwrap();
    assert_eq!(
        responder.newest_epoch(peer).unwrap(),
        claim,
        "the control: the same claim, proved, is recorded"
    );
}

// -- the bound on a claim (test 6; findings 4, F)
// ----------------------------------------------------------------------

/// An epoch inside the skew bound is believed; one beyond it, and `Epoch::MAX`, are not. Each on a
/// verified `Hello`.
#[tokio::test]
async fn a_verified_claim_beyond_the_bound_or_the_maximum_epoch_is_not_recorded() {
    let (responder, _r) = engine();
    let inside = (NodeId::generate(), an_epoch_ahead(60_000));
    let beyond = (NodeId::generate(), an_epoch_ahead(10 * 60_000));
    let maximum = (NodeId::generate(), Epoch::MAX);

    verified_claim(&responder, inside.0, inside.1).await.unwrap();
    assert_eq!(
        responder.newest_epoch(inside.0).unwrap(),
        inside.1,
        "an epoch inside the bound is recorded"
    );
    let _ = verified_claim(&responder, beyond.0, beyond.1).await;
    assert_eq!(
        responder.newest_epoch(beyond.0).unwrap(),
        Epoch::ZERO,
        "an epoch that starts beyond the bound is not recorded"
    );
    let _ = verified_claim(&responder, maximum.0, maximum.1).await;
    assert_eq!(
        responder.newest_epoch(maximum.0).unwrap(),
        Epoch::ZERO,
        "Epoch::MAX is refused, whatever the clock"
    );
}

// -- the tell (test 5, E; test 13, J)
// ----------------------------------------------------------------------------------

/// A responder that knows a newer epoch of the dialler's node tells the dialler, after the
/// dialler's `Confirm` has verified.
#[tokio::test]
async fn a_responder_that_knows_a_newer_epoch_of_the_dialler_tells_it_after_the_confirm() {
    let (old, new, _dirs) = an_old_and_a_new_host_of_one_node();
    let (responder, _r) = engine();
    // The responder learns of the newer epoch from the new host's own verified handshake (test 5
    // asserts the record).
    let (opened, accepted, _ours, _theirs) = handshake(&new, &responder).await;
    let _ = (opened, accepted);

    let (_opened, _accepted, mut ours, _theirs) = handshake(&old, &responder).await;
    let tell = next_frame(&mut ours, Duration::from_secs(2)).await;
    assert!(
        matches!(&tell, Some(Message::Superseded { node, epoch })
            if *node == old.node_id() && *epoch == new.epoch()),
        "the responder tells the old host of the newer epoch of its node: {tell:?}"
    );
}

/// A dialler that knows a newer epoch of the responder's node sends `Confirm` and then the tell.
#[tokio::test]
async fn a_dialler_that_knows_a_newer_epoch_of_the_responder_tells_it_after_its_confirm() {
    let (old, new, _dirs) = an_old_and_a_new_host_of_one_node();
    let (dialler, _d) = engine();
    let (opened, accepted, _ours, _theirs) = handshake(&new, &dialler).await;
    let _ = (opened, accepted);

    let (_opened, _accepted, _ours, mut theirs) = handshake(&dialler, &old).await;
    let tell = next_frame(&mut theirs, Duration::from_secs(2)).await;
    assert!(
        matches!(&tell, Some(Message::Superseded { node, epoch })
            if *node == old.node_id() && *epoch == new.epoch()),
        "the dialler tells the old host of the newer epoch of its node: {tell:?}"
    );
}

/// The control: a responder that has never heard of a newer epoch tells nobody, so a halt that
/// fires on the wrong side, or on no knowledge, is red.
#[tokio::test]
async fn a_responder_that_knows_of_no_newer_epoch_tells_nobody() {
    let (old, _new, _dirs) = an_old_and_a_new_host_of_one_node();
    let (responder, _r) = engine();
    let (_opened, _accepted, mut ours, _theirs) = handshake(&old, &responder).await;
    let tell = next_frame(&mut ours, Duration::from_millis(300)).await;
    assert!(tell.is_none(), "nothing to tell: {tell:?}");
}

/// A responder tells only after the `Confirm` verifies: a socket that sends a bad one is answered
/// with the fault and no tell.
#[tokio::test]
async fn a_responder_does_not_tell_a_socket_whose_confirm_fails() {
    let (old, new, _dirs) = an_old_and_a_new_host_of_one_node();
    let (responder, _r) = engine();
    let (opened, accepted, _ours, _theirs) = handshake(&new, &responder).await;
    let _ = (opened, accepted);

    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    let accepting = accept_handshake(&responder, &mut theirs, SECRET, BINDING);
    let forging = async {
        let epoch = Some(old.epoch());
        raw_hello(&mut ours, old.node_id(), Some(crate::protocol::PROTOCOL), epoch, false).await;
        assert!(matches!(read_frame(&mut ours).await.unwrap(), Message::Welcome { .. }));
        write_frame(&mut ours, &Message::Confirm { proof: vec![0; 32] }).await.unwrap();
        read_frame(&mut ours).await
    };
    let (accepted, answer) =
        tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(accepting, forging) })
            .await
            .expect("the handshake ends");
    assert!(matches!(accepted, Err(ProtocolError::Unauthenticated)), "{accepted:?}");
    assert!(
        matches!(answer, Ok(Message::Fault(_))),
        "the answer to a bad Confirm is the fault and nothing else: {answer:?}"
    );
}

// -- a build before containment, and the probe (R3, review D; test 9)
// ------------------------------------------------

/// A `Hello` that names this protocol and carries neither an epoch nor the probe mark is a build
/// from before containment, and is refused by name.
#[tokio::test]
async fn a_hello_with_no_epoch_that_is_not_a_probe_is_refused_by_name() {
    let (responder, _r) = engine();
    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    let peer = NodeId::generate();
    let refusing = accept_handshake(&responder, &mut theirs, SECRET, BINDING);
    let asking = async {
        raw_hello(&mut ours, peer, Some(crate::protocol::PROTOCOL), None, false).await;
        read_frame(&mut ours).await
    };
    let (refused, answer) =
        tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(refusing, asking) })
            .await
            .expect("the refusal ends the handshake");
    let Err(ProtocolError::Release(reason)) = refused else {
        panic!("expected a release refusal, got {refused:?}");
    };
    assert!(reason.contains("before containment"), "the refusal says what the peer is: {reason}");
    assert_eq!(answer.unwrap(), Message::Fault(reason));
}

/// A probe is answered by a member that would refuse an epoch-less `Hello`: it gets a `Welcome`
/// whose proof verifies, nothing follows it, and the probed side records nothing.
#[tokio::test]
async fn a_probe_is_answered_and_nothing_follows_it() {
    let (responder, _r) = engine();
    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    let peer = NodeId::generate();
    let probed = accept_handshake(&responder, &mut theirs, SECRET, BINDING);
    let probing = async {
        let challenge =
            raw_hello(&mut ours, peer, Some(crate::protocol::PROTOCOL), None, true).await;
        let answer = read_frame(&mut ours).await;
        let verified = matches!(&answer, Ok(Message::Welcome { proof, .. })
            if proof_is_valid(SECRET, &challenge, BINDING, proof));
        // The probe ends here: no Confirm is sent, and the connection closes.
        drop(ours);
        verified
    };
    let (probed, verified) =
        tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(probed, probing) })
            .await
            .expect("a probe ends the handshake");
    assert!(verified, "the probed member answers with a Welcome whose proof verifies");
    assert!(
        !matches!(probed, Err(ProtocolError::Unauthenticated)),
        "a probe that hangs up after the Welcome is not an authentication failure: {probed:?}"
    );
    assert_eq!(
        responder.newest_epoch(peer).unwrap(),
        Epoch::ZERO,
        "the probed side records nothing"
    );
    assert!(responder.halted().is_none(), "and acts on nothing");
}

// -- an equal epoch (test 16, the model row's finding 5) ------------------------------------------

/// "Halt on an equal epoch" can only fire when a host dials its own address (a seed list that names
/// itself): no two hosts hold one `(node, epoch)` in the model. The host is not halted and tells
/// nobody, which is what the mutant `halt-on-equal` breaks.
#[tokio::test]
async fn a_host_that_dials_its_own_address_is_not_halted_and_tells_nobody() {
    let (host, _dir) = engine();
    let (opened, accepted, mut ours, _theirs) = handshake(&host, &host).await;
    let _ = (opened, accepted);
    assert!(host.halted().is_none(), "an equal epoch is not a newer one");
    let tell = next_frame(&mut ours, Duration::from_millis(300)).await;
    assert!(tell.is_none(), "and nobody is told: {tell:?}");
}

// -- the relayed rows and the tell, each mechanism alone (the model row's finding 4)
// ---------------

/// A round of `engine` against a scripted peer of another node, whose answer to the first ask
/// carries `relayed` as its newest-epoch rows. What the round does with the peer after that is not
/// this test's.
async fn a_round_answered_with_relays(engine: &Engine, relayed: Vec<(NodeId, Epoch)>) {
    async fn peer(mut stream: DuplexStream, relayed: Vec<(NodeId, Epoch)>) {
        if let Ok(Message::AskVersions { .. }) = read_frame(&mut stream).await {
            let answer = Message::Vectors {
                servable: VersionVector::new(),
                witnessed: VersionVector::new(),
                facts: None,
                facts_gen: None,
                echo: None,
                replay_from: None,
                newest_epochs: relayed,
            };
            let _ = write_frame(&mut stream, &answer).await;
            while read_frame(&mut stream).await.is_ok() {}
        }
    }
    let (ours, theirs) = tokio::io::duplex(MAX_FRAME);
    let scripted = tokio::spawn(peer(theirs, relayed));
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let mut stalls = PeerStalls::new();
    let round = sync_over(engine, ours, addr, NodeId::from_bytes([9; 16]), None, &mut stalls);
    let _ = tokio::time::timeout(Duration::from_secs(5), round).await;
    scripted.abort();
}

/// Row R: a member that never met the newer host (so its own table holds nothing of it) is taught
/// by a relayed row alone. The old host asks a peer that relays the newer epoch of the old host's
/// node, and halts: the mechanism the ordinary runs hide, because there the tell halts the old host
/// first.
#[tokio::test]
async fn a_relayed_row_alone_halts_a_host_that_never_met_the_newer_one() {
    let (old, new, _dirs) = an_old_and_a_new_host_of_one_node();
    assert!(old.halted().is_none(), "premise: not halted");
    a_round_answered_with_relays(&old, vec![(old.node_id(), new.epoch())]).await;
    assert!(
        old.halted().is_some(),
        "the relayed row of its own node, newer and in the bound, halts it"
    );
}

/// What a relayed row must not do (J-14): about another node, equal to the host's own epoch, older
/// than it, or beyond the host's bound, it halts nobody.
#[tokio::test]
async fn a_relayed_row_about_another_node_or_not_newer_or_beyond_the_bound_halts_nobody() {
    let (old, new, _dirs) = an_old_and_a_new_host_of_one_node();
    let other = NodeId::from_bytes([8; 16]);
    a_round_answered_with_relays(&old, vec![(other, new.epoch())]).await;
    assert!(old.halted().is_none(), "a newer epoch of ANOTHER node");
    a_round_answered_with_relays(&old, vec![(old.node_id(), old.epoch())]).await;
    assert!(old.halted().is_none(), "an epoch equal to its own");
    a_round_answered_with_relays(&old, vec![(old.node_id(), Epoch::ZERO)]).await;
    assert!(old.halted().is_none(), "an older epoch of its own node");
    a_round_answered_with_relays(&old, vec![(old.node_id(), an_epoch_ahead(10 * 60_000))]).await;
    assert!(old.halted().is_none(), "an epoch beyond its bound (a verified source relaying it)");
}

/// The vectors `server` answers a member that asks for them: the relayed rows of that answer.
async fn relayed_rows_of(server: &Engine) -> Vec<(NodeId, Epoch)> {
    let (asker, _dir) = engine();
    let (mut ours, theirs) = tokio::io::duplex(MAX_FRAME);
    let serving = serve_peer(server, theirs, SECRET, BINDING, None, ServeBudgets::serving(), None);
    let asking = async {
        open_handshake(&asker, &mut ours, SECRET, BINDING).await.unwrap();
        let ask = Message::AskVersions { facts: None, facts_gen: None, echo: None };
        write_frame(&mut ours, &ask).await.unwrap();
        let answer = read_frame(&mut ours).await.unwrap();
        drop(ours);
        answer
    };
    let (_served, answer) =
        tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(serving, asking) })
            .await
            .expect("the contact ends");
    match answer {
        Message::Vectors { newest_epochs, .. } => newest_epochs,
        other => panic!("expected the vectors, got {other:?}"),
    }
}

/// Row C: the relay is cut by the cap, so only the member's own table can tell. The server knows
/// the newer epoch of the old host's node from the new host's own handshake, and holds more rows
/// than the cap, all starting later, so the cap cuts the old host's row out of the relay. A relay
/// of at most 1,024 rows lacks it, and the old host, dialling the server, is told all the same.
#[tokio::test]
async fn a_relay_cut_by_the_cap_leaves_the_members_own_table_to_tell() {
    let (old, new, _dirs) = an_old_and_a_new_host_of_one_node();
    let (server, _server_dir) = engine();
    let (opened, accepted, _ours, _theirs) = handshake(&new, &server).await;
    let _ = (opened, accepted);

    let now = kimmy_storage::physical_now_ms();
    let filler = |i: u32| {
        let mut bytes = [0xEE; 16];
        bytes[..4].copy_from_slice(&i.to_be_bytes());
        NodeId::from_bytes(bytes)
    };
    for i in 0..1_100u32 {
        let start = Hlc::new(now + 10_000 + u64::from(i), 0);
        server.note_epoch(filler(i), Epoch::new(start, [2; 8])).unwrap();
    }
    assert_eq!(
        server.newest_epoch(filler(1_099)).unwrap().start_hlc().wall_ms,
        now + 10_000 + 1_099,
        "premise: the table holds the rows"
    );

    let relayed = relayed_rows_of(&server).await;
    assert!(relayed.len() <= 1_024, "the relay is capped: {} rows", relayed.len());
    assert!(
        !relayed.iter().any(|(node, _)| *node == old.node_id()),
        "premise: the cap cut the old host's row out of the relay"
    );

    let (_opened, _accepted, mut ours, _theirs) = handshake(&old, &server).await;
    let tell = next_frame(&mut ours, Duration::from_secs(2)).await;
    assert!(
        matches!(&tell, Some(Message::Superseded { node, epoch })
            if *node == old.node_id() && *epoch == new.epoch()),
        "the member's own table tells the old host: {tell:?}"
    );
}
