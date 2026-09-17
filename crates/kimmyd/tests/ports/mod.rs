//! Ports for a spawned `kimmyd`, chosen so the operating system cannot give
//! them away between the choosing and the binding.
//!
//! A node's HTTP port needs choosing by nobody: it binds port 0 and the
//! harness reads the port it was given from the node's log
//! ([`bound_http_port`]). A cluster node's port is different: it is picked
//! before any node starts, because seed lists name each other, and handed to
//! a node that binds it moments later. The harness used to take a port the kernel had just
//! handed out for an ephemeral bind and release it; in the gap, the kernel was
//! free to hand that same port to any outbound connection as its source port.
//! On a CI runner it did: a peer connection took 35341, and the node meant to
//! listen there exited with `Address already in use` while the harness waited
//! out its whole patience budget for it.
//!
//! So ports come from below every platform's ephemeral range (Linux
//! 32768-60999, macOS 49152-65535), where no outbound connection is given a
//! source port. What can still take one is another process that binds it on
//! purpose, a parallel test process among them; the port is checked free when
//! chosen, each process starts from its own place in the range, and a harness
//! that sees its node exit with the port in use starts again on new ports
//! ([`in_use`]) rather than waiting.

// Shared by the test binaries that spawn nodes, each of which uses part of it.
#![allow(dead_code)]

use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

/// The range ports are chosen from: below every ephemeral range.
const FROM: u32 = 20_000;
const TO: u32 = 32_768;

static NEXT: AtomicU32 = AtomicU32::new(0);

/// Listeners [`occupy_next`] holds, so the port stays taken.
static HELD: Mutex<Vec<std::net::TcpListener>> = Mutex::new(Vec::new());

thread_local! {
    /// How many of this thread's next choices to take on purpose. Per thread,
    /// so one test's collision is not visited on a test running beside it.
    static OCCUPY_NEXT: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// A port no outbound connection can be given, free when chosen.
pub fn choose() -> u16 {
    if NEXT.load(Ordering::Relaxed) == 0 {
        // Where this process starts in the range: its pid and the clock, so
        // two test processes started together rarely walk the same ports.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let start = (std::process::id().wrapping_mul(2_654_435_761) ^ nanos) % (TO - FROM);
        let _ = NEXT.compare_exchange(0, start + 1, Ordering::Relaxed, Ordering::Relaxed);
    }
    for _ in 0..(TO - FROM) {
        let offset = NEXT.fetch_add(1, Ordering::Relaxed) % (TO - FROM);
        let port = (FROM + offset) as u16;
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            if OCCUPY_NEXT.with(|n| n.get()) > 0 {
                OCCUPY_NEXT.with(|n| n.set(n.get() - 1));
                // The collision this module exists to survive, made on
                // purpose: the port was free when chosen and is taken before
                // the node binds it.
                let held = std::net::TcpListener::bind(("127.0.0.1", port)).expect("still free");
                HELD.lock().unwrap().push(held);
            }
            return port;
        }
    }
    panic!("no free port between {FROM} and {TO}");
}

/// Take the next `n` ports [`choose`] returns before any node can bind them,
/// and keep them taken: for a test that the harness survives the collision.
pub fn occupy_next(n: u32) {
    OCCUPY_NEXT.with(|next| next.set(n));
}

/// The ports [`occupy_next`] took and still holds.
pub fn occupied() -> Vec<u16> {
    HELD.lock().unwrap().iter().filter_map(|l| l.local_addr().ok()).map(|a| a.port()).collect()
}

/// Whether a node's stderr says it exited because a port it was given was
/// taken: the one exit a harness answers by starting again on new ports.
pub fn in_use(stderr: &str) -> bool {
    stderr.contains("Address already in use")
}

/// The line a node logs once its HTTP listener is bound, carrying the port.
pub const BOUND_HTTP_LINE: &str = "serving HTTP and WebSocket";

/// What reading a node's HTTP port from its log found.
pub enum Bound {
    /// The port the node was given.
    Port(u16),
    /// Not logged yet, and the node is not listening yet either.
    NotYet,
    /// The harness cannot read the port, and waiting will not change that:
    /// the line is there without a `bind=` it can parse. What went wrong, for
    /// the panic.
    Unreadable(String),
    /// The node is listening and has not logged the line. The line follows
    /// the bind by moments, so this is ordinary for an instant and a changed
    /// or missing line if it lasts: [`LineWait`] tells the two apart.
    ListeningWithoutLine(Vec<u16>),
}

/// How long a node may listen without logging the line before the harness
/// reads that as the line gone. Far above the moments between the bind and
/// the line, far below any patience budget.
pub const LINE_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// Tracks [`Bound::ListeningWithoutLine`] across polls.
#[derive(Default)]
pub struct LineWait {
    since: Option<std::time::Instant>,
}

impl LineWait {
    /// `Err` with what went wrong once a node has listened without the line
    /// for [`LINE_GRACE`], or at once when the line cannot be read.
    pub fn judge(&mut self, bound: Bound, line: &str) -> Result<Option<u16>, String> {
        match bound {
            Bound::Port(port) => Ok(Some(port)),
            Bound::NotYet => {
                self.since = None;
                Ok(None)
            }
            Bound::Unreadable(why) => Err(why),
            Bound::ListeningWithoutLine(ports) => {
                let since = *self.since.get_or_insert_with(std::time::Instant::now);
                if since.elapsed() < LINE_GRACE {
                    return Ok(None);
                }
                Err(format!(
                    "the node has listened on {ports:?} for {LINE_GRACE:?} and never logged \
                     {line:?}, the line the harness reads its HTTP port from"
                ))
            }
        }
    }
}

/// The HTTP port a node that bound port 0 was given, from the line (`line`,
/// ordinarily [`BOUND_HTTP_LINE`]) it logs once it is listening.
///
/// Reading a port from a log couples the harness to that message, so a
/// change to it must fail promptly rather than as a wait for a port that will
/// never be read: a line without a readable `bind=` is [`Bound::Unreadable`],
/// and a node listening on a TCP port other than `known` (its cluster port)
/// with no such line logged is [`Bound::ListeningWithoutLine`].
pub fn bound_http_port(stdout_log: &std::path::Path, line: &str, pid: u32, known: &[u16]) -> Bound {
    let log = std::fs::read_to_string(stdout_log).unwrap_or_default();
    if let Some(found) = log.lines().find(|l| l.contains(line)) {
        let plain = strip_escapes(found);
        let port = plain
            .find("bind=")
            .map(|at| &plain[at + "bind=".len()..])
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|addr| addr.parse::<std::net::SocketAddr>().ok())
            .map(|addr| addr.port());
        return match port {
            Some(port) => Bound::Port(port),
            None => Bound::Unreadable(format!(
                "the node logged {line:?} without a bind=ADDR the harness can read: {plain}"
            )),
        };
    }
    match listening_ports(pid) {
        Some(ports) if ports.iter().any(|p| !known.contains(p)) => {
            Bound::ListeningWithoutLine(ports)
        }
        _ => Bound::NotYet,
    }
}

/// The TCP ports `pid` is listening on, from `lsof`; `None` where it cannot
/// be run, which leaves a missing log line to the patience budget.
pub fn listening_ports(pid: u32) -> Option<Vec<u16>> {
    let out = std::process::Command::new("lsof")
        .args(["-w", "-nP", "-a", "-p", &pid.to_string(), "-iTCP", "-sTCP:LISTEN", "-Fn"])
        .output()
        .ok()?;
    Some(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.strip_prefix('n'))
            .filter_map(|name| name.rsplit(':').next()?.parse().ok())
            .collect(),
    )
}

/// A log line with its colour escapes removed.
fn strip_escapes(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' && chars.peek() == Some(&'[') {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}
