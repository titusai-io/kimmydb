//! Time the walk a member makes to serve a peer that already holds everything,
//! on a copy of a real store, cold (ADR-197). A test tool: an example, so no
//! release builds or ships it.
//!
//! ```text
//! serve_walk_bench <path/to/kimmy.redb> <keys|linear> [--cache-mib N] [--io-stat PATH]
//!                  [--last-rows N]
//! ```
//!
//! Opens the store (a **copy**: opening may write), drops its pages from the
//! operating system's cache, then drains it the way a peer that holds every
//! entry does: window after window from the start of the oplog, each under the
//! serve budget, until the walk reaches the end. Every row is passed over, so
//! the walk costs what reading the rows costs and serves nothing, which is the
//! restarted-member case. `keys` reads the windows from the keys of
//! `OPLOG_ARRIVAL_SEQ`; `linear` reads the oplog itself, as 0.42.0 does, so
//! one binary and one store answer the comparison.
//!
//! With `--last-rows N` the drain covers only the newest N entries (from the
//! entry N arrival positions before the newest), like a requester that holds
//! everything but trails the tail by that many rows; without it, the whole
//! oplog.
//!
//! With `--io-stat` (default `/sys/fs/cgroup/io.stat`, the container's own
//! cgroup) it also reports the read operations and bytes the drain caused, so
//! coldness is shown by the disk and not assumed. Prints one JSON line.
//!
//! The redb cache is `--cache-mib` (default 8, the smallest the store
//! allows), so what the open itself reads stays small beside the walk.

use std::path::PathBuf;
use std::time::Instant;

use kimmy_core::Hlc;
use kimmy_storage::{Engine, ExamineBudget, ServeWalk, WalkPath};

fn main() {
    if let Err(e) = run() {
        eprintln!("serve_walk_bench: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let path =
        PathBuf::from(args.next().ok_or("usage: serve_walk_bench <kimmy.redb> <keys|linear>")?);
    let mode = args.next().ok_or("usage: serve_walk_bench <kimmy.redb> <keys|linear>")?;
    let linear = match mode.as_str() {
        "keys" => false,
        "linear" => true,
        other => return Err(format!("the path is keys or linear, not {other:?}")),
    };
    let mut cache_mib = 8usize;
    let mut io_stat = PathBuf::from("/sys/fs/cgroup/io.stat");
    let mut last_rows: Option<u64> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--cache-mib" => {
                cache_mib =
                    args.next().and_then(|v| v.parse().ok()).ok_or("--cache-mib needs a number")?;
            }
            "--last-rows" => {
                last_rows = Some(
                    args.next().and_then(|v| v.parse().ok()).ok_or("--last-rows needs a number")?,
                );
            }
            "--io-stat" => io_stat = PathBuf::from(args.next().ok_or("--io-stat needs a path")?),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }

    kimmy_storage::set_test_serve_walk_linear(linear);
    let opened = Instant::now();
    let engine = Engine::open_with_cache(&path, Some(cache_mib * 1024 * 1024))
        .map_err(|e| format!("opening {}: {e}", path.display()))?;
    let open_s = opened.elapsed().as_secs_f64();

    // Everything this store holds, as the requester's witnessed vector: every
    // row is then one the requester has.
    let held = engine.witnessed_vector().map_err(|e| e.to_string())?;

    let start = match last_rows {
        Some(rows) => engine.arrival_tail_start(rows).map_err(|e| e.to_string())?,
        None => Hlc::ZERO,
    };
    drop_os_cache(&path)?;
    let io_before = read_io(&io_stat);
    let started = Instant::now();
    let (mut windows, mut from) = (0u64, start);
    let mut slowest = 0.0f64;
    loop {
        let window_started = Instant::now();
        let window = engine
            .serve_entries_to_peer(from, 1024, Some(&held), &[], Some(ExamineBudget::serve()))
            .map_err(|e| e.to_string())?;
        slowest = slowest.max(window_started.elapsed().as_secs_f64());
        windows += 1;
        if window.exhausted {
            break;
        }
        // The next window starts where this one stopped: on the stamp it
        // passed through, or on the last stamp it examined.
        let next = window.passed_through.map_or(window.scanned_to, |stamp| stamp.hlc);
        if next <= from && windows > 1 {
            return Err(format!("the drain stopped advancing at {from:?}"));
        }
        from = next;
    }
    let wall_s = started.elapsed().as_secs_f64();
    let io_after = read_io(&io_stat);

    let snapshot = engine.serve_cost();
    let paths: Vec<String> = WalkPath::ALL
        .iter()
        .map(|p| format!("\"{}\":{}", p.label(), snapshot.paths[ServeWalk::Serve.slot()][p.slot()]))
        .collect();
    println!(
        "{{\"mode\":\"{mode}\",\"cache_mib\":{cache_mib},\"last_rows\":{},\"open_s\":{open_s:.3},\"windows\":{windows},\
         \"rows_examined\":{},\"wall_s\":{wall_s:.3},\"slowest_window_s\":{slowest:.3},\
         \"walk_read_bytes\":{},\"walk_read_s\":{:.3},\"paths\":{{{}}},\"io\":{}}}",
        last_rows.map_or("null".to_string(), |r| r.to_string()),
        snapshot.passed,
        snapshot.read_bytes,
        snapshot.read_ns as f64 / 1e9,
        paths.join(","),
        match (io_before, io_after) {
            (Some(b), Some(a)) => format!(
                "{{\"read_ios\":{},\"read_bytes\":{}}}",
                a.0.saturating_sub(b.0),
                a.1.saturating_sub(b.1)
            ),
            _ => "null".into(),
        }
    );
    Ok(())
}

/// Ask the kernel to drop this file's cached pages, so the walk reads them from
/// the disk. Works on clean pages without privilege; the caller shows that it
/// worked with the cgroup's read counters. Linux only: elsewhere the walk is
/// warm and the numbers say nothing about a cold disk.
#[cfg(target_os = "linux")]
fn drop_os_cache(path: &std::path::Path) -> Result<(), String> {
    use std::os::fd::AsRawFd;
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    // SAFETY: an open descriptor, and a whole-file advice that changes no data.
    let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    if rc != 0 {
        return Err(format!("posix_fadvise(DONTNEED) failed: errno {rc}"));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn drop_os_cache(_: &std::path::Path) -> Result<(), String> {
    eprintln!("serve_walk_bench: the operating system's cache cannot be dropped here; warm run");
    Ok(())
}

/// The read operations and bytes a cgroup v2 `io.stat` reports, summed over its
/// devices, or `None` when the file is not there.
fn read_io(path: &std::path::Path) -> Option<(u64, u64)> {
    let text = std::fs::read_to_string(path).ok()?;
    let (mut ios, mut bytes) = (0u64, 0u64);
    for line in text.lines() {
        for field in line.split_whitespace() {
            if let Some(v) = field.strip_prefix("rios=") {
                ios += v.parse::<u64>().ok()?;
            } else if let Some(v) = field.strip_prefix("rbytes=") {
                bytes += v.parse::<u64>().ok()?;
            }
        }
    }
    Some((ios, bytes))
}
