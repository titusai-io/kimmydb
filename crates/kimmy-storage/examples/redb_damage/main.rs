//! Damage a stopped store's primary commit slot in place, to prove that a start
//! refuses it (ADR-190's redb 4.3 addendum). A test tool: an example, so no
//! release builds or ships it.
//!
//! ```text
//! redb_damage <path/to/kimmy.redb> [--order N] [--invalid-checksum]
//! ```
//!
//! Sets the primary slot's system-root page order to N (default 31). By
//! default it then recomputes the slot's checksum, so the damage is one only
//! redb's page-order check can refuse. With `--invalid-checksum` the checksum
//! is left stale, which redb refuses as a corrupted slot. It refuses anything
//! but a redb 4.x format-3 store, and a store another process holds, and
//! prints the slot bytes it changed.

#[path = "damage.rs"]
mod damage;

use std::io::{Read, Seek, SeekFrom, Write};

fn main() {
    if let Err(e) = run() {
        eprintln!("redb_damage: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let mut path = None;
    let mut order = 31u8;
    let mut valid_checksum = true;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--order" => {
                order = args
                    .next()
                    .and_then(|n| n.parse().ok())
                    .ok_or("--order takes a number from 0 to 31")?;
            }
            "--invalid-checksum" => valid_checksum = false,
            _ if path.is_none() && !arg.starts_with('-') => path = Some(arg),
            _ => return Err(format!("unexpected argument {arg:?}")),
        }
    }
    let path = path.ok_or("usage: redb_damage <kimmy.redb> [--order N] [--invalid-checksum]")?;

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|e| format!("{path}: {e}"))?;
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err(format!("{path} is held by another process; stop it first"));
        }
        Err(std::fs::TryLockError::Error(e)) => return Err(format!("{path}: locking: {e}")),
    }
    let mut header = Vec::with_capacity(damage::HEADER_LEN);
    (&mut file)
        .take(damage::HEADER_LEN as u64)
        .read_to_end(&mut header)
        .map_err(|e| format!("{path}: {e}"))?;
    damage::primary_slot(&header)
        .map_err(|e| format!("{path} is not a store this tool knows: {e}"))?;
    let god = header[damage::GOD_BYTE];
    let change = damage::set_primary_page_order(&mut header, order, valid_checksum)
        .map_err(|e| format!("{path} is not a store this tool knows: {e}"))?;
    file.seek(SeekFrom::Start(change.offset as u64)).map_err(|e| e.to_string())?;
    file.write_all(&change.after).map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;

    println!("{path}");
    println!(
        "  god byte {god:#04x}: primary slot at {}, two-phase commit {}, recovery required {}",
        change.offset,
        god & damage::TWO_PHASE_COMMIT != 0,
        god & damage::RECOVERY_REQUIRED != 0
    );
    println!(
        "  system-root page order set to {order}; slot checksum {} (verifies: {})",
        if valid_checksum { "recomputed" } else { "left stale" },
        damage::slot_checksum_valid(&header, change.offset)
    );
    for (i, (b, a)) in change.before.iter().zip(&change.after).enumerate() {
        if b != a {
            println!(
                "  slot byte {i:>3} (file offset {:>3}): {b:#04x} -> {a:#04x}",
                change.offset + i
            );
        }
    }
    Ok(())
}
