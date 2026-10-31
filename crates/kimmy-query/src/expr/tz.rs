//! Time zones for the date operators (ADR-261).
//!
//! A [`Zone`] answers two questions and nothing else: *what is this zone's
//! offset from UTC at this instant*, and *which instant is this wall-clock
//! time*. The calendar arithmetic that sits on top (`dates.rs`) works on
//! "local milliseconds", the instant shifted by the offset and read as if it
//! were UTC, so it never meets a year outside what a BSON date can hold.
//!
//! # What names are accepted
//!
//! - an IANA zone name from the tz database compiled into the binary
//!   (`America/New_York`, `Asia/Kolkata`, `Etc/GMT+5`, the legacy links such as
//!   `US/Eastern`), matched without regard to case;
//! - `UTC`, `GMT` and `Z`, which are UTC;
//! - a fixed offset: `+hh`, `+hhmm` or `+hh:mm`, with `+` or `-`.
//!
//! Anything else is `unrecognized time zone identifier`. A system tz database is
//! never read, so a name means the same thing on every host.
//!
//! # Wall-clock times that are not one instant
//!
//! When the clocks go back, a local time happens twice; the **first** (earlier)
//! occurrence is chosen. When they go forward, a local time never happens; it is
//! read with the offset in force **before** the gap, which moves it forward by
//! the length of the gap (`02:30` on the spring-forward day is `03:30`). Both are
//! what the reference does and what jiff calls its `compatible` rule.

use jiff::Timestamp;
use jiff::civil::DateTime;
use jiff::tz::TimeZone;
use kimmy_core::{Error, Result};

/// A resolved time zone.
#[derive(Clone, Debug)]
pub(super) enum Zone {
    /// UTC, spelt `UTC`, `GMT` or `Z`, or the absence of a zone argument.
    Utc,
    /// A fixed offset in seconds east of UTC.
    Fixed(i32),
    /// A named zone with its transitions.
    Named(TimeZone),
}

impl Zone {
    /// Read a `timezone` argument.
    pub(super) fn parse(name: &str) -> Result<Zone> {
        if name.starts_with(['+', '-']) {
            return parse_offset(name);
        }
        if matches!(name, "UTC" | "GMT" | "Z") {
            return Ok(Zone::Utc);
        }
        match jiff::tz::db().get(name) {
            Ok(tz) => Ok(Zone::Named(tz)),
            Err(_) => {
                Err(Error::InvalidQuery(format!("unrecognized time zone identifier: \"{name}\"")))
            }
        }
    }

    /// Whether this is UTC (the `Z` default format applies only then).
    pub(super) fn is_utc(&self) -> bool {
        matches!(self, Zone::Utc) || matches!(self, Zone::Fixed(0))
    }

    /// Seconds east of UTC at the instant `utc_ms`.
    pub(super) fn offset_at(&self, utc_ms: i64) -> i32 {
        match self {
            Zone::Utc => 0,
            Zone::Fixed(secs) => *secs,
            Zone::Named(tz) => {
                // Beyond the years the tz crate represents, the offset at the
                // edge of its range is used: far past is the zone's earliest
                // (mean solar) offset, far future its current rule.
                let clamped = utc_ms.clamp(min_ms(), max_ms());
                match Timestamp::from_millisecond(clamped) {
                    Ok(ts) => tz.to_offset(ts).seconds(),
                    Err(_) => 0,
                }
            }
        }
    }

    /// The instant (UTC milliseconds) at which a wall clock in this zone reads
    /// `local_ms`. See the module docs for gaps and folds.
    pub(super) fn local_to_utc(&self, local_ms: i64) -> i64 {
        match self {
            Zone::Utc => local_ms,
            Zone::Fixed(secs) => local_ms.saturating_sub(i64::from(*secs) * 1000),
            Zone::Named(tz) => {
                let outside =
                    local_ms < min_ms() + 86_400_000 * 2 || local_ms > max_ms() - 86_400_000 * 2;
                if outside {
                    let off = self.offset_at(local_ms);
                    return local_ms.saturating_sub(i64::from(off) * 1000);
                }
                let secs = local_ms.div_euclid(1000);
                let sub = local_ms.rem_euclid(1000);
                // The local reading as a civil date-time, via UTC as a ruler.
                let Ok(ts) = Timestamp::from_second(secs) else { return local_ms };
                let dt: DateTime = ts.to_zoned(TimeZone::UTC).datetime();
                match tz.to_ambiguous_timestamp(dt).compatible() {
                    Ok(resolved) => resolved.as_second() * 1000 + sub,
                    Err(_) => local_ms,
                }
            }
        }
    }
}

fn min_ms() -> i64 {
    Timestamp::MIN.as_millisecond()
}

fn max_ms() -> i64 {
    Timestamp::MAX.as_millisecond()
}

/// `+hh`, `+hhmm` or `+hh:mm`.
fn parse_offset(text: &str) -> Result<Zone> {
    let bad = || {
        Error::InvalidQuery(format!(
            "Invalid format for UTC offset: \"{text}\"; expected +/-hh, +/-hhmm or +/-hh:mm"
        ))
    };
    let sign = if text.starts_with('-') { -1 } else { 1 };
    let rest = &text[1..];
    let digits = |s: &str| -> Option<i32> {
        (s.len() == 2 && s.bytes().all(|b| b.is_ascii_digit())).then(|| s.parse().unwrap_or(0))
    };
    let (hh, mm) = match rest.len() {
        2 => (digits(rest), Some(0)),
        4 => (digits(&rest[..2]), digits(&rest[2..])),
        5 if rest.as_bytes()[2] == b':' => (digits(&rest[..2]), digits(&rest[3..])),
        _ => return Err(bad()),
    };
    let (Some(hh), Some(mm)) = (hh, mm) else { return Err(bad()) };
    if hh > 23 || mm > 59 {
        return Err(bad());
    }
    let secs = sign * (hh * 3600 + mm * 60);
    Ok(if secs == 0 { Zone::Utc } else { Zone::Fixed(secs) })
}
