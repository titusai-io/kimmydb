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
//!   `US/Eastern`), spelt in exactly the case the database spells it
//!   (`america/new_york` and `Utc` are refused, as the reference refuses them);
//! - `UTC` and `GMT`, which are UTC (`Z` is not a zone);
//! - a fixed offset, `+hh`, `+hhmm` or `+hh:mm` with `+` or `-`, where `hh` and
//!   `mm` are any two digits: `+24:00` is 24 hours, `+05:60` is 6 hours and `+99`
//!   is 99 hours, which is how the reference reads them.
//!
//! Anything else is `unrecognized time zone identifier`, the reference's one
//! answer for a bad name and a bad offset alike. A system tz database is never
//! read, so a name means the same thing on every host.
//!
//! # Wall-clock times that are not one instant
//!
//! When the clocks go back, a local time happens twice; [`Zone::local_to_utc`]
//! takes the **later** occurrence (standard time), as the reference does for
//! `$dateFromParts`, `$dateFromString` and the midnight of `$dateTrunc`.
//! [`Zone::local_to_utc_keeping`] is the `$dateAdd` rule: a result in a repeated
//! hour keeps the offset the start date had. When they go forward, a local time
//! never happens; it is read with the offset in force **before** the gap, which
//! moves it forward by the length of the gap (`02:30` on the spring-forward day
//! is `03:30`). Both rules are jiff's `later` disambiguation (a gap and a fold
//! agree on it) and the reference's answers in New York, Berlin, Sydney, Havana
//! and Lord Howe.

use jiff::Timestamp;
use jiff::civil::DateTime;
use jiff::tz::{AmbiguousOffset, TimeZone};
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
        let bad = || Error::InvalidQuery(format!("unrecognized time zone identifier: \"{name}\""));
        if name.starts_with(['+', '-']) {
            return parse_offset(name).ok_or_else(bad);
        }
        if matches!(name, "UTC" | "GMT") {
            return Ok(Zone::Utc);
        }
        // The database looks a name up without regard to case; the reference
        // does not, so the spelling must be the canonical one.
        match jiff_tzdb::get(name) {
            Some((canonical, _)) if canonical == name => {}
            _ => return Err(bad()),
        }
        jiff::tz::db().get(name).map(Zone::Named).map_err(|_| bad())
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
        self.resolve(local_ms, None)
    }

    /// As [`Zone::local_to_utc`], except that a wall clock which happens twice
    /// resolves to the occurrence whose offset is `offset` (seconds east of UTC)
    /// when one of the two has it: the `$dateAdd` rule.
    pub(super) fn local_to_utc_keeping(&self, local_ms: i64, offset: i32) -> i64 {
        self.resolve(local_ms, Some(offset))
    }

    fn resolve(&self, local_ms: i64, keep: Option<i32>) -> i64 {
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
                let ambiguous = tz.to_ambiguous_timestamp(dt);
                let resolved = match (keep, ambiguous.offset()) {
                    (Some(want), AmbiguousOffset::Fold { before, .. })
                        if before.seconds() == want =>
                    {
                        before.to_timestamp(dt)
                    }
                    _ => ambiguous.later(),
                };
                match resolved {
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

/// `+hh`, `+hhmm` or `+hh:mm`, each field any two ASCII digits. `None` for
/// anything else, a non-ASCII character in any place included.
fn parse_offset(text: &str) -> Option<Zone> {
    let sign = if text.starts_with('-') { -1 } else { 1 };
    // The sign is one byte; the rest is checked to be ASCII before it is cut by
    // byte position.
    let rest = text.get(1..)?;
    if !rest.is_ascii() {
        return None;
    }
    let b = rest.as_bytes();
    let two = |s: &[u8]| -> Option<i32> {
        (s.len() == 2 && s.iter().all(u8::is_ascii_digit))
            .then(|| i32::from(s[0] - b'0') * 10 + i32::from(s[1] - b'0'))
    };
    let (hh, mm) = match b.len() {
        2 => (two(b)?, 0),
        4 => (two(&b[..2])?, two(&b[2..])?),
        5 if b[2] == b':' => (two(&b[..2])?, two(&b[3..])?),
        _ => return None,
    };
    let secs = sign * (hh * 3600 + mm * 60);
    Some(if secs == 0 { Zone::Utc } else { Zone::Fixed(secs) })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every string over a small alphabet, up to `max` characters.
    fn strings(alphabet: &[&str], max: usize) -> Vec<String> {
        let mut all = vec![String::new()];
        let mut layer = vec![String::new()];
        for _ in 0..max {
            let mut next = Vec::with_capacity(layer.len() * alphabet.len());
            for prefix in &layer {
                for piece in alphabet {
                    next.push(format!("{prefix}{piece}"));
                }
            }
            all.extend(next.iter().cloned());
            layer = next;
        }
        all
    }

    /// What the offset grammar allows, written out independently of the parser:
    /// the sign, then `hh`, `hhmm` or `hh:mm` of ASCII digits. The seconds east.
    fn oracle(text: &str) -> Option<i32> {
        let sign = match text.chars().next()? {
            '+' => 1,
            '-' => -1,
            _ => return None,
        };
        let body: Vec<char> = text.chars().skip(1).collect();
        let digit = |c: char| c.is_ascii_digit().then(|| c as i32 - '0' as i32);
        let pair = |a: char, b: char| Some(digit(a)? * 10 + digit(b)?);
        let (hh, mm) = match body.as_slice() {
            [a, b] => (pair(*a, *b)?, 0),
            [a, b, c, d] => (pair(*a, *b)?, pair(*c, *d)?),
            [a, b, ':', c, d] => (pair(*a, *b)?, pair(*c, *d)?),
            _ => return None,
        };
        Some(sign * (hh * 3600 + mm * 60))
    }

    #[test]
    fn parse_offset_agrees_with_the_grammar_on_every_short_string() {
        // Multi-byte characters of 2, 3 and 4 bytes stand in every position of
        // every string up to five characters: the slices in `parse_offset` meet
        // a character boundary or the string is refused (B1).
        let alphabet = ["0", "5", "9", ":", "a", "é", "€", "😀", "٣"];
        let mut accepted = 0_u32;
        for sign in ["+", "-"] {
            for body in strings(&alphabet, 5) {
                let text = format!("{sign}{body}");
                let expect = oracle(&text);
                let got = Zone::parse(&text);
                match (expect, got) {
                    (Some(secs), Ok(zone)) => {
                        accepted += 1;
                        let want = if secs == 0 { Zone::Utc } else { Zone::Fixed(secs) };
                        assert_eq!(zone, want, "{text:?}");
                    }
                    (None, Err(Error::InvalidQuery(m))) => {
                        assert!(m.contains("unrecognized time zone identifier"), "{text:?}: {m}");
                    }
                    (expect, got) => panic!("{text:?}: oracle {expect:?}, parser {got:?}"),
                }
            }
        }
        // 2 signs times (hh, hhmm, hh:mm over the digits 0, 5, 9).
        assert_eq!(accepted, 2 * (9 + 81 + 81));
    }

    #[test]
    fn parse_zone_never_panics_on_a_name_with_multi_byte_characters() {
        let alphabet = ["A", "m", "/", "_", "é", "€", "😀", "Z", "+", "0"];
        let mut accepted = 0_u32;
        for name in strings(&alphabet, 4) {
            if Zone::parse(&name).is_ok() {
                accepted += 1;
            }
        }
        // Only the offsets and no real name: "+00" and "+0" forms are in the
        // alphabet, so a few strings are zones.
        assert!(accepted >= 1);
        // Real names are accepted in their exact case only.
        assert!(Zone::parse("America/New_York").is_ok());
        for wrong in ["america/new_york", "AMERICA/NEW_YORK", "America/new_york", "Z", "utc", "Gmt"]
        {
            assert!(Zone::parse(wrong).is_err(), "{wrong}");
        }
        // A real name with a multi-byte character spliced into every position.
        let name = "America/New_York";
        for (at, _) in name.char_indices() {
            for glyph in ["é", "€", "😀"] {
                let mut spliced = name.to_string();
                spliced.insert_str(at, glyph);
                assert!(Zone::parse(&spliced).is_err(), "{spliced}");
            }
        }
    }

    #[test]
    fn a_repeated_local_time_resolves_to_the_later_instant_or_the_kept_offset() {
        let ny = Zone::parse("America/New_York").expect("zone");
        // 2024-11-03 01:30 local, which happens at 05:30Z (EDT) and 06:30Z (EST).
        let local = 1_730_597_400_000;
        assert_eq!(ny.local_to_utc(local), 1_730_615_400_000);
        assert_eq!(ny.local_to_utc_keeping(local, -4 * 3600), 1_730_611_800_000);
        assert_eq!(ny.local_to_utc_keeping(local, -5 * 3600), 1_730_615_400_000);
        // A preference that fits neither occurrence gives the later one.
        assert_eq!(ny.local_to_utc_keeping(local, 3600), 1_730_615_400_000);
        // An unambiguous time ignores the preference.
        assert_eq!(
            ny.local_to_utc_keeping(1_720_000_000_000, -5 * 3600),
            1_720_000_000_000 + 4 * 3_600_000
        );
    }
}
