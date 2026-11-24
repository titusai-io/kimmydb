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

    /// The instants the offsets are read at: eras, the one millisecond either side of the transitions of 2024 and of a few earlier years that the zones below have, and the far ends.
    const INSTANTS: [(&str, i64); 49] = [
        ("1800-01-01T00:00:00Z", -5364662400000),
        ("1850-06-15T12:00:00Z", -3772526400000),
        ("1883-11-18T16:59:59.999Z", -2717650800001),
        ("1883-11-18T17:00:00Z", -2717650800000),
        ("1900-01-01T00:00:00Z", -2208988800000),
        ("1918-03-31T07:00:00Z -1ms", -1633280400001),
        ("1918-03-31T07:00:00Z", -1633280400000),
        ("1945-08-15T12:00:00Z", -769348800000),
        ("1969-12-31T23:59:59.999Z", -1),
        ("1970-01-01T00:00:00Z", 0),
        ("1986-01-01T00:00:00Z", 504921600000),
        ("2000-01-01T00:00:00Z", 946684800000),
        ("2007-03-11T07:00:00Z -1ms", 1173596399999),
        ("2007-03-11T07:00:00Z", 1173596400000),
        ("2011-12-30T10:00:00Z -1ms", 1325239199999),
        ("2011-12-30T10:00:00Z", 1325239200000),
        ("2022-03-21T20:30:00Z -1ms", 1647894599999),
        ("2022-03-21T20:30:00Z", 1647894600000),
        ("2024-03-10T02:00:00Z -1ms", 1710035999999),
        ("2024-03-10T02:00:00Z", 1710036000000),
        ("2024-03-10T07:00:00Z -1ms", 1710053999999),
        ("2024-03-10T07:00:00Z", 1710054000000),
        ("2024-03-31T01:00:00Z -1ms", 1711846799999),
        ("2024-03-31T01:00:00Z", 1711846800000),
        ("2024-04-06T14:45:00Z -1ms", 1712414699999),
        ("2024-04-06T14:45:00Z", 1712414700000),
        ("2024-04-06T15:00:00Z -1ms", 1712415599999),
        ("2024-04-06T15:00:00Z", 1712415600000),
        ("2024-04-06T16:00:00Z -1ms", 1712419199999),
        ("2024-04-06T16:00:00Z", 1712419200000),
        ("2024-04-14T02:00:00Z -1ms", 1713059999999),
        ("2024-04-14T02:00:00Z", 1713060000000),
        ("2024-07-01T00:00:00Z", 1719792000000),
        ("2024-09-28T14:45:00Z -1ms", 1727534699999),
        ("2024-09-28T14:45:00Z", 1727534700000),
        ("2024-10-05T16:00:00Z -1ms", 1728143999999),
        ("2024-10-05T16:00:00Z", 1728144000000),
        ("2024-10-06T15:30:00Z -1ms", 1728228599999),
        ("2024-10-06T15:30:00Z", 1728228600000),
        ("2024-10-27T01:00:00Z -1ms", 1729990799999),
        ("2024-10-27T01:00:00Z", 1729990800000),
        ("2024-11-03T06:00:00Z -1ms", 1730613599999),
        ("2024-11-03T06:00:00Z", 1730613600000),
        ("2024-12-31T23:59:59.999Z", 1735689599999),
        ("2038-01-19T03:14:07.999Z", 2147483647999),
        ("2050-07-01T12:00:00Z", 2540289600000),
        ("2099-12-31T23:59:59.999Z", 4102444799999),
        ("2100-03-14T12:00:00Z", 4108708800000),
        ("2100-12-31T23:59:59.999Z", 4133980799999),
    ];

    // -- the tz database and the library that reads it, pinned ------------------------------------------------------------------------
    //
    // `tz_golden.txt` is every answer this module takes from the tz crate and the bundled database: the offset of 16 zones at the
    // instants below (eras, the millisecond either side of transitions, the far ends and the clamp), the bounds of the instants the
    // crate represents, and, for every name the database lists, whether `Zone::parse` accepts it and the offset it reads at 21 instants
    // from 1850 to 2100. A new tz crate or a new database changes the file, and the change has to be explained line by line by the
    // database's release notes (a rule moved, a zone added, a link renamed) and never by the crate's code.
    //
    // To write the file anew after such a change: `KIMMY_BLESS_TZ_GOLDEN=1 cargo test -p kimmy-query tz_golden`.

    /// The instants every name the database lists is read at: 1850, 1900, and then a winter or summer day of 1930, 1950, 1970, 1990, 2000, 2010,
    /// 2020, 2024 (both), 2026, 2030 (both), 2060 and 2100, at noon UTC, with the days a recent database release moved: 1965-03-31 and
    /// 1966-03-01 (Brazil's DST ended at other hours, 2026f) and the winter of 2026 to 2027 and the end of 2027 (Cuba, 2026f).
    const NAME_INSTANTS: [i64; 21] = [
        -3772526400000,
        -2208945600000,
        -1246622400000,
        -629899200000,
        -150033600000,
        -121120200000,
        15681600000,
        632404800000,
        962452800000,
        1263556800000,
        1593604800000,
        1705320000000,
        1719835200000,
        1782907200000,
        1796126400000,
        1800014400000,
        1827662400000,
        1894708800000,
        1909137600000,
        2855908800000,
        4118126400000,
    ];

    const ZONES: [&str; 16] = [
        "America/New_York",
        "America/Sao_Paulo",
        "America/St_Johns",
        "Europe/London",
        "Europe/Berlin",
        "Europe/Moscow",
        "Africa/Casablanca",
        "Asia/Kolkata",
        "Asia/Kathmandu",
        "Asia/Tehran",
        "Australia/Sydney",
        "Australia/Lord_Howe",
        "Pacific/Apia",
        "Pacific/Chatham",
        "Pacific/Kiritimati",
        "Etc/GMT+5",
    ];

    fn golden_text() -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let min = min_ms();
        let max = max_ms();
        writeln!(out, "bounds min_ms={min} max_ms={max}").unwrap();
        // The far ends: the bounds themselves, one inside, and one outside (read at the edge, `offset_at` clamps).
        let mut instants: Vec<(String, i64)> =
            INSTANTS.iter().map(|(label, ms)| (label.to_string(), *ms)).collect();
        instants.push(("min_ms".into(), min));
        instants.push(("min_ms+1".into(), min + 1));
        instants.push(("max_ms".into(), max));
        instants.push(("max_ms-1".into(), max - 1));
        instants.push(("i64::MIN".into(), i64::MIN));
        instants.push(("i64::MAX".into(), i64::MAX));
        for zone in ZONES {
            let parsed = Zone::parse(zone).unwrap_or_else(|e| panic!("{zone}: {e}"));
            for (label, ms) in &instants {
                writeln!(out, "offset {zone} {label} {}", parsed.offset_at(*ms)).unwrap();
            }
        }
        let mut names: Vec<&str> = jiff_tzdb::available().collect();
        names.sort_unstable();
        writeln!(out, "names {}", names.len()).unwrap();
        for name in names {
            match Zone::parse(name) {
                Ok(zone) => {
                    let read: Vec<String> =
                        NAME_INSTANTS.iter().map(|ms| zone.offset_at(*ms).to_string()).collect();
                    writeln!(out, "name {name} {}", read.join(" ")).unwrap()
                }
                Err(_) => writeln!(out, "name {name} REFUSED").unwrap(),
            }
        }
        out
    }

    #[test]
    fn tz_golden_the_offsets_the_bounds_and_the_accepted_names_are_what_the_file_pins() {
        let got = golden_text();
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/expr/tz_golden.txt");
        if std::env::var_os("KIMMY_BLESS_TZ_GOLDEN").is_some() {
            std::fs::write(&path, &got).unwrap();
            return;
        }
        let want = include_str!("tz_golden.txt");
        if got == want {
            return;
        }
        let (got_lines, want_lines): (Vec<&str>, Vec<&str>) =
            (got.lines().collect(), want.lines().collect());
        let in_want: std::collections::HashSet<&str> = want_lines.iter().copied().collect();
        let in_got: std::collections::HashSet<&str> = got_lines.iter().copied().collect();
        let added: Vec<&&str> =
            got_lines.iter().filter(|l| !in_want.contains(**l)).take(40).collect();
        let removed: Vec<&&str> =
            want_lines.iter().filter(|l| !in_got.contains(**l)).take(40).collect();
        panic!(
            "the tz answers changed ({} lines now, {} pinned). Explain each by the tz database's release notes, then \
             KIMMY_BLESS_TZ_GOLDEN=1.\nnow, not pinned (first 40):\n{}\npinned, not now (first 40):\n{}",
            got_lines.len(),
            want_lines.len(),
            added.iter().map(|l| format!("  + {l}")).collect::<Vec<_>>().join("\n"),
            removed.iter().map(|l| format!("  - {l}")).collect::<Vec<_>>().join("\n"),
        );
    }
}
