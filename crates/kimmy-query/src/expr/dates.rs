//! The date operators (ADR-261).
//!
//! `$year $month $dayOfMonth $hour $minute $second $millisecond $dayOfWeek
//! $dayOfYear $week $isoWeek $isoWeekYear $isoDayOfWeek`, `$dateToString`,
//! `$dateFromString`, `$dateFromParts`, `$dateToParts`, `$dateAdd`,
//! `$dateSubtract`, `$dateDiff` and `$dateTrunc`, every one of them with the
//! reference's `timezone` argument.
//!
//! # How the arithmetic is done
//!
//! A date is UTC milliseconds. To read it in a zone, the zone's offset at that
//! instant is added, giving **local milliseconds**: a number that, read as if it
//! were UTC, shows the wall clock. All calendar work (parts, months, weeks, ISO
//! years) happens on local milliseconds with the proleptic Gregorian calendar
//! written out below, so a date many thousands of years from now still has a
//! year, a week and a day. Going back, the zone resolves a wall clock to an
//! instant (`tz.rs` has the rule for the hour that happens twice and the hour
//! that never happens). The tz crate is consulted for offsets only.
//!
//! # Null, missing and wrong types
//!
//! A null or missing date, and a null or missing `timezone`, give null; a value
//! of the wrong type is an error. A `timezone` that names no zone is an error
//! even beside a null date. Every operator that takes a date also takes a
//! Timestamp (its seconds) and an ObjectId (its creation time).

use bson::Bson;
use kimmy_core::{Error, Result};

use super::tz::Zone;
use super::{Declared, Expr, Scope, as_integer, type_name};

const DAY_MS: i64 = 86_400_000;

/// Days from 1970-01-01 to 2000-01-01, the origin `$dateTrunc` bins count from.
const DAYS_TO_2000: i64 = 10_957;

/// Which date operator a node is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Year,
    Month,
    DayOfMonth,
    Hour,
    Minute,
    Second,
    Millisecond,
    DayOfWeek,
    DayOfYear,
    Week,
    IsoWeek,
    IsoWeekYear,
    IsoDayOfWeek,
    ToString,
    FromString,
    FromParts,
    ToParts,
    Add,
    Subtract,
    Diff,
    Trunc,
}

impl Kind {
    pub(super) fn from_name(name: &str) -> Option<Kind> {
        Some(match name {
            "$year" => Kind::Year,
            "$month" => Kind::Month,
            "$dayOfMonth" => Kind::DayOfMonth,
            "$hour" => Kind::Hour,
            "$minute" => Kind::Minute,
            "$second" => Kind::Second,
            "$millisecond" => Kind::Millisecond,
            "$dayOfWeek" => Kind::DayOfWeek,
            "$dayOfYear" => Kind::DayOfYear,
            "$week" => Kind::Week,
            "$isoWeek" => Kind::IsoWeek,
            "$isoWeekYear" => Kind::IsoWeekYear,
            "$isoDayOfWeek" => Kind::IsoDayOfWeek,
            "$dateToString" => Kind::ToString,
            "$dateFromString" => Kind::FromString,
            "$dateFromParts" => Kind::FromParts,
            "$dateToParts" => Kind::ToParts,
            "$dateAdd" => Kind::Add,
            "$dateSubtract" => Kind::Subtract,
            "$dateDiff" => Kind::Diff,
            "$dateTrunc" => Kind::Trunc,
            _ => return None,
        })
    }

    fn name(self) -> &'static str {
        match self {
            Kind::Year => "$year",
            Kind::Month => "$month",
            Kind::DayOfMonth => "$dayOfMonth",
            Kind::Hour => "$hour",
            Kind::Minute => "$minute",
            Kind::Second => "$second",
            Kind::Millisecond => "$millisecond",
            Kind::DayOfWeek => "$dayOfWeek",
            Kind::DayOfYear => "$dayOfYear",
            Kind::Week => "$week",
            Kind::IsoWeek => "$isoWeek",
            Kind::IsoWeekYear => "$isoWeekYear",
            Kind::IsoDayOfWeek => "$isoDayOfWeek",
            Kind::ToString => "$dateToString",
            Kind::FromString => "$dateFromString",
            Kind::FromParts => "$dateFromParts",
            Kind::ToParts => "$dateToParts",
            Kind::Add => "$dateAdd",
            Kind::Subtract => "$dateSubtract",
            Kind::Diff => "$dateDiff",
            Kind::Trunc => "$dateTrunc",
        }
    }

    /// The argument names the operator takes, and which of them it needs.
    fn arguments(self) -> (&'static [&'static str], &'static [&'static str]) {
        match self {
            Kind::ToString => (&["date", "format", "timezone", "onNull"], &["date"]),
            Kind::FromString => {
                (&["dateString", "format", "timezone", "onError", "onNull"], &["dateString"])
            }
            Kind::FromParts => (
                &[
                    "year",
                    "month",
                    "day",
                    "hour",
                    "minute",
                    "second",
                    "millisecond",
                    "isoWeekYear",
                    "isoWeek",
                    "isoDayOfWeek",
                    "timezone",
                ],
                &[],
            ),
            Kind::ToParts => (&["date", "timezone", "iso8601"], &["date"]),
            Kind::Add | Kind::Subtract => {
                (&["startDate", "unit", "amount", "timezone"], &["startDate", "unit", "amount"])
            }
            Kind::Diff => (
                &["startDate", "endDate", "unit", "timezone", "startOfWeek"],
                &["startDate", "endDate", "unit"],
            ),
            Kind::Trunc => {
                (&["date", "unit", "binSize", "timezone", "startOfWeek"], &["date", "unit"])
            }
            _ => (&["date", "timezone"], &["date"]),
        }
    }
}

/// A date operator with its arguments, as parsed.
#[derive(Clone, Debug, PartialEq)]
pub struct DateExpr {
    kind: Kind,
    args: Vec<(&'static str, Expr)>,
    /// The `timezone` when it is a constant, resolved once at parse time so a
    /// bad name fails the pipeline and not the millionth row.
    zone: Option<Zone>,
}

impl PartialEq for Zone {
    fn eq(&self, other: &Zone) -> bool {
        match (self, other) {
            (Zone::Utc, Zone::Utc) => true,
            (Zone::Fixed(a), Zone::Fixed(b)) => a == b,
            (Zone::Named(a), Zone::Named(b)) => a == b,
            _ => false,
        }
    }
}

/// An argument as evaluated.
enum Arg {
    Absent,
    Null,
    Val(Bson),
}

fn nullish(v: &Bson) -> bool {
    matches!(v, Bson::Null | Bson::Undefined)
}

fn err<T>(message: String) -> Result<T> {
    Err(Error::InvalidQuery(message))
}

impl DateExpr {
    /// `{$name: <date>}`, `{$name: [<date>]}` for the single-argument forms, or
    /// `{$name: {<argument>: <expr>, ...}}`.
    pub(super) fn parse(name: &str, raw: &Bson, declared: &mut Declared) -> Result<DateExpr> {
        let kind = Kind::from_name(name).expect("the caller checked the name");
        let (allowed, required) = kind.arguments();
        let simple = allowed == ["date", "timezone"];

        let mut args = Vec::new();
        let named = match raw {
            Bson::Document(doc) if !doc.keys().next().is_some_and(|k| k.starts_with('$')) => {
                Some(doc)
            }
            _ => None,
        };
        if let Some(doc) = named {
            for (key, value) in doc {
                let Some(canon) = allowed.iter().find(|a| **a == key.as_str()) else {
                    return err(format!("Unrecognized argument to {name}: {key}"));
                };
                args.push((*canon, Expr::parse_in(value, declared)?));
            }
        } else if simple {
            let single = match raw {
                Bson::Array(items) if items.len() == 1 => &items[0],
                Bson::Array(items) => {
                    return err(format!(
                        "{name} takes exactly 1 argument(s), found {}",
                        items.len()
                    ));
                }
                other => other,
            };
            args.push(("date", Expr::parse_in(single, declared)?));
        } else {
            return err(format!("{name} only supports an object as its argument"));
        }
        for need in required {
            if !args.iter().any(|(k, _)| k == need) {
                return err(format!("Missing '{need}' parameter to {name}"));
            }
        }
        if kind == Kind::FromParts {
            check_parts_arguments(&args)?;
        }

        let mut zone = None;
        if let Some((_, Expr::Literal(v))) = args.iter().find(|(k, _)| *k == "timezone") {
            match v {
                Bson::String(s) => zone = Some(Zone::parse(s)?),
                Bson::Null | Bson::Undefined => {}
                other => {
                    return err(format!(
                        "timezone must evaluate to a string, found {}",
                        type_name(other)
                    ));
                }
            }
        }
        let this = DateExpr { kind, args, zone };
        // A constant format is checked now, as the reference does.
        if let (Kind::ToString | Kind::FromString, Some(Expr::Literal(Bson::String(f)))) =
            (kind, this.get("format"))
        {
            check_format(kind, f)?;
        }
        Ok(this)
    }

    pub(super) fn children(&self) -> impl Iterator<Item = &Expr> {
        self.args.iter().map(|(_, e)| e)
    }

    fn get(&self, name: &str) -> Option<&Expr> {
        self.args.iter().find(|(k, _)| *k == name).map(|(_, e)| e)
    }

    fn arg(&self, name: &str, scope: &Scope<'_>) -> Result<Arg> {
        match self.get(name) {
            None => Ok(Arg::Absent),
            Some(e) => {
                let v = e.eval_in(scope)?;
                Ok(if nullish(&v) { Arg::Null } else { Arg::Val(v) })
            }
        }
    }

    /// The zone: `Ok(None)` is a null `timezone`, which makes the result null.
    fn zone(&self, scope: &Scope<'_>) -> Result<Option<Zone>> {
        if let Some(z) = &self.zone {
            return Ok(Some(z.clone()));
        }
        match self.arg("timezone", scope)? {
            Arg::Absent => Ok(Some(Zone::Utc)),
            Arg::Null => Ok(None),
            Arg::Val(Bson::String(s)) => Zone::parse(&s).map(Some),
            Arg::Val(other) => {
                err(format!("timezone must evaluate to a string, found {}", type_name(&other)))
            }
        }
    }

    pub(super) fn eval(&self, scope: &Scope<'_>) -> Result<Bson> {
        let op = self.kind.name();
        match self.kind {
            Kind::ToString => self.eval_to_string(scope),
            Kind::FromString => self.eval_from_string(scope),
            Kind::FromParts => self.eval_from_parts(scope),
            Kind::ToParts => self.eval_to_parts(scope),
            Kind::Add | Kind::Subtract => self.eval_add(scope),
            Kind::Diff => self.eval_diff(scope),
            Kind::Trunc => self.eval_trunc(scope),
            _ => {
                let date = self.arg("date", scope)?;
                let Some(zone) = self.zone(scope)? else { return Ok(Bson::Null) };
                let Arg::Val(date) = date else { return Ok(Bson::Null) };
                let ms = date_ms(&date, op)?;
                let p = Parts::of(&zone, ms);
                Ok(match self.kind {
                    Kind::Year => Bson::Int32(p.year as i32),
                    Kind::Month => Bson::Int32(p.month as i32),
                    Kind::DayOfMonth => Bson::Int32(p.day as i32),
                    Kind::Hour => Bson::Int32(p.hour as i32),
                    Kind::Minute => Bson::Int32(p.minute as i32),
                    Kind::Second => Bson::Int32(p.second as i32),
                    Kind::Millisecond => Bson::Int32(p.milli as i32),
                    Kind::DayOfWeek => Bson::Int32(p.dow_sunday1() as i32),
                    Kind::DayOfYear => Bson::Int32(p.day_of_year() as i32),
                    Kind::Week => Bson::Int32(p.week_of_year() as i32),
                    Kind::IsoWeek => Bson::Int32(p.iso().1 as i32),
                    Kind::IsoWeekYear => Bson::Int64(p.iso().0),
                    Kind::IsoDayOfWeek => Bson::Int32(iso_dow(p.days) as i32),
                    _ => unreachable!("handled above"),
                })
            }
        }
    }

    // -- $dateToString -------------------------------------------------

    fn eval_to_string(&self, scope: &Scope<'_>) -> Result<Bson> {
        let format = match self.arg("format", scope)? {
            Arg::Absent => None,
            Arg::Null => return Ok(Bson::Null),
            Arg::Val(Bson::String(f)) => {
                if !matches!(self.get("format"), Some(Expr::Literal(_))) {
                    check_format(Kind::ToString, &f)?;
                }
                Some(f)
            }
            Arg::Val(other) => {
                return err(format!(
                    "$dateToString requires that 'format' be a string, found: {}",
                    type_name(&other)
                ));
            }
        };
        let date = self.arg("date", scope)?;
        let Some(zone) = self.zone(scope)? else { return Ok(Bson::Null) };
        let Arg::Val(date) = date else {
            return match self.get("onNull") {
                Some(e) => e.eval_in(scope),
                None => Ok(Bson::Null),
            };
        };
        let ms = date_ms(&date, "$dateToString")?;
        let p = Parts::of(&zone, ms);
        let default = if zone.is_utc() { "%Y-%m-%dT%H:%M:%S.%LZ" } else { "%Y-%m-%dT%H:%M:%S.%L" };
        Ok(Bson::String(format_parts(format.as_deref().unwrap_or(default), &p)?))
    }

    // -- $dateFromString -----------------------------------------------

    fn eval_from_string(&self, scope: &Scope<'_>) -> Result<Bson> {
        let text = self.arg("dateString", scope)?;
        let format = match self.arg("format", scope)? {
            Arg::Absent => None,
            Arg::Null => Some(None),
            Arg::Val(Bson::String(f)) => {
                check_format(Kind::FromString, &f)?;
                Some(Some(f))
            }
            Arg::Val(other) => {
                return err(format!(
                    "$dateFromString requires that 'format' be a string, found: {}",
                    type_name(&other)
                ));
            }
        };
        // The zone is read before a null input is looked at, so a bad zone name
        // is an error whatever the string is.
        let zone = self.zone(scope)?;
        let Arg::Val(text) = text else {
            return match self.get("onNull") {
                Some(e) => e.eval_in(scope),
                None => Ok(Bson::Null),
            };
        };
        let (Some(zone), true) = (zone, !matches!(format, Some(None))) else {
            return Ok(Bson::Null);
        };
        let explicit_zone = self.get("timezone").is_some();
        let parsed = match &text {
            Bson::String(s) => {
                parse_string(s, format.as_ref().and_then(|f| f.as_deref()), &zone, explicit_zone)
            }
            other => err(format!(
                "$dateFromString requires that 'dateString' be a string, found: {} with value {:?}",
                type_name(other),
                other
            )),
        };
        match parsed {
            Ok(ms) => Ok(Bson::DateTime(bson::DateTime::from_millis(ms))),
            Err(e) => match self.get("onError") {
                // Only a failure to read the string is caught, which is every
                // error left at this point.
                Some(on_error) => on_error.eval_in(scope),
                None => Err(e),
            },
        }
    }

    // -- $dateFromParts ------------------------------------------------

    fn eval_from_parts(&self, scope: &Scope<'_>) -> Result<Bson> {
        let op = "$dateFromParts";
        let iso = self.get("isoWeekYear").is_some();

        // `Ok(None)` is a null part: the result is null.
        let int = |name: &str, default: i64, min: i64, max: i64| -> Result<Option<i64>> {
            match self.arg(name, scope)? {
                Arg::Absent => Ok(Some(default)),
                Arg::Null => Ok(None),
                Arg::Val(v) => {
                    let n = as_integer(&v, &format!("{op} '{name}'")).map_err(|_| {
                        Error::InvalidQuery(format!(
                            "'{name}' must evaluate to an integer, found {} with value {}",
                            type_name(&v),
                            short(&v)
                        ))
                    })?;
                    if n < min || n > max {
                        return err(if name == "year" || name == "isoWeekYear" {
                            format!(
                                "'{name}' must evaluate to an integer in the range 1 to 9999, found {n}"
                            )
                        } else {
                            format!(
                                "'{name}' must evaluate to a value in the range [{min}, {max}]; value {n} is not in range"
                            )
                        });
                    }
                    Ok(Some(n))
                }
            }
        };
        const PART: (i64, i64) = (-32_768, 32_767);

        let mut vals = [0i64; 8];
        let y_name = if iso { "isoWeekYear" } else { "year" };
        let Some(year) = int(y_name, 1970, 1, 9999)? else { return Ok(Bson::Null) };
        if iso {
            let Some(w) = int("isoWeek", 1, PART.0, PART.1)? else { return Ok(Bson::Null) };
            let Some(d) = int("isoDayOfWeek", 1, PART.0, PART.1)? else { return Ok(Bson::Null) };
            vals[0] = w;
            vals[1] = d;
        } else {
            let Some(m) = int("month", 1, PART.0, PART.1)? else { return Ok(Bson::Null) };
            let Some(d) = int("day", 1, PART.0, PART.1)? else { return Ok(Bson::Null) };
            vals[0] = m;
            vals[1] = d;
        }
        for (i, name) in ["hour", "minute", "second", "millisecond"].into_iter().enumerate() {
            let Some(v) = int(name, 0, PART.0, PART.1)? else { return Ok(Bson::Null) };
            vals[2 + i] = v;
        }
        let Some(zone) = self.zone(scope)? else { return Ok(Bson::Null) };

        let days = if iso {
            iso_week1_monday(year) + (vals[0] - 1) * 7 + (vals[1] - 1)
        } else {
            let months = year * 12 + (vals[0] - 1);
            days_from_civil(months.div_euclid(12), months.rem_euclid(12) + 1, 1) + (vals[1] - 1)
        };
        let local =
            days * DAY_MS + vals[2] * 3_600_000 + vals[3] * 60_000 + vals[4] * 1_000 + vals[5];
        Ok(Bson::DateTime(bson::DateTime::from_millis(zone.local_to_utc(local))))
    }

    // -- $dateToParts --------------------------------------------------

    fn eval_to_parts(&self, scope: &Scope<'_>) -> Result<Bson> {
        let date = self.arg("date", scope)?;
        let Some(zone) = self.zone(scope)? else { return Ok(Bson::Null) };
        let iso = match self.arg("iso8601", scope)? {
            Arg::Absent => false,
            Arg::Null => return Ok(Bson::Null),
            Arg::Val(Bson::Boolean(b)) => b,
            Arg::Val(other) => {
                return err(format!(
                    "iso8601 must evaluate to a bool, found {}",
                    type_name(&other)
                ));
            }
        };
        let Arg::Val(date) = date else { return Ok(Bson::Null) };
        let ms = date_ms(&date, "$dateToParts")?;
        let p = Parts::of(&zone, ms);
        let i = |n: i64| Bson::Int32(n as i32);
        let mut out = bson::Document::new();
        if iso {
            let (y, w) = p.iso();
            out.insert("isoWeekYear", Bson::Int32(y as i32));
            out.insert("isoWeek", i(w));
            out.insert("isoDayOfWeek", i(iso_dow(p.days)));
        } else {
            out.insert("year", i(p.year));
            out.insert("month", i(p.month));
            out.insert("day", i(p.day));
        }
        out.insert("hour", i(p.hour));
        out.insert("minute", i(p.minute));
        out.insert("second", i(p.second));
        out.insert("millisecond", i(p.milli));
        Ok(Bson::Document(out))
    }

    // -- $dateAdd, $dateSubtract ---------------------------------------

    fn eval_add(&self, scope: &Scope<'_>) -> Result<Bson> {
        let op = self.kind.name();
        let Arg::Val(start) = self.arg("startDate", scope)? else { return Ok(Bson::Null) };
        let start =
            date_like(&start, &format!("{op} requires startDate to be convertible to a date"))?;
        let Arg::Val(unit) = self.arg("unit", scope)? else { return Ok(Bson::Null) };
        let unit = Unit::read(&unit, op)?;
        let Arg::Val(amount) = self.arg("amount", scope)? else { return Ok(Bson::Null) };
        let amount = as_integer(&amount, "amount").map_err(|_| {
            Error::InvalidQuery(format!("{op} expects integer amount of time units"))
        })?;
        let Some(zone) = self.zone(scope)? else { return Ok(Bson::Null) };
        let amount = if self.kind == Kind::Subtract {
            amount.checked_neg().ok_or_else(|| overflow(op))?
        } else {
            amount
        };
        add_units(start, unit, amount, &zone, op)
            .map(|ms| Bson::DateTime(bson::DateTime::from_millis(ms)))
    }

    // -- $dateDiff -----------------------------------------------------

    fn eval_diff(&self, scope: &Scope<'_>) -> Result<Bson> {
        let op = "$dateDiff";
        let Arg::Val(start) = self.arg("startDate", scope)? else { return Ok(Bson::Null) };
        let start = date_like(&start, "$dateDiff requires startDate to be convertible to a date")?;
        let Arg::Val(end) = self.arg("endDate", scope)? else { return Ok(Bson::Null) };
        let end = date_like(&end, "$dateDiff requires endDate to be convertible to a date")?;
        let Arg::Val(unit) = self.arg("unit", scope)? else { return Ok(Bson::Null) };
        let unit = Unit::read(&unit, op)?;
        let Some(zone) = self.zone(scope)? else { return Ok(Bson::Null) };
        let week_start = match self.arg("startOfWeek", scope)? {
            Arg::Val(v) if unit == Unit::Week => Some(read_week_day(&v, op)?),
            Arg::Null if unit == Unit::Week => return Ok(Bson::Null),
            _ => None,
        };
        let n = diff_units(start, end, unit, &zone, week_start.unwrap_or(0));
        Ok(Bson::Int64(n))
    }

    // -- $dateTrunc ----------------------------------------------------

    fn eval_trunc(&self, scope: &Scope<'_>) -> Result<Bson> {
        let op = "$dateTrunc";
        let Arg::Val(date) = self.arg("date", scope)? else { return Ok(Bson::Null) };
        let date = date_like(&date, "$dateTrunc requires date to be convertible to a date")?;
        let Arg::Val(unit) = self.arg("unit", scope)? else { return Ok(Bson::Null) };
        let unit = Unit::read(&unit, op)?;
        let bin = match self.arg("binSize", scope)? {
            Arg::Absent => 1,
            Arg::Null => return Ok(Bson::Null),
            Arg::Val(v) => {
                let n = as_integer(&v, "binSize").map_err(|_| {
                    Error::InvalidQuery(
                        "$dateTrunc requires 'binSize' to be a 64-bit integer".to_string(),
                    )
                })?;
                if n <= 0 {
                    return err("$dateTrunc requires 'binSize' to be greater than 0".into());
                }
                n
            }
        };
        let Some(zone) = self.zone(scope)? else { return Ok(Bson::Null) };
        let week_start = match self.arg("startOfWeek", scope)? {
            Arg::Val(v) if unit == Unit::Week => read_week_day(&v, op)?,
            Arg::Null if unit == Unit::Week => return Ok(Bson::Null),
            _ => 0,
        };
        Ok(Bson::DateTime(bson::DateTime::from_millis(trunc_units(
            date, unit, bin, &zone, week_start,
        ))))
    }
}

/// `year` with `isoWeekYear`, and the parts of one family with the other, are
/// refused when the operator is parsed.
fn check_parts_arguments(args: &[(&'static str, Expr)]) -> Result<()> {
    let has = |n: &str| args.iter().any(|(k, _)| *k == n);
    let iso = has("isoWeekYear");
    let civil = has("year");
    if iso && (civil || has("month") || has("day")) {
        return err(
            "$dateFromParts: Cannot specify both ISO week-year parts and year/month/day".into()
        );
    }
    if !iso && (has("isoWeek") || has("isoDayOfWeek")) {
        return err("$dateFromParts: 'isoWeek' and 'isoDayOfWeek' need 'isoWeekYear'".into());
    }
    if !iso && !civil {
        return err("$dateFromParts requires either 'year' or 'isoWeekYear' to be present".into());
    }
    Ok(())
}

fn short(v: &Bson) -> String {
    match v {
        Bson::String(s) => format!("{s:?}"),
        other => other.to_string(),
    }
}

fn overflow(op: &str) -> Error {
    Error::InvalidQuery(format!("Overflow occurred when computing {op}"))
}

// ---------------------------------------------------------------------------
// Reading a date
// ---------------------------------------------------------------------------

/// UTC milliseconds of a date, a Timestamp or an ObjectId; `None` for anything else.
fn as_millis(v: &Bson) -> Option<i64> {
    match v {
        Bson::DateTime(dt) => Some(dt.timestamp_millis()),
        Bson::Timestamp(ts) => Some(i64::from(ts.time) * 1000),
        Bson::ObjectId(oid) => Some(oid.timestamp().timestamp_millis()),
        _ => None,
    }
}

fn date_ms(v: &Bson, op: &str) -> Result<i64> {
    as_millis(v)
        .ok_or_else(|| Error::InvalidQuery(format!("{op} needs a date, found {}", type_name(v))))
}

fn date_like(v: &Bson, message: &str) -> Result<i64> {
    as_millis(v).ok_or_else(|| Error::InvalidQuery(message.to_string()))
}

/// The millisecond rendering `$toString` and `$convert` use for a date: the
/// default `$dateToString` in UTC.
pub(super) fn format_iso_utc(ms: i64) -> Result<String> {
    format_parts("%Y-%m-%dT%H:%M:%S.%LZ", &Parts::of(&Zone::Utc, ms))
}

// ---------------------------------------------------------------------------
// The calendar
// ---------------------------------------------------------------------------

/// Days since 1970-01-01 of a proleptic Gregorian date (Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if is_leap(y) => 29,
        _ => 28,
    }
}

/// Monday = 1 .. Sunday = 7; 1970-01-01 was a Thursday.
fn iso_dow(days: i64) -> i64 {
    (days + 3).rem_euclid(7) + 1
}

/// Sunday = 0 .. Saturday = 6.
fn dow_sunday0(days: i64) -> i64 {
    (days + 4).rem_euclid(7)
}

/// The day number of Monday of ISO week 1 of `iso_year` (the week with 4 January).
fn iso_week1_monday(iso_year: i64) -> i64 {
    let jan4 = days_from_civil(iso_year, 1, 4);
    jan4 - (iso_dow(jan4) - 1)
}

/// A date read in a zone.
struct Parts {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    milli: i64,
    /// Days since the epoch of the local date.
    days: i64,
    /// Seconds east of UTC.
    offset: i32,
}

impl Parts {
    fn of(zone: &Zone, utc_ms: i64) -> Parts {
        let offset = zone.offset_at(utc_ms);
        Self::of_local(utc_ms.saturating_add(i64::from(offset) * 1000), offset)
    }

    fn of_local(local: i64, offset: i32) -> Parts {
        let days = local.div_euclid(DAY_MS);
        let tod = local.rem_euclid(DAY_MS);
        let (year, month, day) = civil_from_days(days);
        Parts {
            year,
            month,
            day,
            hour: tod / 3_600_000,
            minute: tod / 60_000 % 60,
            second: tod / 1_000 % 60,
            milli: tod % 1_000,
            days,
            offset,
        }
    }

    /// 1 = Sunday.
    fn dow_sunday1(&self) -> i64 {
        dow_sunday0(self.days) + 1
    }

    fn day_of_year(&self) -> i64 {
        self.days - days_from_civil(self.year, 1, 1) + 1
    }

    /// Weeks begin on Sunday; days before the year's first Sunday are week 0.
    fn week_of_year(&self) -> i64 {
        (self.day_of_year() - 1 + 7 - dow_sunday0(self.days)) / 7
    }

    /// ISO week-year and week number.
    fn iso(&self) -> (i64, i64) {
        let thursday = self.days - (iso_dow(self.days) - 1) + 3;
        let (year, _, _) = civil_from_days(thursday);
        (year, (thursday - days_from_civil(year, 1, 1)) / 7 + 1)
    }
}

// ---------------------------------------------------------------------------
// Units
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Unit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
}

impl Unit {
    fn read(v: &Bson, op: &str) -> Result<Unit> {
        let Bson::String(s) = v else {
            return err(format!("{op} expects unit to be a string"));
        };
        Ok(match s.as_str() {
            "year" => Unit::Year,
            "quarter" => Unit::Quarter,
            "month" => Unit::Month,
            "week" => Unit::Week,
            "day" => Unit::Day,
            "hour" => Unit::Hour,
            "minute" => Unit::Minute,
            "second" => Unit::Second,
            "millisecond" => Unit::Millisecond,
            other => {
                return err(format!(
                    "{op} expects unit to be one of year, quarter, week, month, day, hour, \
                     minute, second, millisecond, but got {other}"
                ));
            }
        })
    }

    /// Length of a fixed-length unit in milliseconds (`None` for the calendar units).
    fn fixed_ms(self) -> Option<i64> {
        match self {
            Unit::Hour => Some(3_600_000),
            Unit::Minute => Some(60_000),
            Unit::Second => Some(1_000),
            Unit::Millisecond => Some(1),
            _ => None,
        }
    }
}

/// `startOfWeek`: a day name or its three-letter form, in any case. Sunday = 0.
fn read_week_day(v: &Bson, op: &str) -> Result<i64> {
    let Bson::String(s) = v else {
        return err(format!("{op} requires 'startOfWeek' to be a string, found {}", type_name(v)));
    };
    Ok(match s.to_ascii_lowercase().as_str() {
        "sunday" | "sun" => 0,
        "monday" | "mon" => 1,
        "tuesday" | "tue" => 2,
        "wednesday" | "wed" => 3,
        "thursday" | "thu" => 4,
        "friday" | "fri" => 5,
        "saturday" | "sat" => 6,
        _ => {
            return err(format!(
                "{op} parameter 'startOfWeek' value must be a day of the week, but got {s}"
            ));
        }
    })
}

/// The UTC instant `amount` units after `start`.
///
/// The calendar units work on the wall clock: a month from 31 January is the
/// last day of February, and a day across a clock change keeps its wall time.
/// An hour, minute, second or millisecond is elapsed time.
fn add_units(start: i64, unit: Unit, amount: i64, zone: &Zone, op: &str) -> Result<i64> {
    let amount = i128::from(amount);
    if let Some(len) = unit.fixed_ms() {
        let ms = i128::from(start) + amount * i128::from(len);
        return i64::try_from(ms).map_err(|_| overflow(op));
    }
    let p = Parts::of(zone, start);
    let tod = start.saturating_add(i64::from(p.offset) * 1000).rem_euclid(DAY_MS);
    let local_days: i128 = match unit {
        Unit::Week => i128::from(p.days) + amount * 7,
        Unit::Day => i128::from(p.days) + amount,
        _ => {
            let per = match unit {
                Unit::Year => 12,
                Unit::Quarter => 3,
                _ => 1,
            };
            let months = i128::from(p.year) * 12 + i128::from(p.month - 1) + amount * per;
            let year = i64::try_from(months.div_euclid(12)).map_err(|_| overflow(op))?;
            let month = months.rem_euclid(12) as i64 + 1;
            // Beyond this the day count would not fit a date anyway.
            if year.unsigned_abs() > 292_000_000 {
                return Err(overflow(op));
            }
            let day = p.day.min(days_in_month(year, month));
            i128::from(days_from_civil(year, month, day))
        }
    };
    let local = local_days * i128::from(DAY_MS) + i128::from(tod);
    let local = i64::try_from(local).map_err(|_| overflow(op))?;
    Ok(zone.local_to_utc(local))
}

/// The UTC instant `amount` calendar or elapsed `unit`s after `start`, for the stages
/// that step through dates (`$setWindowFields` range windows, `$densify`). `unit` is one
/// of the nine unit names; the zone is UTC, so a day and a week are fixed lengths.
pub(crate) fn add_to_date(start: i64, unit: &str, amount: i64, op: &str) -> Result<i64> {
    let unit = Unit::read(&Bson::String(unit.to_string()), op)?;
    add_units(start, unit, amount, &Zone::Utc, op)
}

/// Whether `unit` names one of the nine date units.
pub(crate) fn is_date_unit(unit: &str) -> bool {
    Unit::read(&Bson::String(unit.to_string()), "").is_ok()
}

/// The length of `unit` in milliseconds in UTC, or `None` for month, quarter and year,
/// which have no fixed length.
pub(crate) fn unit_millis(unit: &str) -> Option<i64> {
    match unit {
        "week" => Some(7 * DAY_MS),
        "day" => Some(DAY_MS),
        "hour" => Some(3_600_000),
        "minute" => Some(60_000),
        "second" => Some(1_000),
        "millisecond" => Some(1),
        _ => None,
    }
}

/// The number of unit boundaries crossed going from `start` to `end`.
fn diff_units(start: i64, end: i64, unit: Unit, zone: &Zone, week_start: i64) -> i64 {
    if let Some(len) = unit.fixed_ms() {
        // Boundaries of the zone's own hour, minute...: truncate both ends.
        let a = trunc_fixed(start, len, 1, zone);
        let b = trunc_fixed(end, len, 1, zone);
        return (b - a) / len;
    }
    let a = Parts::of(zone, start);
    let b = Parts::of(zone, end);
    match unit {
        Unit::Year => b.year - a.year,
        Unit::Quarter => (b.year - a.year) * 4 + (b.month - 1) / 3 - (a.month - 1) / 3,
        Unit::Month => (b.year - a.year) * 12 + b.month - a.month,
        Unit::Week => (week_start_day(b.days, week_start) - week_start_day(a.days, week_start)) / 7,
        _ => b.days - a.days,
    }
}

/// The day number of the start of the week containing `days`.
fn week_start_day(days: i64, week_start: i64) -> i64 {
    days - (dow_sunday0(days) - week_start).rem_euclid(7)
}

/// Truncate to a bin of `bin` fixed-length units counted from local
/// 2000-01-01T00:00:00, keeping the instant's own offset (so the second
/// occurrence of a repeated hour truncates to the second).
fn trunc_fixed(utc: i64, len: i64, bin: i64, zone: &Zone) -> i64 {
    let offset = i64::from(zone.offset_at(utc)) * 1000;
    let local = utc.saturating_add(offset);
    let origin = DAYS_TO_2000 * DAY_MS;
    let size = len.saturating_mul(bin);
    let into = (local - origin).rem_euclid(size);
    utc - into
}

fn trunc_units(date: i64, unit: Unit, bin: i64, zone: &Zone, week_start: i64) -> i64 {
    if let Some(len) = unit.fixed_ms() {
        return trunc_fixed(date, len, bin, zone);
    }
    let p = Parts::of(zone, date);
    let floor_bin = |n: i64| n.div_euclid(bin) * bin;
    let days = match unit {
        Unit::Year => days_from_civil(floor_bin(p.year - 2000) + 2000, 1, 1),
        Unit::Quarter => {
            let q = floor_bin((p.year - 2000) * 4 + (p.month - 1) / 3);
            days_from_civil(2000 + q.div_euclid(4), q.rem_euclid(4) * 3 + 1, 1)
        }
        Unit::Month => {
            let m = floor_bin((p.year - 2000) * 12 + p.month - 1);
            days_from_civil(2000 + m.div_euclid(12), m.rem_euclid(12) + 1, 1)
        }
        Unit::Week => {
            // Bins count from the first `startOfWeek` day on or after 2000-01-01.
            let origin = DAYS_TO_2000 + (week_start - dow_sunday0(DAYS_TO_2000)).rem_euclid(7);
            let start = week_start_day(p.days, week_start);
            origin + (start - origin).div_euclid(7 * bin) * 7 * bin
        }
        _ => DAYS_TO_2000 + (p.days - DAYS_TO_2000).div_euclid(bin) * bin,
    };
    zone.local_to_utc(days.saturating_mul(DAY_MS))
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

/// Whether a specifier is one `$dateToString` / `$dateFromString` takes.
fn check_format(kind: Kind, format: &str) -> Result<()> {
    let op = kind.name();
    let allowed: &str = if kind == Kind::ToString { "YmdHMSLjwUVGuzZ%" } else { "dGHjLmMSuVYzZ%" };
    let mut chars = format.chars();
    let mut seen = String::new();
    while let Some(c) = chars.next() {
        if c != '%' {
            continue;
        }
        match chars.next() {
            Some(s) if allowed.contains(s) => seen.push(s),
            Some(s) => return err(format!("Invalid format character '%{s}' in format string")),
            None => return err("Unmatched '%' at end of format string".to_string()),
        }
    }
    if kind == Kind::FromString {
        if seen.contains(['G', 'V', 'u']) && seen.contains(['Y', 'm', 'd', 'j']) {
            return err(format!(
                "{op}: format cannot mix ISO week date and calendar date specifiers"
            ));
        }
        if seen.contains('z') && seen.contains('Z') {
            return err(format!("{op}: format cannot hold both %z and %Z"));
        }
        if seen.contains('j') && seen.contains(['m', 'd']) {
            return err(format!("{op}: format cannot hold %j with %m or %d"));
        }
    }
    Ok(())
}

fn format_parts(format: &str, p: &Parts) -> Result<String> {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(format.len() + 8);
    let mut chars = format.chars();
    let in_range = |name: &str, y: i64| -> Result<i64> {
        if (0..=9999).contains(&y) {
            Ok(y)
        } else {
            err(format!(
                "Could not convert date to string: date component was outside the supported \
                 range of 0-9999: {y} ({name})"
            ))
        }
    };
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let _ = match chars.next() {
            Some('Y') => write!(out, "{:04}", in_range("year", p.year)?),
            Some('m') => write!(out, "{:02}", p.month),
            Some('d') => write!(out, "{:02}", p.day),
            Some('H') => write!(out, "{:02}", p.hour),
            Some('M') => write!(out, "{:02}", p.minute),
            Some('S') => write!(out, "{:02}", p.second),
            Some('L') => write!(out, "{:03}", p.milli),
            Some('j') => write!(out, "{:03}", p.day_of_year()),
            Some('w') => write!(out, "{}", p.dow_sunday1()),
            Some('u') => write!(out, "{}", iso_dow(p.days)),
            Some('U') => write!(out, "{:02}", p.week_of_year()),
            Some('V') => write!(out, "{:02}", p.iso().1),
            Some('G') => write!(out, "{:04}", in_range("isoWeekYear", p.iso().0)?),
            Some('z') => {
                let mins = i64::from(p.offset.abs()) / 60;
                write!(
                    out,
                    "{}{:02}{:02}",
                    if p.offset < 0 { '-' } else { '+' },
                    mins / 60,
                    mins % 60
                )
            }
            Some('Z') => {
                let mins = i64::from(p.offset.abs()) / 60;
                write!(out, "{}{}", if p.offset < 0 { '-' } else { '+' }, mins)
            }
            Some('%') => {
                out.push('%');
                Ok(())
            }
            Some(other) => {
                return err(format!("Invalid format character '%{other}' in format string"));
            }
            None => return err("Unmatched '%' at end of format string".to_string()),
        };
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// $dateFromString
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Read {
    year: Option<i64>,
    month: Option<i64>,
    day: Option<i64>,
    yday: Option<i64>,
    iso_year: Option<i64>,
    iso_week: Option<i64>,
    iso_dow: Option<i64>,
    hour: Option<i64>,
    minute: Option<i64>,
    second: Option<i64>,
    milli: Option<i64>,
    /// Seconds east of UTC, when the string carries one.
    offset: Option<i32>,
}

struct Cursor<'a> {
    text: &'a str,
    pos: usize,
}

impl Cursor<'_> {
    fn fail<T>(&self, why: &str) -> Result<T> {
        err(format!("Error parsing date string '{}'; {}: {why}", self.text, self.pos))
    }

    fn digits(&mut self, n: usize, what: &str) -> Result<i64> {
        let rest = &self.text.as_bytes()[self.pos..];
        if rest.len() < n || !rest[..n].iter().all(u8::is_ascii_digit) {
            return self.fail(&format!("Failed to parse {what}: expected {n} digits"));
        }
        self.pos += n;
        Ok(rest[..n].iter().fold(0, |a, b| a * 10 + i64::from(b - b'0')))
    }

    fn eat(&mut self, c: u8) -> bool {
        if self.text.as_bytes().get(self.pos) == Some(&c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.pos).copied()
    }

    /// `+hh`, `+hhmm` or `+hh:mm`.
    fn offset(&mut self) -> Result<i32> {
        let sign = match self.peek() {
            Some(b'+') => 1,
            Some(b'-') => -1,
            _ => return self.fail("Failed to parse a UTC offset"),
        };
        self.pos += 1;
        let hh = self.digits(2, "the offset hours")?;
        let mm = if self.eat(b':') || self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.digits(2, "the offset minutes")?
        } else {
            0
        };
        if hh > 23 || mm > 59 {
            return self.fail("The UTC offset is out of range");
        }
        Ok(sign * (hh * 3600 + mm * 60) as i32)
    }
}

fn parse_with_format(text: &str, format: &str) -> Result<Read> {
    let mut r = Read::default();
    let mut cur = Cursor { text, pos: 0 };
    let mut spec = format.chars();
    while let Some(c) = spec.next() {
        if c != '%' {
            let mut buf = [0u8; 4];
            for &b in c.encode_utf8(&mut buf).as_bytes() {
                if !cur.eat(b) {
                    return cur.fail(&format!("Literal '{c}' in the format did not match"));
                }
            }
            continue;
        }
        match spec.next() {
            Some('d') => r.day = Some(cur.digits(2, "day of month")?),
            Some('G') => r.iso_year = Some(cur.digits(4, "ISO week year")?),
            Some('H') => r.hour = Some(cur.digits(2, "hour")?),
            Some('j') => r.yday = Some(cur.digits(3, "day of year")?),
            Some('L') => r.milli = Some(cur.digits(3, "millisecond")?),
            Some('m') => r.month = Some(cur.digits(2, "month")?),
            Some('M') => r.minute = Some(cur.digits(2, "minute")?),
            Some('S') => r.second = Some(cur.digits(2, "second")?),
            Some('u') => r.iso_dow = Some(cur.digits(1, "ISO day of week")?),
            Some('V') => r.iso_week = Some(cur.digits(2, "ISO week")?),
            Some('Y') => r.year = Some(cur.digits(4, "year")?),
            Some('z') => r.offset = Some(cur.offset()?),
            Some('Z') => {
                let sign = match cur.peek() {
                    Some(b'+') => 1,
                    Some(b'-') => -1,
                    _ => return cur.fail("Failed to parse the minutes offset"),
                };
                cur.pos += 1;
                let start = cur.pos;
                while cur.pos - start < 3 && cur.peek().is_some_and(|b| b.is_ascii_digit()) {
                    cur.pos += 1;
                }
                if cur.pos == start {
                    return cur.fail("Failed to parse the minutes offset");
                }
                let mins: i64 = text[start..cur.pos].parse().unwrap_or(0);
                r.offset = Some(sign * (mins * 60) as i32);
            }
            Some('%') => {
                if !cur.eat(b'%') {
                    return cur.fail("Literal '%' in the format did not match");
                }
            }
            _ => return cur.fail("Unknown format specifier"),
        }
    }
    if cur.pos != text.len() {
        return cur.fail("Trailing data after the format was consumed");
    }
    Ok(r)
}

/// With no format: `YYYY-MM-DD`, optionally `T` (or a space) and `HH:MM[:SS[.fff]]`,
/// optionally `Z`, `UTC`, `GMT` or an offset.
fn parse_iso_text(text: &str) -> Result<Read> {
    let mut r = Read::default();
    let mut cur = Cursor { text, pos: 0 };
    r.year = Some(cur.digits(4, "year")?);
    if !cur.eat(b'-') {
        return cur.fail("Expected '-' after the year");
    }
    r.month = Some(cur.digits(2, "month")?);
    if !cur.eat(b'-') {
        return cur.fail("Expected '-' after the month");
    }
    r.day = Some(cur.digits(2, "day")?);
    if cur.peek().is_some() {
        if !(cur.eat(b'T') || cur.eat(b't') || cur.eat(b' ')) {
            return cur.fail("Expected 'T' or a space before the time");
        }
        r.hour = Some(cur.digits(2, "hour")?);
        if !cur.eat(b':') {
            return cur.fail("Expected ':' after the hour");
        }
        r.minute = Some(cur.digits(2, "minute")?);
        if cur.eat(b':') {
            r.second = Some(cur.digits(2, "second")?);
            if cur.eat(b'.') || cur.eat(b',') {
                let start = cur.pos;
                while cur.peek().is_some_and(|b| b.is_ascii_digit()) {
                    cur.pos += 1;
                }
                let frac = &text[start..cur.pos];
                if frac.is_empty() {
                    return cur.fail("Expected digits after the decimal point");
                }
                let three: String = frac.chars().chain("000".chars()).take(3).collect();
                r.milli = Some(three.parse().unwrap_or(0));
            }
        }
        let _ = cur.eat(b' ');
        match cur.peek() {
            None => {}
            Some(b'Z' | b'z') => {
                cur.pos += 1;
                r.offset = Some(0);
            }
            Some(b'+' | b'-') => r.offset = Some(cur.offset()?),
            Some(_)
                if text[cur.pos..].eq_ignore_ascii_case("UTC")
                    || text[cur.pos..].eq_ignore_ascii_case("GMT") =>
            {
                cur.pos += 3;
                r.offset = Some(0);
            }
            Some(_) => return cur.fail("Unexpected characters after the time"),
        }
    }
    if cur.pos != text.len() {
        return cur.fail("Trailing data");
    }
    Ok(r)
}

fn parse_string(text: &str, format: Option<&str>, zone: &Zone, explicit_zone: bool) -> Result<i64> {
    let r = match format {
        Some(f) => parse_with_format(text, f)?,
        None => parse_iso_text(text)?,
    };
    if r.offset.is_some() && explicit_zone {
        return err(format!(
            "you cannot pass in a date/time string with time zone information ('{text}') \
             together with a timezone argument"
        ));
    }
    let bad = |why: &str| err(format!("Error parsing date string '{text}'; {why}"));

    let h = r.hour.unwrap_or(0);
    let mi = r.minute.unwrap_or(0);
    let s = r.second.unwrap_or(0);
    let ms = r.milli.unwrap_or(0);
    if h > 23 || mi > 59 || s > 59 {
        return bad("The parsed time was invalid");
    }

    let iso_used = r.iso_year.is_some() || r.iso_week.is_some() || r.iso_dow.is_some();
    let days = if iso_used {
        let Some(iy) = r.iso_year else {
            return bad(
                "an incomplete date/time string has been found, with elements missing: ISO week requires %G",
            );
        };
        let w = r.iso_week.unwrap_or(1);
        let d = r.iso_dow.unwrap_or(1);
        if !(1..=7).contains(&d) {
            return bad("The parsed ISO day of week was invalid");
        }
        let weeks_in_year = (iso_week1_monday(iy + 1) - iso_week1_monday(iy)) / 7;
        if !(1..=weeks_in_year).contains(&w) {
            return bad("The parsed ISO week was invalid");
        }
        iso_week1_monday(iy) + (w - 1) * 7 + (d - 1)
    } else if let Some(yd) = r.yday {
        let y = r.year.unwrap_or(1970);
        let n = if is_leap(y) { 366 } else { 365 };
        if !(1..=n).contains(&yd) {
            return bad("The parsed day of year was invalid");
        }
        days_from_civil(y, 1, 1) + yd - 1
    } else {
        let y = r.year.unwrap_or(1970);
        let m = r.month.unwrap_or(1);
        let d = r.day.unwrap_or(1);
        if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
            return bad("The parsed date was invalid");
        }
        days_from_civil(y, m, d)
    };
    let local = days * DAY_MS + h * 3_600_000 + mi * 60_000 + s * 1_000 + ms;
    Ok(match r.offset {
        Some(off) => local - i64::from(off) * 1000,
        None => zone.local_to_utc(local),
    })
}
