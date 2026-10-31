//! The date operators and their time zones (ADR-261), one row at a time.
//!
//! Expected values for zones were worked out from the tz database by hand and
//! cross-checked against Python's `zoneinfo`; every case also appears, with its
//! expected output, in the reviewer's mongosh probe for the reference.

use bson::oid::ObjectId;
use bson::{Bson, Document, Timestamp, doc};

use crate::expr::Expr;

fn d(y: i32, mo: u8, da: u8, h: u8, mi: u8, s: u8) -> Bson {
    dm(y, mo, da, h, mi, s, 0)
}

fn dm(y: i32, mo: u8, da: u8, h: u8, mi: u8, s: u8, ms: u16) -> Bson {
    Bson::DateTime(
        bson::DateTime::builder()
            .year(y)
            .month(mo)
            .day(da)
            .hour(h)
            .minute(mi)
            .second(s)
            .millisecond(ms)
            .build()
            .expect("a valid date"),
    )
}

fn try_eval(expr: Document, row: Document) -> kimmy_core::Result<Bson> {
    let expr = Bson::Document(expr);
    Expr::parse(&expr).unwrap_or_else(|e| panic!("{expr:?}: {e}")).eval(&row)
}

fn ev(expr: Document, row: Document) -> Bson {
    try_eval(expr.clone(), row).unwrap_or_else(|e| panic!("{expr:?}: {e}"))
}

fn refused(expr: Document, row: Document) -> String {
    try_eval(expr, row).expect_err("must be refused").to_string()
}

fn parse_refused(expr: Document) -> String {
    Expr::parse(&Bson::Document(expr)).expect_err("must be refused at parse").to_string()
}

/// A date from the reference's epoch milliseconds, as its answers are printed.
fn at(ms: i64) -> Bson {
    Bson::DateTime(bson::DateTime::from_millis(ms))
}

fn i(n: i32) -> Bson {
    Bson::Int32(n)
}

fn s(t: &str) -> Bson {
    Bson::String(t.to_string())
}

fn tzd(op: &str, date: Bson, tz: &str) -> Bson {
    ev(doc! { op: { "date": "$d", "timezone": tz } }, doc! { "d": date })
}

// -- the tz database ----------------------------------------------------------

#[test]
fn the_bundled_tz_database_is_the_one_the_docs_name() {
    assert_eq!(jiff_tzdb::VERSION, Some("2026e"), "update docs/aggregation.md with the crate");
}

// -- parts, with and without a zone ------------------------------------------

#[test]
fn utc_parts_and_the_three_argument_forms() {
    let row = doc! { "d": dm(2026, 8, 12, 13, 45, 7, 250) };
    assert_eq!(ev(doc! { "$year": "$d" }, row.clone()), i(2026));
    assert_eq!(ev(doc! { "$month": ["$d"] }, row.clone()), i(8));
    assert_eq!(ev(doc! { "$dayOfMonth": { "date": "$d" } }, row.clone()), i(12));
    assert_eq!(ev(doc! { "$hour": "$d" }, row.clone()), i(13));
    assert_eq!(ev(doc! { "$minute": "$d" }, row.clone()), i(45));
    assert_eq!(ev(doc! { "$second": "$d" }, row.clone()), i(7));
    assert_eq!(ev(doc! { "$millisecond": "$d" }, row), i(250));
}

#[test]
fn a_zone_shifts_the_parts_and_can_change_the_day() {
    let late = d(2024, 1, 1, 0, 0, 0);
    // 2023-12-31 19:00 in New York (EST, -5).
    assert_eq!(tzd("$year", late.clone(), "America/New_York"), i(2023));
    assert_eq!(tzd("$month", late.clone(), "America/New_York"), i(12));
    assert_eq!(tzd("$dayOfMonth", late.clone(), "America/New_York"), i(31));
    assert_eq!(tzd("$hour", late, "America/New_York"), i(19));
}

#[test]
fn a_zone_with_a_half_hour_offset() {
    let t = d(2024, 3, 15, 13, 45, 0);
    assert_eq!(tzd("$hour", t.clone(), "Asia/Kolkata"), i(19));
    assert_eq!(tzd("$minute", t.clone(), "Asia/Kolkata"), i(15));
    assert_eq!(tzd("$minute", t.clone(), "+05:30"), i(15));
    assert_eq!(tzd("$hour", t, "Asia/Kathmandu"), i(19)); // +05:45
}

#[test]
fn fixed_offsets_in_every_accepted_spelling() {
    let t = d(2024, 3, 15, 13, 45, 0);
    for (tz, hour, minute) in [
        ("+05:30", 19, 15),
        ("-0800", 5, 45),
        ("+05", 18, 45),
        ("-08:00", 5, 45),
        ("+0000", 13, 45),
        ("UTC", 13, 45),
        ("GMT", 13, 45),
        ("Etc/GMT+5", 8, 45),
    ] {
        assert_eq!(tzd("$hour", t.clone(), tz), i(hour), "{tz}");
        assert_eq!(tzd("$minute", t.clone(), tz), i(minute), "{tz}");
    }
}

#[test]
fn a_null_date_is_read_before_a_bad_zone_and_a_bad_zone_is_refused_when_read() {
    // The reference (extra.out): a missing date beside a bad zone is null,
    // whether the zone is a constant or computed.
    assert_eq!(
        ev(doc! { "$hour": { "date": "$nope", "timezone": "Nowhere/Land" } }, doc! {}),
        Bson::Null
    );
    assert_eq!(
        ev(
            doc! { "$hour": { "date": "$nope", "timezone": { "$concat": ["Nowhere/", "Land"] } } },
            doc! {}
        ),
        Bson::Null
    );
    assert_eq!(
        ev(doc! { "$hour": { "date": Bson::Null, "timezone": "Nowhere/Land" } }, doc! {}),
        Bson::Null
    );
    assert_eq!(ev(doc! { "$hour": { "date": "$nope", "timezone": 5 } }, doc! {}), Bson::Null);
    // With a date the zone is read: a bad name or a non-string is an error.
    let row = doc! { "d": d(2024, 1, 1, 0, 0, 0), "tz": "Nowhere/Land", "five": 5 };
    for tz in ["Mars/Olympus", "$tz", "$five"] {
        let e = refused(doc! { "$hour": { "date": "$d", "timezone": tz } }, row.clone());
        assert!(
            e.contains("unrecognized time zone identifier")
                || e.contains("must evaluate to a string"),
            "{tz}: {e}"
        );
    }
    let e = refused(
        doc! { "$hour": { "date": "$d", "timezone": { "$concat": ["Nowhere/", "Land"] } } },
        row.clone(),
    );
    assert!(e.contains("unrecognized time zone identifier: \"Nowhere/Land\""), "{e}");
    let e = refused(doc! { "$hour": { "date": "$d", "timezone": 5 } }, row);
    assert!(e.contains("timezone must evaluate to a string"), "{e}");
}

#[test]
fn a_date_of_the_wrong_type_beats_a_null_zone() {
    // $hour of a string with a null zone is an error in the reference (16006);
    // $dateToParts answers null (extra.out "toParts string date, null zone").
    let e = refused(doc! { "$hour": { "date": "x", "timezone": Bson::Null } }, doc! {});
    assert!(e.contains("$hour needs a date"), "{e}");
    assert_eq!(
        ev(doc! { "$dateToParts": { "date": "x", "timezone": Bson::Null } }, doc! {}),
        Bson::Null
    );
}

#[test]
fn zone_spellings_follow_the_reference() {
    // `$hour` of 2024-03-15T13:45Z in each accepted spelling (extra.out, date.out).
    let t = d(2024, 3, 15, 13, 45, 0);
    for (tz, hour) in [
        ("Etc/UTC", 13),
        ("EST5EDT", 9),
        ("Factory", 13),
        ("UCT", 13),
        ("Zulu", 13),
        ("Asia/Calcutta", 19),
        ("+00", 13),
        ("-00:00", 13),
        ("+23:59", 13),
        ("-23:59", 13),
        // Past 23:59 or 59 minutes the reference still reads the digits.
        ("+24:00", 13),
        ("+05:60", 19),
        ("+99", 16),
        ("+05:99", 20),
    ] {
        assert_eq!(tzd("$hour", t.clone(), tz), i(hour), "{tz}");
    }
    for bad in [
        "Z",
        "utc",
        "Gmt",
        "america/new_york",
        "AMERICA/NEW_YORK",
        "localtime",
        "posixrules",
        "+0",
        "+1:30",
        "+12:34:56",
        "+0530x",
        "+5",
        "+",
        "-",
        "",
        "+aé1",
        "+é",
        "+1é1",
        "-xé9",
        "+0é",
        "+é:0",
        "+٣٣",
        "+😀",
    ] {
        let e =
            refused(doc! { "$hour": { "date": "$d", "timezone": bad } }, doc! { "d": t.clone() });
        assert!(e.contains(&format!("unrecognized time zone identifier: \"{bad}\"")), "{bad}: {e}");
    }
}

#[test]
fn a_non_ascii_zone_read_from_a_document_is_refused_and_does_not_panic() {
    // B1: the zone comes from a stored field; the reference answers
    // 40485 unrecognized time zone identifier: "+aé1".
    let expr = doc! { "$hour": { "date": "$d", "timezone": "$tz" } };
    for bad in ["+aé1", "+1é1", "-xé9", "+é", "+12é", "+😀:1", "+٣٣", "é", "+1:é", "+\u{0}é"]
    {
        let e = refused(expr.clone(), doc! { "d": d(2024, 3, 15, 13, 45, 0), "tz": bad });
        assert!(e.contains("unrecognized time zone identifier"), "{bad}: {e}");
    }
    // And through the other operators that read a zone.
    for op in ["$dateToString", "$dateToParts"] {
        let e = refused(
            doc! { op: { "date": "$d", "timezone": "$tz" } },
            doc! { "d": d(2024, 3, 15, 13, 45, 0), "tz": "+aé1" },
        );
        assert!(e.contains("unrecognized time zone identifier"), "{op}: {e}");
    }
}
#[test]
fn a_null_or_missing_zone_or_date_is_null() {
    let t = d(2024, 3, 15, 13, 45, 0);
    let expr = doc! { "$hour": { "date": "$d", "timezone": "$tz" } };
    assert_eq!(ev(expr.clone(), doc! { "d": t.clone() }), Bson::Null);
    assert_eq!(ev(expr.clone(), doc! { "d": t, "tz": Bson::Null }), Bson::Null);
    assert_eq!(ev(expr.clone(), doc! { "tz": "UTC" }), Bson::Null);
    assert_eq!(ev(expr, doc! { "d": Bson::Null, "tz": "UTC" }), Bson::Null);
    assert_eq!(ev(doc! { "$year": "$nope" }, doc! {}), Bson::Null);
}

#[test]
fn a_non_date_is_refused_and_a_timestamp_or_object_id_is_read() {
    let e = refused(doc! { "$year": "$d" }, doc! { "d": "2024-01-01" });
    assert!(e.contains("$year needs a date"), "{e}");
    let e = refused(doc! { "$dayOfWeek": "$d" }, doc! { "d": 5 });
    assert!(e.contains("$dayOfWeek needs a date"), "{e}");
    let ts = Bson::Timestamp(Timestamp { time: 1_710_510_307, increment: 1 });
    assert_eq!(ev(doc! { "$year": "$d" }, doc! { "d": ts }), i(2024));
    let oid = ObjectId::parse_str("65f4a1230000000000000000").expect("oid"); // 2024-03-15T...
    assert_eq!(ev(doc! { "$year": "$d" }, doc! { "d": oid }), i(2024));
}

#[test]
fn unknown_and_missing_arguments_are_refused_at_parse() {
    assert!(parse_refused(doc! { "$year": { "date": "$d", "tz": "UTC" } }).contains("tz"));
    assert!(parse_refused(doc! { "$year": { "timezone": "UTC" } }).contains("date"));
    assert!(parse_refused(doc! { "$year": ["$a", "$b"] }).contains("exactly 1"));
    assert!(
        parse_refused(doc! { "$dateAdd": { "startDate": "$d", "unit": "day" } }).contains("amount")
    );
    assert!(parse_refused(doc! { "$dateAdd": ["$d", "day", 1] }).contains("object"));
}

// -- day, week and year numbering --------------------------------------------

#[test]
fn week_numbering_around_year_boundaries() {
    // (date, dayOfWeek, isoDayOfWeek, dayOfYear, week, isoWeek, isoWeekYear)
    let cases = [
        // Friday 2021-01-01: before the first Sunday, and ISO week 53 of 2020.
        (d(2021, 1, 1, 0, 0, 0), 6, 5, 1, 0, 53, 2020),
        // Sunday 2021-01-03: first Sunday is $week 1, still ISO week 53.
        (d(2021, 1, 3, 0, 0, 0), 1, 7, 3, 1, 53, 2020),
        // Monday 2021-01-04: ISO week 1.
        (d(2021, 1, 4, 0, 0, 0), 2, 1, 4, 1, 1, 2021),
        // Monday 2018-12-31 belongs to ISO week 1 of 2019.
        (d(2018, 12, 31, 0, 0, 0), 2, 1, 365, 52, 1, 2019),
        // Monday 2024-12-30, leap year, belongs to ISO week 1 of 2025.
        (d(2024, 12, 30, 0, 0, 0), 2, 1, 365, 52, 1, 2025),
        (d(2024, 12, 31, 0, 0, 0), 3, 2, 366, 52, 1, 2025),
        // Sunday 2016-01-03 is ISO week 53 of 2015, $week 1.
        (d(2016, 1, 3, 0, 0, 0), 1, 7, 3, 1, 53, 2015),
    ];
    for (date, dow, idow, doy, week, iw, iwy) in cases {
        let row = doc! { "d": date.clone() };
        assert_eq!(ev(doc! { "$dayOfWeek": "$d" }, row.clone()), i(dow), "{date}");
        assert_eq!(ev(doc! { "$isoDayOfWeek": "$d" }, row.clone()), i(idow), "{date}");
        assert_eq!(ev(doc! { "$dayOfYear": "$d" }, row.clone()), i(doy), "{date}");
        assert_eq!(ev(doc! { "$week": "$d" }, row.clone()), i(week), "{date}");
        assert_eq!(ev(doc! { "$isoWeek": "$d" }, row.clone()), i(iw), "{date}");
        // $isoWeekYear is a long.
        assert_eq!(ev(doc! { "$isoWeekYear": "$d" }, row), Bson::Int64(iwy), "{date}");
    }
}

#[test]
fn week_numbers_follow_the_zone_not_utc() {
    // Sunday 2021-01-03 02:00 UTC is still Saturday 2021-01-02 in New York.
    let t = d(2021, 1, 3, 2, 0, 0);
    assert_eq!(tzd("$dayOfWeek", t.clone(), "UTC"), i(1));
    assert_eq!(tzd("$dayOfWeek", t.clone(), "America/New_York"), i(7));
    assert_eq!(tzd("$week", t, "America/New_York"), i(0));
}

#[test]
fn pre_epoch_and_negative_years() {
    let row = doc! { "d": Bson::DateTime(bson::DateTime::from_millis(-1)) };
    assert_eq!(ev(doc! { "$millisecond": "$d" }, row.clone()), i(999));
    assert_eq!(ev(doc! { "$year": "$d" }, row.clone()), i(1969));
    assert_eq!(ev(doc! { "$second": "$d" }, row), i(59));
    // 0000-01-01 and the year before it (year -1 is not a leap year).
    let zero = Bson::DateTime(bson::DateTime::from_millis(-62_167_219_200_000));
    assert_eq!(ev(doc! { "$year": "$d" }, doc! { "d": zero }), i(0));
    let before =
        Bson::DateTime(bson::DateTime::from_millis(-62_167_219_200_000 - 365 * 86_400_000));
    assert_eq!(ev(doc! { "$year": "$d" }, doc! { "d": before.clone() }), i(-1));
    assert_eq!(ev(doc! { "$dayOfYear": "$d" }, doc! { "d": before }), i(1));
}

// -- $dateToString -------------------------------------------------------------

#[test]
fn date_to_string_specifiers() {
    let row = doc! { "d": dm(2024, 3, 15, 13, 45, 7, 250) };
    let f = |fmt: &str| ev(doc! { "$dateToString": { "date": "$d", "format": fmt } }, row.clone());
    assert_eq!(f("%Y-%m-%d %H:%M:%S.%L"), s("2024-03-15 13:45:07.250"));
    assert_eq!(f("%j"), s("075"));
    assert_eq!(f("%w"), s("6")); // Friday, Sunday = 1
    assert_eq!(f("%u"), s("5"));
    assert_eq!(f("%U"), s("10"));
    assert_eq!(f("%V"), s("11"));
    assert_eq!(f("%G"), s("2024"));
    // %Z is the signed minutes with no `+`: "0" for UTC (date.diag.out).
    assert_eq!(f("%z %Z"), s("+0000 0"));
    assert_eq!(f("100%%"), s("100%"));
    let jan = doc! { "d": d(2021, 1, 1, 0, 0, 0) };
    let g = |fmt: &str| ev(doc! { "$dateToString": { "date": "$d", "format": fmt } }, jan.clone());
    assert_eq!(g("%G-W%V-%u"), s("2020-W53-5"));
    assert_eq!(g("%U"), s("00"));
}

#[test]
fn date_to_string_in_a_zone() {
    let row = doc! { "d": dm(2024, 3, 15, 13, 45, 7, 250) };
    let f = |tz: &str, fmt: Option<&str>| {
        let mut spec = doc! { "date": "$d", "timezone": tz };
        if let Some(fmt) = fmt {
            spec.insert("format", fmt);
        }
        ev(doc! { "$dateToString": spec }, row.clone())
    };
    // %Z has no `+` for an offset east of UTC (date.diag.out).
    assert_eq!(f("Asia/Kolkata", Some("%H:%M %z %Z")), s("19:15 +0530 330"));
    assert_eq!(f("America/New_York", Some("%H:%M %z %Z")), s("09:45 -0400 -240"));
    assert_eq!(f("-0800", Some("%z %Z")), s("-0800 -480"));
    assert_eq!(f("-03:30", Some("%z %Z")), s("-0330 -210"));
    assert_eq!(f("+00:00", Some("%z %Z")), s("+0000 0"));
    // The default format ends in Z in every zone, as the reference prints it.
    assert_eq!(f("UTC", None), s("2024-03-15T13:45:07.250Z"));
    assert_eq!(f("America/New_York", None), s("2024-03-15T09:45:07.250Z"));
    assert_eq!(f("+05:30", None), s("2024-03-15T19:15:07.250Z"));
    assert_eq!(f("+00:00", None), s("2024-03-15T13:45:07.250Z"));
}
#[test]
fn a_historical_offset_is_honoured_with_its_seconds_dropped_from_z() {
    // New York before 1883 kept local mean time, UTC-04:56:02.
    let t = d(1800, 1, 1, 12, 0, 0);
    assert_eq!(tzd("$minute", t.clone(), "America/New_York"), i(3));
    assert_eq!(tzd("$second", t.clone(), "America/New_York"), i(58));
    let r = ev(
        doc! { "$dateToString": { "date": "$d", "format": "%z %Z", "timezone": "America/New_York" } },
        doc! { "d": t },
    );
    assert_eq!(r, s("-0456 -296"));
    // Kolkata's local mean time was +05:53:28 (extra.out).
    let kolkata = ev(
        doc! { "$dateToString": { "date": "$d", "format": "%H:%M:%S %z %Z", "timezone": "Asia/Kolkata" } },
        doc! { "d": d(1800, 1, 1, 12, 0, 0) },
    );
    assert_eq!(kolkata, s("17:53:28 +0553 353"));
    let new_york = ev(
        doc! { "$dateToString": { "date": "$d", "format": "%H:%M:%S %z %Z", "timezone": "America/New_York" } },
        doc! { "d": d(1800, 1, 1, 12, 0, 0) },
    );
    assert_eq!(new_york, s("07:03:58 -0456 -296"));
}

#[test]
fn date_to_string_null_format_and_errors() {
    let row = doc! { "d": d(2024, 3, 15, 0, 0, 0) };
    assert_eq!(ev(doc! { "$dateToString": { "date": "$nope" } }, row.clone()), Bson::Null);
    assert_eq!(
        ev(doc! { "$dateToString": { "date": "$nope", "onNull": "never" } }, row.clone()),
        s("never")
    );
    assert_eq!(
        ev(doc! { "$dateToString": { "date": "$d", "format": "$nope" } }, row.clone()),
        Bson::Null
    );
    // A missing date with a null format answers onNull (extra.out).
    assert_eq!(
        ev(
            doc! { "$dateToString": { "date": "$nope", "format": Bson::Null, "onNull": "x" } },
            row.clone()
        ),
        s("x")
    );
    // A bad zone is refused even beside a missing date, constant or computed,
    // and so is a bad computed format (extra.out); the date does not shield them.
    for tz in [s("Bad/Zone"), Bson::String("$tz".into())] {
        let e = refused(
            doc! { "$dateToString": { "date": "$nope", "timezone": tz, "onNull": "x" } },
            doc! { "tz": "Bad/Zone" },
        );
        assert!(e.contains("unrecognized time zone identifier"), "{e}");
    }
    let e = refused(
        doc! { "$dateToString": { "date": "$nope", "format": "$f", "onNull": "x" } },
        doc! { "f": "%q" },
    );
    assert!(e.contains("Invalid format character '%q'"), "{e}");
    let e = refused(
        doc! { "$dateToString": { "date": "$d", "format": "$f" } },
        doc! { "d": d(2024, 1, 1, 0, 0, 0), "f": 5 },
    );
    assert!(e.contains("'format' be a string"), "{e}");
    let e = parse_refused(doc! { "$dateToString": { "date": "$d", "format": "%q" } });
    assert!(e.contains("Invalid format character '%q'"), "{e}");
    let e = parse_refused(doc! { "$dateToString": { "date": "$d", "format": "x%" } });
    assert!(e.contains("Unmatched '%'"), "{e}");
    // %Y is four digits and only 0000..9999.
    let big = doc! { "d": Bson::DateTime(bson::DateTime::from_millis(-62_198_841_600_000)) };
    let e = refused(doc! { "$dateToString": { "date": "$d", "format": "%Y" } }, big);
    assert!(e.contains("0-9999"), "{e}");
}

// -- $dateToParts and $dateFromParts -------------------------------------------

#[test]
fn date_to_parts() {
    let row = doc! { "d": dm(2021, 1, 1, 13, 45, 7, 250) };
    assert_eq!(
        ev(doc! { "$dateToParts": { "date": "$d" } }, row.clone()),
        Bson::Document(doc! {
            "year": 2021, "month": 1, "day": 1, "hour": 13, "minute": 45, "second": 7, "millisecond": 250
        })
    );
    assert_eq!(
        ev(
            doc! { "$dateToParts": { "date": "$d", "iso8601": true, "timezone": "Asia/Kolkata" } },
            row.clone()
        ),
        Bson::Document(doc! {
            "isoWeekYear": 2020, "isoWeek": 53, "isoDayOfWeek": 5, "hour": 19, "minute": 15, "second": 7, "millisecond": 250
        })
    );
    assert_eq!(ev(doc! { "$dateToParts": { "date": "$nope" } }, row.clone()), Bson::Null);
    assert_eq!(
        ev(doc! { "$dateToParts": { "date": "$d", "iso8601": "$nope" } }, row.clone()),
        Bson::Null
    );
    let e = refused(doc! { "$dateToParts": { "date": "$d", "iso8601": 1 } }, row);
    assert!(e.contains("iso8601 must evaluate to a bool"), "{e}");
}

#[test]
fn date_from_parts_defaults_and_overflow() {
    let from = |spec: Document| ev(doc! { "$dateFromParts": spec }, doc! {});
    assert_eq!(from(doc! { "year": 2024 }), d(2024, 1, 1, 0, 0, 0));
    assert_eq!(
        from(
            doc! { "year": 2024, "month": 3, "day": 15, "hour": 13, "minute": 45, "second": 7, "millisecond": 250 }
        ),
        dm(2024, 3, 15, 13, 45, 7, 250)
    );
    // Values outside their range carry into the next unit, in both directions.
    assert_eq!(from(doc! { "year": 2017, "month": 14, "day": 1 }), d(2018, 2, 1, 0, 0, 0));
    assert_eq!(from(doc! { "year": 2024, "month": 3, "day": 0 }), d(2024, 2, 29, 0, 0, 0));
    assert_eq!(
        from(doc! { "year": 2024, "month": 1, "day": 1, "hour": 25 }),
        d(2024, 1, 2, 1, 0, 0)
    );
    assert_eq!(
        from(doc! { "year": 2024, "month": 1, "day": 1, "minute": -1 }),
        d(2023, 12, 31, 23, 59, 0)
    );
    assert_eq!(
        from(doc! { "year": 2024, "month": 1, "day": 1, "millisecond": 1500 }),
        dm(2024, 1, 1, 0, 0, 1, 500)
    );
    assert_eq!(from(doc! { "year": 2024.0, "month": 6 }), d(2024, 6, 1, 0, 0, 0));
}

#[test]
fn date_from_parts_second_and_millisecond_have_no_range() {
    // extra.out: `millisecond: 40000` and the second are not range-checked.
    let from = |spec: Document| ev(doc! { "$dateFromParts": spec }, doc! {});
    assert_eq!(from(doc! { "year": 2024, "millisecond": 40000 }), at(1_704_067_240_000));
    assert_eq!(from(doc! { "year": 2024, "second": 40000 }), at(1_704_067_200_000 + 40_000_000));
    assert_eq!(
        from(doc! { "year": 2024, "second": 1_000_000_000_i64 }),
        at(1_704_067_200_000 + 1_000_000_000_000)
    );
    assert_eq!(
        from(doc! { "year": 2024, "millisecond": -1_000_000_000_i64 }),
        at(1_704_067_200_000 - 1_000_000_000)
    );
    // A sum no date can hold is refused, and cannot wrap.
    for (part, v) in [("second", i64::MAX), ("millisecond", i64::MAX), ("second", i64::MIN)] {
        let e = refused(doc! { "$dateFromParts": { "year": 2024, part: v } }, doc! {});
        assert!(e.contains("Overflow"), "{part}: {e}");
    }
}

#[test]
fn date_from_parts_nulls_come_before_every_check() {
    // extra.out: the hour, minute, second and millisecond are read first, then
    // the zone, then the year, month and day; the first null answers null.
    let from = |spec: Document| ev(doc! { "$dateFromParts": spec }, doc! {});
    assert_eq!(from(doc! { "year": "x", "hour": Bson::Null }), Bson::Null);
    assert_eq!(from(doc! { "year": 10000, "month": Bson::Null }), Bson::Null);
    assert_eq!(from(doc! { "year": 2024, "month": "x", "hour": Bson::Null }), Bson::Null);
    assert_eq!(from(doc! { "year": 2024, "hour": Bson::Null, "timezone": "Bad/Zone" }), Bson::Null);
    assert_eq!(
        from(
            doc! { "year": 2024, "hour": Bson::Null, "timezone": { "$concat": ["Bad/", "Zone"] } }
        ),
        Bson::Null
    );
    // Without a null the same arguments are refused.
    let e = refused(doc! { "$dateFromParts": { "year": 2024, "timezone": "Bad/Zone" } }, doc! {});
    assert!(e.contains("unrecognized time zone identifier"), "{e}");
    let e = refused(doc! { "$dateFromParts": { "year": 10000 } }, doc! {});
    assert!(e.contains("1 to 9999"), "{e}");
}

#[test]
fn date_from_parts_iso() {
    let from = |spec: Document| ev(doc! { "$dateFromParts": spec }, doc! {});
    assert_eq!(from(doc! { "isoWeekYear": 2021 }), d(2021, 1, 4, 0, 0, 0));
    assert_eq!(
        from(doc! { "isoWeekYear": 2020, "isoWeek": 53, "isoDayOfWeek": 5 }),
        d(2021, 1, 1, 0, 0, 0)
    );
    // Week 54 of 2020 carries into 2021's first week.
    assert_eq!(
        from(doc! { "isoWeekYear": 2020, "isoWeek": 54, "isoDayOfWeek": 1 }),
        d(2021, 1, 4, 0, 0, 0)
    );
    assert_eq!(
        from(doc! { "isoWeekYear": 2021, "isoWeek": 1, "isoDayOfWeek": 0 }),
        d(2021, 1, 3, 0, 0, 0)
    );
}

#[test]
fn date_from_parts_errors_and_nulls() {
    let row = doc! { "n": Bson::Null, "s": "x" };
    assert_eq!(
        ev(doc! { "$dateFromParts": { "year": 2024, "month": "$n" } }, row.clone()),
        Bson::Null
    );
    assert_eq!(ev(doc! { "$dateFromParts": { "year": "$missing" } }, row.clone()), Bson::Null);
    assert_eq!(
        ev(doc! { "$dateFromParts": { "year": 2024, "timezone": "$n" } }, row.clone()),
        Bson::Null
    );
    let e = refused(doc! { "$dateFromParts": { "year": "$s" } }, row.clone());
    assert!(e.contains("'year' must evaluate to an integer"), "{e}");
    let e = refused(doc! { "$dateFromParts": { "year": 2024, "month": 1.5 } }, row.clone());
    assert!(e.contains("'month' must evaluate to an integer"), "{e}");
    let e = refused(doc! { "$dateFromParts": { "year": 10000 } }, row.clone());
    assert!(e.contains("1 to 9999"), "{e}");
    let e = refused(doc! { "$dateFromParts": { "year": 0 } }, row.clone());
    assert!(e.contains("1 to 9999"), "{e}");
    let e = refused(doc! { "$dateFromParts": { "year": 2024, "hour": 40000 } }, row.clone());
    assert!(e.contains("[-32768, 32767]"), "{e}");
    for (part, v) in [("month", 40000), ("minute", 40000), ("minute", -40000), ("day", 40000)] {
        let e = refused(doc! { "$dateFromParts": { "year": 2024, part: v } }, row.clone());
        assert!(e.contains("[-32768, 32767]"), "{part}: {e}");
    }
    let e =
        refused(doc! { "$dateFromParts": { "isoWeekYear": 2024, "isoWeek": 40000 } }, row.clone());
    assert!(e.contains("'isoWeek' must evaluate to a value in the range"), "{e}");
    let e = refused(doc! { "$dateFromParts": { "isoWeekYear": 0 } }, row);
    assert!(e.contains("'isoWeekYear' must evaluate to an integer in the range 1 to 9999"), "{e}");
    assert!(
        parse_refused(doc! { "$dateFromParts": { "month": 2 } })
            .contains("'year' or 'isoWeekYear'")
    );
    assert!(
        parse_refused(doc! { "$dateFromParts": { "year": 2024, "isoWeekYear": 2024 } })
            .contains("both")
    );
    assert!(
        parse_refused(doc! { "$dateFromParts": { "year": 2024, "isoWeek": 2 } })
            .contains("isoWeekYear")
    );
}

#[test]
fn date_from_parts_resolves_ambiguous_and_missing_local_times() {
    let ny = |h: i32, mi: i32, day: i32, month: i32| {
        ev(
            doc! { "$dateFromParts": { "year": 2024, "month": month, "day": day, "hour": h, "minute": mi, "timezone": "America/New_York" } },
            doc! {},
        )
    };
    // Spring forward 2024-03-10: 02:30 never happened; it reads as 03:30 EDT.
    assert_eq!(ny(2, 30, 10, 3), d(2024, 3, 10, 7, 30, 0));
    assert_eq!(ny(1, 59, 10, 3), d(2024, 3, 10, 6, 59, 0)); // EST
    assert_eq!(ny(3, 0, 10, 3), d(2024, 3, 10, 7, 0, 0)); // EDT
    // Fall back 2024-11-03: 01:30 happened twice; the later (EST) is chosen.
    assert_eq!(ny(1, 30, 3, 11), d(2024, 11, 3, 6, 30, 0));
    assert_eq!(ny(2, 30, 3, 11), d(2024, 11, 3, 7, 30, 0)); // EST
    // Fixed offsets have no such thing.
    assert_eq!(
        ev(doc! { "$dateFromParts": { "year": 2024, "hour": 12, "timezone": "+05:30" } }, doc! {}),
        d(2024, 1, 1, 6, 30, 0)
    );
}

#[test]
fn a_repeated_local_time_is_the_later_instant_in_every_zone() {
    // The reference's answers, extra.out "DST folds and gaps" (epoch ms).
    let parts = |spec: Document| ev(doc! { "$dateFromParts": spec }, doc! {});
    let cases: [(Document, i64); 10] = [
        (
            doc! { "year": 2024, "month": 4, "day": 7, "hour": 2, "minute": 30, "timezone": "Australia/Sydney" },
            1_712_421_000_000,
        ),
        (
            doc! { "year": 2024, "month": 10, "day": 27, "hour": 2, "minute": 30, "timezone": "Europe/Berlin" },
            1_729_992_600_000,
        ),
        (
            doc! { "year": 2024, "month": 11, "day": 3, "hour": 0, "minute": 30, "timezone": "America/Havana" },
            1_730_611_800_000,
        ),
        (
            doc! { "year": 2024, "month": 4, "day": 7, "hour": 1, "minute": 45, "timezone": "Australia/Lord_Howe" },
            1_712_416_500_000,
        ),
        (
            doc! { "year": 2024, "month": 11, "day": 3, "hour": 1, "timezone": "America/New_York" },
            1_730_613_600_000,
        ),
        (
            doc! { "year": 2024, "month": 11, "day": 3, "hour": 1, "minute": 59, "second": 59, "millisecond": 999, "timezone": "America/New_York" },
            1_730_617_199_999,
        ),
        (
            doc! { "year": 2024, "month": 11, "day": 3, "hour": 0, "minute": 59, "timezone": "America/New_York" },
            1_730_609_940_000,
        ),
        (
            doc! { "year": 2024, "month": 11, "day": 2, "hour": 25, "minute": 30, "timezone": "America/New_York" },
            1_730_615_400_000,
        ),
        // The gaps: read with the offset before the gap.
        (
            doc! { "year": 2024, "month": 9, "day": 8, "hour": 0, "minute": 30, "timezone": "America/Santiago" },
            1_725_769_800_000,
        ),
        (
            doc! { "year": 2024, "month": 3, "day": 31, "hour": 2, "minute": 30, "timezone": "Europe/Berlin" },
            1_711_848_600_000,
        ),
    ];
    for (spec, ms) in cases {
        assert_eq!(parts(spec.clone()), at(ms), "{spec}");
    }
    assert_eq!(
        parts(
            doc! { "year": 2024, "month": 10, "day": 6, "hour": 2, "minute": 15, "timezone": "Australia/Lord_Howe" }
        ),
        at(1_728_143_100_000)
    );
    // The same wall clocks through $dateFromString.
    assert_eq!(
        from_string(doc! { "dateString": "2024-10-27T02:30:00", "timezone": "Europe/Berlin" }),
        at(1_729_992_600_000)
    );
    assert_eq!(
        from_string(
            doc! { "dateString": "07/04/2024 02:30", "format": "%d/%m/%Y %H:%M", "timezone": "Australia/Sydney" }
        ),
        at(1_712_421_000_000)
    );
}
// -- $dateFromString -----------------------------------------------------------

fn from_string(spec: Document) -> Bson {
    ev(doc! { "$dateFromString": spec }, doc! {})
}

#[test]
fn date_from_string_without_a_format() {
    let f = |t: &str| from_string(doc! { "dateString": t });
    assert_eq!(f("2017-02-08T12:10:40.787"), dm(2017, 2, 8, 12, 10, 40, 787));
    assert_eq!(f("2017-02-08T12:10:40.787Z"), dm(2017, 2, 8, 12, 10, 40, 787));
    assert_eq!(f("2017-02-08"), d(2017, 2, 8, 0, 0, 0));
    assert_eq!(f("2017-02-08 12:10"), d(2017, 2, 8, 12, 10, 0));
    assert_eq!(f("2017-02-08T12:10:40.7"), dm(2017, 2, 8, 12, 10, 40, 700));
    // An offset in the string is applied.
    assert_eq!(f("2017-02-08T12:10:40+0100"), d(2017, 2, 8, 11, 10, 40));
    assert_eq!(f("2017-02-08T12:10:40-05:30"), d(2017, 2, 8, 17, 40, 40));
}

#[test]
fn date_from_string_with_a_format() {
    let f = |t: &str, fmt: &str| from_string(doc! { "dateString": t, "format": fmt });
    assert_eq!(f("15-06-2017", "%d-%m-%Y"), d(2017, 6, 15, 0, 0, 0));
    assert_eq!(f("06/15/2017 08:09:10.011", "%m/%d/%Y %H:%M:%S.%L"), dm(2017, 6, 15, 8, 9, 10, 11));
    assert_eq!(f("2021-W01-1", "%G-W%V-%u"), d(2021, 1, 4, 0, 0, 0));
    assert_eq!(f("2017-02-08 12:00 +0530", "%Y-%m-%d %H:%M %z"), d(2017, 2, 8, 6, 30, 0));
    assert_eq!(f("2017-02-08 12:00 -08:00", "%Y-%m-%d %H:%M %z"), d(2017, 2, 8, 20, 0, 0));
    assert_eq!(f("2017-02-08 12:00 +330", "%Y-%m-%d %H:%M %Z"), d(2017, 2, 8, 6, 30, 0));
    assert_eq!(f("2024-01-01 100%", "%Y-%m-%d 100%%"), d(2024, 1, 1, 0, 0, 0));
    // A format may stop at the hour; the ISO form needs only %G (extra.out).
    assert_eq!(f("2024-03-15 10", "%Y-%m-%d %H"), at(1_710_496_800_000));
    assert_eq!(f("2021", "%G"), at(1_609_718_400_000));
}

#[test]
fn date_from_string_refuses_what_the_reference_refuses() {
    // %j is not a %-specifier of $dateFromString (18536).
    let e =
        parse_refused(doc! { "$dateFromString": { "dateString": "2024-366", "format": "%Y-%j" } });
    assert!(e.contains("Invalid format character '%j'"), "{e}");
    // A format without a whole date is an incomplete string, a conversion error
    // that onError takes (extra.out "fmt %Y only" and the others).
    for (t, f) in [("2024", "%Y"), ("2024-03", "%Y-%m"), ("03-15", "%m-%d"), ("10:15", "%H:%M")] {
        let e = refused(doc! { "$dateFromString": { "dateString": t, "format": f } }, doc! {});
        assert!(
            e.contains(&format!(
                "an incomplete date/time string has been found, with elements missing: \"{t}\""
            )),
            "{t}: {e}"
        );
        assert_eq!(
            ev(doc! { "$dateFromString": { "dateString": t, "format": f, "onError": 1 } }, doc! {}),
            i(1)
        );
    }
    // A comma is not a decimal point (extra.out "iso comma frac").
    let e = refused(doc! { "$dateFromString": { "dateString": "2017-02-08T12:10:40,5" } }, doc! {});
    assert!(e.contains("Error parsing date string"), "{e}");
}

#[test]
fn date_from_string_in_a_zone_resolves_local_times() {
    let f = |t: &str, tz: &str| from_string(doc! { "dateString": t, "timezone": tz });
    assert_eq!(f("2024-07-01T12:00:00", "America/New_York"), d(2024, 7, 1, 16, 0, 0));
    assert_eq!(f("2024-01-01T12:00:00", "America/New_York"), d(2024, 1, 1, 17, 0, 0));
    // Nonexistent and repeated local times, as for $dateFromParts: the repeated
    // 01:30 is the later one, 06:30Z (date.diag.out "tz fold").
    assert_eq!(f("2024-03-10T02:30:00", "America/New_York"), d(2024, 3, 10, 7, 30, 0));
    assert_eq!(f("2024-11-03T01:30:00", "America/New_York"), d(2024, 11, 3, 6, 30, 0));
    assert_eq!(f("2024-11-03T02:30:00", "America/New_York"), d(2024, 11, 3, 7, 30, 0));
    assert_eq!(f("2024-03-15T12:00:00", "Asia/Kolkata"), d(2024, 3, 15, 6, 30, 0));
    assert_eq!(f("2024-03-15T12:00:00", "-0800"), d(2024, 3, 15, 20, 0, 0));
}

#[test]
fn date_from_string_errors_on_error_and_on_null() {
    let row = doc! { "t": "2024-02-30", "n": Bson::Null };
    let bad = doc! { "$dateFromString": { "dateString": "$t" } };
    let e = refused(bad, row.clone());
    assert!(e.contains("Error parsing date string '2024-02-30'"), "{e}");
    assert_eq!(
        ev(doc! { "$dateFromString": { "dateString": "$t", "onError": "oops" } }, row.clone()),
        s("oops")
    );
    // A value that is not a string is a conversion failure, so onError takes it.
    assert_eq!(
        ev(doc! { "$dateFromString": { "dateString": 5, "onError": "oops" } }, row.clone()),
        s("oops")
    );
    let e = refused(doc! { "$dateFromString": { "dateString": 5 } }, row.clone());
    assert!(e.contains("'dateString' be a string"), "{e}");
    assert_eq!(ev(doc! { "$dateFromString": { "dateString": "$n" } }, row.clone()), Bson::Null);
    assert_eq!(
        ev(doc! { "$dateFromString": { "dateString": "$n", "onNull": 7 } }, row.clone()),
        i(7)
    );
    // A null input beats a null format or zone; a null format or zone otherwise gives null.
    assert_eq!(
        ev(
            doc! { "$dateFromString": { "dateString": "$n", "format": "$n", "onNull": 7 } },
            row.clone()
        ),
        i(7)
    );
    assert_eq!(
        ev(
            doc! { "$dateFromString": { "dateString": "2024-01-01", "timezone": "$n", "onError": 1 } },
            row.clone()
        ),
        Bson::Null
    );
    // A bad zone or format is never caught by onError.
    let e = refused(
        doc! { "$dateFromString": { "dateString": "2024-01-01", "timezone": "Nope/Zone", "onError": 1 } },
        row.clone(),
    );
    assert!(e.contains("unrecognized time zone identifier"), "{e}");
    let e = parse_refused(
        doc! { "$dateFromString": { "dateString": "x", "format": "%q", "onError": 1 } },
    );
    assert!(e.contains("Invalid format character"), "{e}");
    // A zone in the string and a zone argument together.
    let e = refused(
        doc! { "$dateFromString": { "dateString": "2024-01-01T00:00:00Z", "timezone": "UTC" } },
        row.clone(),
    );
    assert!(e.contains("time zone information"), "{e}");
    // Trailing data, a literal that does not match, short fields.
    for (t, f) in
        [("2024-01-01x", "%Y-%m-%d"), ("2024/01/01", "%Y-%m-%d"), ("24-01-01", "%Y-%m-%d")]
    {
        let e = refused(doc! { "$dateFromString": { "dateString": t, "format": f } }, row.clone());
        assert!(e.contains("Error parsing date string"), "{t}: {e}");
    }
}

// -- $dateAdd and $dateSubtract ------------------------------------------------

fn add(start: Bson, unit: &str, amount: i64, tz: Option<&str>) -> Bson {
    let mut spec = doc! { "startDate": "$d", "unit": unit, "amount": amount };
    if let Some(tz) = tz {
        spec.insert("timezone", tz);
    }
    ev(doc! { "$dateAdd": spec }, doc! { "d": start })
}

#[test]
fn date_add_clamps_to_the_end_of_the_month() {
    assert_eq!(add(d(2024, 1, 31, 10, 0, 0), "month", 1, None), d(2024, 2, 29, 10, 0, 0));
    assert_eq!(add(d(2023, 1, 31, 10, 0, 0), "month", 1, None), d(2023, 2, 28, 10, 0, 0));
    assert_eq!(add(d(2024, 2, 29, 10, 0, 0), "year", 1, None), d(2025, 2, 28, 10, 0, 0));
    assert_eq!(add(d(2024, 2, 29, 10, 0, 0), "year", 4, None), d(2028, 2, 29, 10, 0, 0));
    assert_eq!(add(d(2024, 11, 30, 10, 0, 0), "quarter", 1, None), d(2025, 2, 28, 10, 0, 0));
    assert_eq!(add(d(2024, 3, 31, 10, 0, 0), "month", -1, None), d(2024, 2, 29, 10, 0, 0));
    assert_eq!(add(d(2024, 1, 31, 10, 0, 0), "month", 13, None), d(2025, 2, 28, 10, 0, 0));
    assert_eq!(add(d(2024, 3, 15, 10, 0, 0), "week", 2, None), d(2024, 3, 29, 10, 0, 0));
    assert_eq!(add(d(2024, 3, 15, 10, 0, 0), "day", -15, None), d(2024, 2, 29, 10, 0, 0));
    assert_eq!(add(d(2024, 3, 15, 10, 0, 0), "hour", 25, None), d(2024, 3, 16, 11, 0, 0));
    assert_eq!(add(d(2024, 3, 15, 10, 0, 0), "minute", 90, None), d(2024, 3, 15, 11, 30, 0));
    assert_eq!(add(d(2024, 3, 15, 10, 0, 0), "second", -1, None), d(2024, 3, 15, 9, 59, 59));
    assert_eq!(
        add(d(2024, 3, 15, 10, 0, 0), "millisecond", 1500, None),
        dm(2024, 3, 15, 10, 0, 1, 500)
    );
}

#[test]
fn date_add_keeps_wall_time_across_a_clock_change_for_days_only() {
    // 2024-03-09 12:00 EST + 1 day = 2024-03-10 12:00 EDT: 23 hours later.
    let start = d(2024, 3, 9, 17, 0, 0);
    assert_eq!(add(start.clone(), "day", 1, Some("America/New_York")), d(2024, 3, 10, 16, 0, 0));
    assert_eq!(add(start.clone(), "week", 1, Some("America/New_York")), d(2024, 3, 16, 16, 0, 0));
    assert_eq!(add(start.clone(), "month", 1, Some("America/New_York")), d(2024, 4, 9, 16, 0, 0));
    // In UTC it is exactly 24 hours, and so is 24 hours in New York.
    assert_eq!(add(start.clone(), "day", 1, None), d(2024, 3, 10, 17, 0, 0));
    assert_eq!(add(start, "hour", 24, Some("America/New_York")), d(2024, 3, 10, 17, 0, 0));
    // Over the autumn change a day is 25 hours: 2024-11-02 12:00 EDT + 1 day.
    assert_eq!(
        add(d(2024, 11, 2, 16, 0, 0), "day", 1, Some("America/New_York")),
        d(2024, 11, 3, 17, 0, 0)
    );
    assert_eq!(
        add(d(2024, 11, 4, 17, 0, 0), "day", -2, Some("America/New_York")),
        d(2024, 11, 2, 16, 0, 0)
    );
}

#[test]
fn date_add_landing_in_a_gap_moves_forward() {
    // 2024-03-09 02:30 EST + 1 day = 02:30 on the 10th, which does not exist.
    let start = d(2024, 3, 9, 7, 30, 0);
    assert_eq!(add(start, "day", 1, Some("America/New_York")), d(2024, 3, 10, 7, 30, 0));
}

#[test]
fn date_add_landing_in_a_repeated_hour_keeps_the_start_offset() {
    // extra.out: from EDT into the fold gives the EDT instant, from EST the
    // EST one.
    let ny = Some("America/New_York");
    let z = |s: &str| match from_string(doc! { "dateString": s }) {
        Bson::DateTime(t) => t.timestamp_millis(),
        other => panic!("{other:?}"),
    };
    assert_eq!(add(at(z("2024-11-02T05:30:00Z")), "day", 1, ny), at(1_730_611_800_000));
    assert_eq!(add(at(z("2024-10-03T05:30:00Z")), "month", 1, ny), at(1_730_611_800_000));
    assert_eq!(add(at(z("2024-11-03T06:30:00Z")), "day", 1, ny), at(1_730_701_800_000));
    assert_eq!(add(at(z("2024-11-03T06:30:00Z")), "day", -1, ny), at(1_730_525_400_000));
    assert_eq!(
        add(at(z("2024-10-26T00:30:00Z")), "day", 1, Some("Europe/Berlin")),
        at(1_729_989_000_000)
    );
    // Into a gap: the offset before it.
    assert_eq!(
        add(at(z("2024-09-07T04:30:00Z")), "day", 1, Some("America/Santiago")),
        at(1_725_769_800_000)
    );
    // $dateSubtract from EST into the fold.
    let sub = ev(
        doc! { "$dateSubtract": { "startDate": "$d", "unit": "day", "amount": 1, "timezone": "America/New_York" } },
        doc! { "d": at(z("2024-11-04T06:30:00Z")) },
    );
    assert_eq!(sub, at(1_730_615_400_000));
}

#[test]
fn date_subtract_and_errors() {
    let row = doc! { "d": d(2024, 3, 31, 0, 0, 0), "n": Bson::Null };
    assert_eq!(
        ev(
            doc! { "$dateSubtract": { "startDate": "$d", "unit": "month", "amount": 1 } },
            row.clone()
        ),
        d(2024, 2, 29, 0, 0, 0)
    );
    assert_eq!(
        ev(
            doc! { "$dateSubtract": { "startDate": "$d", "unit": "day", "amount": -1 } },
            row.clone()
        ),
        d(2024, 4, 1, 0, 0, 0)
    );
    for spec in [
        doc! { "startDate": "$n", "unit": "day", "amount": 1 },
        doc! { "startDate": "$d", "unit": "$n", "amount": 1 },
        doc! { "startDate": "$d", "unit": "day", "amount": "$n" },
        doc! { "startDate": "$d", "unit": "day", "amount": 1, "timezone": "$n" },
        doc! { "startDate": "$missing", "unit": "day", "amount": 1 },
    ] {
        assert_eq!(ev(doc! { "$dateAdd": spec }, row.clone()), Bson::Null);
    }
    // Any null answers null before a type, unit or zone is judged (extra.out).
    for spec in [
        doc! { "startDate": "x", "unit": Bson::Null, "amount": 1 },
        doc! { "startDate": "x", "unit": "day", "amount": Bson::Null },
        doc! { "startDate": "$d", "unit": "decade", "amount": Bson::Null },
        doc! { "startDate": "$d", "unit": "day", "amount": 1.5, "timezone": Bson::Null },
        doc! { "startDate": "$d", "unit": "day", "amount": Bson::Null, "timezone": "Bad/Zone" },
        doc! { "startDate": "$missing", "unit": "day", "amount": 1, "timezone": "Bad/Zone" },
    ] {
        assert_eq!(ev(doc! { "$dateAdd": spec.clone() }, row.clone()), Bson::Null, "{spec}");
        assert_eq!(ev(doc! { "$dateSubtract": spec.clone() }, row.clone()), Bson::Null, "{spec}");
    }
    let e = refused(
        doc! { "$dateAdd": { "startDate": "$d", "unit": "day", "amount": 1, "timezone": "Bad/Zone" } },
        row.clone(),
    );
    assert!(e.contains("unrecognized time zone identifier"), "{e}");
    let e =
        refused(doc! { "$dateAdd": { "startDate": "x", "unit": "day", "amount": 1 } }, row.clone());
    assert!(e.contains("startDate to be convertible to a date"), "{e}");
    let e = refused(
        doc! { "$dateAdd": { "startDate": "$d", "unit": "decade", "amount": 1 } },
        row.clone(),
    );
    assert!(e.contains("expects unit to be one of"), "{e}");
    let e =
        refused(doc! { "$dateAdd": { "startDate": "$d", "unit": 5, "amount": 1 } }, row.clone());
    assert!(e.contains("unit to be a string"), "{e}");
    let e = refused(
        doc! { "$dateAdd": { "startDate": "$d", "unit": "Day", "amount": 1 } },
        row.clone(),
    );
    assert!(e.contains("expects unit to be one of"), "{e}");
    let e = refused(
        doc! { "$dateAdd": { "startDate": "$d", "unit": "day", "amount": 1.5 } },
        row.clone(),
    );
    assert!(e.contains("integer amount"), "{e}");
    let e = refused(
        doc! { "$dateAdd": { "startDate": "$d", "unit": "year", "amount": i64::MAX } },
        row.clone(),
    );
    assert!(e.contains("Overflow"), "{e}");
    let e = refused(
        doc! { "$dateSubtract": { "startDate": "$d", "unit": "day", "amount": i64::MIN } },
        row,
    );
    assert!(e.contains("Overflow"), "{e}");
}

// -- $dateDiff -------------------------------------------------------------------

fn diff(a: Bson, b: Bson, unit: &str, extra: Document) -> Bson {
    let mut spec = doc! { "startDate": "$a", "endDate": "$b", "unit": unit };
    for (k, v) in extra {
        spec.insert(k, v);
    }
    ev(doc! { "$dateDiff": spec }, doc! { "a": a, "b": b })
}

#[test]
fn date_diff_counts_boundaries_crossed() {
    let a = d(2021, 12, 31, 23, 59, 0);
    let b = d(2022, 1, 1, 0, 1, 0);
    assert_eq!(diff(a.clone(), b.clone(), "year", doc! {}), Bson::Int64(1));
    assert_eq!(diff(a.clone(), b.clone(), "quarter", doc! {}), Bson::Int64(1));
    assert_eq!(diff(a.clone(), b.clone(), "month", doc! {}), Bson::Int64(1));
    assert_eq!(diff(a.clone(), b.clone(), "week", doc! {}), Bson::Int64(0));
    assert_eq!(diff(a.clone(), b.clone(), "day", doc! {}), Bson::Int64(1));
    assert_eq!(diff(a.clone(), b.clone(), "hour", doc! {}), Bson::Int64(1));
    assert_eq!(diff(a.clone(), b.clone(), "minute", doc! {}), Bson::Int64(2));
    assert_eq!(diff(a.clone(), b.clone(), "second", doc! {}), Bson::Int64(120));
    assert_eq!(diff(a.clone(), b.clone(), "millisecond", doc! {}), Bson::Int64(120_000));
    // Reversed: negative.
    assert_eq!(diff(b.clone(), a, "year", doc! {}), Bson::Int64(-1));
    assert_eq!(diff(b.clone(), b, "day", doc! {}), Bson::Int64(0));
    // 31 January to 1 February is a month boundary; 1 to 31 January is not.
    assert_eq!(
        diff(d(2024, 1, 31, 0, 0, 0), d(2024, 2, 1, 0, 0, 0), "month", doc! {}),
        Bson::Int64(1)
    );
    assert_eq!(
        diff(d(2024, 1, 1, 0, 0, 0), d(2024, 1, 31, 0, 0, 0), "month", doc! {}),
        Bson::Int64(0)
    );
}

#[test]
fn date_diff_weeks_use_start_of_week() {
    let sun = d(2024, 3, 3, 12, 0, 0);
    let mon = d(2024, 3, 4, 12, 0, 0);
    assert_eq!(diff(sun.clone(), mon.clone(), "week", doc! {}), Bson::Int64(0));
    assert_eq!(
        diff(sun.clone(), mon.clone(), "week", doc! { "startOfWeek": "monday" }),
        Bson::Int64(1)
    );
    assert_eq!(
        diff(sun.clone(), mon.clone(), "week", doc! { "startOfWeek": "MON" }),
        Bson::Int64(1)
    );
    // Ignored for any other unit, even if it is nonsense.
    assert_eq!(
        diff(sun.clone(), mon.clone(), "day", doc! { "startOfWeek": "blursday" }),
        Bson::Int64(1)
    );
    let e = refused(
        doc! { "$dateDiff": { "startDate": "$a", "endDate": "$b", "unit": "week", "startOfWeek": "blursday" } },
        doc! { "a": sun, "b": mon },
    );
    assert!(e.contains("startOfWeek"), "{e}");
}

#[test]
fn date_diff_in_a_zone() {
    // 23:00 and 00:30 in New York (EST) on either side of local midnight,
    // but the same UTC day.
    let a = d(2024, 3, 10, 4, 0, 0);
    let b = d(2024, 3, 10, 5, 30, 0);
    assert_eq!(diff(a.clone(), b.clone(), "day", doc! {}), Bson::Int64(0));
    assert_eq!(diff(a, b, "day", doc! { "timezone": "America/New_York" }), Bson::Int64(1));
    // Hour boundaries are the zone's: 13:45Z to 14:10Z is 19:15 to 19:40 IST.
    assert_eq!(
        diff(
            d(2024, 3, 15, 13, 45, 0),
            d(2024, 3, 15, 14, 10, 0),
            "hour",
            doc! { "timezone": "Asia/Kolkata" }
        ),
        Bson::Int64(0)
    );
    assert_eq!(
        diff(d(2024, 3, 15, 13, 45, 0), d(2024, 3, 15, 14, 10, 0), "hour", doc! {}),
        Bson::Int64(1)
    );
    // Days across the spring-forward change are calendar days.
    assert_eq!(
        diff(
            d(2024, 3, 9, 17, 0, 0),
            d(2024, 3, 11, 16, 0, 0),
            "day",
            doc! { "timezone": "America/New_York" }
        ),
        Bson::Int64(2)
    );
}

#[test]
fn date_diff_far_dates_and_overflow() {
    // extra.out: the whole of the reference's JavaScript range is exact.
    let (lo, hi) = (at(-8_640_000_000_000_000), at(8_640_000_000_000_000));
    assert_eq!(diff(lo.clone(), hi.clone(), "year", doc! {}), Bson::Int64(547_581));
    assert_eq!(diff(lo, hi, "millisecond", doc! {}), Bson::Int64(17_280_000_000_000_000));
    // Past the 64-bit range the reference's answer is garbage (0); here it is refused.
    let spec = doc! { "startDate": "$a", "endDate": "$b", "unit": "millisecond" };
    let e = refused(doc! { "$dateDiff": spec }, doc! { "a": at(i64::MIN), "b": at(i64::MAX) });
    assert!(e.contains("Overflow"), "{e}");
}

#[test]
fn date_diff_minutes_and_seconds_use_the_seconds_of_a_local_mean_offset() {
    // New York before 1883 is -04:56:02 (extra.out "diff minute LMT NY").
    let ny = doc! { "timezone": "America/New_York" };
    let t = |h, mi, sec, ms| dm(1800, 1, 1, h, mi, sec, ms);
    assert_eq!(diff(t(12, 0, 1, 0), t(12, 0, 3, 0), "minute", ny.clone()), Bson::Int64(1));
    assert_eq!(diff(t(12, 0, 1, 900), t(12, 0, 2, 100), "second", ny), Bson::Int64(1));
}

#[test]
fn date_diff_nulls_and_errors() {
    let row = doc! { "a": d(2024, 1, 1, 0, 0, 0), "n": Bson::Null };
    for spec in [
        doc! { "startDate": "$n", "endDate": "$a", "unit": "day" },
        doc! { "startDate": "$a", "endDate": "$n", "unit": "day" },
        doc! { "startDate": "$a", "endDate": "$a", "unit": "$n" },
        doc! { "startDate": "$a", "endDate": "$a", "unit": "day", "timezone": "$n" },
    ] {
        assert_eq!(ev(doc! { "$dateDiff": spec }, row.clone()), Bson::Null);
    }
    // Any null answers null before a type or unit is judged (extra.out).
    for spec in [
        doc! { "startDate": "x", "endDate": Bson::Null, "unit": "day" },
        doc! { "startDate": "x", "endDate": "$a", "unit": Bson::Null },
        doc! { "startDate": "$a", "endDate": "$a", "unit": "decade", "timezone": Bson::Null },
        doc! { "startDate": "$a", "endDate": "$a", "unit": "decade", "timezone": "$n" },
    ] {
        assert_eq!(ev(doc! { "$dateDiff": spec.clone() }, row.clone()), Bson::Null, "{spec}");
    }
    // startOfWeek is not read for a unit other than week; a number is refused for week.
    let (sun, mon) = (d(2024, 3, 3, 12, 0, 0), d(2024, 3, 4, 12, 0, 0));
    assert_eq!(
        diff(sun.clone(), mon.clone(), "day", doc! { "startOfWeek": Bson::Null }),
        Bson::Int64(1)
    );
    let e = refused(
        doc! { "$dateDiff": { "startDate": "$a", "endDate": "$b", "unit": "week", "startOfWeek": 1 } },
        doc! { "a": sun, "b": mon },
    );
    assert!(e.contains("startOfWeek"), "{e}");
    let e = refused(
        doc! { "$dateDiff": { "startDate": "$a", "endDate": 5, "unit": "day" } },
        row.clone(),
    );
    assert!(e.contains("endDate to be convertible to a date"), "{e}");
    let e =
        refused(doc! { "$dateDiff": { "startDate": "$a", "endDate": "$a", "unit": "eon" } }, row);
    assert!(e.contains("unit to be one of"), "{e}");
}

// -- $dateTrunc ------------------------------------------------------------------

fn trunc(date: Bson, unit: &str, extra: Document) -> Bson {
    let mut spec = doc! { "date": "$d", "unit": unit };
    for (k, v) in extra {
        spec.insert(k, v);
    }
    ev(doc! { "$dateTrunc": spec }, doc! { "d": date })
}

#[test]
fn date_trunc_units() {
    let t = dm(2024, 3, 15, 13, 45, 7, 250);
    assert_eq!(trunc(t.clone(), "year", doc! {}), d(2024, 1, 1, 0, 0, 0));
    assert_eq!(trunc(t.clone(), "quarter", doc! {}), d(2024, 1, 1, 0, 0, 0));
    assert_eq!(trunc(t.clone(), "month", doc! {}), d(2024, 3, 1, 0, 0, 0));
    assert_eq!(trunc(t.clone(), "day", doc! {}), d(2024, 3, 15, 0, 0, 0));
    assert_eq!(trunc(t.clone(), "hour", doc! {}), d(2024, 3, 15, 13, 0, 0));
    assert_eq!(trunc(t.clone(), "minute", doc! {}), d(2024, 3, 15, 13, 45, 0));
    assert_eq!(trunc(t.clone(), "second", doc! {}), d(2024, 3, 15, 13, 45, 7));
    assert_eq!(trunc(t.clone(), "millisecond", doc! {}), t);
    // Friday: the week began on Sunday the 10th, or Monday the 11th.
    assert_eq!(trunc(t.clone(), "week", doc! {}), d(2024, 3, 10, 0, 0, 0));
    assert_eq!(trunc(t.clone(), "week", doc! { "startOfWeek": "monday" }), d(2024, 3, 11, 0, 0, 0));
    assert_eq!(trunc(t.clone(), "week", doc! { "startOfWeek": "friday" }), d(2024, 3, 15, 0, 0, 0));
    assert_eq!(trunc(t, "week", doc! { "startOfWeek": "saturday" }), d(2024, 3, 9, 0, 0, 0));
    // Before the epoch.
    assert_eq!(trunc(d(1969, 12, 31, 23, 59, 59), "day", doc! {}), d(1969, 12, 31, 0, 0, 0));
    assert_eq!(trunc(d(1969, 12, 31, 23, 59, 59), "year", doc! {}), d(1969, 1, 1, 0, 0, 0));
}

#[test]
fn date_trunc_bins_count_from_2000() {
    let t = d(2024, 3, 15, 13, 45, 7);
    assert_eq!(trunc(t.clone(), "year", doc! { "binSize": 5 }), d(2020, 1, 1, 0, 0, 0));
    assert_eq!(
        trunc(d(1999, 6, 1, 0, 0, 0), "year", doc! { "binSize": 5 }),
        d(1995, 1, 1, 0, 0, 0)
    );
    // 290 months since 2000-01: bins of 7 start at 287, which is 2023-12.
    assert_eq!(trunc(t.clone(), "month", doc! { "binSize": 7 }), d(2023, 12, 1, 0, 0, 0));
    assert_eq!(trunc(t.clone(), "quarter", doc! { "binSize": 3 }), d(2024, 1, 1, 0, 0, 0));
    // 212173 hours since 2000-01-01, bins of 5 start at 212170.
    assert_eq!(trunc(t.clone(), "hour", doc! { "binSize": 5 }), d(2024, 3, 15, 10, 0, 0));
    assert_eq!(trunc(t.clone(), "minute", doc! { "binSize": 15 }), d(2024, 3, 15, 13, 45, 0));
    // Day bins: 2000-01-01 + n * 10 days.
    assert_eq!(
        trunc(d(2000, 1, 11, 5, 0, 0), "day", doc! { "binSize": 10 }),
        d(2000, 1, 11, 0, 0, 0)
    );
    assert_eq!(
        trunc(d(2000, 1, 10, 5, 0, 0), "day", doc! { "binSize": 10 }),
        d(2000, 1, 1, 0, 0, 0)
    );
    // Week bins begin at the first startOfWeek day on or after 2000-01-01.
    assert_eq!(
        trunc(d(2000, 1, 2, 5, 0, 0), "week", doc! { "binSize": 2 }),
        d(2000, 1, 2, 0, 0, 0)
    );
    assert_eq!(
        trunc(d(2000, 1, 15, 5, 0, 0), "week", doc! { "binSize": 2 }),
        d(2000, 1, 2, 0, 0, 0)
    );
    assert_eq!(
        trunc(d(2000, 1, 16, 5, 0, 0), "week", doc! { "binSize": 2 }),
        d(2000, 1, 16, 0, 0, 0)
    );
}

#[test]
fn date_trunc_in_a_zone() {
    // Local midnight in New York: before the change it is EST, after it EDT.
    assert_eq!(
        trunc(d(2024, 3, 10, 12, 0, 0), "day", doc! { "timezone": "America/New_York" }),
        d(2024, 3, 10, 5, 0, 0)
    );
    assert_eq!(
        trunc(d(2024, 3, 11, 12, 0, 0), "day", doc! { "timezone": "America/New_York" }),
        d(2024, 3, 11, 4, 0, 0)
    );
    // 13:45Z is 19:15 in Kolkata; the local hour starts at 19:00 = 13:30Z.
    assert_eq!(
        trunc(d(2024, 3, 15, 13, 45, 0), "hour", doc! { "timezone": "Asia/Kolkata" }),
        d(2024, 3, 15, 13, 30, 0)
    );
    // The second 01:30 of the autumn change truncates to the second 01:00.
    assert_eq!(
        trunc(d(2024, 11, 3, 6, 30, 0), "hour", doc! { "timezone": "America/New_York" }),
        d(2024, 11, 3, 6, 0, 0)
    );
    assert_eq!(
        trunc(d(2024, 3, 15, 13, 45, 0), "month", doc! { "timezone": "Asia/Kolkata" }),
        d(2024, 2, 29, 18, 30, 0)
    );
}

#[test]
fn date_trunc_to_a_repeated_or_skipped_midnight() {
    // extra.out: a day whose midnight happens twice answers the later one; one
    // whose midnight is skipped answers the instant after the gap's start.
    let t = |when: i64, unit: &str, extra: Document| trunc(at(when), unit, extra);
    // 2024-11-03T12:00Z in Havana: the later 00:00 is 05:00Z.
    assert_eq!(
        t(1_730_635_200_000, "day", doc! { "timezone": "America/Havana" }),
        at(1_730_610_000_000)
    );
    // 2024-09-08T12:00Z in Santiago: midnight does not exist, 00:00 reads with the offset before.
    assert_eq!(
        t(1_725_796_800_000, "day", doc! { "timezone": "America/Santiago" }),
        at(1_725_768_000_000)
    );
    assert_eq!(
        t(
            1_725_969_600_000,
            "week",
            doc! { "timezone": "America/Santiago", "startOfWeek": "sunday" }
        ),
        at(1_725_768_000_000)
    );
    // 2024-11-03T06:30Z (the second 01:30 in New York), bins of two hours.
    assert_eq!(
        t(1_730_615_400_000, "hour", doc! { "binSize": 2, "timezone": "America/New_York" }),
        at(1_730_610_000_000)
    );
    assert_eq!(
        t(1_730_615_400_000, "day", doc! { "timezone": "America/New_York" }),
        at(1_730_606_400_000)
    );
    // Minutes in bins of 7 in a +05:45 zone.
    assert_eq!(
        t(1_710_510_300_000, "minute", doc! { "binSize": 7, "timezone": "Asia/Kathmandu" }),
        at(1_710_510_120_000)
    );
}

#[test]
fn date_trunc_to_the_minute_ignores_the_seconds_of_a_local_mean_offset() {
    // extra.out: New York in 1800 is -04:56:02, and $dateTrunc to a minute
    // answers 12:00:00Z for 12:00:30Z (the offset's seconds are ignored).
    let ny = doc! { "timezone": "America/New_York" };
    let noon = -5_364_619_200_000_i64;
    assert_eq!(trunc(at(noon + 30_000), "minute", ny.clone()), at(noon));
    assert_eq!(trunc(at(noon + 500), "second", ny), at(noon));
}

#[test]
fn date_trunc_bins_from_the_reference() {
    let t = |date: &str, unit: &str, extra: Document| {
        trunc(from_string(doc! { "dateString": date }), unit, extra)
    };
    assert_eq!(t("1994-06-01T00:00:00", "year", doc! { "binSize": 3 }), at(757_382_400_000));
    assert_eq!(t("2024-06-01T00:00:00", "quarter", doc! { "binSize": 5 }), at(1_696_118_400_000));
    assert_eq!(
        t("2024-06-01T00:00:00", "week", doc! { "binSize": 3, "startOfWeek": "monday" }),
        at(1_716_163_200_000)
    );
    // A bin far past the date's own place answers the origin (day, 1e9).
    assert_eq!(
        t("2024-06-01T00:00:00", "day", doc! { "binSize": 1_000_000_000 }),
        at(946_684_800_000)
    );
    // A double or decimal bin is read as a whole number.
    assert_eq!(t("2024-03-15T13:45:07.250", "day", doc! { "binSize": 2.0 }), at(1_710_460_800_000));
    // startOfWeek is not read for a unit other than week, null or nonsense.
    for sow in [Bson::Null, s("blursday")] {
        assert_eq!(
            t("2024-03-15T13:45:07.250", "day", doc! { "startOfWeek": sow }),
            at(1_710_460_800_000)
        );
    }
}

#[test]
fn date_trunc_refuses_a_bin_the_unit_cannot_count() {
    // extra.out: 5439006 for year, month, week and day at the largest long,
    // 5439002 for hour; a millisecond bin answers.
    let t = |date: &str, unit: &str, bin: i64| {
        try_eval(
            doc! { "$dateTrunc": { "date": { "$toDate": date }, "unit": unit, "binSize": bin } },
            doc! {},
        )
    };
    for (date, unit) in [
        ("1999-06-01T00:00:00Z", "year"),
        ("2024-06-01T00:00:00Z", "year"),
        ("1999-06-01T00:00:00Z", "month"),
        ("1999-06-01T00:00:00Z", "week"),
        ("1999-06-01T00:00:00Z", "day"),
    ] {
        let e = t(date, unit, i64::MAX).expect_err("must be refused").to_string();
        assert!(e.contains("dateTrunc unsupported binSize value"), "{unit}: {e}");
    }
    // Year at 2^31 - 1 is refused (the review's case), and a day bin that is
    // inside the range still answers.
    let e =
        t("2024-06-01T00:00:00Z", "year", i64::from(i32::MAX)).expect_err("refused").to_string();
    assert!(e.contains("unsupported binSize"), "{e}");
    let e = t("1999-06-01T00:00:00Z", "hour", i64::MAX).expect_err("refused").to_string();
    assert!(e.contains("dateTrunc overflowed"), "{e}");
    // A millisecond bin of 2^63 - 1 answers: the bin below the date is
    // origin - (2^63 - 1), which a 64-bit date holds (mongosh shows it as NaN).
    assert_eq!(
        t("1999-06-01T00:00:00Z", "millisecond", i64::MAX).expect("answers"),
        at(946_684_800_000 - i64::MAX)
    );
}

#[test]
fn extreme_dates_and_bins_never_panic() {
    // Debug builds check every overflow, so a wrap or a panic anywhere in the
    // calendar arithmetic fails this loop. An answer or a refusal are both fine.
    let dates = [
        i64::MIN,
        i64::MIN + 1,
        -9_200_000_000_000_000_000,
        -8_640_000_000_000_000,
        -62_198_841_600_000,
        -1,
        0,
        946_684_800_000,
        253_402_300_800_000,
        8_640_000_000_000_000,
        9_200_000_000_000_000_000,
        i64::MAX - 1,
        i64::MAX,
    ];
    let bins = [1, 2, 7, 1_000_000_000, 1 << 31, 1 << 40, 1 << 62, i64::MAX];
    let units =
        ["year", "quarter", "month", "week", "day", "hour", "minute", "second", "millisecond"];
    let zones = ["UTC", "America/New_York", "+05:30", "Asia/Kolkata", "+99"];
    let amounts = [0, 1, -1, 1_000_000, i64::from(i32::MAX), i64::MAX, i64::MIN];
    let mut answered = 0_u32;
    for date in dates {
        for unit in units {
            for tz in zones {
                let mut one = |expr: Document| {
                    if try_eval(expr, doc! { "a": at(date), "b": at(date.saturating_neg()) })
                        .is_ok()
                    {
                        answered += 1;
                    }
                };
                one(
                    doc! { "$dateDiff": { "startDate": "$a", "endDate": "$b", "unit": unit, "timezone": tz, "startOfWeek": "monday" } },
                );
                for bin in bins {
                    one(
                        doc! { "$dateTrunc": { "date": "$a", "unit": unit, "binSize": bin, "timezone": tz, "startOfWeek": "friday" } },
                    );
                }
                for amount in amounts {
                    one(
                        doc! { "$dateAdd": { "startDate": "$a", "unit": unit, "amount": amount, "timezone": tz } },
                    );
                    one(
                        doc! { "$dateSubtract": { "startDate": "$a", "unit": unit, "amount": amount, "timezone": tz } },
                    );
                }
            }
        }
        for tz in zones {
            for op in [
                "$year",
                "$isoWeekYear",
                "$week",
                "$dayOfYear",
                "$hour",
                "$isoWeek",
                "$dateToParts",
                "$dateToString",
            ] {
                let _ =
                    try_eval(doc! { op: { "date": "$a", "timezone": tz } }, doc! { "a": at(date) });
            }
        }
    }
    assert!(answered > 1000, "{answered}");
    // And the extreme parts of $dateFromParts and $dateFromString.
    for v in [i64::MIN, -1_i64 << 62, 1 << 62, i64::MAX] {
        for part in ["hour", "minute", "second", "millisecond", "month", "day"] {
            let _ = try_eval(doc! { "$dateFromParts": { "year": 2024, part: v } }, doc! {});
        }
        let _ = try_eval(
            doc! { "$dateFromParts": { "isoWeekYear": 2024, "isoWeek": v, "isoDayOfWeek": v } },
            doc! {},
        );
    }
}

#[test]
fn a_truncation_below_the_smallest_date_is_refused() {
    // The start of the unit that holds the smallest date lies below it, so no
    // 64-bit date can answer; it must be a refusal and not a wrapped number.
    for unit in ["year", "quarter", "month", "week", "day", "hour"] {
        let e = refused(
            doc! { "$dateTrunc": { "date": "$d", "unit": unit } },
            doc! { "d": at(i64::MIN) },
        );
        assert!(e.contains("dateTrunc overflowed"), "{unit}: {e}");
    }
    // One day above the smallest date the start of the day fits.
    let day = ev(
        doc! { "$dateTrunc": { "date": "$d", "unit": "day" } },
        doc! { "d": at(i64::MIN + 86_400_000 * 2) },
    );
    assert!(matches!(day, Bson::DateTime(_)), "{day:?}");
}

#[test]
fn the_ends_of_the_reference_date_range() {
    // extra.out: the year of the largest and smallest dates mongosh can show.
    let year = |ms: i64| ev(doc! { "$year": "$d" }, doc! { "d": at(ms) });
    assert_eq!(year(8_640_000_000_000_000), i(275_760));
    assert_eq!(year(-8_640_000_000_000_000), i(-271_821));
}

#[test]
fn date_trunc_nulls_and_errors() {
    let row = doc! { "d": d(2024, 1, 1, 0, 0, 0), "n": Bson::Null };
    for spec in [
        doc! { "date": "$n", "unit": "day" },
        doc! { "date": "$d", "unit": "day", "binSize": "$n" },
        doc! { "date": "$d", "unit": "day", "timezone": "$n" },
    ] {
        assert_eq!(ev(doc! { "$dateTrunc": spec }, row.clone()), Bson::Null);
    }
    // Any null answers null before a type or unit is judged (extra.out).
    for spec in [
        doc! { "date": "x", "unit": Bson::Null },
        doc! { "date": "$d", "unit": "decade", "binSize": Bson::Null },
        doc! { "date": "$d", "unit": "decade", "timezone": Bson::Null },
        doc! { "date": "$d", "unit": "day", "binSize": 0, "timezone": "$n" },
        doc! { "date": "$d", "unit": "day", "timezone": "Bad/Zone", "binSize": Bson::Null },
    ] {
        assert_eq!(ev(doc! { "$dateTrunc": spec.clone() }, row.clone()), Bson::Null, "{spec}");
    }
    for bad in [0, -3] {
        let e = refused(
            doc! { "$dateTrunc": { "date": "$d", "unit": "day", "binSize": bad } },
            row.clone(),
        );
        assert!(e.contains("binSize"), "{e}");
    }
    let e = refused(
        doc! { "$dateTrunc": { "date": "$d", "unit": "day", "binSize": 1.5 } },
        row.clone(),
    );
    assert!(e.contains("binSize"), "{e}");
    let e = refused(doc! { "$dateTrunc": { "date": 5, "unit": "day" } }, row);
    assert!(e.contains("date to be convertible to a date"), "{e}");
}

#[test]
fn zone_names_are_case_sensitive_and_links_work() {
    let t = d(2024, 1, 1, 12, 0, 0);
    assert_eq!(tzd("$hour", t.clone(), "America/New_York"), i(7));
    assert_eq!(tzd("$hour", t.clone(), "US/Eastern"), i(7));
    assert_eq!(tzd("$hour", t.clone(), "Etc/UTC"), i(12));
    // The reference refuses a name in any other case (date.diag.out).
    for wrong in ["america/new_york", "us/eastern", "etc/utc", "Utc"] {
        let e =
            refused(doc! { "$hour": { "date": "$d", "timezone": wrong } }, doc! { "d": t.clone() });
        assert!(e.contains("unrecognized time zone identifier"), "{wrong}: {e}");
    }
}
