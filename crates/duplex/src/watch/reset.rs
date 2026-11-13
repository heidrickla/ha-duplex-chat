//! When a usage limit resets: the reset text a Claude Code transcript line or a Codex
//! rollout message carries, read in a time zone, with repeated and skipped hours handled.

use super::MAX_RESET_SECONDS;
use jiff::civil::{Date, DateTime};
use jiff::tz::{AmbiguousOffset, TimeZone};
use jiff::{Span, Timestamp};
use regex::{Captures, Regex};
use serde_json::Value;
use std::sync::OnceLock;

const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const WEEKDAYS: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];

fn claude_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)resets\s+(?:(?P<month>[A-Za-z]{3})[a-z]*\.?\s+(?P<day>\d{1,2}),?\s+|(?P<dow>[A-Za-z]{3})[a-z]*\.?\s+)?(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<half>[ap]m)\b(?:\s*\((?P<zone>[^)]+)\))?",
        )
        .expect("a fixed pattern")
    })
}

fn codex_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"try again at (?P<month>[A-Za-z]{3})[a-z]* (?P<day>\d{1,2})(?:st|nd|rd|th)?, (?P<year>\d{4}) (?P<hour>\d{1,2}):(?P<minute>\d{2}) (?P<half>[AP]M)",
        )
        .expect("a fixed pattern")
    })
}

/// `moment` cut to the whole millisecond, the precision transcripts and rollouts write: an
/// event stamped in the adapter's first millisecond is not before its start.
pub fn to_the_millisecond(moment: f64) -> f64 {
    let micros = (moment * 1e6).round() as i64;
    (micros / 1000) as f64 / 1000.0
}

/// An ISO 8601 UTC timestamp as epoch seconds.
pub fn epoch(stamp: &Value) -> Option<f64> {
    let parsed: Timestamp = stamp.as_str()?.parse().ok()?;
    Some(parsed.as_second() as f64 + f64::from(parsed.subsec_nanosecond()) / 1e9)
}

fn number(captures: &Captures<'_>, name: &str) -> Option<i64> {
    captures.name(name)?.as_str().parse().ok()
}

/// The wall-clock time a reset text writes, each part checked in its own range before any
/// is converted: a date that exists, hour 1 to 12 with AM or PM and 0 to 23 without,
/// minute 0 to 59.
fn civil_time(date: Date, hour: i64, minute: i64, half: Option<&str>) -> Option<DateTime> {
    let hours_ok = if half.is_some() {
        (1..=12).contains(&hour)
    } else {
        (0..24).contains(&hour)
    };
    if !hours_ok || !(0..60).contains(&minute) {
        return None;
    }
    let hour = match half {
        Some(h) => hour % 12 + if h.eq_ignore_ascii_case("pm") { 12 } else { 0 },
        None => hour,
    };
    Some(date.at(i8::try_from(hour).ok()?, i8::try_from(minute).ok()?, 0, 0))
}

/// What a civil time names in a zone: its instants as epoch seconds, ascending, and, for a
/// time the clocks skip, where they would put it. A repeated hour names two instants, a
/// skipped one none.
fn named(civil: DateTime, zone: &TimeZone) -> (Vec<i64>, Option<i64>) {
    let as_utc = civil
        .to_zoned(TimeZone::UTC)
        .map_or(0, |z| z.timestamp().as_second());
    match zone.to_ambiguous_timestamp(civil).offset() {
        AmbiguousOffset::Unambiguous { offset } => {
            (vec![as_utc - i64::from(offset.seconds())], None)
        }
        AmbiguousOffset::Fold { before, after } => {
            let mut both = vec![
                as_utc - i64::from(before.seconds()),
                as_utc - i64::from(after.seconds()),
            ];
            both.sort_unstable();
            (both, None)
        }
        AmbiguousOffset::Gap { before, .. } => {
            (Vec::new(), Some(as_utc - i64::from(before.seconds())))
        }
    }
}

/// (the earliest instant after `failed_at` that `civil` names within the usage-limit window,
/// or None; whether the time is refused). A time a clock change skips that would lie after
/// the failure has no instant, so the reset is not known: it is refused, not moved to the
/// next day.
fn first_after(civil: DateTime, zone: &TimeZone, failed_at: f64) -> (Option<f64>, bool) {
    let (instants, skipped) = named(civil, zone);
    if skipped.is_some_and(|s| s as f64 > failed_at) {
        return (None, true);
    }
    let chosen = instants
        .into_iter()
        .map(|i| i as f64)
        .find(|at| *at > failed_at);
    (
        chosen.filter(|at| *at <= failed_at + MAX_RESET_SECONDS),
        false,
    )
}

fn on(failed_at: f64, zone: &TimeZone) -> Option<Date> {
    let seconds = failed_at.floor() as i64;
    Some(
        Timestamp::from_second(seconds)
            .ok()?
            .to_zoned(zone.clone())
            .date(),
    )
}

/// The date in this year and the next that exist.
fn dates(year: i16, month: i8, day: i8) -> Vec<Date> {
    [year, year + 1]
        .into_iter()
        .filter_map(|y| Date::new(y, month, day).ok())
        .collect()
}

fn month_number(name: &str) -> Option<i8> {
    let lower = name.to_ascii_lowercase();
    MONTHS
        .iter()
        .position(|m| *m == lower)
        .and_then(|i| i8::try_from(i + 1).ok())
}

/// The first instant after `failed_at` that "resets <time> (<zone>)" names, or "resets <Mon>
/// <day>, <time>" for that date, or "resets <Weekday> <time>" for the next such day. A phrase
/// with no zone is read in `local`. Both instants of a repeated hour are considered, and a
/// time in a skipped hour is refused. None when the text has no such phrase, a bad time, date
/// or zone, or names no instant after the failure within the usage-limit window.
pub fn claude_reset(text: &str, failed_at: f64, local: &TimeZone) -> Option<f64> {
    let found = claude_re().captures(text)?;
    let hour = number(&found, "hour")?;
    let minute = number(&found, "minute").unwrap_or(0);
    let half = found.name("half").map(|m| m.as_str());
    let zone = match found.name("zone") {
        Some(name) => TimeZone::get(name.as_str().trim()).ok()?,
        None => local.clone(),
    };
    let today = on(failed_at, &zone)?;
    let days: Vec<Date> = if let Some(month) = found.name("month") {
        let days = dates(
            today.year(),
            month_number(month.as_str())?,
            i8::try_from(number(&found, "day")?).ok()?,
        );
        if days.is_empty() {
            return None;
        }
        days
    } else if let Some(dow) = found.name("dow") {
        let lower = dow.as_str().to_ascii_lowercase();
        let wanted = i8::try_from(WEEKDAYS.iter().position(|d| *d == lower)?).ok()?;
        (0..8)
            .filter_map(|k| today.checked_add(Span::new().days(k)).ok())
            .filter(|d| d.weekday().to_monday_zero_offset() == wanted)
            .collect()
    } else {
        vec![today, today.checked_add(Span::new().days(1)).ok()?]
    };
    let mut civils = Vec::new();
    for day in days {
        civils.push(civil_time(day, hour, minute, half)?);
    }
    for civil in civils {
        let (chosen, refused) = first_after(civil, &zone, failed_at);
        if refused {
            return None;
        }
        if chosen.is_some() {
            return chosen;
        }
    }
    None
}

/// When a Codex usage limit resets, after `failed_at`: the latest `codex` rate-limit
/// snapshot's full windows (the last of them to reset), else the message's "try again at
/// <Mon> <Dth>, <YYYY> <h:mm> <AM|PM>" read in `local`, its repeated and skipped hours
/// handled as for Claude's.
pub fn codex_reset(
    message: &str,
    snapshot: Option<&Value>,
    failed_at: f64,
    local: &TimeZone,
) -> Option<f64> {
    let resets: Vec<f64> = ["primary", "secondary"]
        .iter()
        .filter_map(|name| snapshot?.get(*name)?.as_object())
        .filter_map(|window| {
            let used = window.get("used_percent")?.as_f64()?;
            let at = window.get("resets_at")?.as_f64()?;
            (used >= 100.0 && failed_at < at && at <= failed_at + MAX_RESET_SECONDS).then_some(at)
        })
        .collect();
    if let Some(latest) = resets.into_iter().reduce(f64::max) {
        return Some(latest);
    }
    let found = codex_re().captures(message)?;
    let date = Date::new(
        i16::try_from(number(&found, "year")?).ok()?,
        month_number(found.name("month")?.as_str())?,
        i8::try_from(number(&found, "day")?).ok()?,
    )
    .ok()?;
    let civil = civil_time(
        date,
        number(&found, "hour")?,
        number(&found, "minute")?,
        found.name("half").map(|m| m.as_str()),
    )?;
    first_after(civil, local, failed_at).0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn at(stamp: &str) -> f64 {
        epoch(&json!(stamp)).expect("an ISO instant")
    }

    #[test]
    fn every_golden_case_reads_as_recorded() {
        let doc: Value =
            serde_json::from_str(include_str!("../../tests/fixtures/reset-cases.json")).unwrap();
        let cases = doc["cases"].as_array().unwrap();
        assert!(cases.len() >= 30);
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let local = TimeZone::get(case["local"].as_str().unwrap()).unwrap();
            let failed = at(case["failed"].as_str().unwrap());
            let text = case["text"].as_str().unwrap();
            let got = if case["runtime"] == "claude" {
                claude_reset(text, failed, &local)
            } else {
                codex_reset(text, None, failed, &local)
            };
            let want = case["at"].as_str().map(at);
            assert_eq!(got, want, "{name}");
        }
    }

    #[test]
    fn a_rate_limit_snapshot_with_a_full_window_wins_over_the_message() {
        let local = TimeZone::UTC;
        let failed = at("2026-10-10T00:01:35Z");
        let snapshot = json!({
            "primary": {"used_percent": 100.0, "resets_at": failed + 3600.0},
            "secondary": {"used_percent": 100.0, "resets_at": failed + 7200.0}
        });
        assert_eq!(
            codex_reset("", Some(&snapshot), failed, &local),
            Some(failed + 7200.0)
        );
        let part = json!({"primary": {"used_percent": 60.0, "resets_at": failed + 3600.0}});
        assert_eq!(codex_reset("", Some(&part), failed, &local), None);
        let past = json!({"primary": {"used_percent": 100.0, "resets_at": failed - 1.0}});
        assert_eq!(codex_reset("", Some(&past), failed, &local), None);
    }

    #[test]
    fn a_reset_beyond_the_window_is_not_a_usage_limit() {
        let local = TimeZone::UTC;
        let failed = at("2026-01-01T00:00:00Z");
        assert_eq!(
            claude_reset("resets Mar 1, 8pm (UTC)", failed, &local),
            None
        );
        assert!(claude_reset("resets Jan 20, 8pm (UTC)", failed, &local).is_some());
    }

    #[test]
    fn timestamps_cut_to_the_millisecond() {
        assert_eq!(to_the_millisecond(1_788_560_861.308_4), 1_788_560_861.308);
        assert_eq!(to_the_millisecond(1.9999), 1.999);
        assert_eq!(
            epoch(&json!("2026-09-04T22:27:41.308Z")),
            Some(1_788_560_861.308)
        );
        assert_eq!(epoch(&json!("not a time")), None);
        assert_eq!(epoch(&json!(5)), None);
    }
}
