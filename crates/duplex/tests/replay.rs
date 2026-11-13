//! The session readers driven by lines shaped like those Claude Code and Codex write.
//!
//! Each folder under `tests/fixtures/replay/<runtime>-<version>/<name>/` has a manifest. The
//! lines are synthetic (the keys the readers use, with example ids); a new release is added
//! by copying that shape from a real file by hand.

use duplex_adapter::watch::reset::epoch;
use duplex_adapter::watch::sources::{ClaudeSource, CodexSource, claude_answered, claude_limit};
use duplex_adapter::watch::{Event, Source, Turn, TurnState, claude_turn};
use jiff::tz::TimeZone;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn replay() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/replay")
}

fn read_lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("duplex-replay-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const SESSION: &str = "3d7e1c52-9a40-4b6e-8f21-5c0a7d9e4b13";
const LIMIT_TEXT: &str = "You've hit your session limit \u{b7} resets 8pm (America/Chicago)";

fn claude_limit_file() -> PathBuf {
    replay().join(format!("claude-2.1.321/claude-limit/{SESSION}.jsonl"))
}

fn chicago() -> TimeZone {
    TimeZone::get("America/Chicago").unwrap()
}

#[test]
fn every_fixture_has_a_manifest_naming_its_runtime_and_version() {
    let mut folders = 0;
    for runtime in std::fs::read_dir(replay()).unwrap().flatten() {
        for fixture in std::fs::read_dir(runtime.path()).unwrap().flatten() {
            let manifest: Value = serde_json::from_str(
                &std::fs::read_to_string(fixture.path().join("manifest.json")).unwrap(),
            )
            .unwrap();
            let expected = format!(
                "{}-{}",
                manifest["runtime"].as_str().unwrap(),
                manifest["version"].as_str().unwrap()
            );
            assert_eq!(runtime.file_name().to_string_lossy(), expected);
            folders += 1;
        }
    }
    assert!(folders >= 7, "{folders} fixtures");
}

#[test]
fn session_records_read_as_their_turn_state() {
    let folder = replay().join("claude-2.1.321");
    let expected = [
        ("session-busy", Turn::of(TurnState::Busy)),
        ("session-idle", Turn::of(TurnState::Idle)),
        ("session-waiting", Turn::waiting("permission_prompt")),
        ("session-blocked", Turn::waiting("blocked")),
    ];
    for (name, turn) in expected {
        let record: Value = serde_json::from_str(
            &std::fs::read_to_string(folder.join(name).join("4242.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(claude_turn(&record), turn, "{name}");
    }
}

#[test]
fn the_limit_line_is_found_with_its_reset() {
    let lines = read_lines(&claude_limit_file());
    let found: Vec<_> = lines
        .iter()
        .filter_map(|l| claude_limit(l, 0.0, &chicago()))
        .collect();
    assert_eq!(found.len(), 1);
    let limit = &found[0];
    assert_eq!(limit.run_id, "11111111-1111-4111-8111-111111111104");
    assert_eq!(limit.source, "claude-transcript");
    assert_eq!(limit.matched, LIMIT_TEXT);
    assert_eq!(limit.failed_at, 1_788_560_861.308);
    assert_eq!(limit.reset_at, 1_788_570_000.0);
    // The text alone gives the same instant as the line's own quota value.
    let bare: Vec<Value> = lines
        .iter()
        .map(|l| {
            let mut copy = l.clone();
            copy.as_object_mut().unwrap().remove("quotaLimits");
            copy
        })
        .collect();
    let from_text: Vec<_> = bare
        .iter()
        .filter_map(|l| claude_limit(l, 0.0, &chicago()))
        .collect();
    assert_eq!(from_text[0].reset_at, limit.reset_at);
}

#[test]
fn a_failure_before_the_adapter_started_is_not_reported() {
    let lines = read_lines(&claude_limit_file());
    let after = 1_788_560_861.309;
    assert!(
        lines
            .iter()
            .all(|l| claude_limit(l, after, &chicago()).is_none())
    );
}

#[test]
fn a_tool_result_is_not_a_new_turn_and_a_typed_line_is() {
    let answered: Vec<(String, bool)> = read_lines(&claude_limit_file())
        .iter()
        .filter(|l| l["type"] == "user")
        .map(|l| {
            (
                l["uuid"].as_str().unwrap()[24..].to_owned(),
                claude_answered(l),
            )
        })
        .collect();
    assert_eq!(
        answered,
        [
            ("111111111103".to_owned(), false),
            ("111111111105".to_owned(), true)
        ]
    );
    assert!(!claude_answered(
        &json!({"type": "user", "isMeta": true, "message": {"content": "x"}})
    ));
    assert!(!claude_answered(
        &json!({"type": "assistant", "message": {"content": "x"}})
    ));
}

#[test]
fn the_claude_source_reads_the_transcript_as_a_limit_then_an_answer() {
    let home = Scratch::new("claude");
    let record = Arc::new(|| json!({"cwd": "/home/user/project", "status": "idle"}));
    let mut source = ClaudeSource::new(SESSION, record, &home.0, 0.0, chicago());
    assert!(
        source.sample().1.is_empty(),
        "nothing yet: the transcript starts later"
    );
    let target = home.0.join(".claude/projects/-home-user-project");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::copy(claude_limit_file(), target.join(format!("{SESSION}.jsonl"))).unwrap();
    let (turn, events) = source.sample();
    assert_eq!(turn, Turn::of(TurnState::Idle));
    let kinds: Vec<&str> = events
        .iter()
        .map(|e| match e {
            Event::Limit(_) => "limit",
            Event::Answered => "answered",
        })
        .collect();
    assert_eq!(kinds, ["limit", "answered"]);
    assert!(source.sample().1.is_empty(), "each event is given once");
}

#[test]
fn a_transcript_read_again_from_its_start_gives_each_event_once() {
    let home = Scratch::new("claude-again");
    let record = Arc::new(|| json!({"status": "idle"}));
    let mut source = ClaudeSource::new(SESSION, record, &home.0, 0.0, chicago());
    let target = home.0.join(".claude/projects/elsewhere");
    std::fs::create_dir_all(&target).unwrap();
    let path = target.join(format!("{SESSION}.jsonl"));
    std::fs::copy(claude_limit_file(), &path).unwrap();
    assert_eq!(
        source.sample().1.len(),
        2,
        "found by its session id under any folder"
    );
    // A resumed session copies its history into a fresh file.
    std::fs::remove_file(&path).unwrap();
    std::fs::copy(claude_limit_file(), &path).unwrap();
    assert!(source.sample().1.is_empty());
}

/// A Codex source that has taken its first reading, with the fixture's first `count` lines
/// then appearing in the rollout of its thread.
fn stage(home: &Scratch, fixture: &Path, count: usize, local: TimeZone) -> CodexSource {
    let name = fixture.file_name().unwrap().to_string_lossy().into_owned();
    let thread = name
        .trim_end_matches(".jsonl")
        .rsplitn(6, '-')
        .take(5)
        .collect::<Vec<_>>();
    let thread_id = thread.into_iter().rev().collect::<Vec<_>>().join("-");
    let mut source = CodexSource::new(&thread_id, &home.0, 0.0, local);
    assert_eq!(source.sample(), (Turn::UNKNOWN, vec![]));
    let folder = home.0.join(".codex/sessions/2026/10/09");
    std::fs::create_dir_all(&folder).unwrap();
    let raw = std::fs::read_to_string(fixture).unwrap();
    let kept: String = raw.split_inclusive('\n').take(count).collect();
    std::fs::write(folder.join(&name), kept).unwrap();
    source
}

fn fixture(folder: &str) -> PathBuf {
    let dir = replay().join(format!("codex-0.167.1/{folder}"));
    std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .unwrap()
}

#[test]
fn a_codex_turn_is_busy_until_its_task_completes() {
    let home = Scratch::new("codex-turns");
    let path = fixture("codex-turns");
    let mut source = stage(&home, &path, 2, TimeZone::UTC);
    assert_eq!(
        source.peek_turn(),
        Turn::of(TurnState::Busy),
        "peek folds the unread lines"
    );
    assert_eq!(
        source.peek_turn(),
        Turn::of(TurnState::Busy),
        "and consumes nothing"
    );
    let (turn, events) = source.sample();
    assert_eq!(turn, Turn::of(TurnState::Busy));
    assert_eq!(events, [Event::Answered]);
}

#[test]
fn a_finished_codex_turn_is_idle() {
    let home = Scratch::new("codex-done");
    let mut source = stage(&home, &fixture("codex-turns"), 4, TimeZone::UTC);
    assert_eq!(
        source.sample(),
        (Turn::of(TurnState::Idle), vec![Event::Answered])
    );
}

#[test]
fn an_aborted_codex_turn_is_idle() {
    let home = Scratch::new("codex-abort");
    let mut source = stage(&home, &fixture("codex-turn-aborted"), 2, TimeZone::UTC);
    assert_eq!(
        source.sample(),
        (Turn::of(TurnState::Idle), vec![Event::Answered])
    );
}

#[test]
fn a_usage_limit_resets_when_its_full_window_does() {
    let home = Scratch::new("codex-limit");
    let mut source = stage(&home, &fixture("codex-usage-limit"), 5, TimeZone::UTC);
    let (turn, events) = source.sample();
    assert_eq!(
        turn,
        Turn::of(TurnState::Busy),
        "a new turn began after the failure"
    );
    let [Event::Answered, Event::Limit(limit), Event::Answered] = events.as_slice() else {
        panic!("{events:?}");
    };
    assert_eq!(limit.run_id, "01a00000-0000-7000-8000-0000000000b1");
    assert_eq!(limit.source, "codex-rollout");
    assert_eq!(
        limit.reset_at, 1_791_950_327.0,
        "the snapshot's full window, not the message"
    );
    assert_eq!(limit.failed_at, 1_791_590_495.0);
    assert!(
        limit
            .matched
            .contains("try again at Oct 13th, 2026 10:58 PM")
    );
}

#[test]
fn without_a_snapshot_the_message_time_is_read_in_the_local_zone() {
    let home = Scratch::new("codex-message");
    let path = fixture("codex-usage-limit");
    let raw = std::fs::read_to_string(&path).unwrap();
    // The usage-limit rollout without its token_count lines.
    let lines: Vec<&str> = raw.lines().filter(|l| !l.contains("token_count")).collect();
    let trimmed = home
        .0
        .join("rollout-2026-10-09T13-14-33-01a00000-0000-7000-8000-000000000002.jsonl");
    std::fs::write(&trimmed, lines.join("\n") + "\n").unwrap();
    let mut source = stage(&home, &trimmed, 3, chicago());
    let (_, events) = source.sample();
    let limit = events
        .into_iter()
        .find_map(|e| {
            if let Event::Limit(l) = e {
                Some(l)
            } else {
                None
            }
        })
        .expect("a limit");
    assert_eq!(Some(limit.reset_at), epoch(&json!("2026-10-14T03:58:00Z")));
}

#[test]
fn a_failure_the_rollout_held_before_the_adapter_started_is_history() {
    let home = Scratch::new("codex-history");
    let path = fixture("codex-usage-limit");
    let name = path.file_name().unwrap().to_owned();
    let folder = home.0.join(".codex/sessions/2026/10/09");
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::copy(&path, folder.join(&name)).unwrap();
    let thread = "01a00000-0000-7000-8000-000000000002";
    let mut source = CodexSource::new(thread, &home.0, 0.0, TimeZone::UTC);
    assert_eq!(
        source.sample(),
        (Turn::of(TurnState::Busy), vec![]),
        "the state comes from history, no events"
    );
}
