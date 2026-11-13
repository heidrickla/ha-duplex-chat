//! What the adapter reads from its own session: whether a turn is running, and whether the
//! session hit a usage limit.
//!
//! A Claude session says it in `~/.claude/sessions/<pid>.json` (turn state) and its
//! transcript `~/.claude/projects/<cwd slug>/<session id>.jsonl` (limit); a Codex task says
//! both in its rollout `~/.codex/sessions/YYYY/MM/DD/rollout-*-<thread id>.jsonl`. A source
//! returns what it saw; reporting it to the broker is the adapter's job. Reads never fail
//! for a missing or half-written file, and only a line written after the adapter started
//! can report a limit.

pub mod reset;
pub mod sources;
pub mod tail;

use duplex_proto::contract::WAIT_KINDS;
use regex::Regex;
use serde_json::Value;
use std::sync::OnceLock;

/// A reset more than this far past the failure is not a usage-limit window.
pub const MAX_RESET_SECONDS: f64 = 31.0 * 86400.0;
pub const MATCHED_CHARS: usize = 300;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TurnState {
    Busy,
    Idle,
    Waiting,
    Unknown,
}

impl TurnState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Busy => "busy",
            Self::Idle => "idle",
            Self::Waiting => "waiting",
            Self::Unknown => "unknown",
        }
    }
}

/// A turn reading. `waiting_for` is one of `WAIT_KINDS`, so no free text reaches a report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Turn {
    pub state: TurnState,
    pub waiting_for: Option<&'static str>,
}

impl Turn {
    pub const UNKNOWN: Self = Self::of(TurnState::Unknown);

    pub const fn of(state: TurnState) -> Self {
        Self {
            state,
            waiting_for: None,
        }
    }

    pub fn waiting(kind: &'static str) -> Self {
        debug_assert!(WAIT_KINDS.contains(&kind));
        Self {
            state: TurnState::Waiting,
            waiting_for: Some(kind),
        }
    }
}

/// A usage-limit failure and when it resets, both epoch seconds.
#[derive(Clone, Debug, PartialEq)]
pub struct Limit {
    pub run_id: String,
    pub source: &'static str,
    pub matched: String,
    pub failed_at: f64,
    pub reset_at: f64,
}

/// What a sample finds beside the turn.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Limit(Limit),
    /// The session took a new turn after a limit failure.
    Answered,
}

pub trait Source: Send {
    /// The turn state now, and the limit events written since the last sample.
    fn sample(&mut self) -> (Turn, Vec<Event>);
    /// The turn state now, consuming nothing. A claim's turn is a function of everything the
    /// reader has parsed before the claim; the watch interval never appears in it.
    fn peek_turn(&mut self) -> Turn;
}

fn wait_dialog() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[A-Za-z]+Dialog$").expect("a fixed pattern"))
}

/// The kind of wait a session record names: a known one, `dialog_open` for a dialog id,
/// else `user_input`. None when it names nothing. The value's own words are never kept.
pub fn wait_kind(value: &Value) -> Option<&'static str> {
    let text = value.as_str()?.trim();
    if text.is_empty() {
        return None;
    }
    if wait_dialog().is_match(text) {
        return Some("dialog_open");
    }
    // Each run of spaces, underscores and hyphens becomes one underscore.
    let mut known = String::new();
    let mut in_run = false;
    for c in text.to_lowercase().chars() {
        if c.is_whitespace() || c == '_' || c == '-' {
            if !in_run {
                known.push('_');
            }
            in_run = true;
        } else {
            known.push(c);
            in_run = false;
        }
    }
    // `blocked` and `user_input` are not names a record writes.
    WAIT_KINDS
        .iter()
        .copied()
        .find(|k| *k == known && !matches!(*k, "blocked" | "user_input"))
        .or(Some("user_input"))
}

/// A Claude Code session record as a turn. A blocked session is waiting.
pub fn claude_turn(record: &Value) -> Turn {
    let status = record.get("status").and_then(Value::as_str);
    let state = match status {
        Some("busy") => TurnState::Busy,
        Some("idle") => TurnState::Idle,
        Some("waiting" | "blocked") => TurnState::Waiting,
        _ => return Turn::UNKNOWN,
    };
    if state != TurnState::Waiting {
        return Turn::of(state);
    }
    let kind = record
        .get("waitingFor")
        .and_then(wait_kind)
        .or(if status == Some("blocked") {
            Some("blocked")
        } else {
            None
        });
    Turn {
        state,
        waiting_for: kind,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_wait_names_a_kind_never_its_own_words() {
        assert_eq!(
            wait_kind(&json!("permission_prompt")),
            Some("permission_prompt")
        );
        assert_eq!(
            wait_kind(&json!("Permission Prompt")),
            Some("permission_prompt")
        );
        assert_eq!(wait_kind(&json!("input-needed")), Some("input_needed"));
        assert_eq!(
            wait_kind(&json!("ToolPermissionDialog")),
            Some("dialog_open")
        );
        assert_eq!(
            wait_kind(&json!("Do you want to proceed?")),
            Some("user_input")
        );
        assert_eq!(wait_kind(&json!("blocked")), Some("user_input"));
        assert_eq!(wait_kind(&json!("  ")), None);
        assert_eq!(wait_kind(&json!(7)), None);
    }

    #[test]
    fn a_session_record_reads_as_its_turn() {
        assert_eq!(
            claude_turn(&json!({"status": "busy"})),
            Turn::of(TurnState::Busy)
        );
        assert_eq!(
            claude_turn(&json!({"status": "idle"})),
            Turn::of(TurnState::Idle)
        );
        assert_eq!(
            claude_turn(&json!({"status": "waiting", "waitingFor": "permission_prompt"})),
            Turn::waiting("permission_prompt")
        );
        assert_eq!(claude_turn(&json!({"status": "waiting"})).waiting_for, None);
        assert_eq!(
            claude_turn(&json!({"status": "blocked"})),
            Turn::waiting("blocked")
        );
        assert_eq!(
            claude_turn(&json!({"status": "blocked", "waitingFor": "worker_request"})),
            Turn::waiting("worker_request")
        );
        assert_eq!(claude_turn(&json!({"status": "napping"})), Turn::UNKNOWN);
        assert_eq!(claude_turn(&json!({})), Turn::UNKNOWN);
    }
}
