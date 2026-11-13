//! The session readers: a Claude Code session (its record for the turn, its transcript for
//! limits) and a Codex task (its rollout for both).

use super::reset::{claude_reset, codex_reset, epoch, to_the_millisecond};
use super::tail::{HISTORY_BYTES, Read, Tail};
use super::{Event, Limit, MATCHED_CHARS, MAX_RESET_SECONDS, Source, Turn, TurnState, claude_turn};
use jiff::tz::TimeZone;
use serde_json::Value;
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A bounded set of event keys. A key joins it when the sample that saw it commits, so a
/// sample that fails leaves it as it was.
struct Seen {
    limit: usize,
    order: VecDeque<String>,
    keys: HashSet<String>,
}

impl Seen {
    fn new() -> Self {
        Self {
            limit: 4096,
            order: VecDeque::new(),
            keys: HashSet::new(),
        }
    }

    fn contains(&self, key: &str) -> bool {
        self.keys.contains(key)
    }

    fn add_all(&mut self, keys: impl IntoIterator<Item = String>) {
        for key in keys {
            if self.keys.remove(&key) {
                self.order.retain(|k| *k != key);
            }
            self.keys.insert(key.clone());
            self.order.push_back(key);
        }
        while self.order.len() > self.limit {
            if let Some(old) = self.order.pop_front() {
                self.keys.remove(&old);
            }
        }
    }
}

fn message_content(line: &Value) -> Option<&Value> {
    line.get("message")?.as_object()?.get("content")
}

fn claude_text(line: &Value) -> String {
    match message_content(line) {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
            .map(|b| b.get("text").and_then(Value::as_str).unwrap_or(""))
            .collect::<Vec<_>>()
            .join(" "),
        _ => String::new(),
    }
}

/// The usage-limit failure a transcript line records, if it is one that happened after
/// `started_at` and says when it resets. The line's own `quotaLimits.resetsAt` is exact;
/// the "resets <time> (<zone>)" text is to the hour.
pub fn claude_limit(line: &Value, started_at: f64, local: &TimeZone) -> Option<Limit> {
    if line.get("isApiErrorMessage") != Some(&Value::Bool(true))
        || line.get("error").and_then(Value::as_str) != Some("rate_limit")
    {
        return None;
    }
    let run_id = line.get("uuid")?.as_str()?.to_owned();
    let failed_at = epoch(line.get("timestamp")?)?;
    if failed_at < started_at {
        return None;
    }
    let text = claude_text(line);
    let exact = line
        .get("quotaLimits")
        .and_then(|q| q.get("resetsAt"))
        .and_then(Value::as_f64)
        .filter(|at| failed_at < *at && *at <= failed_at + MAX_RESET_SECONDS);
    let reset_at = exact.or_else(|| claude_reset(&text, failed_at, local))?;
    Some(Limit {
        run_id,
        source: "claude-transcript",
        matched: text.chars().take(MATCHED_CHARS).collect(),
        failed_at,
        reset_at,
    })
}

/// A `user` line that starts a turn: not a tool result, not meta.
pub fn claude_answered(line: &Value) -> bool {
    if line.get("type").and_then(Value::as_str) != Some("user") {
        return false;
    }
    if line
        .get("isMeta")
        .is_some_and(|v| !matches!(v, Value::Bool(false) | Value::Null))
    {
        return false;
    }
    match message_content(line) {
        Some(Value::Array(blocks)) => !blocks
            .iter()
            .any(|b| b.get("type").and_then(Value::as_str) == Some("tool_result")),
        _ => true,
    }
}

/// Claude Code's project folder name for a working directory.
pub fn claude_slug(cwd: &str) -> String {
    cwd.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

type Record = Arc<dyn Fn() -> Value + Send + Sync>;

/// A Claude Code session. An event is a transcript line with a `uuid`, and the uuid names
/// it: a transcript that is replaced or read again from its start (a resumed session copies
/// its history) gives each event once, and only those written since the adapter started.
pub struct ClaudeSource {
    record: Record,
    started_at: f64,
    local: TimeZone,
    tail: Tail,
    seen: Seen,
}

fn transcript(record: &Record, projects: &Path, session: &str) -> Option<PathBuf> {
    let file = format!("{session}.jsonl");
    if let Some(cwd) = record()
        .get("cwd")
        .and_then(Value::as_str)
        .filter(|c| !c.is_empty())
    {
        let path = projects.join(claude_slug(cwd)).join(&file);
        if path.is_file() {
            return Some(path);
        }
    }
    // The session may have moved since it started: its folder is the first cwd.
    let mut folders: Vec<PathBuf> = std::fs::read_dir(projects)
        .ok()?
        .filter_map(Result::ok)
        .map(|e| e.path().join(&file))
        .filter(|p| p.is_file())
        .collect();
    folders.sort();
    folders.into_iter().next()
}

impl ClaudeSource {
    pub fn new(
        session_id: &str,
        record: Record,
        home: &Path,
        started_at: f64,
        local: TimeZone,
    ) -> Self {
        let projects = home.join(".claude").join("projects");
        let session = session_id.to_owned();
        let located = Arc::clone(&record);
        let mut tail = Tail::new(move || transcript(&located, &projects, &session));
        tail.begin(0);
        Self {
            record,
            started_at: to_the_millisecond(started_at),
            local,
            tail,
            seen: Seen::new(),
        }
    }

    fn event(&self, read: &Read, staged: &mut HashSet<String>) -> Option<Event> {
        let line = &read.line;
        let stamp = line.get("timestamp").and_then(epoch);
        if read.replayed && stamp.is_none_or(|s| s < self.started_at) {
            return None;
        }
        let event = claude_limit(line, self.started_at, &self.local)
            .map(Event::Limit)
            .or_else(|| claude_answered(line).then_some(Event::Answered))?;
        if let Some(uuid) = line.get("uuid").and_then(Value::as_str) {
            let key = format!("claude\u{1}{uuid}");
            if self.seen.contains(&key) || staged.contains(&key) {
                return None;
            }
            staged.insert(key);
        } else if read.replayed {
            return None; // a line of a file met late that nothing identifies
        }
        Some(event)
    }
}

impl Source for ClaudeSource {
    fn peek_turn(&mut self) -> Turn {
        claude_turn(&(self.record)())
    }

    /// The turn and the events written since the last sample. Nothing is consumed until the
    /// whole sample has been handled.
    fn sample(&mut self) -> (Turn, Vec<Event>) {
        let mut events = Vec::new();
        let mut staged = HashSet::new();
        for read in self.tail.poll() {
            if let Some(event) = self.event(&read, &mut staged) {
                events.push(event);
            }
        }
        let turn = self.peek_turn();
        self.tail.commit();
        self.seen.add_all(staged);
        (turn, events)
    }
}

/// What a Codex task's rollout has shown so far: its turn and the latest `codex` rate-limit
/// snapshot of that turn.
#[derive(Clone, Debug)]
struct CodexState {
    turn: Turn,
    snapshot: Option<Value>,
}

/// A Codex task: its rollout says the turn state and the usage limit. An event is an
/// `event_msg` line and its key is its turn id, type and timestamp: a rollout that is read
/// again from its start (a fork copies its history) gives each event once, and only those
/// written since the adapter started.
pub struct CodexSource {
    started_at: f64,
    local: TimeZone,
    tail: Tail,
    seen: Seen,
    state: CodexState,
}

/// The thread's rollout: the most recently written of the files named for it (name breaks a
/// tie); once the thread is archived, its file there.
fn rollout(codex: &Path, thread: &str) -> Option<PathBuf> {
    let suffix = format!("-{thread}.jsonl");
    let named = |dir: &Path| -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.file_name().is_some_and(|n| {
                    let n = n.to_string_lossy();
                    n.starts_with("rollout-") && n.ends_with(&suffix)
                })
            })
            .collect()
    };
    let subdirs = |dir: &Path| -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect()
    };
    let mut found = Vec::new();
    for year in subdirs(&codex.join("sessions")) {
        for month in subdirs(&year) {
            for day in subdirs(&month) {
                found.extend(named(&day));
            }
        }
    }
    if found.is_empty() {
        found = named(&codex.join("archived_sessions"));
    }
    found.into_iter().max_by_key(|p| {
        let written = std::fs::metadata(p)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_nanos());
        (written, p.file_name().map(std::ffi::OsStr::to_owned))
    })
}

fn codex_event(line: &Value) -> Option<&Value> {
    (line.get("type").and_then(Value::as_str) == Some("event_msg"))
        .then(|| line.get("payload").filter(|p| p.is_object()))
        .flatten()
}

fn note(state: &mut CodexState, event: &Value) {
    let kind = event.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "task_started" => {
            state.turn = Turn::of(TurnState::Busy);
            state.snapshot = None;
        }
        "task_complete" | "turn_aborted" => state.turn = Turn::of(TurnState::Idle),
        _ => {}
    }
    let limits = event.get("rate_limits").filter(|l| l.is_object());
    if kind == "token_count"
        && let Some(limits) = limits
        && limits.get("limit_id").and_then(Value::as_str) == Some("codex")
    {
        // Another model's bucket never replaces it.
        state.snapshot = Some(limits.clone());
    }
}

impl CodexSource {
    pub fn new(thread_id: &str, home: &Path, started_at: f64, local: TimeZone) -> Self {
        let codex = home.join(".codex");
        let thread = thread_id.to_owned();
        let mut tail = Tail::new(move || rollout(&codex, &thread));
        // What the rollout ended in, folded and committed as a sample of those lines.
        let history: Vec<Read> = tail
            .begin(HISTORY_BYTES)
            .into_iter()
            .map(|line| Read {
                line,
                replayed: false,
            })
            .collect();
        let mut source = Self {
            started_at: to_the_millisecond(started_at),
            local,
            tail,
            seen: Seen::new(),
            state: CodexState {
                turn: Turn::UNKNOWN,
                snapshot: None,
            },
        };
        let (state, _, staged) = source.fold(history);
        source.state = state;
        source.seen.add_all(staged);
        source
    }

    fn failure(&self, state: &CodexState, event: &Value, line: &Value) -> Option<Limit> {
        let error = event.get("error").filter(|e| e.is_object())?;
        if error.get("codex_error_info").and_then(Value::as_str) != Some("usage_limit_exceeded") {
            return None;
        }
        let failed_at = event
            .get("completed_at")
            .and_then(Value::as_f64)
            .or_else(|| line.get("timestamp").and_then(epoch))?;
        let run_id = event.get("turn_id")?.as_str()?.to_owned();
        if failed_at < self.started_at {
            return None;
        }
        let message = error.get("message").and_then(Value::as_str).unwrap_or("");
        let reset_at = codex_reset(message, state.snapshot.as_ref(), failed_at, &self.local)?;
        Some(Limit {
            run_id,
            source: "codex-rollout",
            matched: message.chars().take(MATCHED_CHARS).collect(),
            failed_at,
            reset_at,
        })
    }

    fn handle(
        &self,
        state: &mut CodexState,
        read: &Read,
        staged: &mut HashSet<String>,
    ) -> Vec<Event> {
        let line = &read.line;
        let Some(event) = codex_event(line) else {
            return Vec::new();
        };
        let stamp = line.get("timestamp").and_then(epoch);
        if read.replayed && stamp.is_none_or(|s| s < self.started_at) {
            return Vec::new();
        }
        let text = |v: Option<&Value>| match v {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        };
        let kind = text(event.get("type"));
        let key = format!(
            "codex\u{1}{}\u{1}{kind}\u{1}{}",
            text(event.get("turn_id")),
            text(line.get("timestamp"))
        );
        if self.seen.contains(&key) || staged.contains(&key) {
            return Vec::new();
        }
        staged.insert(key);
        note(state, event);
        match kind.as_str() {
            "task_started" => vec![Event::Answered],
            "task_complete" => self
                .failure(state, event, line)
                .map(Event::Limit)
                .into_iter()
                .collect(),
            _ => Vec::new(),
        }
    }

    /// The one reducer: `reads` folded into a copy of the committed state, with the events
    /// they give and the event keys they add. It changes nothing on the source.
    fn fold(
        &self,
        reads: impl IntoIterator<Item = Read>,
    ) -> (CodexState, Vec<Event>, HashSet<String>) {
        let mut state = self.state.clone();
        let mut staged = HashSet::new();
        let mut events = Vec::new();
        for read in reads {
            events.extend(self.handle(&mut state, &read, &mut staged));
        }
        (state, events, staged)
    }
}

impl Source for CodexSource {
    /// The turn the rollout shows now: the unread lines folded into a copy of the committed
    /// state. Nothing is committed.
    fn peek_turn(&mut self) -> Turn {
        let unread = self.tail.peek();
        self.fold(unread).0.turn
    }

    /// The turn and the events written since the last sample. The state and the position
    /// move on together, once the whole sample has been handled.
    fn sample(&mut self) -> (Turn, Vec<Event>) {
        let unread = self.tail.poll();
        let (state, events, staged) = self.fold(unread);
        self.tail.commit();
        self.state = state;
        self.seen.add_all(staged);
        (self.state.turn, events)
    }
}
