//! Fakes for the waker's tests: a local HTTP broker, a launcher that only records, fixed
//! tools and a scratch folder.

#![allow(dead_code)]

use duplex_waker::broker::Request;
use duplex_waker::launch::{Ended, Launcher, Plan, Started, Tools};
use duplex_waker::registry::{ProcEntry, ProcState, ProcessTable, WinProcesses};
use serde_json::Value;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// One answer of `command_lines`.
pub type Lines = Result<Vec<(u32, String)>, String>;

pub const TOKEN: &str = "waker-test-token-7f3a9c";

/// A `started_status` entry: the report is recorded, then the answer is lost.
pub const ANSWER_LOST: u16 = 0;

#[derive(Default)]
pub struct State {
    /// Answer every connection by closing it unanswered.
    pub down: bool,
    pub list_status: Option<u16>,
    pub listed: Vec<Value>,
    /// Statuses for the next `started` posts; 201 once empty. ANSWER_LOST records the
    /// report, then closes the connection unanswered.
    pub started_status: VecDeque<u16>,
    /// Reports recorded: one per `report_id`, as the broker keeps them.
    pub started: Vec<(String, Value)>,
    /// Every report body posted, repeats included.
    pub posts: Vec<Value>,
    /// List requests even after a recorded start, as the broker does once its retry
    /// window has passed.
    pub relist: bool,
    pub auth: Vec<String>,
}

pub struct FakeBroker {
    pub url: String,
    pub state: Arc<Mutex<State>>,
}

impl FakeBroker {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State::default()));
        let shared = Arc::clone(&state);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                serve(stream, &shared);
            }
        });
        Self { url, state }
    }

    pub fn list(&self, requests: &[Value]) {
        self.state.lock().unwrap().listed = requests.to_vec();
    }

    pub fn started(&self) -> Vec<(String, Value)> {
        self.state.lock().unwrap().started.clone()
    }
}

fn serve(stream: TcpStream, state: &Mutex<State>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut first = String::new();
    if reader.read_line(&mut first).is_err() {
        return;
    }
    let mut length = 0usize;
    let mut auth = String::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            length = v.trim().parse().unwrap_or(0);
        }
        if lower.starts_with("authorization:") {
            auth = line["authorization:".len()..].trim().to_owned();
        }
    }
    let mut body = vec![0u8; length];
    let _ = reader.read_exact(&mut body);
    let mut s = state.lock().unwrap();
    if s.down {
        return;
    }
    s.auth.push(auth);
    let mut parts = first.split_whitespace();
    let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let (status, reply) = if method == "GET" && path == "/api/waker/requests" {
        match s.list_status {
            // An error status still carries the list, so only the status says it is not one.
            Some(code) => (code, serde_json::to_string(&s.listed).unwrap()),
            // As the broker does, a request with a recorded start is not listed again.
            None => {
                let listed: Vec<&Value> = s
                    .listed
                    .iter()
                    .filter(|r| s.relist || !s.started.iter().any(|(id, _)| r["id"] == id.as_str()))
                    .collect();
                (200, serde_json::to_string(&listed).unwrap())
            }
        }
    } else if let Some(id) = path
        .strip_prefix("/api/waker/requests/")
        .and_then(|rest| rest.strip_suffix("/started"))
    {
        let code = s.started_status.pop_front().unwrap_or(201);
        if code != 201 && code != ANSWER_LOST {
            (code, "{}".to_owned())
        } else {
            let value: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
            s.posts.push(value.clone());
            // A repeat of a recorded report_id is answered as taken and changes nothing.
            let repeat = value["report_id"]
                .as_str()
                .filter(|rid| !rid.is_empty())
                .is_some_and(|rid| {
                    s.started
                        .iter()
                        .any(|(_, kept)| kept["report_id"].as_str() == Some(rid))
                });
            if !repeat {
                s.started.push((id.to_owned(), value));
            }
            if code == ANSWER_LOST {
                return;
            }
            (if repeat { 200 } else { 201 }, "{}".to_owned())
        }
    } else {
        (404, "{}".to_owned())
    };
    drop(s);
    let mut out = stream;
    let _ = write!(
        out,
        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
        reply.len()
    );
}

/// A port nothing listens on.
pub fn closed_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    url
}

#[derive(Default)]
pub struct FakeLauncher {
    pub starts: Vec<(String, String, Plan)>,
    pub running: Option<u32>,
    pub fail: Option<String>,
    /// Handed out by the next `reap`.
    pub ended: Vec<Ended>,
    /// Handed out by later `reap` calls, one batch per call, once `ended` is empty.
    pub reaps: VecDeque<Vec<Ended>>,
}

impl Launcher for FakeLauncher {
    fn start(&mut self, request_id: &str, session: &str, plan: &Plan) -> Result<Started, String> {
        self.starts
            .push((request_id.to_owned(), session.to_owned(), plan.clone()));
        match &self.fail {
            Some(why) => Err(why.clone()),
            None => Ok(Started {
                pid: 4242,
                detail: "pid 4242 running".to_owned(),
            }),
        }
    }

    fn running(&mut self, _session: &str) -> Option<u32> {
        self.running
    }

    fn reap(&mut self) -> Vec<Ended> {
        if self.ended.is_empty() {
            self.reaps.pop_front().unwrap_or_default()
        } else {
            std::mem::take(&mut self.ended)
        }
    }
}

pub struct FakeTools;

impl Tools for FakeTools {
    fn claude(&self) -> Option<PathBuf> {
        Some(PathBuf::from(r"C:\fake\claude.exe"))
    }
    fn codex(&self) -> Option<PathBuf> {
        Some(PathBuf::from(r"C:\fake\codex.exe"))
    }
    fn claude_model(&self, _session: &str) -> Option<String> {
        Some("claude-haiku-5-5".to_owned())
    }
}

/// A scratch folder removed when dropped.
pub struct Scratch(pub PathBuf);

impl Scratch {
    pub fn new(name: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "duplex-waker-test-{}-{}-{name}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn dir(&self, name: &str) -> PathBuf {
        let dir = self.0.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub const SESSION: &str = "5b0c7e2a-1f3d-4c8e-9a6b-2d4f6e8a0c13";

/// A listed request for a Claude session working in `workspace`.
pub fn claude_request(id: &str, workspace: &Path) -> Value {
    serde_json::json!({
        "id": id,
        "binding_id": "50e5f631f2324a6982fc2e8da9b8f4d8",
        "provider": "claude",
        "native_id": SESSION,
        "title": "duplex wake probe 2",
        "workspace": workspace.to_string_lossy(),
        "reason": "limit_resume",
        "message_id": "6058b30915944089b93e6d61672f5134",
        "due_at": "2026-11-11T07:00:00Z",
    })
}

pub fn request(value: &Value) -> Request {
    serde_json::from_value(value.clone()).unwrap()
}

/// One process in a scripted snapshot, with what reading its command line gives
/// (Ok(None): exited; Err: unreadable).
pub type FakeProc = (ProcEntry, Result<Option<String>, String>);
/// One answer of `snapshot`.
pub type Snapshot = Result<Vec<FakeProc>, String>;

/// A process table: states from the real one or a fixed answer, and snapshots from a
/// script (each `snapshot` call takes the next answer; the last one repeats).
pub struct FakeProcs {
    pub real_states: bool,
    pub state: fn(u32) -> ProcState,
    script: Mutex<VecDeque<Snapshot>>,
    current: Mutex<Vec<FakeProc>>,
    pub line_calls: AtomicUsize,
}

impl FakeProcs {
    /// Real process states; no claude.exe running.
    pub fn real() -> Self {
        Self::with(true, |_| ProcState::Gone, vec![Ok(Vec::new())])
    }

    /// Snapshots of claude.exe processes only, as (pid, command line).
    pub fn with(real_states: bool, state: fn(u32) -> ProcState, lines: Vec<Lines>) -> Self {
        let snaps = lines
            .into_iter()
            .map(|answer| {
                answer.map(|procs| {
                    procs
                        .into_iter()
                        .map(|(pid, line)| (claude(pid, 4), Ok(Some(line))))
                        .collect()
                })
            })
            .collect();
        Self::scripted(real_states, state, snaps)
    }

    pub fn scripted(real_states: bool, state: fn(u32) -> ProcState, snaps: Vec<Snapshot>) -> Self {
        Self {
            real_states,
            state,
            script: Mutex::new(snaps.into()),
            current: Mutex::new(Vec::new()),
            line_calls: AtomicUsize::new(0),
        }
    }
}

pub fn claude(pid: u32, parent: u32) -> ProcEntry {
    ProcEntry {
        pid,
        parent,
        image: "claude.exe".to_owned(),
    }
}

impl ProcessTable for FakeProcs {
    fn state(&self, pid: u32) -> ProcState {
        if self.real_states {
            WinProcesses.state(pid)
        } else {
            (self.state)(pid)
        }
    }

    fn snapshot(&self) -> Result<Vec<ProcEntry>, String> {
        self.line_calls.fetch_add(1, Ordering::SeqCst);
        let mut script = self.script.lock().unwrap();
        let answer = if script.len() > 1 {
            script.pop_front().unwrap()
        } else {
            script.front().cloned().unwrap_or(Ok(Vec::new()))
        };
        let procs = answer?;
        let entries = procs.iter().map(|(e, _)| e.clone()).collect();
        *self.current.lock().unwrap() = procs;
        Ok(entries)
    }

    fn command_line(&self, pid: u32) -> Result<Option<String>, String> {
        let current = self.current.lock().unwrap();
        current
            .iter()
            .find(|(e, _)| e.pid == pid)
            .map_or(Ok(None), |(_, line)| line.clone())
    }
}

/// The desktop app's command line for a session, as measured on 2.1.321.
pub fn desktop_line(session: &str) -> String {
    format!(
        r"C:\Users\user\AppData\Local\Claude-Data\claude_x\claude-code\2.1.321\b\claude.exe --output-format stream-json --resume={session} --allowedTools=x"
    )
}
