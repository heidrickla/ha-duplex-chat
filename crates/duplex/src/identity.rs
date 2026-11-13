//! Who the adapter serves: a Claude Code session (woken through its inbox) or a Codex task
//! (woken with `codex queue`).

use crate::config::{profile_dir, roaming_app_data};
use crate::log;
use crate::watch::Source;
use crate::watch::sources::{ClaudeSource, CodexSource};
use jiff::tz::TimeZone;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

const WINDOWS_PIPE_PREFIX: &str = r"\\.\pipe\";
/// What `codex queue --resume` prints, exiting 0, when the line was queued but the resume
/// failed; the active writer is another app-server holding the thread.
const RESUME_WARNING: &str = "Warning: message was queued, but could not resume thread";
const ACTIVE_WRITER: &str = "already has an active writer";
pub const UNKNOWN: &str = "unknown";

/// The only variables a process this adapter starts is given: where it finds its own login
/// and tools, never a credential held by the session that runs the adapter.
pub const CHILD_ENV: [&str; 16] = [
    "CODEX_HOME",
    "USERPROFILE",
    "HOME",
    "APPDATA",
    "LOCALAPPDATA",
    "PATH",
    "PATHEXT",
    "SystemRoot",
    "SystemDrive",
    "ComSpec",
    "TEMP",
    "TMP",
    "HOMEDRIVE",
    "HOMEPATH",
    "USERNAME",
    "LANG",
];

/// The environment for a process the adapter starts: `CHILD_ENV` names only.
pub fn child_env(source: impl Iterator<Item = (String, String)>) -> Vec<(String, String)> {
    // Windows names are case-insensitive: match them so, and keep the name as the system spells it.
    source
        .filter(|(name, _)| {
            CHILD_ENV
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(name))
        })
        .collect()
}

/// A command with a cleared environment holding only `CHILD_ENV` names from the current one.
fn command(program: &[String]) -> Command {
    let mut command = Command::new(&program[0]);
    command.args(&program[1..]);
    command.env_clear();
    command.envs(child_env(std::env::vars()));
    command.stdin(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    command
}

/// Runs a command to its end or `limit`, returning its status and the end of its stderr.
fn run_for(
    mut command: Command,
    limit: Duration,
) -> io::Result<(std::process::ExitStatus, String, String)> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let drain = |mut pipe: Box<dyn Read + Send>| {
        std::thread::spawn(move || {
            let mut text = String::new();
            let _ = pipe.read_to_string(&mut text);
            text
        })
    };
    let out = drain(Box::new(child.stdout.take().expect("piped")));
    let err = drain(Box::new(child.stderr.take().expect("piped")));
    let deadline = Instant::now() + limit;
    loop {
        if let Some(status) = child.try_wait()? {
            let out = out.join().unwrap_or_default();
            let err = err.join().unwrap_or_default();
            return Ok((status, out, err));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out after {} s", limit.as_secs()),
            ));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The last `n` characters of `text`.
fn tail(text: &str, n: usize) -> String {
    let skip = text.chars().count().saturating_sub(n);
    text.chars().skip(skip).collect()
}

/// Writes one message into a Claude Code session's inbox: an auth line, then the message.
/// A Windows named pipe is opened as a file; anything else is a Unix socket.
pub fn post_to_inbox(path: &str, token: &str, text: &str) -> io::Result<()> {
    let payload = format!(
        "{}\n{}\n",
        json!({"type": "auth", "token": token}),
        json!({"type": "user", "message": {"role": "user", "content": text}})
    );
    if path.starts_with(WINDOWS_PIPE_PREFIX) {
        let mut pipe = std::fs::OpenOptions::new().write(true).open(path)?;
        return pipe.write_all(payload.as_bytes());
    }
    #[cfg(unix)]
    {
        let mut socket = std::os::unix::net::UnixStream::connect(path)?;
        socket.set_write_timeout(Some(Duration::from_secs(10)))?;
        socket.write_all(payload.as_bytes())
    }
    #[cfg(not(unix))]
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "an inbox that is not a named pipe needs Unix sockets",
    ))
}

/// A Claude Code session: its inbox and id come from the environment its server inherits.
#[derive(Clone)]
pub struct SessionIdentity {
    pub session_id: String,
    pub pid: String,
    inbox: String,
    inbox_token: String,
}

impl SessionIdentity {
    /// The session id and inbox are required. `CLAUDE_PID` is optional: Claude Code gives it
    /// to Bash but not to MCP servers.
    pub fn from_env(env: &HashMap<String, String>) -> Option<Self> {
        let get = |name: &str| env.get(name).cloned().unwrap_or_default();
        let (session, inbox, token) = (
            get("CLAUDE_CODE_SESSION_ID"),
            get("CLAUDE_CODE_MESSAGING_SOCKET"),
            get("CLAUDE_CODE_MESSAGING_TOKEN"),
        );
        if session.is_empty() || inbox.is_empty() || token.is_empty() {
            return None;
        }
        Some(Self {
            session_id: session,
            pid: get("CLAUDE_PID"),
            inbox,
            inbox_token: token,
        })
    }

    /// This session's record in `~/.claude/sessions`: `<pid>.json` when the pid is known, else
    /// the `*.json` whose sessionId matches. Never the key files.
    pub fn registry(&self, home: &Path) -> Value {
        let folder = home.join(".claude").join("sessions");
        let candidates: Vec<PathBuf> = if self.pid.is_empty() {
            let mut all: Vec<PathBuf> = std::fs::read_dir(&folder)
                .into_iter()
                .flatten()
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "json"))
                .collect();
            all.sort();
            all
        } else {
            vec![folder.join(format!("{}.json", self.pid))]
        };
        for path in candidates {
            let Some(data) = std::fs::read_to_string(&path)
                .ok()
                .and_then(|t| serde_json::from_str::<Value>(&t).ok())
            else {
                continue;
            };
            if data.get("sessionId").and_then(Value::as_str) == Some(&self.session_id) {
                return data;
            }
        }
        json!({})
    }

    fn cwd(&self, home: &Path) -> String {
        self.registry(home)
            .get("cwd")
            .and_then(Value::as_str)
            .filter(|c| !c.is_empty())
            .map_or_else(current_dir, str::to_owned)
    }
}

fn current_dir() -> String {
    std::env::current_dir().map_or_else(|_| ".".to_owned(), |d| d.display().to_string())
}

/// What `codex --version` reports for the binary the wake runs, or "unknown".
pub fn codex_version(argv: &[String]) -> String {
    let mut program = argv.to_vec();
    program.push("--version".to_owned());
    let Ok((status, out, _)) = run_for(command(&program), Duration::from_secs(10)) else {
        return UNKNOWN.to_owned();
    };
    let found = out.split_whitespace().find(|word| {
        word.chars().next().is_some_and(|c| c.is_ascii_digit()) && word.matches('.').count() >= 2
    });
    match (status.success(), found) {
        (true, Some(version)) => version.to_owned(),
        _ => UNKNOWN.to_owned(),
    }
}

/// The program that queues a line into a Codex thread: `DUPLEX_CODEX`, else the vendored
/// binary of the npm package when present, else `codex` from the PATH.
pub fn default_codex_argv() -> Vec<String> {
    if let Some(program) = std::env::var_os("DUPLEX_CODEX").filter(|v| !v.is_empty()) {
        return vec![program.to_string_lossy().into_owned()];
    }
    if let Some(roaming) = roaming_app_data() {
        let vendored = roaming.join(
            "npm/node_modules/@openai/codex/node_modules/@openai/codex-win32-x64/vendor/x86_64-pc-windows-msvc/bin/codex.exe",
        );
        if vendored.is_file() {
            return vec![vendored.display().to_string()];
        }
    }
    vec!["codex".to_owned()]
}

/// A Codex task that registered itself with `bridge_register`. Codex gives its MCP servers
/// no thread id, so the task passes its own `CODEX_THREAD_ID`. The wake is
/// `codex queue --resume`: the app-server holding the thread runs the queued line once the
/// task is idle, and a thread nothing holds is resumed in the local daemon.
#[derive(Clone)]
pub struct CodexTask {
    pub thread_id: String,
    pub title_text: String,
    pub cwd: String,
    pub codex_argv: Vec<String>,
}

impl CodexTask {
    pub fn new(thread_id: &str, title: &str, cwd: &str) -> Self {
        Self {
            thread_id: thread_id.to_owned(),
            title_text: title.to_owned(),
            cwd: cwd.to_owned(),
            codex_argv: default_codex_argv(),
        }
    }

    /// Every failure is an `io::Error`, as for the Claude inbox: a hung `codex queue` is a
    /// failed wake, not an error that ends the adapter's run loop.
    fn wake(&self, text: &str) -> io::Result<()> {
        let mut program = self.codex_argv.clone();
        program.extend(
            [
                "queue",
                "--thread",
                &self.thread_id,
                "--message",
                text,
                "--resume",
            ]
            .map(str::to_owned),
        );
        let (status, _, stderr) = run_for(command(&program), Duration::from_secs(60))
            .map_err(|e| io::Error::new(e.kind(), format!("codex queue {e}")))?;
        if !status.success() {
            let code = status
                .code()
                .map_or_else(|| "without a code".to_owned(), |c| c.to_string());
            return Err(io::Error::other(format!(
                "codex queue exited {code}: {}",
                tail(&stderr, 200)
            )));
        }
        // Exit 0 with the warning: the line is queued and only the resume failed.
        if stderr.contains(RESUME_WARNING) {
            let detail = tail(stderr.trim(), 300);
            if stderr.contains(ACTIVE_WRITER) {
                log::info(format!(
                    "queued; another app-server holds the thread: {detail}"
                ));
            } else {
                log::warn(format!("queued, but the thread was not resumed: {detail}"));
            }
        }
        Ok(())
    }
}

/// Who an adapter serves.
#[derive(Clone)]
pub enum Participant {
    Claude(SessionIdentity),
    Codex(CodexTask),
}

impl Participant {
    pub fn provider(&self) -> &'static str {
        match self {
            Self::Claude(_) => "claude",
            Self::Codex(_) => "codex",
        }
    }

    pub fn endpoint_ref(&self) -> &'static str {
        match self {
            Self::Claude(_) => "claude-inbox",
            Self::Codex(_) => "codex-queue",
        }
    }

    /// The inbox starts an idle session and drains at the next tool round of a running turn,
    /// so auto, queue (held by the broker until the turn ends) and steer write it. An MCP
    /// server has no route to interrupt its session, so never restart. `codex queue --resume`
    /// starts an idle thread and otherwise runs after the current turn; only the desktop
    /// app's own app-server can steer or interrupt its turns.
    pub fn send_modes(&self) -> &'static [&'static str] {
        match self {
            Self::Claude(_) => &["auto", "queue", "steer"],
            Self::Codex(_) => &["auto", "queue"],
        }
    }

    /// Whether a successful wake proves the line was taken. The Claude inbox answers nothing
    /// (a refused token is closed unanswered, an accepted message gets no reply), so a wake
    /// proves nothing and the delivery stays dispatching, under the broker's ladder, until
    /// read. `codex queue` exits 0 only once the line is in the queue.
    pub fn wake_confirms(&self) -> bool {
        matches!(self, Self::Codex(_))
    }

    pub fn native_id(&self) -> &str {
        match self {
            Self::Claude(s) => &s.session_id,
            Self::Codex(t) => &t.thread_id,
        }
    }

    pub fn title(&self, home: &Path) -> String {
        match self {
            Self::Claude(s) => {
                let name = s
                    .registry(home)
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_owned();
                if name.is_empty() {
                    let cwd = s.cwd(home);
                    let base = Path::new(&cwd)
                        .file_name()
                        .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
                    format!(
                        "{base} {}",
                        s.session_id.chars().take(6).collect::<String>()
                    )
                } else {
                    name
                }
            }
            Self::Codex(t) => t.title_text.clone(),
        }
    }

    pub fn workspace(&self, home: &Path) -> String {
        match self {
            Self::Claude(s) => s.cwd(home),
            Self::Codex(t) => t.cwd.clone(),
        }
    }

    /// The runtime version hosting this session: from the session record for Claude (MCP
    /// servers get no version variable), from `codex --version` for Codex.
    pub fn runtime(&self, home: &Path) -> HashMap<String, String> {
        match self {
            Self::Claude(s) => {
                let version = s
                    .registry(home)
                    .get("version")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                    .unwrap_or(UNKNOWN)
                    .to_owned();
                HashMap::from([("claude_code".to_owned(), version)])
            }
            Self::Codex(t) => HashMap::from([("codex".to_owned(), codex_version(&t.codex_argv))]),
        }
    }

    pub fn wake(&self, text: &str) -> io::Result<()> {
        match self {
            Self::Claude(s) => post_to_inbox(&s.inbox, &s.inbox_token, text),
            Self::Codex(t) => t.wake(text),
        }
    }

    pub fn source(&self, home: &Path, started_at: f64) -> Box<dyn Source> {
        let local = TimeZone::system();
        match self {
            Self::Claude(s) => {
                let identity = s.clone();
                let home_dir = home.to_owned();
                let record = Arc::new(move || identity.registry(&home_dir));
                Box::new(ClaudeSource::new(
                    &s.session_id,
                    record,
                    home,
                    started_at,
                    local,
                ))
            }
            Self::Codex(t) => Box::new(CodexSource::new(&t.thread_id, home, started_at, local)),
        }
    }
}

/// The home folder the adapter reads sessions under.
pub fn default_home() -> PathBuf {
    profile_dir()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn a_session_needs_its_id_its_inbox_and_its_token() {
        let full = env(&[
            ("CLAUDE_CODE_SESSION_ID", "s-1"),
            ("CLAUDE_CODE_MESSAGING_SOCKET", "/tmp/x.sock"),
            ("CLAUDE_CODE_MESSAGING_TOKEN", "t"),
        ]);
        let found = SessionIdentity::from_env(&full).expect("a session");
        assert_eq!((found.session_id.as_str(), found.pid.as_str()), ("s-1", ""));
        for missing in [
            "CLAUDE_CODE_SESSION_ID",
            "CLAUDE_CODE_MESSAGING_SOCKET",
            "CLAUDE_CODE_MESSAGING_TOKEN",
        ] {
            let mut partial = full.clone();
            partial.remove(missing);
            assert!(SessionIdentity::from_env(&partial).is_none(), "{missing}");
        }
        assert!(SessionIdentity::from_env(&env(&[])).is_none());
    }

    #[test]
    fn a_child_gets_only_the_names_on_the_list_in_any_case() {
        let parent = [
            ("PATH", "p"),
            ("Path", "q"),
            ("SystemRoot", "C:\\Windows"),
            ("OPENAI_API_KEY", "secret"),
            ("ANTHROPIC_API_KEY", "secret"),
            ("CLAUDE_CODE_MESSAGING_TOKEN", "secret"),
            ("GITHUB_TOKEN", "secret"),
            ("HOME", "/home/u"),
        ]
        .map(|(k, v)| (k.to_owned(), v.to_owned()));
        let kept = child_env(parent.into_iter());
        let names: Vec<&str> = kept.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(names, ["PATH", "Path", "SystemRoot", "HOME"]);
        assert!(kept.iter().all(|(_, v)| v != "secret"));
        for name in CHILD_ENV {
            let lower = name.to_ascii_uppercase();
            assert!(
                !["TOKEN", "KEY", "SECRET", "PASSWORD"]
                    .iter()
                    .any(|w| lower.contains(w)),
                "{name}"
            );
        }
    }

    #[test]
    fn a_claude_record_is_found_by_its_pid_or_its_session_id() {
        let home = std::env::temp_dir().join(format!("duplex-identity-{}", std::process::id()));
        let sessions = home.join(".claude").join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        std::fs::write(
            sessions.join("11.json"),
            r#"{"sessionId": "other", "name": "Not mine"}"#,
        )
        .unwrap();
        std::fs::write(sessions.join("22.json"), r#"{"sessionId": "s-1", "name": " Builder ", "cwd": "/work/app", "version": "2.1.321"}"#).unwrap();
        let by_scan = Participant::Claude(SessionIdentity {
            session_id: "s-1".into(),
            pid: String::new(),
            inbox: String::new(),
            inbox_token: String::new(),
        });
        assert_eq!(by_scan.title(&home), "Builder");
        assert_eq!(by_scan.workspace(&home), "/work/app");
        assert_eq!(by_scan.runtime(&home)["claude_code"], "2.1.321");
        let by_pid = Participant::Claude(SessionIdentity {
            session_id: "s-1".into(),
            pid: "11".into(),
            inbox: String::new(),
            inbox_token: String::new(),
        });
        // The pid's record is another session's: no name, and the folder is the process's own.
        let here = std::env::current_dir().unwrap();
        let base = here.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(by_pid.title(&home), format!("{base} s-1"));
        assert_eq!(by_pid.runtime(&home)["claude_code"], UNKNOWN);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn a_wake_through_a_missing_pipe_fails_as_an_io_error() {
        let inbox = if cfg!(windows) {
            r"\\.\pipe\duplex-test-no-such-pipe"
        } else {
            "/nonexistent/duplex.sock"
        };
        assert!(post_to_inbox(inbox, "t", "line").is_err());
    }

    /// The inbox as Claude Code serves it: an auth line, then the message, one JSON per line.
    fn check_inbox_lines(raw: &str) {
        let lines: Vec<Value> = raw
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2, "{raw:?}");
        assert_eq!(lines[0], json!({"type": "auth", "token": "inbox-token"}));
        assert_eq!(
            lines[1],
            json!({"type": "user", "message": {"role": "user", "content": "You've got mail \u{b7} é"}})
        );
    }

    #[cfg(windows)]
    #[test]
    fn a_wake_through_a_named_pipe_writes_the_auth_line_then_the_message() {
        use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
        use windows_sys::Win32::Storage::FileSystem::{PIPE_ACCESS_INBOUND, ReadFile};
        use windows_sys::Win32::System::Pipes::{
            ConnectNamedPipe, CreateNamedPipeW, PIPE_TYPE_BYTE, PIPE_WAIT,
        };
        let name = format!(r"\\.\pipe\duplex-test-inbox-{}", std::process::id());
        let wide: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        // SAFETY: a pipe server owned by this test; the handle is closed before it returns.
        let handle = unsafe {
            CreateNamedPipeW(
                wide.as_ptr(),
                PIPE_ACCESS_INBOUND,
                PIPE_TYPE_BYTE | PIPE_WAIT,
                1,
                4096,
                4096,
                0,
                std::ptr::null(),
            )
        };
        assert_ne!(handle, INVALID_HANDLE_VALUE);
        let address = handle as usize;
        let server = std::thread::spawn(move || {
            let handle = address as windows_sys::Win32::Foundation::HANDLE;
            let mut raw = Vec::new();
            // SAFETY: the handle is the pipe above; the buffer outlives each call.
            unsafe {
                ConnectNamedPipe(handle, std::ptr::null_mut());
                loop {
                    let mut buffer = [0u8; 1024];
                    let mut got = 0u32;
                    if ReadFile(
                        handle,
                        buffer.as_mut_ptr().cast(),
                        1024,
                        &mut got,
                        std::ptr::null_mut(),
                    ) == 0
                        || got == 0
                    {
                        break;
                    }
                    raw.extend_from_slice(&buffer[..got as usize]);
                }
            }
            String::from_utf8(raw).unwrap()
        });
        post_to_inbox(&name, "inbox-token", "You've got mail \u{b7} é").unwrap();
        let raw = server.join().unwrap();
        // SAFETY: the server thread is done with the handle.
        unsafe { CloseHandle(handle) };
        check_inbox_lines(&raw);
    }

    #[cfg(unix)]
    #[test]
    fn a_wake_through_a_unix_socket_writes_the_auth_line_then_the_message() {
        use std::io::Read as _;
        let dir = std::env::temp_dir().join(format!("duplex-inbox-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("inbox.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut raw = String::new();
            stream.read_to_string(&mut raw).unwrap();
            raw
        });
        post_to_inbox(
            path.to_str().unwrap(),
            "inbox-token",
            "You've got mail \u{b7} é",
        )
        .unwrap();
        check_inbox_lines(&server.join().unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_modes_a_participant_takes_and_whether_its_wake_confirms() {
        let claude = Participant::Claude(SessionIdentity {
            session_id: "s".into(),
            pid: String::new(),
            inbox: String::new(),
            inbox_token: String::new(),
        });
        let codex = Participant::Codex(CodexTask {
            thread_id: "t".into(),
            title_text: "T".into(),
            cwd: "/w".into(),
            codex_argv: vec!["codex".into()],
        });
        assert_eq!(claude.send_modes(), ["auto", "queue", "steer"]);
        assert_eq!(codex.send_modes(), ["auto", "queue"]);
        assert!(!claude.wake_confirms() && codex.wake_confirms());
        assert_eq!(
            (claude.endpoint_ref(), codex.endpoint_ref()),
            ("claude-inbox", "codex-queue")
        );
    }
}
