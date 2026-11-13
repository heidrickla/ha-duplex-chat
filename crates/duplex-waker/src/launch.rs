//! How a request becomes a process: the route per provider, the child's environment,
//! and the launcher that starts it with no window.

use crate::broker::Request;
use crate::log::Log;
use crate::registry::{Own, ProcState, ProcessTable, other_holder};
use std::ffi::{OsStr, OsString};
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const CLAUDE_ROUTE: &str = "claude-print-resume";
pub const CODEX_ROUTE: &str = "codex-queue-resume";

/// The wake line the session adapter sends (`duplex_proto::wake`): a request is always for
/// a direct conversation.
pub fn wake_line(session: &str, message: &str) -> String {
    duplex_proto::wake::wake_line(session, message, "direct")
}

/// What to run for one request.
#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    pub route: &'static str,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub settle: Settle,
    /// A session id no other process may hold while this one runs: the run is ended
    /// when one opens it (a Claude transcript has no lock).
    pub exclusive: Option<String>,
}

/// When a start counts as made.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Settle {
    /// The command must exit 0 within this time (`codex queue` exits once the line is queued).
    ExitZero(Duration),
    /// The process runs a whole turn; it must not have failed within this time.
    Running(Duration),
}

/// The executables and Claude's per-session model, found on this machine.
pub trait Tools {
    fn claude(&self) -> Option<PathBuf>;
    fn codex(&self) -> Option<PathBuf>;
    /// The model the desktop app last used for this CLI session id.
    fn claude_model(&self, session: &str) -> Option<String>;
}

/// Ids go on a command line: only letters, digits and hyphens, so none reads as a flag.
pub fn plain_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && !value.starts_with('-')
        && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

fn plain_model(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 80
        && !value.starts_with('-')
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '[' | ']'))
}

/// Refuses a request whose ids are not plain ids. Nothing on this machine is read for
/// such a request, so its refusal is the same everywhere.
pub fn check_ids(request: &Request) -> Result<(), String> {
    for (name, value) in [
        ("id", &request.id),
        ("binding_id", &request.binding_id),
        ("native_id", &request.native_id),
        ("message_id", &request.message_id),
    ] {
        if !plain_id(value) {
            return Err(format!("request {name} is not a plain id"));
        }
    }
    Ok(())
}

/// The plan for a request, or why there is none.
pub fn plan(request: &Request, tools: &dyn Tools) -> Result<Plan, String> {
    check_ids(request)?;
    if request.reason != "limit_resume" {
        return Err(format!("reason {:?} is not handled", request.reason));
    }
    let line = wake_line(&request.native_id, &request.message_id);
    match request.provider.as_str() {
        "claude" => {
            // `--resume <id>` finds the transcript under the session's own folder.
            let cwd = request
                .workspace
                .as_deref()
                .map(PathBuf::from)
                .filter(|p| p.is_absolute() && p.is_dir())
                .ok_or("the session's workspace folder does not exist")?;
            let program = tools.claude().ok_or("claude.exe not found")?;
            let mut args = vec![
                "-p".to_owned(),
                "--resume".to_owned(),
                request.native_id.clone(),
            ];
            if let Some(model) = tools
                .claude_model(&request.native_id)
                .filter(|m| plain_model(m))
            {
                args.extend(["--model".to_owned(), model]);
            }
            args.push(line);
            Ok(Plan {
                route: CLAUDE_ROUTE,
                program,
                args,
                cwd: Some(cwd),
                settle: Settle::Running(Duration::from_secs(5)),
                exclusive: Some(request.native_id.clone()),
            })
        }
        "codex" => {
            let program = tools.codex().ok_or("codex.exe not found")?;
            let args = vec![
                "queue".to_owned(),
                "--thread".to_owned(),
                request.native_id.clone(),
                "--message".to_owned(),
                line,
                "--resume".to_owned(),
            ];
            Ok(Plan {
                route: CODEX_ROUTE,
                program,
                args,
                cwd: None,
                settle: Settle::ExitZero(Duration::from_secs(60)),
                exclusive: None,
            })
        }
        other => Err(format!("provider {other:?} is not handled")),
    }
}

/// The only variables a child gets: what Windows, claude.exe and codex.exe need to find
/// the user's folders and their own subscription logins (`~/.claude`, `~/.codex`),
/// none of them a credential. Anything else, an API key or token among it, stays out, so
/// a resumed session runs on its subscription login. `DUPLEX_ADAPTER_CONFIG` is a path,
/// kept so a waker run beside the installed one reaches its own broker.
pub const CHILD_ENV: &[&str] = &[
    "ALLUSERSPROFILE",
    "APPDATA",
    "COMMONPROGRAMFILES",
    "COMMONPROGRAMFILES(X86)",
    "COMMONPROGRAMW6432",
    "COMPUTERNAME",
    "COMSPEC",
    "DUPLEX_ADAPTER_CONFIG",
    "HOMEDRIVE",
    "HOMEPATH",
    "LOCALAPPDATA",
    "NUMBER_OF_PROCESSORS",
    "OS",
    "PATH",
    "PATHEXT",
    "PROCESSOR_ARCHITECTURE",
    "PROCESSOR_IDENTIFIER",
    "PROCESSOR_LEVEL",
    "PROCESSOR_REVISION",
    "PROGRAMDATA",
    "PROGRAMFILES",
    "PROGRAMFILES(X86)",
    "PROGRAMW6432",
    "PSMODULEPATH",
    "PUBLIC",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "USERDOMAIN",
    "USERNAME",
    "USERPROFILE",
    "WINDIR",
];

/// Whether a variable may pass to a child: its name is in CHILD_ENV, in any case.
pub fn passed(name: &OsStr) -> bool {
    let upper = name.to_string_lossy().to_ascii_uppercase();
    CHILD_ENV.contains(&upper.as_str())
}

/// The one place a child's command is built: the plan's program, arguments and folder,
/// an environment made only of allowlisted variables from `parent` (nothing is added
/// after the filter), no window, no stdin and no captured output.
pub fn command(plan: &Plan, parent: impl IntoIterator<Item = (OsString, OsString)>) -> Command {
    const CREATE_NO_WINDOW: u32 = windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;
    let mut command = Command::new(&plan.program);
    command.args(&plan.args);
    if let Some(cwd) = &plan.cwd {
        command.current_dir(cwd);
    }
    command.creation_flags(CREATE_NO_WINDOW);
    command.stdin(Stdio::null());
    command.stdout(Stdio::null());
    command.stderr(Stdio::null());
    command.env_clear();
    command.envs(parent.into_iter().filter(|(name, _)| passed(name)));
    command
}

/// A started process.
#[derive(Debug, PartialEq)]
pub struct Started {
    pub pid: u32,
    pub detail: String,
}

/// A process the launcher started that has ended.
#[derive(Debug, PartialEq)]
pub struct Ended {
    pub request_id: String,
    pub route: &'static str,
    pub pid: u32,
    pub code: Option<i32>,
    /// Ended by the launcher because another process opened its session.
    pub opened_elsewhere: bool,
}

pub const OPENED_ELSEWHERE: &str = "the session was opened while the waker ran it";

/// How often a running exclusive start looks for another process holding its session.
pub const WATCH: Duration = Duration::from_secs(1);

pub trait Launcher {
    fn start(&mut self, request_id: &str, session: &str, plan: &Plan) -> Result<Started, String>;
    /// The pid of a process this launcher started for the session and that still runs.
    fn running(&mut self, session: &str) -> Option<u32>;
    /// Processes that ended since the last call.
    fn reap(&mut self) -> Vec<Ended>;
}

struct Tracked {
    request_id: String,
    route: &'static str,
    session: String,
    pid: u32,
    child: Arc<Mutex<Child>>,
    opened: Arc<AtomicBool>,
}

/// Starts real processes with no window and keeps each until it ends. An exclusive
/// start is watched every second for its whole run: another claude.exe naming its
/// session, or any read that fails, ends it at once.
pub struct SystemLauncher {
    procs: Arc<dyn ProcessTable + Send + Sync>,
    log: Log,
    children: Vec<Tracked>,
}

impl SystemLauncher {
    pub fn new(procs: Arc<dyn ProcessTable + Send + Sync>, log: Log) -> Self {
        Self {
            procs,
            log,
            children: Vec::new(),
        }
    }
}

fn lock(child: &Mutex<Child>) -> std::sync::MutexGuard<'_, Child> {
    child
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Watches one exclusive run until it ends; kills it the moment its session is seen
/// held elsewhere or cannot be checked.
fn watch(
    procs: Arc<dyn ProcessTable + Send + Sync>,
    log: Log,
    session: String,
    own: Option<Own>,
    child: Arc<Mutex<Child>>,
    opened: Arc<AtomicBool>,
) {
    let pid = lock(&child).id();
    loop {
        if !matches!(lock(&child).try_wait(), Ok(None)) {
            return;
        }
        let found = match own {
            Some(own) => other_holder(procs.as_ref(), &session, Some(own)),
            None => Err(format!("start time of pid {pid} unreadable")),
        };
        let why = match found {
            Ok(None) => {
                std::thread::sleep(WATCH);
                continue;
            }
            Ok(Some(other)) => format!("pid {other} opened session {session}"),
            Err(why) => format!("session {session} cannot be checked: {why}"),
        };
        opened.store(true, Ordering::SeqCst);
        let killed = lock(&child).kill().is_ok();
        log.line(&format!(
            "pid {pid} {}: {why}",
            if killed {
                "killed"
            } else {
                "could not be killed"
            }
        ));
        return;
    }
}

impl Launcher for SystemLauncher {
    fn start(&mut self, request_id: &str, session: &str, plan: &Plan) -> Result<Started, String> {
        let child = command(plan, std::env::vars_os())
            .spawn()
            .map_err(|e| format!("could not start {}: {}", file_name(&plan.program), e.kind()))?;
        let pid = child.id();
        let child = Arc::new(Mutex::new(child));
        let opened = Arc::new(AtomicBool::new(false));
        if let Some(exclusive) = &plan.exclusive {
            let own = match self.procs.state(pid) {
                ProcState::Running(created) => Some(Own { pid, created }),
                _ => None,
            };
            let (procs, log, session, child, opened) = (
                Arc::clone(&self.procs),
                self.log.clone(),
                exclusive.clone(),
                Arc::clone(&child),
                Arc::clone(&opened),
            );
            std::thread::spawn(move || watch(procs, log, session, own, child, opened));
        }
        let (limit, must_exit) = match plan.settle {
            Settle::ExitZero(limit) => (limit, true),
            Settle::Running(limit) => (limit, false),
        };
        let deadline = Instant::now() + limit;
        loop {
            let status = lock(&child).try_wait();
            match status {
                _ if opened.load(Ordering::SeqCst) => {
                    let _ = lock(&child).kill();
                    return Err(OPENED_ELSEWHERE.to_owned());
                }
                Ok(Some(status)) if status.success() => {
                    return Ok(Started {
                        pid,
                        detail: format!("pid {pid} exited 0"),
                    });
                }
                Ok(Some(status)) => {
                    return Err(format!("pid {pid} exited {}", code_text(status.code())));
                }
                Ok(None) if Instant::now() >= deadline => break,
                Ok(None) => std::thread::sleep(Duration::from_millis(250)),
                Err(e) => return Err(format!("pid {pid} could not be watched: {}", e.kind())),
            }
        }
        if must_exit {
            let mut child = lock(&child);
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "pid {pid} did not finish within {} s",
                limit.as_secs()
            ));
        }
        self.children.push(Tracked {
            request_id: request_id.to_owned(),
            route: plan.route,
            session: session.to_owned(),
            pid,
            child,
            opened,
        });
        Ok(Started {
            pid,
            detail: format!("pid {pid} running"),
        })
    }

    fn running(&mut self, session: &str) -> Option<u32> {
        self.children
            .iter()
            .filter(|t| t.session == session)
            .find_map(|t| matches!(lock(&t.child).try_wait(), Ok(None)).then_some(t.pid))
    }

    fn reap(&mut self) -> Vec<Ended> {
        let mut done = Vec::new();
        self.children.retain(|t| {
            let code = match lock(&t.child).try_wait() {
                Ok(None) => return true,
                Ok(Some(status)) => status.code(),
                Err(_) => None,
            };
            done.push(Ended {
                request_id: t.request_id.clone(),
                route: t.route,
                pid: t.pid,
                code,
                opened_elsewhere: t.opened.load(Ordering::SeqCst),
            });
            false
        });
        done
    }
}

pub fn code_text(code: Option<i32>) -> String {
    code.map_or_else(|| "without a code".to_owned(), |c| c.to_string())
}

fn file_name(path: &Path) -> String {
    path.file_name().map_or_else(
        || path.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    )
}
