//! Whether a Claude session has a live process, established from evidence or not at all.
//!
//! Each running Claude Code process writes `~/.claude/sessions/<pid>.json` with its
//! `sessionId` and `procStart` (its creation time as a Windows FILETIME), and the desktop
//! app starts each one with the session id on its command line (`--resume=<id>`). A
//! session is closed only when every record was read, none holds it for a running
//! process, and every running claude.exe command line was read and none names it. Any
//! read that fails means the answer is unknown, never closed: a resume beside a live
//! process forks the transcript silently.

use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

/// The image whose command lines are searched for the session id.
pub const CLAUDE_IMAGE: &str = "claude.exe";

#[derive(Debug, PartialEq)]
pub enum Liveness {
    Closed,
    Live {
        pid: u32,
    },
    /// Absence could not be established; the start is skipped.
    Unknown(String),
}

#[derive(Debug, PartialEq)]
pub enum ProcState {
    /// The system reports no such process, or one that has exited.
    Gone,
    /// Running, with its creation time.
    Running(u64),
    /// Anything else: the process could not be examined.
    Unreadable,
}

/// One running process from a snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct ProcEntry {
    pub pid: u32,
    pub parent: u32,
    pub image: String,
}

pub trait ProcessTable {
    fn state(&self, pid: u32) -> ProcState;
    /// Every running process. An error when the list cannot be read whole.
    fn snapshot(&self) -> Result<Vec<ProcEntry>, String>;
    /// A process's command line; None when it has exited, an error when unreadable.
    fn command_line(&self, pid: u32) -> Result<Option<String>, String>;
}

/// A process the waker started: its pid and creation time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Own {
    pub pid: u32,
    pub created: u64,
}

/// A claude.exe whose command line names the session, other than `own` and the
/// processes it started. A descendant counts as the waker's only when it was created no
/// earlier than `own`, so a reused parent pid cannot hide a process. Every command line
/// is read; any read that fails is an error.
pub fn other_holder(
    procs: &dyn ProcessTable,
    session_id: &str,
    own: Option<Own>,
) -> Result<Option<u32>, String> {
    let all = procs.snapshot()?;
    let mut mine: Vec<u32> = own.iter().map(|o| o.pid).collect();
    loop {
        let before = mine.len();
        for entry in &all {
            if mine.contains(&entry.parent) && !mine.contains(&entry.pid) {
                mine.push(entry.pid);
            }
        }
        if mine.len() == before {
            break;
        }
    }
    let lower = session_id.to_ascii_lowercase();
    for entry in all
        .iter()
        .filter(|e| e.image.eq_ignore_ascii_case(CLAUDE_IMAGE))
    {
        if let Some(own) = own.filter(|_| mine.contains(&entry.pid)) {
            if entry.pid == own.pid {
                continue;
            }
            match procs.state(entry.pid) {
                ProcState::Running(created) if created >= own.created => continue,
                ProcState::Gone => continue,
                ProcState::Running(_) => {}
                ProcState::Unreadable => return Err(format!("process {} unreadable", entry.pid)),
            }
        }
        let Some(line) = procs.command_line(entry.pid)? else {
            continue;
        };
        if line.to_ascii_lowercase().contains(&lower) {
            return Ok(Some(entry.pid));
        }
    }
    Ok(None)
}

#[derive(Deserialize)]
struct Record {
    pid: u32,
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(rename = "procStart")]
    proc_start: String,
}

pub fn session_liveness(dir: &Path, session_id: &str, procs: &dyn ProcessTable) -> Liveness {
    match established(dir, session_id, procs) {
        Ok(None) => Liveness::Closed,
        Ok(Some(pid)) => Liveness::Live { pid },
        Err(why) => Liveness::Unknown(why),
    }
}

/// The pid of a live process holding the session, None once absence is established.
fn established(
    dir: &Path,
    session_id: &str,
    procs: &dyn ProcessTable,
) -> Result<Option<u32>, String> {
    for record in read_records(dir)? {
        if record.session_id != session_id {
            continue;
        }
        // A record without a readable start time cannot be told from a reused pid.
        let recorded = record
            .proc_start
            .parse::<u64>()
            .map_err(|_| format!("session record {} has no start time", record.pid))?;
        match procs.state(record.pid) {
            ProcState::Gone => {}
            ProcState::Running(created) if created == recorded => return Ok(Some(record.pid)),
            ProcState::Running(_) => {}
            ProcState::Unreadable => return Err(format!("process {} unreadable", record.pid)),
        }
    }
    other_holder(procs, session_id, None)
}

/// Every `*.json` record in the folder. A record that cannot be parsed twice, 200 ms
/// apart, is an error: it may be a live session's record caught mid-write.
fn read_records(dir: &Path) -> Result<Vec<Record>, String> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("session records unreadable: {}", e.kind()))?;
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| format!("session records unreadable: {}", e.kind()))?;
        let path = entry.path();
        if path.extension().is_some_and(|x| x == "json") {
            paths.push(path);
        }
    }
    paths.sort();
    paths
        .iter()
        .map(|path| {
            read_record(path).ok_or_else(|| {
                let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
                format!("session record {} unreadable", name.unwrap_or_default())
            })
        })
        .collect()
}

fn read_record(path: &Path) -> Option<Record> {
    let parse = || {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|t| serde_json::from_str::<Record>(&t).ok())
    };
    parse().or_else(|| {
        std::thread::sleep(Duration::from_millis(200));
        parse()
    })
}

/// The real process table: OpenProcess, GetProcessTimes, a ToolHelp snapshot and
/// NtQueryInformationProcess for command lines.
pub struct WinProcesses;

struct Handle(windows_sys::Win32::Foundation::HANDLE);

impl Drop for Handle {
    fn drop(&mut self) {
        // SAFETY: the handle was returned open by the call that made this value.
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}

fn open(pid: u32) -> Result<Handle, u32> {
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};
    // SAFETY: OpenProcess has no preconditions; a null result is checked.
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            Err(GetLastError())
        } else {
            Ok(Handle(handle))
        }
    }
}

fn exited(handle: &Handle) -> bool {
    use windows_sys::Win32::Foundation::STILL_ACTIVE;
    use windows_sys::Win32::System::Threading::GetExitCodeProcess;
    let mut code = 0u32;
    // SAFETY: an open handle and an out-pointer to a local.
    unsafe { GetExitCodeProcess(handle.0, &mut code) != 0 && code != STILL_ACTIVE as u32 }
}

fn command_line(handle: &Handle) -> Option<String> {
    use windows_sys::Wdk::System::Threading::{
        NtQueryInformationProcess, ProcessCommandLineInformation,
    };
    use windows_sys::Win32::Foundation::UNICODE_STRING;
    // A u64 buffer keeps the UNICODE_STRING header aligned.
    let mut buffer = vec![0u64; 512];
    for _ in 0..2 {
        let bytes = u32::try_from(buffer.len() * 8).ok()?;
        let mut needed = 0u32;
        // SAFETY: the buffer is writable for `bytes` bytes and outlives the call.
        let status = unsafe {
            NtQueryInformationProcess(
                handle.0,
                ProcessCommandLineInformation,
                buffer.as_mut_ptr().cast(),
                bytes,
                &mut needed,
            )
        };
        if status >= 0 {
            // SAFETY: on success the buffer starts with a UNICODE_STRING whose Buffer
            // points inside the same allocation, Length bytes long.
            let text = unsafe {
                let header = &*buffer.as_ptr().cast::<UNICODE_STRING>();
                if header.Buffer.is_null() {
                    return Some(String::new());
                }
                std::slice::from_raw_parts(header.Buffer, usize::from(header.Length) / 2)
            };
            return Some(String::from_utf16_lossy(text));
        }
        let want = (needed as usize).div_ceil(8).max(buffer.len() * 2);
        buffer = vec![0u64; want];
    }
    None
}

impl ProcessTable for WinProcesses {
    fn state(&self, pid: u32) -> ProcState {
        use windows_sys::Win32::Foundation::{ERROR_INVALID_PARAMETER, FILETIME, STILL_ACTIVE};
        use windows_sys::Win32::System::Threading::{GetExitCodeProcess, GetProcessTimes};
        let handle = match open(pid) {
            Ok(handle) => handle,
            // OpenProcess's answer for a pid no process holds.
            Err(ERROR_INVALID_PARAMETER) => return ProcState::Gone,
            Err(_) => return ProcState::Unreadable,
        };
        let mut code = 0u32;
        let zero = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let (mut created, mut exit, mut kernel, mut user) = (zero, zero, zero, zero);
        // SAFETY: an open handle and out-pointers to locals.
        let (coded, timed) = unsafe {
            (
                GetExitCodeProcess(handle.0, &mut code) != 0,
                GetProcessTimes(handle.0, &mut created, &mut exit, &mut kernel, &mut user) != 0,
            )
        };
        match (coded, timed) {
            (true, _) if code != STILL_ACTIVE as u32 => ProcState::Gone,
            (true, true) => ProcState::Running(
                (u64::from(created.dwHighDateTime) << 32) | u64::from(created.dwLowDateTime),
            ),
            _ => ProcState::Unreadable,
        }
    }

    fn snapshot(&self) -> Result<Vec<ProcEntry>, String> {
        use windows_sys::Win32::Foundation::{
            ERROR_NO_MORE_FILES, GetLastError, INVALID_HANDLE_VALUE,
        };
        use windows_sys::Win32::System::Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW,
            TH32CS_SNAPPROCESS,
        };
        // SAFETY: the snapshot handle is checked and closed by Handle; the entry is a
        // local with dwSize set as the API requires.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err("process list unreadable".to_owned());
        }
        let snapshot = Handle(snapshot);
        let mut entry = PROCESSENTRY32W {
            dwSize: std::mem::size_of::<PROCESSENTRY32W>() as u32,
            ..Default::default()
        };
        let mut out = Vec::new();
        // SAFETY: as above.
        let mut more = unsafe { Process32FirstW(snapshot.0, &mut entry) } != 0;
        while more {
            let len = entry.szExeFile.iter().position(|&c| c == 0).unwrap_or(260);
            out.push(ProcEntry {
                pid: entry.th32ProcessID,
                parent: entry.th32ParentProcessID,
                image: String::from_utf16_lossy(&entry.szExeFile[..len]),
            });
            // SAFETY: as above.
            more = unsafe { Process32NextW(snapshot.0, &mut entry) } != 0;
        }
        // SAFETY: reads the calling thread's last error, set by the failing Process32*W.
        if unsafe { GetLastError() } != ERROR_NO_MORE_FILES {
            return Err("process list ended early".to_owned());
        }
        Ok(out)
    }

    fn command_line(&self, pid: u32) -> Result<Option<String>, String> {
        use windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER;
        match open(pid) {
            Ok(handle) => match command_line(&handle) {
                Some(text) => Ok(Some(text)),
                // Exited, but a handle held elsewhere keeps its id: it has no command line.
                None if exited(&handle) => Ok(None),
                None => Err(format!("command line of pid {pid} unreadable")),
            },
            // Exited since the snapshot.
            Err(ERROR_INVALID_PARAMETER) => Ok(None),
            Err(code) => Err(format!("pid {pid} unreadable (error {code})")),
        }
    }
}
