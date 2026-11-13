#![cfg(windows)]

mod common;

use common::{FakeProcs, Scratch};
use duplex_waker::launch::{
    CLAUDE_ROUTE, CODEX_ROUTE, Ended, Launcher, OPENED_ELSEWHERE, Plan, Settle, SystemLauncher,
};
use duplex_waker::log::Log;
use duplex_waker::registry::{CLAUDE_IMAGE, ProcState, ProcessTable, WinProcesses};
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// A session id no other process on the machine names.
fn fresh_session() -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!(
        "{nanos:08x}-7e57-4a1e-8d3f-{:06x}{:06x}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// cmd.exe copied under claude.exe's image name: a real process the watch looks at.
fn fake_claude(dir: &Path) -> PathBuf {
    let system = std::env::var("SYSTEMROOT").unwrap_or_else(|_| r"C:\Windows".to_owned());
    let target = dir.join(CLAUDE_IMAGE);
    std::fs::copy(Path::new(&system).join(r"System32\cmd.exe"), &target).unwrap();
    target
}

fn ping() -> PathBuf {
    let system = std::env::var("SYSTEMROOT").unwrap_or_else(|_| r"C:\Windows".to_owned());
    Path::new(&system).join(r"System32\PING.EXE")
}

fn exclusive(program: PathBuf, args: &[&str], session: &str) -> Plan {
    Plan {
        route: CLAUDE_ROUTE,
        program,
        args: args.iter().map(|a| (*a).to_owned()).collect(),
        cwd: None,
        settle: Settle::Running(Duration::from_millis(300)),
        exclusive: Some(session.to_owned()),
    }
}

/// Waits for the launcher's first ended run.
fn ended_within(launcher: &mut SystemLauncher, limit: Duration) -> Option<(Ended, Duration)> {
    let start = Instant::now();
    while start.elapsed() < limit {
        if let Some(ended) = launcher.reap().into_iter().next() {
            return Some((ended, start.elapsed()));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}

/// The desktop opening a session: a claude.exe outside the waker's tree with
/// `--resume=<id>` on its command line, held open by its stdin until killed.
fn open_elsewhere(fake: &Path, session: &str) -> Child {
    Command::new(fake)
        .args(["/d", "/k", "rem", &format!("--resume={session}")])
        .creation_flags(CREATE_NO_WINDOW)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

#[test]
fn a_run_is_killed_within_two_seconds_of_its_session_opening_elsewhere() {
    let scratch = Scratch::new("opened");
    let fake = fake_claude(scratch.path());
    let session = fresh_session();
    let log = Log::new(&scratch.path().join("waker.log"));
    let mut launcher = SystemLauncher::new(Arc::new(WinProcesses), log.clone());
    let plan = exclusive(ping(), &["-n", "30", "127.0.0.1"], &session);
    let started = launcher.start("d1", &session, &plan).expect("started");

    // Nothing else holds the session: the run goes on.
    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        launcher.reap().is_empty(),
        "killed with nothing holding the session"
    );
    assert_eq!(launcher.running(&session), Some(started.pid));

    let mut other = open_elsewhere(&fake, &session);
    let found = ended_within(&mut launcher, Duration::from_secs(5));
    let _ = other.kill();
    let _ = other.wait();
    let (ended, latency) = found.expect("the run was ended");
    eprintln!("kill observed {} ms after the open", latency.as_millis());
    assert!(ended.opened_elsewhere);
    assert_eq!((ended.request_id.as_str(), ended.pid), ("d1", started.pid));
    assert!(latency < Duration::from_millis(2500), "{latency:?}");
    let text = std::fs::read_to_string(log.path()).unwrap();
    assert!(
        text.contains(&format!(
            "pid {} killed: pid {} opened session",
            started.pid,
            other.id()
        )),
        "{text}"
    );
}

#[test]
fn the_waker_s_own_child_and_its_descendants_never_count_as_another_holder() {
    let scratch = Scratch::new("own");
    let fake = fake_claude(scratch.path());
    let session = fresh_session();
    // The child names the session, and so does the claude.exe it starts in turn.
    let inner = scratch.path().join("inner.cmd");
    std::fs::write(
        &inner,
        format!("@\"%~dp0{CLAUDE_IMAGE}\" /d /c \"ping -n 4 127.0.0.1 >nul & rem --resume={session}\"\r\n"),
    )
    .unwrap();
    let log = Log::new(&scratch.path().join("waker.log"));
    let mut launcher = SystemLauncher::new(Arc::new(WinProcesses), log.clone());
    let inner_arg = inner.to_string_lossy().into_owned();
    let plan = exclusive(fake.clone(), &["/d", "/c", &inner_arg, &session], &session);
    launcher.start("d1", &session, &plan).expect("started");

    std::thread::sleep(Duration::from_millis(1200));
    let naming = WinProcesses
        .snapshot()
        .unwrap()
        .into_iter()
        .filter(|e| e.image.eq_ignore_ascii_case(CLAUDE_IMAGE))
        .filter(|e| {
            WinProcesses
                .command_line(e.pid)
                .ok()
                .flatten()
                .is_some_and(|l| l.contains(&session))
        })
        .count();
    assert_eq!(
        naming, 2,
        "the child and its own claude.exe both name the session"
    );

    let (ended, _) = ended_within(&mut launcher, Duration::from_secs(10)).expect("it ends");
    assert!(
        !ended.opened_elsewhere,
        "{}",
        std::fs::read_to_string(log.path()).unwrap_or_default()
    );
    assert_eq!(ended.code, Some(0));
}

#[test]
fn a_process_list_that_cannot_be_read_mid_run_ends_the_run() {
    let scratch = Scratch::new("unread");
    let session = fresh_session();
    let log = Log::new(&scratch.path().join("waker.log"));
    let procs = FakeProcs::scripted(
        true,
        |_| ProcState::Gone,
        vec![Ok(Vec::new()), Err("process list unreadable".to_owned())],
    );
    let mut launcher = SystemLauncher::new(Arc::new(procs), log.clone());
    let plan = exclusive(ping(), &["-n", "30", "127.0.0.1"], &session);
    let start = Instant::now();
    launcher.start("d1", &session, &plan).expect("started");
    let (ended, _) = ended_within(&mut launcher, Duration::from_secs(5)).expect("it ends");
    assert!(ended.opened_elsewhere);
    assert!(start.elapsed() < Duration::from_millis(2500));
    let text = std::fs::read_to_string(log.path()).unwrap();
    assert!(
        text.contains("cannot be checked: process list unreadable"),
        "{text}"
    );
}

#[test]
fn a_child_whose_start_time_cannot_be_read_is_not_kept() {
    let scratch = Scratch::new("ownunread");
    let session = fresh_session();
    let log = Log::new(&scratch.path().join("waker.log"));
    let procs = FakeProcs::scripted(false, |_| ProcState::Unreadable, vec![Ok(Vec::new())]);
    let mut launcher = SystemLauncher::new(Arc::new(procs), log);
    let plan = exclusive(ping(), &["-n", "30", "127.0.0.1"], &session);
    match launcher.start("d1", &session, &plan) {
        Err(why) => assert_eq!(why, OPENED_ELSEWHERE),
        Ok(_) => {
            let (ended, _) = ended_within(&mut launcher, Duration::from_secs(3)).expect("ends");
            assert!(ended.opened_elsewhere);
        }
    }
    assert_eq!(launcher.running(&session), None);
}

#[test]
fn a_run_with_no_exclusive_session_is_not_watched() {
    let scratch = Scratch::new("codex");
    let log = Log::new(&scratch.path().join("waker.log"));
    let procs = Arc::new(FakeProcs::scripted(
        true,
        |_| ProcState::Gone,
        vec![Err("process list unreadable".to_owned())],
    ));
    let mut launcher = SystemLauncher::new(procs.clone(), log);
    let plan = Plan {
        route: CODEX_ROUTE,
        program: ping(),
        args: vec!["-n".into(), "2".into(), "127.0.0.1".into()],
        cwd: None,
        settle: Settle::Running(Duration::from_millis(300)),
        exclusive: None,
    };
    launcher.start("d2", "thread", &plan).expect("started");
    let (ended, _) = ended_within(&mut launcher, Duration::from_secs(5)).expect("it ends");
    assert!(!ended.opened_elsewhere);
    assert_eq!(ended.code, Some(0));
    assert_eq!(procs.line_calls.load(Ordering::SeqCst), 0);
}
