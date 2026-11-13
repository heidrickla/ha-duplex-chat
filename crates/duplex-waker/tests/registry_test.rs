#![cfg(windows)]

mod common;

use common::{FakeProcs, SESSION, Scratch, claude, desktop_line};
use duplex_waker::config::WakerConfig;
use duplex_waker::registry::{
    Liveness, Own, ProcEntry, ProcState, ProcessTable, WinProcesses, other_holder, session_liveness,
};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn record(scratch: &Scratch, pid: u32, session: &str, start: &str) {
    let text = format!(r#"{{"pid":{pid},"sessionId":"{session}","procStart":"{start}"}}"#);
    std::fs::write(scratch.path().join(format!("{pid}.json")), text).unwrap();
}

fn table(state: fn(u32) -> ProcState) -> FakeProcs {
    FakeProcs::with(false, state, vec![Ok(Vec::new())])
}

fn is_unknown(liveness: &Liveness) -> bool {
    matches!(liveness, Liveness::Unknown(_))
}

#[test]
fn own_process_start_is_a_filetime_near_now() {
    let ProcState::Running(created) = WinProcesses.state(std::process::id()) else {
        panic!("own process unreadable");
    };
    // FILETIME counts 100 ns from 1601; Unix time starts 11644473600 s later.
    let unix = created / 10_000_000 - 11_644_473_600;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        unix <= now && now - unix < 3600,
        "created {unix}, now {now}"
    );
}

#[test]
fn a_pid_no_process_holds_reads_gone() {
    // Pids are multiples of 4 on Windows; this one is never handed out.
    assert_eq!(WinProcesses.state(4_294_967_293), ProcState::Gone);
}

#[test]
fn command_lines_are_read_from_running_processes() {
    let marker = "waker-command-line-marker-5c1e";
    // cmd /k waits on its piped stdin until the test closes it.
    let mut child = Command::new("cmd.exe")
        .args(["/d", "/k", "rem", marker])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let snapshot = WinProcesses.snapshot();
    let line = WinProcesses.command_line(child.id());
    let _ = child.kill();
    let _ = child.wait();
    let snapshot = snapshot.expect("the process list reads whole");
    let entry = snapshot
        .iter()
        .find(|e| e.pid == child.id())
        .expect("the child is in the snapshot");
    assert!(entry.image.eq_ignore_ascii_case("cmd.exe"));
    assert_eq!(entry.parent, std::process::id());
    let line = line.expect("readable").expect("running");
    assert!(line.contains(marker), "{line}");
}

#[test]
fn a_waker_with_its_own_home_has_its_own_instance_name() {
    use duplex_waker::config::instance_name;
    use std::ffi::OsStr;
    assert_eq!(instance_name(None), "Local\\duplex-waker");
    let a = instance_name(Some(OsStr::new(r"C:\Temp\Waker-A")));
    let b = instance_name(Some(OsStr::new(r"C:\Temp\Waker-B")));
    assert!(a.starts_with("Local\\duplex-waker-") && a != b && a != instance_name(None));
    assert_eq!(
        a,
        instance_name(Some(OsStr::new(r"c:\temp\waker-a"))),
        "case-folded like the path"
    );
}

#[test]
fn an_exited_process_someone_still_holds_reads_as_exited_not_unreadable() {
    let mut child = Command::new("cmd.exe")
        .args(["/d", "/k", "rem"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    child.kill().unwrap();
    // `child` still holds the process handle, so the id stays openable after the exit.
    child.wait().unwrap();
    assert_eq!(WinProcesses.command_line(child.id()), Ok(None));
}

#[test]
fn a_matching_record_decides_by_process_and_start_time() {
    let scratch = Scratch::new("liveness");
    record(&scratch, 100, "other-session", "5");
    record(&scratch, 200, SESSION, "7");
    let at = |state| session_liveness(scratch.path(), SESSION, &table(state));
    assert_eq!(at(|_| ProcState::Running(7)), Liveness::Live { pid: 200 });
    assert_eq!(at(|_| ProcState::Running(8)), Liveness::Closed);
    assert_eq!(at(|_| ProcState::Gone), Liveness::Closed);
    assert!(is_unknown(&at(|_| ProcState::Unreadable)));
}

#[test]
fn a_command_line_naming_the_session_is_live_without_a_record() {
    let scratch = Scratch::new("cmdline");
    let procs = FakeProcs::with(
        false,
        |_| ProcState::Gone,
        vec![Ok(vec![(4100, desktop_line(&SESSION.to_uppercase()))])],
    );
    assert_eq!(
        session_liveness(scratch.path(), SESSION, &procs),
        Liveness::Live { pid: 4100 }
    );
}

/// Every way of not seeing a live process reads as unknown, never as closed.
#[test]
fn absence_of_evidence_is_never_closed() {
    let scratch = Scratch::new("evidence");
    let missing = scratch.path().join("missing");
    let gone = |_| ProcState::Gone;
    assert!(
        is_unknown(&session_liveness(&missing, SESSION, &table(gone))),
        "a records folder that cannot be read"
    );

    let unlisted = FakeProcs::with(false, gone, vec![Err("process list unreadable".into())]);
    assert!(
        is_unknown(&session_liveness(scratch.path(), SESSION, &unlisted)),
        "a process list that cannot be read"
    );

    // The list reads whole, but one claude.exe's command line does not.
    let hidden = FakeProcs::scripted(
        false,
        gone,
        vec![Ok(vec![
            (claude(4100, 4), Ok(Some(desktop_line("other-session")))),
            (
                claude(4200, 4),
                Err("command line of pid 4200 unreadable".into()),
            ),
        ])],
    );
    assert!(
        is_unknown(&session_liveness(scratch.path(), SESSION, &hidden)),
        "a command line that cannot be read"
    );

    let partial = Scratch::new("partial");
    std::fs::write(partial.path().join("300.json"), r#"{"pid":300,"sessio"#).unwrap();
    // Key files beside the records are never read.
    std::fs::write(partial.path().join("300.abc.key"), "not json").unwrap();
    assert!(
        is_unknown(&session_liveness(partial.path(), SESSION, &table(gone))),
        "a record that cannot be parsed"
    );

    let undated = Scratch::new("undated");
    std::fs::write(
        undated.path().join("400.json"),
        format!(r#"{{"pid":400,"sessionId":"{SESSION}"}}"#),
    )
    .unwrap();
    assert!(
        is_unknown(&session_liveness(
            undated.path(),
            SESSION,
            &table(|_| ProcState::Running(1))
        )),
        "a matching record without a start time"
    );

    let unreadable = Scratch::new("unreadable");
    record(&unreadable, 500, SESSION, "9");
    assert!(
        is_unknown(&session_liveness(
            unreadable.path(),
            SESSION,
            &table(|_| ProcState::Unreadable)
        )),
        "a matching record whose process cannot be examined"
    );
}

#[test]
fn closed_needs_a_clean_folder_and_clean_command_lines() {
    let scratch = Scratch::new("clean");
    record(&scratch, 100, "other-session", "5");
    let procs = FakeProcs::with(
        false,
        |_| ProcState::Running(5),
        vec![Ok(vec![(4100, desktop_line("other-session"))])],
    );
    assert_eq!(
        session_liveness(scratch.path(), SESSION, &procs),
        Liveness::Closed
    );
}

#[test]
fn config_never_shows_its_token() {
    let scratch = Scratch::new("config");
    let path = scratch.path().join("waker.json");
    std::fs::write(
        &path,
        r#"{"broker_url":"http://192.0.2.1:8790","token":"secret-value"}"#,
    )
    .unwrap();
    let config = WakerConfig::load(&path).unwrap();
    assert!(!format!("{config:?}").contains("secret-value"));
    std::fs::write(
        &path,
        r#"{"broker_url":"https://x","token":"secret-value"}"#,
    )
    .unwrap();
    let err = WakerConfig::load(&path).unwrap_err();
    assert!(!err.contains("secret-value"));
    std::fs::write(&path, r#"{"broker_url":"http://x","token":"#).unwrap();
    assert!(!WakerConfig::load(&path).unwrap_err().contains("secret"));
}

/// The waker's own run and what it started are not "another holder"; anything else
/// naming the session is, including a process under a reused parent pid.
#[test]
fn only_the_waker_s_own_tree_is_excluded() {
    let own = Own {
        pid: 10,
        created: 100,
    };
    let node = ProcEntry {
        pid: 11,
        parent: 10,
        image: "node.exe".to_owned(),
    };
    let line = Ok(Some(desktop_line(SESSION)));
    let started_by_own = vec![
        (claude(10, 1), line.clone()),
        (node.clone(), Ok(None)),
        (claude(12, 11), line.clone()),
    ];
    // Pid 12 was created after the waker's child: a descendant.
    let procs = FakeProcs::scripted(
        false,
        |pid| ProcState::Running(if pid == 12 { 150 } else { 100 }),
        vec![Ok(started_by_own.clone())],
    );
    assert_eq!(other_holder(&procs, SESSION, Some(own)), Ok(None));

    let mut opened = started_by_own.clone();
    opened.push((claude(13, 999), line.clone()));
    let procs = FakeProcs::scripted(false, |_| ProcState::Running(100), vec![Ok(opened)]);
    assert_eq!(other_holder(&procs, SESSION, Some(own)), Ok(Some(13)));

    // Pid 14 names pid 10 as its parent but is older than the waker's child.
    let mut reused = started_by_own;
    reused.push((claude(14, 10), line));
    let procs = FakeProcs::scripted(
        false,
        |pid| ProcState::Running(if pid == 14 { 50 } else { 150 }),
        vec![Ok(reused)],
    );
    assert_eq!(other_holder(&procs, SESSION, Some(own)), Ok(Some(14)));
}
