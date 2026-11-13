#![cfg(windows)]

mod common;

use common::*;
use duplex_waker::broker::HttpBroker;
use duplex_waker::launch::{CLAUDE_ROUTE, CODEX_ROUTE, Ended, OPENED_ELSEWHERE, Settle};
use duplex_waker::log::Log;
use duplex_waker::registry::{ProcState, ProcessTable, WinProcesses};
use duplex_waker::waker::{Waker, report_id};
use serde_json::json;
use std::path::Path;

struct Rig {
    scratch: Scratch,
    log: Log,
}

impl Rig {
    fn new(name: &str) -> Self {
        let scratch = Scratch::new(name);
        let log = Log::new(&scratch.path().join("waker.log"));
        Self { scratch, log }
    }

    fn sessions(&self) -> std::path::PathBuf {
        self.scratch.dir("sessions")
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(self.log.path()).unwrap_or_default()
    }
}

fn own_start() -> u64 {
    match WinProcesses.state(std::process::id()) {
        ProcState::Running(created) => created,
        other => panic!("own process reads as {other:?}"),
    }
}

fn write_record(dir: &Path, pid: u32, session: &str, proc_start: u64) {
    let record = json!({
        "pid": pid, "sessionId": session, "procStart": proc_start.to_string(),
        "kind": "interactive", "status": "idle",
    });
    std::fs::write(dir.join(format!("{pid}.json")), record.to_string()).unwrap();
}

#[test]
fn listed_request_is_started_once_and_reported() {
    let rig = Rig::new("listed");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    drop(waker);

    assert_eq!(launcher.starts.len(), 1);
    let (id, session, plan) = &launcher.starts[0];
    assert_eq!((id.as_str(), session.as_str()), ("d1", SESSION));
    assert_eq!(plan.route, CLAUDE_ROUTE);
    assert_eq!(plan.cwd.as_deref(), Some(workspace.as_path()));
    assert_eq!(
        plan.args[..5],
        ["-p", "--resume", SESSION, "--model", "claude-haiku-5-5"]
    );
    assert!(plan.args[5].contains("message 6058b30915944089b93e6d61672f5134"));
    assert!(matches!(plan.settle, Settle::Running(_)));
    let started = broker.started();
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].0, "d1");
    assert_eq!(started[0].1["ok"], true);
    assert_eq!(started[0].1["route"], CLAUDE_ROUTE);
    let log = rig.log_text();
    assert!(
        log.contains("request d1") && log.contains("started by claude-print-resume"),
        "{log}"
    );
}

#[test]
fn a_start_whose_report_failed_is_not_made_again() {
    let rig = Rig::new("once");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    // The report and its first retry are lost; the broker lists the request again meanwhile.
    broker
        .state
        .lock()
        .unwrap()
        .started_status
        .extend([500, 500]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    waker.tick();
    waker.tick();
    drop(waker);

    assert_eq!(launcher.starts.len(), 1, "one listed request, one start");
    let started = broker.started();
    assert_eq!(started.len(), 1, "the report is retried until taken, once");
    assert_eq!(started[0].1["ok"], true);
}

#[test]
fn a_request_no_longer_listed_is_never_started() {
    let rig = Rig::new("unlisted");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    // The first report is lost, so the waker still holds the request when it leaves the list.
    broker
        .state
        .lock()
        .unwrap()
        .started_status
        .extend([500, 500]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    broker.list(&[]);
    waker.tick();
    waker.tick();
    broker.state.lock().unwrap().down = true;
    waker.tick();
    drop(waker);

    assert_eq!(launcher.starts.len(), 1, "only the listing starts anything");
}

#[test]
fn a_live_session_is_skipped_and_reported() {
    let rig = Rig::new("live");
    let workspace = rig.scratch.dir("workspace");
    let sessions = rig.sessions();
    // This test process stands in for the live claude.exe: its pid, its real start time.
    write_record(&sessions, std::process::id(), SESSION, own_start());
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    let procs = FakeProcs::real();
    let mut waker = Waker::new(&http, &mut launcher, &procs, &FakeTools, sessions, &rig.log);
    waker.tick();
    drop(waker);

    assert!(
        launcher.starts.is_empty(),
        "a live session is never resumed beside its process"
    );
    let started = broker.started();
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].1["ok"], false);
    assert_eq!(started[0].1["detail"], "session is live");
    assert_eq!(started[0].1["route"], CLAUDE_ROUTE);
}

#[test]
fn a_left_over_record_does_not_block_the_start() {
    let rig = Rig::new("stale");
    let workspace = rig.scratch.dir("workspace");
    let sessions = rig.sessions();
    // Same pid, another creation time: the pid was reused after that session ended.
    write_record(&sessions, std::process::id(), SESSION, own_start() - 1);
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    let procs = FakeProcs::real();
    let mut waker = Waker::new(&http, &mut launcher, &procs, &FakeTools, sessions, &rig.log);
    waker.tick();
    drop(waker);
    assert_eq!(launcher.starts.len(), 1);
}

#[test]
fn a_running_waker_start_counts_as_live() {
    let rig = Rig::new("running");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher {
        running: Some(777),
        ..FakeLauncher::default()
    };
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    drop(waker);
    assert!(launcher.starts.is_empty());
    assert_eq!(broker.started()[0].1["ok"], false);
}

#[test]
fn an_unreachable_broker_starts_nothing_and_logs_once() {
    let rig = Rig::new("unreachable");
    let http = HttpBroker::new(&closed_url(), TOKEN);
    let mut launcher = FakeLauncher::default();
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    for _ in 0..3 {
        waker.tick();
    }
    drop(waker);
    assert!(launcher.starts.is_empty());
    let log = rig.log_text();
    assert_eq!(log.matches("broker unreachable").count(), 1, "{log}");
}

#[test]
fn a_listing_answered_with_an_error_status_starts_nothing() {
    let rig = Rig::new("status");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    broker.state.lock().unwrap().list_status = Some(500);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    drop(waker);
    assert!(
        launcher.starts.is_empty(),
        "only a 2xx listing is a listing"
    );
    assert!(rig.log_text().contains("HTTP 500"));
}

#[test]
fn an_outage_is_logged_once_and_its_end_once() {
    let rig = Rig::new("outage");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    broker.state.lock().unwrap().down = true;
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    waker.tick();
    assert!(launcher_is_idle(&broker));
    broker.state.lock().unwrap().down = false;
    waker.tick();
    drop(waker);
    let log = rig.log_text();
    assert_eq!(log.matches("broker unreachable").count(), 1, "{log}");
    assert_eq!(log.matches("broker reachable again").count(), 1, "{log}");
    assert_eq!(launcher.starts.len(), 1);
}

fn launcher_is_idle(broker: &FakeBroker) -> bool {
    broker.started().is_empty()
}

#[test]
fn the_token_is_sent_but_never_logged() {
    let rig = Rig::new("token");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[
        claude_request("d1", &workspace),
        json!({"id": "d2", "binding_id": "b2", "provider": "codex",
               "native_id": "019a3f2e-7c41-7d05-b8e2-4f1a6c9d0e37", "title": "probe",
               "workspace": null, "reason": "limit_resume", "message_id": "m2", "due_at": null}),
    ]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher {
        fail: Some("could not start".to_owned()),
        ..FakeLauncher::default()
    };
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    broker.state.lock().unwrap().list_status = Some(401);
    waker.tick();
    broker.state.lock().unwrap().list_status = None;
    broker
        .state
        .lock()
        .unwrap()
        .started_status
        .extend([500, 401]);
    waker.tick();
    broker.state.lock().unwrap().down = true;
    waker.tick();
    drop(waker);

    let state = broker.state.lock().unwrap();
    assert!(state.auth.iter().all(|a| a == &format!("Bearer {TOKEN}")));
    assert!(!state.auth.is_empty());
    let log = rig.log_text();
    assert!(!log.is_empty());
    assert!(!log.contains(TOKEN), "{log}");
}

#[test]
fn codex_requests_queue_the_wake_line_and_resume() {
    let rig = Rig::new("codex");
    let broker = FakeBroker::start();
    broker.list(
        &[json!({"id": "d3", "binding_id": "b3", "provider": "codex",
        "native_id": "019a3f2e-7c41-7d05-b8e2-4f1a6c9d0e37", "title": "probe",
        "workspace": "D:\\nowhere", "reason": "limit_resume", "message_id": "m3",
        "due_at": "2026-11-11T07:00:00Z"})],
    );
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    drop(waker);
    let plan = &launcher.starts[0].2;
    assert_eq!(plan.route, CODEX_ROUTE);
    assert_eq!(
        plan.args[..3],
        ["queue", "--thread", "019a3f2e-7c41-7d05-b8e2-4f1a6c9d0e37"]
    );
    assert_eq!(plan.args[3], "--message");
    assert_eq!(plan.args[5], "--resume");
    assert!(matches!(plan.settle, Settle::ExitZero(_)));
}

#[test]
fn a_request_with_unsafe_fields_is_refused_not_started() {
    let rig = Rig::new("unsafe");
    let workspace = rig.scratch.dir("workspace");
    let mut flag = claude_request("d1", &workspace);
    flag["native_id"] = json!("--dangerously-skip-permissions");
    let mut missing = claude_request("d2", &workspace);
    missing["workspace"] = json!(rig.scratch.path().join("gone").to_string_lossy());
    let mut reason = claude_request("d3", &workspace);
    reason["reason"] = json!("anything");
    let mut provider = claude_request("d4", &workspace);
    provider["provider"] = json!("chatgpt");
    let broker = FakeBroker::start();
    broker.list(&[flag, missing, reason, provider]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    drop(waker);
    assert!(launcher.starts.is_empty());
    let started = broker.started();
    assert_eq!(started.len(), 4);
    assert!(started.iter().all(|(_, r)| r["ok"] == false));
}

#[test]
fn request_parses_the_contract_shape() {
    let r = request(&claude_request("d1", Path::new(r"C:\x")));
    assert_eq!(r.provider, "claude");
    assert_eq!(r.title.as_deref(), Some("duplex wake probe 2"));
}

#[test]
fn liveness_is_checked_again_just_before_the_launch() {
    let rig = Rig::new("recheck");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    // Closed at the first look; the desktop app opens the session before the launch.
    let procs = FakeProcs::with(
        true,
        |_| ProcState::Gone,
        vec![Ok(Vec::new()), Ok(vec![(4100, desktop_line(SESSION))])],
    );
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    drop(waker);
    assert_eq!(
        procs.line_calls.load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert!(
        launcher.starts.is_empty(),
        "a session opened meanwhile is not resumed"
    );
    let started = broker.started();
    assert_eq!(started[0].1["ok"], false);
    assert_eq!(started[0].1["detail"], "session is live");
}

#[test]
fn an_unestablished_session_is_skipped_and_reported() {
    let rig = Rig::new("unestablished");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    // The command lines read clean; only the missing records folder is unseen.
    let procs = FakeProcs::real();
    let missing = rig.scratch.path().join("no-sessions-folder");
    let mut waker = Waker::new(&http, &mut launcher, &procs, &FakeTools, missing, &rig.log);
    waker.tick();
    drop(waker);
    assert!(launcher.starts.is_empty());
    let started = broker.started();
    assert_eq!(started[0].1["ok"], false);
    assert!(
        started[0].1["detail"]
            .as_str()
            .unwrap()
            .starts_with("cannot establish the session is closed")
    );
}

#[test]
fn a_run_ended_because_its_session_was_opened_is_reported_failed() {
    let rig = Rig::new("opened");
    let broker = FakeBroker::start();
    broker.list(&[]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher::default();
    launcher.ended.push(Ended {
        request_id: "d1".to_owned(),
        route: CLAUDE_ROUTE,
        pid: 4242,
        code: Some(1),
        opened_elsewhere: true,
    });
    launcher.ended.push(Ended {
        request_id: "d2".to_owned(),
        route: CLAUDE_ROUTE,
        pid: 4243,
        code: Some(0),
        opened_elsewhere: false,
    });
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    drop(waker);
    let started = broker.started();
    assert_eq!(
        started.len(),
        1,
        "only the ended run is reported: {started:?}"
    );
    assert_eq!(started[0].0, "d1");
    assert_eq!(started[0].1["ok"], false);
    assert_eq!(started[0].1["route"], CLAUDE_ROUTE);
    assert_eq!(started[0].1["detail"], OPENED_ELSEWHERE);
}

fn killed(request_id: &str) -> Ended {
    Ended {
        request_id: request_id.to_owned(),
        route: CLAUDE_ROUTE,
        pid: 4242,
        code: Some(1),
        opened_elsewhere: true,
    }
}

/// Every report of a run reaches the broker once, in the order it happened: a kill
/// report never replaces or overtakes a start report the broker has not taken.
#[test]
fn a_run_s_reports_reach_the_broker_in_order_when_the_start_report_was_lost() {
    let rig = Rig::new("ordered");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    // The start report fails; so does the next attempt, made after the run was killed.
    broker
        .state
        .lock()
        .unwrap()
        .started_status
        .extend([500, 500]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    // The first tick starts the run; the second reaps it, killed.
    let mut launcher = FakeLauncher {
        reaps: [vec![], vec![killed("d1")]].into(),
        ..FakeLauncher::default()
    };
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    waker.tick();
    waker.tick();
    waker.tick();
    drop(waker);

    assert_eq!(launcher.starts.len(), 1);
    let started = broker.started();
    let sent: Vec<(&str, bool)> = started
        .iter()
        .map(|(id, body)| (id.as_str(), body["ok"].as_bool().unwrap()))
        .collect();
    assert_eq!(sent, [("d1", true), ("d1", false)]);
    assert_eq!(started[1].1["detail"], OPENED_ELSEWHERE);
}

/// A refused report is dropped alone; the reports after it still go, in order.
#[test]
fn a_refused_report_drops_only_itself() {
    let rig = Rig::new("refused");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    broker
        .state
        .lock()
        .unwrap()
        .started_status
        .extend([500, 422]);
    let http = HttpBroker::new(&broker.url, TOKEN);
    // The first tick starts the run; the second reaps it, killed.
    let mut launcher = FakeLauncher {
        reaps: [vec![], vec![killed("d1")]].into(),
        ..FakeLauncher::default()
    };
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    waker.tick();
    waker.tick();
    waker.tick();
    drop(waker);

    let sent: Vec<bool> = broker
        .started()
        .iter()
        .map(|(_, body)| body["ok"].as_bool().unwrap())
        .collect();
    assert_eq!(sent, [false]);
}

/// A report the broker recorded but whose answer was lost is sent again under the same
/// id, so the broker keeps it once; the run's next report takes the next id.
#[test]
fn a_report_whose_answer_was_lost_is_resent_under_its_id_and_kept_once() {
    let rig = Rig::new("lostanswer");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    broker
        .state
        .lock()
        .unwrap()
        .started_status
        .push_back(ANSWER_LOST);
    let http = HttpBroker::new(&broker.url, TOKEN);
    let mut launcher = FakeLauncher {
        reaps: [vec![], vec![killed("d1")]].into(),
        ..FakeLauncher::default()
    };
    let procs = FakeProcs::real();
    let mut waker = Waker::new(
        &http,
        &mut launcher,
        &procs,
        &FakeTools,
        rig.sessions(),
        &rig.log,
    );
    let start = waker.started_ms;
    waker.tick();
    waker.tick();
    drop(waker);

    let posts: Vec<String> = broker
        .state
        .lock()
        .unwrap()
        .posts
        .iter()
        .map(|p| p["report_id"].as_str().unwrap_or("").to_owned())
        .collect();
    let first = report_id("d1", start, 1);
    assert_eq!(posts, [first.clone(), first, report_id("d1", start, 2)]);
    let kept: Vec<bool> = broker
        .started()
        .iter()
        .map(|(_, body)| body["ok"].as_bool().unwrap())
        .collect();
    assert_eq!(kept, [true, false], "each report is kept once, in order");
}

/// A restarted waker numbers its reports afresh but under its own start, so its first
/// report for a request is not taken for an earlier process's first.
#[test]
fn a_restarted_waker_s_reports_never_reuse_an_earlier_process_s_ids() {
    let rig = Rig::new("restart");
    let workspace = rig.scratch.dir("workspace");
    let broker = FakeBroker::start();
    broker.list(&[claude_request("d1", &workspace)]);
    broker.state.lock().unwrap().relist = true;
    let http = HttpBroker::new(&broker.url, TOKEN);
    let procs = FakeProcs::real();
    for started_ms in [1_794_380_000_000, 1_794_380_060_000] {
        // A fresh process: nothing carried over but the broker.
        let mut launcher = FakeLauncher::default();
        let mut waker = Waker::new(
            &http,
            &mut launcher,
            &procs,
            &FakeTools,
            rig.sessions(),
            &rig.log,
        );
        waker.started_ms = started_ms;
        waker.tick();
        drop(waker);
        assert_eq!(launcher.starts.len(), 1);
    }
    let posts: Vec<String> = broker
        .state
        .lock()
        .unwrap()
        .posts
        .iter()
        .map(|p| p["report_id"].as_str().unwrap_or("").to_owned())
        .collect();
    assert_eq!(
        posts,
        [
            report_id("d1", 1_794_380_000_000, 1),
            report_id("d1", 1_794_380_060_000, 1)
        ]
    );
    assert_eq!(
        broker.started().len(),
        2,
        "the second process's start is recorded"
    );
}
