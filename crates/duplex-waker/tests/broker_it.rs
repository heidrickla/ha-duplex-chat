//! The waker against the real broker app from this tree (`duplex_broker/src`), served by
//! uvicorn on a local port. Needs a Python with the broker's runtime dependencies
//! (fastapi, uvicorn, websockets), named by `DUPLEX_BROKER_PYTHON`; run with
//! `cargo nextest run --run-ignored only`.
#![cfg(windows)]

mod common;

use common::{FakeLauncher, FakeProcs, FakeTools, Scratch, closed_url};
use duplex_waker::broker::{Broker, BrokerError, HttpBroker, Report, Request};
use duplex_waker::launch::{CLAUDE_ROUTE, Ended, OPENED_ELSEWHERE};
use duplex_waker::log::Log;
use duplex_waker::waker::{Waker, report_id};
use serde_json::{Value, json};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const WAKER_TOKEN: &str = "it-waker-token-3b8e";
const ENROLL: &str = "it-enroll-token-91c2";
const PAIRING: &str = "it-pairing-5d0a";

/// Serves the tree's broker with no start grace and a fast sweep, so a resume armed in
/// the past is listed within a second; `IT_RETRY_AFTER` shortens the wait before a
/// started request is listed again.
const SERVE: &str = r#"
import os, sys, uvicorn
import duplex_broker.waker as waker
from duplex_broker.api import Settings, create_app
waker.RETRY_AFTER = float(os.environ.get("IT_RETRY_AFTER", waker.RETRY_AFTER))
from duplex_broker.store import Store
store = Store(sys.argv[1], owner="owner")
settings = Settings(waker_token=os.environ["IT_WAKER"], enrollment_token=os.environ["IT_ENROLL"],
                    pairing_code=os.environ["IT_PAIRING"], waker_grace=0.0, collaboration_sweep=0.2)
uvicorn.run(create_app(store, settings), host="127.0.0.1", port=int(sys.argv[2]), log_level="warning")
"#;

struct Served {
    child: Child,
    url: String,
}

impl Drop for Served {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn serve(scratch: &Scratch, retry_after: Option<f64>) -> Served {
    let python = std::env::var("DUPLEX_BROKER_PYTHON")
        .expect("DUPLEX_BROKER_PYTHON names a Python with the broker's dependencies");
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("duplex_broker")
        .join("src");
    assert!(src.join("duplex_broker").is_dir(), "{}", src.display());
    let url = closed_url();
    let port = url.rsplit(':').next().unwrap().to_owned();
    let script = scratch.path().join("serve.py");
    std::fs::write(&script, SERVE).unwrap();
    let child = Command::new(python)
        .arg(&script)
        .arg(scratch.path().join("broker.db"))
        .arg(&port)
        .env("PYTHONPATH", &src)
        .env("IT_WAKER", WAKER_TOKEN)
        .env("IT_ENROLL", ENROLL)
        .env("IT_PAIRING", PAIRING)
        .envs(retry_after.map(|s| ("IT_RETRY_AFTER", s.to_string())))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let served = Served { child, url };
    let deadline = Instant::now() + Duration::from_secs(30);
    while ureq::get(&format!("{}/health", served.url)).call().is_err() {
        assert!(
            Instant::now() < deadline,
            "the broker did not answer /health"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    served
}

fn post(url: &str, bearer: &str, body: Value) -> Value {
    let mut call = ureq::post(url);
    if !bearer.is_empty() {
        call = call.header("authorization", &format!("Bearer {bearer}"));
    }
    let mut answer = call
        .send_json(&body)
        .unwrap_or_else(|e| panic!("POST {url}: {e}"));
    if answer.status() == 204 {
        return Value::Null;
    }
    answer.body_mut().read_json().unwrap()
}

fn get(url: &str, bearer: &str) -> Value {
    ureq::get(url)
        .header("authorization", &format!("Bearer {bearer}"))
        .call()
        .unwrap_or_else(|e| panic!("GET {url}: {e}"))
        .body_mut()
        .read_json()
        .unwrap()
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// What becomes of one report post.
#[derive(Clone, Copy)]
enum Fate {
    /// The broker is unreachable: nothing arrives.
    Lost,
    /// The broker records the report; its answer never comes back.
    AnswerLost,
}

/// The real broker over HTTP, with the first report posts meeting the scripted fates.
struct Scripted {
    inner: HttpBroker,
    fates: RefCell<VecDeque<Fate>>,
    report_ids: RefCell<Vec<String>>,
}

impl Scripted {
    fn new(base: &str, fates: &[Fate]) -> Self {
        Self {
            inner: HttpBroker::new(base, WAKER_TOKEN),
            fates: RefCell::new(fates.iter().copied().collect()),
            report_ids: RefCell::new(Vec::new()),
        }
    }
}

impl Broker for Scripted {
    fn requests(&self) -> Result<Vec<Request>, BrokerError> {
        self.inner.requests()
    }
    fn started(&self, request_id: &str, report: &Report) -> Result<(), BrokerError> {
        self.report_ids.borrow_mut().push(report.report_id.clone());
        let fate = self.fates.borrow_mut().pop_front();
        match fate {
            Some(Fate::Lost) => Err(BrokerError::Unreachable("connection refused".to_owned())),
            Some(Fate::AnswerLost) => {
                self.inner.started(request_id, report)?;
                Err(BrokerError::Unreachable("connection reset".to_owned()))
            }
            None => self.inner.started(request_id, report),
        }
    }
}

/// Registers a Claude session and arms a usage-limit resume for it whose reset has
/// passed; returns the session's token, the owner's device token and the listed request.
fn arm(base: &str, workspace: &Path, broker: &dyn Broker) -> (String, String, String) {
    let native = "5a1d0c7e-2b4f-4e8a-9c3d-7f6e5d4c3b2a";
    let registered = post(
        &format!("{base}/api/adapter/register"),
        ENROLL,
        json!({"provider": "claude", "native_id": native, "title": "waker it",
               "workspace": workspace.to_string_lossy(), "endpoint_ref": "claude-inbox",
               "capabilities": {"receive_messages": "verified", "publish_replies": "verified"},
               "current_token": ""}),
    );
    let session_token = registered["token"].as_str().unwrap().to_owned();
    let at = now();
    post(
        &format!("{base}/api/adapter/limits"),
        &session_token,
        json!({"run_id": "it-run", "source": "claude-transcript", "matched": "usage limit",
               "failed_at": at - 120.0, "reset_at": at - 60.0}),
    );
    let owner = post(
        &format!("{base}/api/device/enroll"),
        "",
        json!({"pairing_code": PAIRING, "device_name": "waker-it"}),
    )["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let deadline = Instant::now() + Duration::from_secs(10);
    let delivery = loop {
        let listed = broker.requests().expect("the waker list reads");
        if let Some(first) = listed.first() {
            break first.id.clone();
        }
        assert!(Instant::now() < deadline, "the resume was never listed");
        std::thread::sleep(Duration::from_millis(100));
    };
    (session_token, owner, delivery)
}

/// The broker's record of the request's reports, oldest first, as (ok, detail).
fn recorded(base: &str, owner: &str, delivery: &str) -> Vec<(bool, String)> {
    let mut starts: Vec<(bool, String)> = get(&format!("{base}/api/waker/starts"), owner)
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["request_id"] == delivery)
        .map(|s| {
            (
                s["ok"].as_bool().unwrap(),
                s["detail"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    // Newest first from the broker.
    starts.reverse();
    starts
}

/// A start whose report was lost, then a kill. The start report is retried; the broker
/// records the retry but its answer is lost, so it is sent again under the same id. The
/// broker keeps `ok: true` once and then `ok: false`, in order, and returns the delivery
/// to the queue for the session that opened.
#[test]
#[ignore = "needs the real broker: DUPLEX_BROKER_PYTHON"]
fn a_killed_run_whose_start_report_was_lost_is_reported_in_order_and_requeued() {
    let scratch = Scratch::new("broker-it");
    let workspace: PathBuf = scratch.dir("workspace");
    let sessions = scratch.dir("sessions");
    let served = serve(&scratch, None);
    let base = served.url.clone();
    let broker = Scripted::new(&base, &[Fate::Lost, Fate::AnswerLost]);
    let (session_token, owner, delivery) = arm(&base, &workspace, &broker);

    let log = Log::new(&scratch.path().join("waker.log"));
    // The first tick starts the run; the second reaps it, killed when the desktop opened
    // the session.
    let mut launcher = FakeLauncher {
        reaps: [
            vec![],
            vec![Ended {
                request_id: delivery.clone(),
                route: CLAUDE_ROUTE,
                pid: 4242,
                code: Some(1),
                opened_elsewhere: true,
            }],
        ]
        .into(),
        ..FakeLauncher::default()
    };
    let procs = FakeProcs::real();
    let mut waker = Waker::new(&broker, &mut launcher, &procs, &FakeTools, sessions, &log);
    let start = waker.started_ms;
    // The start; its report does not arrive.
    waker.tick();
    // The started session's adapter takes the delivery, as `claude -p` does.
    let claimed = post(
        &format!("{base}/api/adapter/claim"),
        &session_token,
        json!({}),
    );
    assert_eq!(claimed["delivery"]["id"], delivery.as_str());
    // The kill waits behind the start report, whose retry is recorded but unanswered.
    waker.tick();
    // The start report again, answered as taken, then the kill report.
    waker.tick();
    drop(waker);

    let seen = recorded(&base, &owner, &delivery);
    eprintln!("broker starts for {delivery}: {seen:?}");
    assert_eq!(
        seen,
        [
            (true, "pid 4242 running".to_owned()),
            (false, OPENED_ELSEWHERE.to_owned())
        ],
        "each report once, in order"
    );
    let sent = broker.report_ids.borrow().clone();
    eprintln!("report ids sent: {sent:?}");
    let first = report_id(&delivery, start, 1);
    assert_eq!(
        sent,
        [
            first.clone(),
            first.clone(),
            first,
            report_id(&delivery, start, 2)
        ]
    );

    let queued = get(
        &format!("{base}/api/adapter/deliveries?state=queued"),
        &session_token,
    );
    let ids: Vec<&str> = queued
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|d| d["id"].as_str())
        .collect();
    eprintln!("queued deliveries: {ids:?}");
    assert!(
        ids.contains(&delivery.as_str()),
        "the delivery was not requeued"
    );
    let again = post(
        &format!("{base}/api/adapter/claim"),
        &session_token,
        json!({}),
    );
    assert_eq!(
        again["delivery"]["id"],
        delivery.as_str(),
        "the opened session gets it"
    );
}

/// A waker that restarts numbers its reports afresh. Its first report for a request the
/// broker lists again is recorded, not taken for the earlier process's first.
#[test]
#[ignore = "needs the real broker: DUPLEX_BROKER_PYTHON"]
fn a_restarted_waker_s_start_report_is_recorded() {
    let scratch = Scratch::new("broker-it-restart");
    let workspace: PathBuf = scratch.dir("workspace");
    // A started request is listed again at once, as it is 15 minutes later in service.
    let served = serve(&scratch, Some(0.0));
    let base = served.url.clone();
    let broker = Scripted::new(&base, &[]);
    let (_, owner, delivery) = arm(&base, &workspace, &broker);
    let procs = FakeProcs::real();
    let mut starts = Vec::new();
    for (process, sessions) in [(0u64, scratch.dir("s1")), (60_000, scratch.dir("s2"))] {
        // A fresh waker process: nothing carried over but the broker.
        let log = Log::new(&scratch.path().join(format!("waker{process}.log")));
        let mut launcher = FakeLauncher::default();
        let mut waker = Waker::new(&broker, &mut launcher, &procs, &FakeTools, sessions, &log);
        waker.started_ms += process;
        starts.push(waker.started_ms);
        waker.tick();
        drop(waker);
        assert_eq!(
            launcher.starts.len(),
            1,
            "process {process} started the session"
        );
    }
    let sent = broker.report_ids.borrow().clone();
    eprintln!("report ids sent: {sent:?}");
    assert_eq!(
        sent,
        [
            report_id(&delivery, starts[0], 1),
            report_id(&delivery, starts[1], 1)
        ]
    );
    let seen = recorded(&base, &owner, &delivery);
    eprintln!("broker starts for {delivery}: {seen:?}");
    assert_eq!(
        seen.len(),
        2,
        "the restarted waker's start was not recorded: {seen:?}"
    );
}
