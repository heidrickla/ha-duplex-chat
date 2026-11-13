//! The adapter against a recording broker double that runs in every test run: what each claim
//! carries, what a delivery it cannot do ends as, and how a Codex task is woken.

use duplex_adapter::adapter::Adapter;
use duplex_adapter::config::AdapterConfig;
use duplex_adapter::identity::{CodexTask, Participant, SessionIdentity};
use duplex_adapter::watch::{Event, Source, Turn, TurnState};
use duplex_proto::wake::wake_line;
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// What the double saw and what it will answer to the next claims.
#[derive(Default)]
struct Seen {
    requests: Vec<(String, Value)>,
    claims: VecDeque<Value>,
}

struct Broker {
    url: String,
    seen: Arc<Mutex<Seen>>,
}

impl Broker {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Seen::default()));
        let shared = Arc::clone(&seen);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut first = String::new();
                if reader.read_line(&mut first).is_err() {
                    continue;
                }
                let mut length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; length];
                let _ = reader.read_exact(&mut body);
                let path = first.split_whitespace().nth(1).unwrap_or("").to_owned();
                let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let mut state = shared.lock().unwrap();
                state.requests.push((path.clone(), body));
                let (status, reply) = match path.as_str() {
                    "/api/adapter/register" => (
                        201,
                        json!({"token": "t-1", "binding": {"id": "b1", "title": "Builder"}, "owner_name": "Sam"}),
                    ),
                    "/api/adapter/claim" => {
                        (200, state.claims.pop_front().unwrap_or_else(|| json!({})))
                    }
                    "/api/adapter/limits" => (201, json!({"id": "b1:r1:200", "state": "armed"})),
                    _ => (200, json!({})),
                };
                let reply = reply.to_string();
                let _ = write!(
                    &stream,
                    "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                    reply.len()
                );
            }
        });
        Self { url, seen }
    }

    fn bodies(&self, path: &str) -> Vec<Value> {
        let state = self.seen.lock().unwrap();
        state
            .requests
            .iter()
            .filter(|(p, _)| p == path)
            .map(|(_, b)| b.clone())
            .collect()
    }

    fn will_claim(&self, answer: Value) {
        self.seen.lock().unwrap().claims.push_back(answer);
    }
}

/// A session reader that answers the turns it is given, the last one for good.
#[derive(Clone)]
struct Turns(Arc<Mutex<VecDeque<Turn>>>);

impl Turns {
    fn of(turns: &[Turn]) -> Self {
        Self(Arc::new(Mutex::new(turns.iter().copied().collect())))
    }

    fn next(&self) -> Turn {
        let mut turns = self.0.lock().unwrap();
        if turns.len() > 1 {
            turns.pop_front().unwrap()
        } else {
            turns.front().copied().unwrap_or(Turn::UNKNOWN)
        }
    }
}

impl Source for Turns {
    fn sample(&mut self) -> (Turn, Vec<Event>) {
        (self.next(), Vec::new())
    }

    fn peek_turn(&mut self) -> Turn {
        self.next()
    }
}

/// A session reader that sample by sample says what the transcript showed.
struct Watched(VecDeque<Vec<Event>>);

impl Source for Watched {
    fn sample(&mut self) -> (Turn, Vec<Event>) {
        (
            Turn::of(TurnState::Idle),
            self.0.pop_front().unwrap_or_default(),
        )
    }

    fn peek_turn(&mut self) -> Turn {
        Turn::of(TurnState::Idle)
    }
}

fn claude() -> Participant {
    let env = HashMap::from([
        ("CLAUDE_CODE_SESSION_ID".to_owned(), "s-1".to_owned()),
        (
            "CLAUDE_CODE_MESSAGING_SOCKET".to_owned(),
            "/tmp/none.sock".to_owned(),
        ),
        ("CLAUDE_CODE_MESSAGING_TOKEN".to_owned(), "inbox".to_owned()),
    ]);
    Participant::Claude(SessionIdentity::from_env(&env).expect("a session"))
}

/// The adapter on `broker`, its wake recorded instead of sent.
fn adapter(broker: &Broker, turns: &Turns) -> (Adapter, Arc<Mutex<Vec<String>>>) {
    adapter_reading(broker, Box::new(turns.clone()))
}

fn adapter_reading(broker: &Broker, source: Box<dyn Source>) -> (Adapter, Arc<Mutex<Vec<String>>>) {
    let config = AdapterConfig {
        broker_url: broker.url.clone(),
        enrollment_token: "enroll".to_owned(),
        poll_seconds: 0.1,
        token_dir: None,
    };
    let woken = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&woken);
    let adapter = Adapter::with_parts(
        config,
        claude(),
        std::env::temp_dir(),
        source,
        Box::new(move |text| {
            record.lock().unwrap().push(text.to_owned());
            Ok(())
        }),
    );
    adapter.register().expect("registered");
    (adapter, woken)
}

fn turn_of(claim: &Value) -> (String, Option<String>, u64) {
    let turn = &claim["turn"];
    (
        turn["state"].as_str().unwrap().to_owned(),
        turn["waiting_for"].as_str().map(str::to_owned),
        turn["seq"].as_u64().unwrap(),
    )
}

fn delivery(mode: &str) -> Value {
    json!({
        "delivery": {"id": "d1"},
        "message": {"id": "m1", "mode": mode, "text": "never in the wake"},
        "conversation": {"kind": "direct"},
    })
}

const CLAIM: &str = "/api/adapter/claim";
const STATE: &str = "/api/adapter/deliveries/d1/state";

#[test]
fn every_claim_carries_the_turn_read_just_before_it_numbered_in_order_and_from_one_after_a_registration()
 {
    let broker = Broker::start();
    let turns = Turns::of(&[
        Turn::of(TurnState::Busy),
        Turn::of(TurnState::Idle),
        Turn::waiting("permission_prompt"),
    ]);
    let (adapter, _) = adapter(&broker, &turns);
    for _ in 0..3 {
        assert!(!adapter.poll_once().unwrap());
    }
    let sent: Vec<_> = broker.bodies(CLAIM).iter().map(turn_of).collect();
    assert_eq!(
        sent,
        [
            ("busy".to_owned(), None, 1),
            ("idle".to_owned(), None, 2),
            (
                "waiting".to_owned(),
                Some("permission_prompt".to_owned()),
                3
            ),
        ]
    );

    // The broker starts its count again when the session registers, and so does the adapter.
    adapter.register().unwrap();
    assert!(!adapter.poll_once().unwrap());
    let again = broker.bodies(CLAIM);
    assert_eq!(turn_of(&again[3]).2, 1);
}

#[test]
fn a_mode_the_session_never_declared_fails_the_delivery_naming_what_it_supports() {
    let broker = Broker::start();
    let (adapter, woken) = adapter(&broker, &Turns::of(&[Turn::of(TurnState::Busy)]));
    broker.will_claim(delivery("restart"));
    assert!(adapter.poll_once().unwrap());
    let failed = broker.bodies(STATE);
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["state"], "failed");
    assert_eq!(
        failed[0]["reason"],
        "restart is not supported by \"Builder\" (supports: auto, queue, steer)"
    );
    assert!(woken.lock().unwrap().is_empty(), "nothing was woken");
}

#[test]
fn a_steer_into_a_session_with_no_running_turn_fails_the_delivery() {
    let broker = Broker::start();
    let (adapter, woken) = adapter(&broker, &Turns::of(&[Turn::of(TurnState::Idle)]));
    broker.will_claim(delivery("steer"));
    assert!(adapter.poll_once().unwrap());
    let failed = broker.bodies(STATE);
    assert_eq!(failed[0]["state"], "failed");
    assert_eq!(
        failed[0]["reason"],
        "\"Builder\" has no running turn (supports: auto, queue, steer)"
    );
    assert!(woken.lock().unwrap().is_empty());
}

#[test]
fn a_mode_it_takes_wakes_the_session_with_the_fixed_line_and_never_the_text() {
    let broker = Broker::start();
    let (adapter, woken) = adapter(&broker, &Turns::of(&[Turn::of(TurnState::Busy)]));
    broker.will_claim(delivery("steer"));
    assert!(adapter.poll_once().unwrap());
    assert!(
        broker.bodies(STATE).is_empty(),
        "a Claude wake is unconfirmed until the read"
    );
    let lines = woken.lock().unwrap();
    assert_eq!(*lines, [wake_line("s-1", "m1", "direct")]);
    assert!(!lines[0].contains("never in the wake"));
}

#[test]
fn a_limit_failure_is_reported_once_per_run_and_skipped_when_the_session_answers() {
    let failure = || {
        Event::Limit(duplex_adapter::watch::Limit {
            run_id: "r1".to_owned(),
            source: "claude-transcript",
            matched: "You've hit your session limit".to_owned(),
            failed_at: 100.0,
            reset_at: 200.0,
        })
    };
    let broker = Broker::start();
    let source = Watched(VecDeque::from([
        vec![failure()],
        vec![failure()],
        vec![Event::Answered],
    ]));
    let (adapter, _) = adapter_reading(&broker, Box::new(source));

    adapter.watch_once();
    adapter.watch_once(); // the same run seen again
    let reports = broker.bodies("/api/adapter/limits");
    assert_eq!(reports.len(), 1, "one report for one run");
    assert_eq!(
        reports[0],
        json!({"run_id": "r1", "source": "claude-transcript", "matched": "You've hit your session limit",
               "failed_at": 100.0, "reset_at": 200.0})
    );
    assert!(
        broker
            .bodies("/api/adapter/limits/b1:r1:200/skip")
            .is_empty()
    );

    adapter.watch_once(); // a new turn of the session answered it
    assert_eq!(
        broker.bodies("/api/adapter/limits/b1:r1:200/skip"),
        [json!({"reason": "answered"})]
    );
    assert_eq!(broker.bodies("/api/adapter/limits").len(), 1);
}

/// A `codex` that records its arguments in `woke.txt` beside it and exits 0.
fn fake_codex(dir: &Path) -> PathBuf {
    let (name, body) = if cfg!(windows) {
        (
            "fake-codex.cmd",
            "@echo off\r\necho %* > \"%~dp0woke.txt\"\r\nexit /b 0\r\n",
        )
    } else {
        (
            "fake-codex",
            "#!/bin/sh\necho \"$@\" > \"$(dirname \"$0\")/woke.txt\"\n",
        )
    };
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    path
}

#[test]
fn a_codex_task_is_woken_with_codex_queue_resume_for_its_thread() {
    let dir = std::env::temp_dir().join(format!("duplex-codex-wake-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let task = CodexTask {
        thread_id: "t-9".to_owned(),
        title_text: "Task".to_owned(),
        cwd: dir.display().to_string(),
        codex_argv: vec![fake_codex(&dir).display().to_string()],
    };
    let line = wake_line("t-9", "m1", "direct");
    Participant::Codex(task).wake(&line).unwrap();
    let recorded = std::fs::read_to_string(dir.join("woke.txt")).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    for part in [
        "queue",
        "--thread",
        "t-9",
        "--message",
        "--resume",
        "bridge_receive",
    ] {
        assert!(recorded.contains(part), "{part} missing from {recorded}");
    }
}
