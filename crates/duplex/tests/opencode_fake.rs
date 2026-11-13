//! `duplex opencode` against two doubles that run in every test run: a recording broker and a
//! scripted OpenCode server that behaves as OpenCode 2.3.0 was measured to (2026-11-13). An idle
//! session starts a round on any input; a steer joins the running round; a queued input waits
//! for the running round to stop and gets a round of its own; an interrupt ends the execution
//! `interrupted`. A round answers "echo: <its inputs, joined by | >". Input containing HOLD keeps
//! its round running until the test finishes it.

use duplex_adapter::opencode::Log;
use duplex_adapter::opencode::config::{SessionMap, SessionSpec};
use duplex_adapter::opencode::server::{Discovery, OpenCode, base64};
use duplex_adapter::opencode::session::{Managed, Shared};
use duplex_proto::BrokerClient;
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PASSWORD: &str = "oc-pass-Zq81xVw4";

struct Request {
    method: String,
    path: String,
    auth: String,
    body: Value,
}

fn read_request(stream: &TcpStream) -> Option<Request> {
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut first = String::new();
    reader.read_line(&mut first).ok()?;
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
    reader.read_exact(&mut body).ok()?;
    let mut parts = first.split_whitespace();
    Some(Request {
        method: parts.next()?.to_owned(),
        path: parts.next()?.to_owned(),
        auth,
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
    })
}

fn respond(stream: &TcpStream, status: u16, body: &Value) {
    let text = if body.is_null() {
        String::new()
    } else {
        body.to_string()
    };
    let mut stream = stream;
    let _ = write!(
        stream,
        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
        text.len()
    );
}

/// Serves `handle` on a fresh port until `closed` is set; then every connection is dropped
/// unanswered, as a closed app's port refuses.
fn serve(handle: impl Fn(&Request) -> (u16, Value) + Send + 'static) -> (String, Arc<AtomicBool>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let closed = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&closed);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            if flag.load(Ordering::SeqCst) {
                let _ = stream.shutdown(std::net::Shutdown::Both);
                continue;
            }
            if let Some(request) = read_request(&stream) {
                let (status, body) = handle(&request);
                respond(&stream, status, &body);
            }
        }
    });
    (url, closed)
}

// The broker double.

#[derive(Default)]
struct BrokerSeen {
    requests: Vec<(String, Value)>,
    claims: VecDeque<Value>,
    /// Answers to give, by path, before the normal one: (status, body).
    refuse: HashMap<String, VecDeque<(u16, Value)>>,
    in_flight: Value,
    registrations: u64,
}

struct Broker {
    url: String,
    seen: Arc<Mutex<BrokerSeen>>,
}

impl Broker {
    fn start() -> Self {
        let seen = Arc::new(Mutex::new(BrokerSeen {
            in_flight: json!([]),
            ..BrokerSeen::default()
        }));
        let shared = Arc::clone(&seen);
        let (url, _) = serve(move |request| {
            let mut state = shared.lock().unwrap();
            let path = request.path.clone();
            state.requests.push((path.clone(), request.body.clone()));
            if let Some(answer) = state.refuse.get_mut(&path).and_then(VecDeque::pop_front) {
                return answer;
            }
            match path.split('?').next().unwrap_or("") {
                "/api/adapter/register" => {
                    state.registrations += 1;
                    let n = state.registrations;
                    (
                        201,
                        json!({"token": format!("tok-{n}"), "binding": {"id": "b1", "title": request.body["title"]}}),
                    )
                }
                "/api/adapter/claim" => {
                    (200, state.claims.pop_front().unwrap_or_else(|| json!({})))
                }
                "/api/adapter/deliveries" => (200, state.in_flight.clone()),
                _ => (200, json!({})),
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

    fn paths(&self) -> Vec<String> {
        let state = self.seen.lock().unwrap();
        state.requests.iter().map(|(p, _)| p.clone()).collect()
    }

    fn will_claim(&self, answer: Value) {
        self.seen.lock().unwrap().claims.push_back(answer);
    }

    fn will_answer(&self, path: &str, status: u16, body: Value) {
        self.seen
            .lock()
            .unwrap()
            .refuse
            .entry(path.to_owned())
            .or_default()
            .push_back((status, body));
    }

    /// Each state posted for a delivery, with its reason.
    fn states(&self, delivery: &str) -> Vec<(String, Option<String>)> {
        self.bodies(&format!("/api/adapter/deliveries/{delivery}/state"))
            .iter()
            .map(|b| {
                (
                    b["state"].as_str().unwrap().to_owned(),
                    b["reason"].as_str().map(str::to_owned),
                )
            })
            .collect()
    }

    fn replies(&self) -> Vec<Value> {
        self.bodies("/api/adapter/messages")
    }
}

// The OpenCode double.

#[derive(Default)]
struct FakeSession {
    title: String,
    model: Value,
    permissions: Value,
    log: Vec<Value>,
    running: bool,
    /// The running round's inputs.
    round: Vec<String>,
    queued: VecDeque<(String, String)>,
}

#[derive(Default)]
struct OcState {
    password: String,
    sessions: HashMap<String, FakeSession>,
    /// Every request: (method, path, whether its auth was right, body).
    requests: Vec<(String, String, bool, Value)>,
    pending: Vec<Value>,
    decisions: Vec<(String, Value)>,
    next: u64,
}

fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        * 1000.0
}

impl OcState {
    fn id(&mut self, prefix: &str) -> String {
        self.next += 1;
        format!("{prefix}_{:04}", self.next)
    }

    fn push(&mut self, sid: &str, mut message: Value) {
        message["time"] = json!({"created": now_ms()});
        self.sessions.get_mut(sid).unwrap().log.push(message);
    }

    /// Takes an input in: it starts a round, or joins or waits for the running one.
    fn input(&mut self, sid: &str, text: &str, delivery: &str) -> String {
        let id = self.id("msg");
        let session = self.sessions.get_mut(sid).unwrap();
        if session.running && delivery == "queue" {
            session.queued.push_back((id.clone(), text.to_owned()));
            return id;
        }
        let starts = !session.running;
        session.running = true;
        session.round.push(text.to_owned());
        self.push(sid, json!({"id": id, "type": "user", "text": text}));
        if starts && !text.contains("HOLD") {
            self.finish(sid);
        }
        id
    }

    /// The running round stops with its echo; a queued input then starts the next round.
    fn finish(&mut self, sid: &str) {
        let id = self.id("msg");
        let session = self.sessions.get_mut(sid).unwrap();
        let text = format!("echo: {}", session.round.join(" | "));
        session.round.clear();
        self.push(
            sid,
            json!({"id": id, "type": "assistant", "finish": "stop",
                   "content": [{"type": "reasoning", "text": "..."}, {"type": "text", "text": text}]}),
        );
        let next = self.sessions.get_mut(sid).unwrap().queued.pop_front();
        match next {
            Some((input, text)) => {
                let session = self.sessions.get_mut(sid).unwrap();
                session.round.push(text.clone());
                self.push(sid, json!({"id": input, "type": "user", "text": text}));
                if !text.contains("HOLD") {
                    self.finish(sid);
                }
            }
            None => {
                let idle = self.id("msg");
                self.push(
                    sid,
                    json!({"id": idle, "type": "idle", "outcome": "succeeded"}),
                );
                self.sessions.get_mut(sid).unwrap().running = false;
            }
        }
    }

    fn interrupt(&mut self, sid: &str) -> bool {
        if !self.sessions[sid].running {
            return false;
        }
        let (a, i) = (self.id("msg"), self.id("msg"));
        self.push(
            sid,
            json!({"id": a, "type": "assistant", "finish": "error", "content": []}),
        );
        self.push(
            sid,
            json!({"id": i, "type": "idle", "outcome": "interrupted"}),
        );
        let session = self.sessions.get_mut(sid).unwrap();
        session.running = false;
        session.round.clear();
        true
    }

    fn handle(&mut self, request: &Request) -> (u16, Value) {
        let authorized = request.auth
            == format!(
                "Basic {}",
                base64(format!("opencode:{}", self.password).as_bytes())
            );
        self.requests.push((
            request.method.clone(),
            request.path.clone(),
            authorized,
            request.body.clone(),
        ));
        if !authorized {
            return (
                401,
                json!({"_tag": "UnauthorizedError", "message": "Unauthorized"}),
            );
        }
        let (path, query) = request.path.split_once('?').unwrap_or((&request.path, ""));
        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        let missing = |sid: &str| {
            (
                404,
                json!({"_tag": "SessionNotFoundError", "message": format!("Session not found: {sid}")}),
            )
        };
        match (request.method.as_str(), parts.as_slice()) {
            ("GET", ["api", "info"]) => (200, json!({"version": "2.3.0", "pid": 1})),
            ("GET", ["api", "session", "active"]) => {
                let data: serde_json::Map<String, Value> = self
                    .sessions
                    .iter()
                    .filter(|(_, s)| s.running)
                    .map(|(id, _)| (id.clone(), json!({"type": "running"})))
                    .collect();
                (200, json!({"data": data}))
            }
            ("POST", ["api", "session"]) => {
                let id = self.id("ses");
                let body = &request.body;
                self.sessions.insert(
                    id.clone(),
                    FakeSession {
                        title: body["title"].as_str().unwrap_or_default().to_owned(),
                        model: body["model"].clone(),
                        permissions: body["permissions"].clone(),
                        ..FakeSession::default()
                    },
                );
                (200, json!({"data": {"id": id}}))
            }
            ("GET", ["api", "session", sid]) => match self.sessions.get(*sid) {
                Some(s) => (
                    200,
                    json!({"data": {"id": sid, "title": s.title, "model": s.model, "permissions": s.permissions}}),
                ),
                None => missing(sid),
            },
            ("PATCH", ["api", "session", sid]) => match self.sessions.get_mut(*sid) {
                Some(s) => {
                    s.permissions = request.body["permissions"].clone();
                    (200, json!({"data": {"id": sid}}))
                }
                None => missing(sid),
            },
            ("POST", ["api", "session", sid, "model"]) => match self.sessions.get_mut(*sid) {
                Some(s) => {
                    s.model = request.body["model"].clone();
                    (204, Value::Null)
                }
                None => missing(sid),
            },
            ("POST", ["api", "session", sid, "prompt"]) => {
                if !self.sessions.contains_key(*sid) {
                    return missing(sid);
                }
                let text = request.body["text"].as_str().unwrap_or_default().to_owned();
                let delivery = request.body["delivery"]
                    .as_str()
                    .unwrap_or("steer")
                    .to_owned();
                let id = self.input(sid, &text, &delivery);
                (
                    200,
                    json!({"data": {"id": id, "sessionID": sid, "type": "user", "delivery": delivery}}),
                )
            }
            ("POST", ["api", "session", sid, "interrupt"]) => {
                (200, json!({"interrupted": self.interrupt(sid)}))
            }
            ("GET", ["api", "session", sid, "message"]) => {
                let Some(session) = self.sessions.get(*sid) else {
                    return missing(sid);
                };
                let params: HashMap<&str, &str> =
                    query.split('&').filter_map(|p| p.split_once('=')).collect();
                let limit: usize = params
                    .get("limit")
                    .and_then(|l| l.parse().ok())
                    .unwrap_or(100);
                let start: usize = params
                    .get("cursor")
                    .and_then(|c| c.strip_prefix('c'))
                    .and_then(|c| c.parse().ok())
                    .unwrap_or(0);
                let newest_first: Vec<&Value> = session.log.iter().rev().collect();
                let page: Vec<Value> = newest_first
                    .iter()
                    .skip(start)
                    .take(limit)
                    .map(|m| (*m).clone())
                    .collect();
                let next =
                    (start + limit < newest_first.len()).then(|| format!("c{}", start + limit));
                (
                    200,
                    json!({"data": page, "cursor": {"previous": null, "next": next}}),
                )
            }
            ("GET", ["api", "session", sid, "permission"]) => {
                if !self.sessions.contains_key(*sid) {
                    return missing(sid);
                }
                let data: Vec<Value> = self
                    .pending
                    .iter()
                    .filter(|p| p["sessionID"] == *sid)
                    .cloned()
                    .collect();
                (200, json!({"data": data}))
            }
            ("POST", ["api", "session", _, "permission", rid, "reply"]) => {
                let before = self.pending.len();
                self.pending.retain(|p| p["id"] != *rid);
                if self.pending.len() == before {
                    return (404, json!({"message": "no such request"}));
                }
                self.decisions
                    .push(((*rid).to_owned(), request.body.clone()));
                (204, Value::Null)
            }
            _ => (
                404,
                json!({"message": format!("no route {}", request.path)}),
            ),
        }
    }
}

struct FakeOpenCode {
    state: Arc<Mutex<OcState>>,
    closed: Arc<AtomicBool>,
    url: String,
}

impl FakeOpenCode {
    fn start(password: &str) -> Self {
        let state = Arc::new(Mutex::new(OcState {
            password: password.to_owned(),
            ..OcState::default()
        }));
        Self::on(state)
    }

    /// Another launch of the app over the same sessions: a new port and a new password.
    fn relaunch(&self, password: &str) -> Self {
        self.closed.store(true, Ordering::SeqCst);
        self.state.lock().unwrap().password = password.to_owned();
        Self::on(Arc::clone(&self.state))
    }

    fn on(state: Arc<Mutex<OcState>>) -> Self {
        let shared = Arc::clone(&state);
        let (url, closed) = serve(move |request| shared.lock().unwrap().handle(request));
        Self { state, closed, url }
    }

    fn st(&self) -> std::sync::MutexGuard<'_, OcState> {
        self.state.lock().unwrap()
    }

    fn calls(&self, method: &str, suffix: &str) -> Vec<Value> {
        self.st()
            .requests
            .iter()
            .filter(|(m, p, ok, _)| {
                *ok && m == method && p.split('?').next().unwrap().ends_with(suffix)
            })
            .map(|(_, _, _, b)| b.clone())
            .collect()
    }
}

// The adapter under test.

static DIRS: AtomicU64 = AtomicU64::new(0);

struct Setup {
    shared: Shared,
    dir: PathBuf,
}

impl Drop for Setup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn write_service(dir: &Path, url: &str, password: &str) {
    std::fs::write(
        dir.join("state-service.json"),
        json!({"id": "x", "version": "2.3.0", "url": url, "pid": 1, "password": password})
            .to_string(),
    )
    .unwrap();
}

fn setup(broker: &Broker, opencode: &FakeOpenCode, password: &str) -> Setup {
    let dir = std::env::temp_dir().join(format!(
        "duplex-oc-fake-{}-{}",
        std::process::id(),
        DIRS.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    write_service(&dir, &opencode.url, password);
    let shared = Shared {
        broker: BrokerClient::new(&broker.url, Duration::from_secs(5)),
        enrollment_token: "enroll".to_owned(),
        opencode: OpenCode::new(
            Discovery {
                state_file: dir.join("state-service.json"),
                password_file: dir.join("config-service.json"),
            },
            Duration::from_secs(5),
        ),
        sessions: SessionMap::new(dir.join("opencode-sessions.json")),
        token_dir: None,
        log: Log::new(Some(dir.join("opencode.log"))),
        poll: Duration::from_millis(10),
        stop_poll: Duration::from_millis(10),
        turn_timeout: Duration::from_secs(600),
    };
    Setup { shared, dir }
}

fn spec() -> SessionSpec {
    SessionSpec {
        title: "DeepSeek".to_owned(),
        cwd: "C:\\work".to_owned(),
        provider_id: "opencode".to_owned(),
        model_id: "deepseek-v4.1-flash-free".to_owned(),
    }
}

struct World {
    broker: Broker,
    opencode: FakeOpenCode,
    setup: Setup,
    session: Managed,
}

impl World {
    fn new() -> Self {
        let broker = Broker::start();
        let opencode = FakeOpenCode::start(PASSWORD);
        let setup = setup(&broker, &opencode, PASSWORD);
        let mut world = Self {
            broker,
            opencode,
            setup,
            session: Managed::new(spec()),
        };
        assert!(!world.step(), "nothing to claim yet");
        world
    }

    fn step(&mut self) -> bool {
        self.setup.shared.step(&mut self.session).expect("a step")
    }

    fn sid(&self) -> String {
        self.session.session_id.clone()
    }

    /// Claims `delivery` in one step.
    fn claim(&mut self, delivery: Value) {
        self.broker.will_claim(delivery);
        assert!(self.step(), "the delivery was claimed");
    }

    fn finish(&self) {
        let sid = self.sid();
        self.opencode.st().finish(&sid);
    }
}

fn delivery(id: &str, conversation: &str, mode: &str, text: &str) -> Value {
    json!({
        "delivery": {"id": id},
        "message": {"id": format!("m-{id}"), "mode": mode, "text": text, "actor": {"kind": "user", "id": "owner"}},
        "conversation": {"id": conversation, "kind": "direct"},
    })
}

fn prompts(world: &World) -> Vec<(String, String)> {
    world
        .opencode
        .calls("POST", "/prompt")
        .iter()
        .map(|b| {
            (
                b["text"].as_str().unwrap().to_owned(),
                b["delivery"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn responded(world: &World, delivery: &str) -> bool {
    world.broker.states(delivery).last().map(|s| s.0.as_str()) == Some("responded")
}

const SUPPORTS: &str = "(supports: auto, queue, steer, restart)";

#[test]
fn registration_declares_the_four_modes_and_no_tasks_and_the_session_runs_every_tool() {
    let world = World::new();
    let registered = &world.broker.bodies("/api/adapter/register")[0];
    assert_eq!(registered["provider"], "opencode");
    assert_eq!(registered["native_id"], json!(world.sid()));
    assert_eq!(registered["title"], "DeepSeek");
    assert_eq!(
        registered["capabilities"]["send_modes"],
        json!(["auto", "queue", "steer", "restart"])
    );
    assert_eq!(registered["capabilities"]["turn_state"], json!(true));
    assert!(registered["capabilities"].get("tasks").is_none());
    assert!(registered["capabilities"].get("limits").is_none());
    assert_eq!(registered["adapter"]["runtime"]["opencode"], "2.3.0");
    let created = &world.opencode.calls("POST", "/api/session")[0];
    assert_eq!(
        created["model"],
        json!({"providerID": "opencode", "id": "deepseek-v4.1-flash-free"})
    );
    assert_eq!(created["location"]["directory"], "C:\\work");
    assert_eq!(
        created["permissions"],
        json!([{"action": "*", "resource": "*", "effect": "allow"},
               {"action": "question", "resource": "*", "effect": "deny"}])
    );
    // The reading after registration is the first of its lease.
    assert_eq!(world.broker.bodies("/api/adapter/turn")[0]["seq"], 1);
}

#[test]
fn a_session_kept_from_an_earlier_run_is_held_again_with_its_model_and_rules_restored() {
    let mut world = World::new();
    let sid = world.sid();
    {
        let mut state = world.opencode.st();
        let session = state.sessions.get_mut(&sid).unwrap();
        session.model = json!({"providerID": "opencode", "id": "other"});
        session.permissions = json!([]);
    }
    // A new process: it finds the id in the session map, not by creating a session.
    world.session = Managed::new(spec());
    assert!(!world.step());
    assert_eq!(world.sid(), sid);
    assert_eq!(world.opencode.calls("POST", "/api/session").len(), 1);
    let state = world.opencode.st();
    assert_eq!(state.sessions[&sid].model["id"], "deepseek-v4.1-flash-free");
    assert_eq!(state.sessions[&sid].permissions[0]["effect"], "allow");
}

#[test]
fn auto_starts_an_idle_session_and_its_round_answers_under_the_delivery_s_reply_id() {
    let mut world = World::new();
    world.claim(delivery("d1", "c1", "auto", "hello"));
    assert_eq!(
        prompts(&world),
        vec![("hello".to_owned(), "steer".to_owned())]
    );
    let accepted = &world.broker.bodies("/api/adapter/deliveries/d1/state")[0];
    assert_eq!(accepted["state"], "accepted");
    assert!(
        accepted["native_turn_id"]
            .as_str()
            .unwrap()
            .starts_with("msg_")
    );
    assert!(!world.step());
    let replies = world.broker.replies();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0]["text"], "echo: hello");
    assert_eq!(replies[0]["client_message_id"], "reply-d1");
    assert_eq!(replies[0]["parent_id"], "m-d1");
    assert_eq!(replies[0]["conversation_id"], "c1");
    assert!(responded(&world, "d1"));
    assert_eq!(world.session.waiting(), 0);
}

#[test]
fn auto_and_steer_join_the_running_turn_and_each_conversation_gets_one_reply() {
    let mut world = World::new();
    world.claim(delivery("d1", "c1", "auto", "HOLD a"));
    world.claim(delivery("d2", "c1", "steer", "b"));
    world.claim(delivery("d3", "c2", "auto", "c"));
    assert_eq!(
        prompts(&world),
        vec![
            ("HOLD a".to_owned(), "steer".to_owned()),
            ("b".to_owned(), "steer".to_owned()),
            ("c".to_owned(), "steer".to_owned()),
        ]
    );
    assert!(!world.step());
    assert!(world.broker.replies().is_empty(), "the round still runs");
    world.finish();
    assert!(!world.step());
    let replies = world.broker.replies();
    let ids: Vec<&str> = replies
        .iter()
        .map(|r| r["client_message_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["reply-d1", "reply-d3"], "one reply per conversation");
    assert_eq!(replies[0]["text"], "echo: HOLD a | b | c");
    for d in ["d1", "d2", "d3"] {
        assert!(responded(&world, d), "{d}");
    }
}

#[test]
fn queue_waits_for_the_running_round_and_gets_a_reply_of_its_own() {
    let mut world = World::new();
    world.claim(delivery("d1", "c1", "auto", "HOLD first"));
    world.claim(delivery("d2", "c1", "queue", "second"));
    assert_eq!(
        prompts(&world)[1],
        ("second".to_owned(), "queue".to_owned())
    );
    world.finish();
    assert!(!world.step());
    let texts: Vec<(String, String)> = world
        .broker
        .replies()
        .iter()
        .map(|r| {
            (
                r["client_message_id"].as_str().unwrap().to_owned(),
                r["text"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        texts,
        vec![
            ("reply-d1".to_owned(), "echo: HOLD first".to_owned()),
            ("reply-d2".to_owned(), "echo: second".to_owned()),
        ]
    );
    assert!(responded(&world, "d1") && responded(&world, "d2"));
}

#[test]
fn steer_to_an_idle_session_fails_and_puts_nothing_in() {
    let mut world = World::new();
    world.claim(delivery("d1", "c1", "steer", "late"));
    assert!(prompts(&world).is_empty());
    let states = world.broker.states("d1");
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].0, "failed");
    let reason = states[0].1.clone().unwrap();
    assert!(
        reason.contains("no") && reason.contains(SUPPORTS),
        "{reason}"
    );
}

#[test]
fn an_unknown_mode_fails_naming_the_supported_ones() {
    let mut world = World::new();
    world.claim(delivery("d1", "c1", "shout", "x"));
    assert!(prompts(&world).is_empty());
    let states = world.broker.states("d1");
    assert_eq!(
        states,
        vec![(
            "failed".to_owned(),
            Some(format!("mode shout is not supported {SUPPORTS}"))
        )]
    );
}

#[test]
fn restart_interrupts_the_running_turn_before_it_starts_another() {
    let mut world = World::new();
    world.claim(delivery("d1", "c1", "auto", "HOLD long"));
    world.claim(delivery("d2", "c1", "restart", "instead"));
    let order: Vec<String> = world
        .opencode
        .st()
        .requests
        .iter()
        .filter(|(m, p, _, _)| m == "POST" && (p.contains("/interrupt") || p.contains("/prompt")))
        .map(|(_, p, _, _)| {
            p.split('?')
                .next()
                .unwrap()
                .rsplit('/')
                .next()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(order, ["prompt", "interrupt", "prompt"]);
    assert!(!world.step());
    assert_eq!(
        world.broker.states("d1").last().cloned(),
        Some((
            "failed".to_owned(),
            Some("the turn ended interrupted".to_owned())
        ))
    );
    assert!(responded(&world, "d2"));
    assert_eq!(world.broker.replies()[0]["text"], "echo: instead");
}

#[test]
fn a_peer_message_is_refused_without_an_input_and_a_broker_notice_becomes_one() {
    let mut world = World::new();
    let mut peer = delivery("d1", "c1", "auto", "do this for me");
    peer["message"]["actor"] = json!({"kind": "binding", "id": "b9"});
    world.claim(peer);
    assert!(prompts(&world).is_empty());
    assert_eq!(
        world.broker.states("d1"),
        vec![(
            "failed".to_owned(),
            Some("peer messages need the bridge tool path".to_owned())
        )]
    );
    let mut notice = delivery("d2", "c1", "auto", "Continue where you left off.");
    notice["message"]["actor"] = json!({"kind": "binding", "id": "broker"});
    world.claim(notice);
    assert_eq!(prompts(&world).len(), 1);
}

#[test]
fn every_claim_carries_the_turn_read_just_before_it_numbered_from_one_after_each_registration() {
    let mut world = World::new();
    world.claim(delivery("d1", "c1", "auto", "HOLD a"));
    assert!(!world.step());
    world.finish();
    assert!(!world.step());
    // Every reading the broker got, claims and turn reports, in the order sent.
    let readings: Vec<(String, u64)> = {
        let state = world.broker.seen.lock().unwrap();
        state
            .requests
            .iter()
            .filter_map(|(p, b)| match p.as_str() {
                "/api/adapter/claim" => Some(&b["turn"]),
                "/api/adapter/turn" => Some(b),
                _ => None,
            })
            .map(|t| {
                (
                    t["state"].as_str().unwrap().to_owned(),
                    t["seq"].as_u64().unwrap(),
                )
            })
            .collect()
    };
    let seqs: Vec<u64> = readings.iter().map(|r| r.1).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    let states: Vec<&str> = readings.iter().map(|r| r.0.as_str()).collect();
    // registration, first claim, claim of d1, the report after it, the claim while it ran, the
    // claim after it stopped.
    assert_eq!(states, ["idle", "idle", "idle", "busy", "busy", "idle"]);
    assert!(
        world
            .broker
            .bodies("/api/adapter/claim")
            .iter()
            .all(|b| b["turn"]["waiting_for"].is_null())
    );
    // A refused token registers again, and the count starts over.
    world
        .broker
        .will_answer("/api/adapter/claim", 401, json!({"detail": "bad token"}));
    assert!(world.setup.shared.step(&mut world.session).is_err());
    assert!(!world.step());
    assert_eq!(world.broker.bodies("/api/adapter/register").len(), 2);
    let turns = world.broker.bodies("/api/adapter/turn");
    assert_eq!(turns.last().unwrap()["seq"], 1);
    assert_eq!(
        world.broker.bodies("/api/adapter/claim").last().unwrap()["turn"]["seq"],
        2
    );
}

#[test]
fn a_permission_request_is_allowed_once_without_waiting_for_anyone() {
    let mut world = World::new();
    let sid = world.sid();
    world.opencode.st().pending.push(json!({
        "id": "per_1", "sessionID": sid, "action": "read", "resources": ["x.env"],
    }));
    world.opencode.st().pending.push(json!({
        "id": "per_2", "sessionID": "ses_other", "action": "read", "resources": ["y"],
    }));
    assert!(!world.step());
    let state = world.opencode.st();
    assert_eq!(
        state.decisions,
        vec![("per_1".to_owned(), json!({"decision": "once"}))]
    );
    // Another session's request is not this adapter's to answer.
    assert_eq!(state.pending.len(), 1);
}

#[test]
fn a_session_deleted_while_the_app_was_closed_is_created_again_as_a_new_binding() {
    let mut world = World::new();
    let first = world.sid();
    let relaunched = world.opencode.relaunch(PASSWORD);
    assert!(world.setup.shared.step(&mut world.session).is_err());
    relaunched.st().sessions.clear();
    write_service(&world.setup.dir, &relaunched.url, PASSWORD);
    world.opencode = relaunched;
    world.claim(delivery("d1", "c1", "auto", "are you there"));
    assert!(!world.step());
    assert_ne!(world.sid(), first);
    let registered = world.broker.bodies("/api/adapter/register");
    assert_eq!(registered.len(), 2);
    assert_eq!(registered[1]["native_id"], json!(world.sid()));
    assert!(responded(&world, "d1"));
}

#[test]
fn a_relaunched_app_is_found_at_its_new_port_with_its_new_password() {
    let mut world = World::new();
    let sid = world.sid();
    let relaunched = world.opencode.relaunch("oc-pass-second-Kk29");
    // The app is closed: nothing is claimed, and the broker hears the turn is unknown.
    let claims = world.broker.bodies("/api/adapter/claim").len();
    assert!(world.setup.shared.step(&mut world.session).is_err());
    assert!(world.setup.shared.step(&mut world.session).is_err());
    assert_eq!(world.broker.bodies("/api/adapter/claim").len(), claims);
    let turns = world.broker.bodies("/api/adapter/turn");
    assert_eq!(turns.last().unwrap()["state"], "unknown");
    // It opens again on another port, and writes its new address and password.
    write_service(&world.setup.dir, &relaunched.url, "oc-pass-second-Kk29");
    world.opencode = relaunched;
    world.claim(delivery("d1", "c1", "auto", "after the relaunch"));
    assert!(!world.step());
    assert_eq!(world.sid(), sid, "the same session, held again");
    assert!(responded(&world, "d1"));
    let unauthorized = world.opencode.st().requests.iter().filter(|r| !r.2).count();
    assert_eq!(
        unauthorized, 0,
        "every call to the new launch carried its password"
    );
}

#[test]
fn the_password_reaches_only_the_server_never_the_log_the_broker_or_an_error() {
    let mut world = World::new();
    world.claim(delivery("d1", "c1", "auto", "hello"));
    assert!(!world.step());
    // A wrong password and a closed app are logged as what they are.
    world.opencode.st().password = "changed-elsewhere".to_owned();
    let err = world.setup.shared.step(&mut world.session).unwrap_err();
    world.setup.shared.log.warn(&err);
    let closed = world.opencode.relaunch("unused");
    closed.closed.store(true, Ordering::SeqCst);
    let err = world.setup.shared.step(&mut world.session).unwrap_err();
    world.setup.shared.log.warn(&err);
    let discovery = Discovery {
        state_file: world.setup.dir.join("state-service.json"),
        password_file: world.setup.dir.join("config-service.json"),
    };
    world
        .setup
        .shared
        .log
        .info(format!("{:?}", discovery.read()));
    let log = std::fs::read_to_string(world.setup.dir.join("opencode.log")).unwrap();
    assert!(
        log.contains("refused the password") && log.contains("not running"),
        "{log}"
    );
    let encoded = base64(format!("opencode:{PASSWORD}").as_bytes());
    assert!(!log.contains(PASSWORD) && !log.contains(&encoded), "{log}");
    let broker = world.broker.seen.lock().unwrap();
    for (_, body) in &broker.requests {
        let text = body.to_string();
        assert!(
            !text.contains(PASSWORD) && !text.contains(&encoded),
            "{text}"
        );
    }
}

#[test]
fn a_reply_the_broker_refuses_fails_the_delivery_and_tells_the_sender() {
    let mut world = World::new();
    world.broker.will_answer(
        "/api/adapter/messages",
        422,
        json!({"detail": "text is longer than 65536 bytes"}),
    );
    world.claim(delivery("d1", "c1", "auto", "hello"));
    assert!(!world.step());
    assert_eq!(
        world.broker.states("d1").last().cloned(),
        Some((
            "failed".to_owned(),
            Some("the reply was refused: text is longer than 65536 bytes".to_owned())
        ))
    );
}

#[test]
fn a_lost_accepted_answer_is_sent_again_and_the_reply_waits_for_it() {
    let mut world = World::new();
    world.broker.will_answer(
        "/api/adapter/deliveries/d1/state",
        503,
        json!({"detail": "busy"}),
    );
    world
        .broker
        .will_claim(delivery("d1", "c1", "auto", "hello"));
    assert!(world.setup.shared.step(&mut world.session).is_err());
    assert_eq!(
        world.session.held(),
        ["d1"],
        "held before the broker heard of it"
    );
    assert!(world.broker.replies().is_empty());
    assert!(!world.step());
    let states: Vec<String> = world.broker.states("d1").into_iter().map(|s| s.0).collect();
    assert_eq!(states, ["accepted", "accepted", "responded"]);
    // The reply went only after the broker had the delivery as accepted.
    let paths = world.broker.paths();
    let reply = paths
        .iter()
        .position(|p| p == "/api/adapter/messages")
        .unwrap();
    let before = paths[..reply]
        .iter()
        .filter(|p| *p == "/api/adapter/deliveries/d1/state")
        .count();
    assert_eq!(before, 2);
}

#[test]
fn reconcile_fails_what_no_round_holds_and_acknowledges_what_one_does() {
    let mut world = World::new();
    world.claim(delivery("d1", "c1", "auto", "HOLD running"));
    world.broker.seen.lock().unwrap().in_flight = json!([
        {"id": "d1", "state": "dispatching", "native_turn_id": null},
        {"id": "d7", "state": "accepted", "native_turn_id": "msg_x"},
    ]);
    // The broker was lost and found again: the next step squares its view first.
    world
        .broker
        .will_answer("/api/adapter/claim", 503, json!({"detail": "restarting"}));
    assert!(world.setup.shared.step(&mut world.session).is_err());
    assert!(!world.step());
    assert_eq!(
        world.broker.states("d7"),
        vec![(
            "failed".to_owned(),
            Some("the adapter no longer holds this turn".to_owned())
        )]
    );
    let d1: Vec<String> = world.broker.states("d1").into_iter().map(|s| s.0).collect();
    assert_eq!(d1, ["accepted", "accepted"]);
    world.finish();
    assert!(!world.step());
    assert!(responded(&world, "d1"));
}
