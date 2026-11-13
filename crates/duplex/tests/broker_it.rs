//! The adapter against the real broker app from this tree (`duplex_broker/src`), served by
//! uvicorn on a local port. Needs a Python with the broker's runtime dependencies (fastapi,
//! uvicorn, websockets), named by `DUPLEX_BROKER_PYTHON`; run with
//! `cargo nextest run --run-ignored only`.

use duplex_adapter::adapter::Adapter;
use duplex_adapter::config::AdapterConfig;
use duplex_adapter::identity::{CodexTask, Participant, SessionIdentity};
use duplex_adapter::watch::{Event, Limit, Source, Turn, TurnState};
use serde_json::{Value, json};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const ENROLL: &str = "it-enroll-token-91c2";
const PAIRING: &str = "it-pairing-5d0a";

const SERVE: &str = r#"
import os, sys, uvicorn
from duplex_broker.api import Settings, create_app
from duplex_broker.store import Store
store = Store(sys.argv[1], owner="owner")
settings = Settings(enrollment_token=os.environ["IT_ENROLL"], pairing_code=os.environ["IT_PAIRING"],
                    owner_name="Sam", collaboration_sweep=0.2)
uvicorn.run(create_app(store, settings), host="127.0.0.1", port=int(sys.argv[2]), log_level="warning")
"#;

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("duplex-it-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

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

fn serve(dir: &Path) -> Served {
    let python = std::env::var("DUPLEX_BROKER_PYTHON")
        .expect("DUPLEX_BROKER_PYTHON names a Python with the broker's dependencies");
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("duplex_broker")
        .join("src");
    assert!(src.join("duplex_broker").is_dir(), "{}", src.display());
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let script = dir.join("serve.py");
    std::fs::write(&script, SERVE).unwrap();
    let child = Command::new(python)
        .arg(&script)
        .arg(dir.join("broker.db"))
        .arg(port.to_string())
        .env("PYTHONPATH", &src)
        .env("IT_ENROLL", ENROLL)
        .env("IT_PAIRING", PAIRING)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let served = Served {
        child,
        url: format!("http://127.0.0.1:{port}"),
    };
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

fn call(method: &str, url: &str, bearer: &str, body: Option<Value>) -> Value {
    let auth = format!("Bearer {bearer}");
    let answer = match (method, body) {
        ("GET", _) => ureq::get(url).header("authorization", &auth).call(),
        (_, Some(body)) => ureq::post(url)
            .header("authorization", &auth)
            .send_json(&body),
        (_, None) => ureq::post(url).header("authorization", &auth).send_empty(),
    };
    let mut answer = answer.unwrap_or_else(|e| panic!("{method} {url}: {e}"));
    let text = answer.body_mut().read_to_string().unwrap();
    if text.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&text).unwrap()
    }
}

/// A scripted session reader.
struct Scripted {
    turn: Arc<Mutex<Turn>>,
    events: Arc<Mutex<Vec<Event>>>,
}

impl Source for Scripted {
    fn sample(&mut self) -> (Turn, Vec<Event>) {
        (
            *self.turn.lock().unwrap(),
            std::mem::take(&mut *self.events.lock().unwrap()),
        )
    }
    fn peek_turn(&mut self) -> Turn {
        *self.turn.lock().unwrap()
    }
}

struct Harness {
    _scratch: Scratch,
    served: Served,
    owner: String,
    adapter: Adapter,
    woken: Arc<Mutex<Vec<String>>>,
    turn: Arc<Mutex<Turn>>,
    events: Arc<Mutex<Vec<Event>>>,
}

fn harness(name: &str, participant: Participant) -> Harness {
    let scratch = Scratch::new(name);
    let served = serve(&scratch.0);
    let owner = call(
        "POST",
        &format!("{}/api/device/enroll", served.url),
        "",
        Some(json!({"pairing_code": PAIRING, "device_name": "it"})),
    )["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let config = AdapterConfig {
        broker_url: served.url.clone(),
        enrollment_token: ENROLL.to_owned(),
        poll_seconds: 0.1,
        token_dir: Some(scratch.0.join("bindings")),
    };
    let (turn, events) = (
        Arc::new(Mutex::new(Turn::of(TurnState::Idle))),
        Arc::new(Mutex::new(Vec::new())),
    );
    let woken = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&woken);
    let source = Box::new(Scripted {
        turn: Arc::clone(&turn),
        events: Arc::clone(&events),
    });
    let adapter = Adapter::with_parts(
        config,
        participant,
        scratch.0.clone(),
        source,
        Box::new(move |text| {
            sink.lock().unwrap().push(text.to_owned());
            Ok(())
        }),
    );
    Harness {
        _scratch: scratch,
        served,
        owner,
        adapter,
        woken,
        turn,
        events,
    }
}

fn claude() -> Participant {
    Participant::Claude(
        SessionIdentity::from_env(&std::collections::HashMap::from([
            (
                "CLAUDE_CODE_SESSION_ID".to_owned(),
                "5a1d0c7e-2b4f-4e8a-9c3d-7f6e5d4c3b2a".to_owned(),
            ),
            (
                "CLAUDE_CODE_MESSAGING_SOCKET".to_owned(),
                "unused".to_owned(),
            ),
            (
                "CLAUDE_CODE_MESSAGING_TOKEN".to_owned(),
                "unused".to_owned(),
            ),
        ]))
        .unwrap(),
    )
}

impl Harness {
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.served.url)
    }

    /// The owner opens a direct conversation with the session and says `text` in it.
    fn say(&self, text: &str) -> (String, String) {
        let binding = self.adapter.binding_id();
        let dm = call(
            "POST",
            &self.url("/api/direct-conversations"),
            &self.owner,
            Some(json!({"binding_id": binding})),
        );
        let id = dm["id"].as_str().unwrap().to_owned();
        let sent = call(
            "POST",
            &self.url(&format!("/api/conversations/{id}/messages")),
            &self.owner,
            Some(json!({"client_message_id": "c-1", "text": text})),
        );
        (id, sent["message"]["id"].as_str().unwrap().to_owned())
    }

    fn deliveries(&self, conversation: &str) -> Vec<String> {
        call(
            "GET",
            &self.url(&format!("/api/conversations/{conversation}/events")),
            &self.owner,
            None,
        )
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["type"] == "delivery.changed")
        .map(|e| e["payload"]["state"].as_str().unwrap().to_owned())
        .collect()
    }
}

#[test]
#[ignore = "needs the real broker: DUPLEX_BROKER_PYTHON"]
fn a_claude_session_is_woken_reads_its_message_and_replies() {
    let h = harness("claude", claude());
    h.adapter.register().expect("registers");
    assert!(h.adapter.is_registered());
    assert_eq!(
        h.adapter.owner_name(),
        "Sam",
        "the broker names its owner at registration"
    );
    let (dm, stored_id) = h.say("what is 2+2");
    // Adapters see IDs hyphenated, so a redactor leaves them alone.
    let message_id = duplex_proto::ids::reply_id(&stored_id);

    assert!(h.adapter.poll_once().unwrap(), "a delivery was claimed");
    let woken = h.woken.lock().unwrap().clone();
    assert_eq!(woken.len(), 1);
    assert!(woken[0].starts_with("[duplex-chat] You've got mail for session 5a1d0c7e-2b4f-4e8a-9c3d-7f6e5d4c3b2a: message "), "{}", woken[0]);
    assert!(
        woken[0].contains(&message_id) && woken[0].contains("direct conversation"),
        "{}",
        woken[0]
    );
    assert!(
        !woken[0].contains("2+2"),
        "the text never goes in the wake line"
    );
    assert_eq!(
        h.deliveries(&dm).last().unwrap(),
        "dispatching",
        "a Claude wake proves nothing until the read"
    );
    assert!(!h.adapter.poll_once().unwrap(), "nothing else waits");

    let entries = h.adapter.receive(None, None).expect("receives");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["text"], "what is 2+2");
    assert_eq!(entries[0]["message_id"], message_id);
    assert_eq!(entries[0]["from"]["kind"], "user");
    let rules = entries[0]["policy"]["rules"].as_str().unwrap();
    assert!(
        rules.starts_with("A direct conversation with Sam:"),
        "{rules}"
    );
    assert_eq!(h.deliveries(&dm).last().unwrap(), "consumed");
    assert!(h.adapter.receive(None, None).unwrap().is_empty());

    let sent = h
        .adapter
        .send(&dm, "four", Some(&message_id), None, None)
        .expect("sends");
    assert_eq!(sent["answered"], true);
    assert_eq!(h.deliveries(&dm).last().unwrap(), "responded");
    let events = call(
        "GET",
        &h.url(&format!("/api/conversations/{dm}/events")),
        &h.owner,
        None,
    );
    assert!(
        events
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == "message.accepted" && e["payload"]["text"] == "four")
    );

    let receipt = h
        .adapter
        .message(sent["message_id"].as_str().unwrap())
        .expect("a receipt");
    assert_eq!(receipt["message"]["text"], "four");
    let (history, more) = h.adapter.history_batch(&dm, 0, 50).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(more, None);
    let status = h.adapter.status();
    assert_eq!(status["registered"], true);
    assert_eq!(status["adapter"]["version"], env!("CARGO_PKG_VERSION"));
}

#[test]
#[ignore = "needs the real broker: DUPLEX_BROKER_PYTHON"]
fn a_codex_wake_is_confirmed_and_a_second_registration_keeps_the_binding() {
    let task = CodexTask {
        thread_id: "01a00000-0000-7000-8000-000000000009".into(),
        title_text: "Codex it".into(),
        cwd: "/work".into(),
        codex_argv: vec!["codex".into()],
    };
    let h = harness("codex", Participant::Codex(task));
    h.adapter.register().unwrap();
    let first = h.adapter.binding_id();
    let (dm, _) = h.say("hello");
    assert!(h.adapter.poll_once().unwrap());
    assert_eq!(
        h.deliveries(&dm).last().unwrap(),
        "accepted",
        "codex queue exits 0 once the line is queued"
    );
    // The held token lets the same session register again as the same binding.
    h.adapter.register().unwrap();
    assert_eq!(h.adapter.binding_id(), first);
}

#[test]
#[ignore = "needs the real broker: DUPLEX_BROKER_PYTHON"]
fn a_mode_the_session_never_declared_fails_the_delivery_with_the_reason() {
    let h = harness("mode", claude());
    h.adapter.register().unwrap();
    let binding = h.adapter.binding_id();
    let dm = call(
        "POST",
        &h.url("/api/direct-conversations"),
        &h.owner,
        Some(json!({"binding_id": binding})),
    );
    let id = dm["id"].as_str().unwrap();
    // Idle session: steer has nothing to steer into.
    call(
        "POST",
        &h.url(&format!("/api/conversations/{id}/messages")),
        &h.owner,
        Some(json!({"client_message_id": "c-2", "text": "now", "mode": "steer"})),
    );
    let _ = h.adapter.poll_once();
    let states = h.deliveries(id);
    assert!(
        h.woken.lock().unwrap().is_empty(),
        "nothing woke the session: {states:?}"
    );
    assert_eq!(
        states.last().map(String::as_str),
        Some("failed"),
        "{states:?}"
    );
}

#[test]
#[ignore = "needs the real broker: DUPLEX_BROKER_PYTHON"]
fn the_watch_reports_the_turn_and_a_limit_failure() {
    let h = harness("watch", claude());
    h.adapter.register().unwrap();
    *h.turn.lock().unwrap() = Turn::waiting("permission_prompt");
    h.events.lock().unwrap().push(Event::Limit(Limit {
        run_id: "run-1".into(),
        source: "claude-transcript",
        matched: "You've hit your session limit".into(),
        failed_at: 1_788_560_861.0,
        reset_at: 1_788_570_000.0,
    }));
    h.adapter.watch_once();
    let status = h.adapter.status();
    assert_eq!(status["turn"], "waiting");
    assert_eq!(status["watch_error"], "", "{status}");
    let sessions = call("GET", &h.url("/api/sessions"), &h.owner, None);
    let mine = sessions
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["id"] == h.adapter.binding_id().as_str())
        .expect("listed");
    assert_eq!(mine["turn_state"], "waiting");
}

/// The `duplex mcp` binary on stdio.
struct Mcp {
    child: Child,
    input: std::process::ChildStdin,
    output: std::io::BufReader<std::process::ChildStdout>,
    next: u64,
}

impl Mcp {
    fn start(envs: &[(&str, String)]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_duplex"));
        command
            .arg("mcp")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        // Whatever session runs the tests must not be taken for the one under test.
        for (name, _) in std::env::vars() {
            if name.starts_with("CLAUDE") || name.starts_with("CODEX") || name.starts_with("DUPLEX")
            {
                command.env_remove(name);
            }
        }
        for (k, v) in envs {
            command.env(k, v);
        }
        let mut child = command.spawn().unwrap();
        let input = child.stdin.take().unwrap();
        let output = std::io::BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            input,
            output,
            next: 0,
        }
    }

    fn rpc(&mut self, method: &str, params: Value) -> Value {
        use std::io::{BufRead, Write};
        self.next += 1;
        let line = json!({"jsonrpc": "2.0", "id": self.next, "method": method, "params": params});
        writeln!(self.input, "{line}").unwrap();
        self.input.flush().unwrap();
        let mut answer = String::new();
        self.output.read_line(&mut answer).unwrap();
        let answer: Value =
            serde_json::from_str(&answer).unwrap_or_else(|e| panic!("{e}: {answer:?}"));
        assert_eq!(answer["id"], self.next);
        answer
    }

    /// A tool's answer: whether it was an error, and its text as JSON when it is.
    fn tool(&mut self, name: &str, arguments: Value) -> (bool, Value) {
        let answer = self.rpc("tools/call", json!({"name": name, "arguments": arguments}));
        let result = &answer["result"];
        let text = result["content"][0]["text"].as_str().unwrap().to_owned();
        (
            result["isError"].as_bool().unwrap(),
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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
#[ignore = "needs the real broker: DUPLEX_BROKER_PYTHON"]
fn the_mcp_server_serves_a_codex_task_from_registration_to_reply() {
    let scratch = Scratch::new("mcp");
    let served = serve(&scratch.0);
    let owner = call(
        "POST",
        &format!("{}/api/device/enroll", served.url),
        "",
        Some(json!({"pairing_code": PAIRING, "device_name": "it"})),
    )["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let config = scratch.0.join("adapter.json");
    std::fs::write(
        &config,
        json!({"broker_url": served.url, "enrollment_token": ENROLL, "poll_seconds": 0.1})
            .to_string(),
    )
    .unwrap();
    let home = scratch.0.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let codex = fake_codex(&scratch.0);
    let mut mcp = Mcp::start(&[
        ("DUPLEX_ADAPTER_CONFIG", config.display().to_string()),
        ("DUPLEX_CODEX", codex.display().to_string()),
        ("USERPROFILE", home.display().to_string()),
        ("HOME", home.display().to_string()),
    ]);
    let hello = mcp.rpc("initialize", json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "it", "version": "0"}}));
    assert_eq!(hello["result"]["serverInfo"]["name"], "duplex");
    assert_eq!(
        mcp.rpc("tools/list", json!({}))["result"]["tools"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
    let (inactive, status) = mcp.tool("bridge_status", json!({}));
    assert!(!inactive && status["active"] == false, "{status}");

    let thread = "01a00000-0000-7000-8000-00000000000a";
    let (refused, why) = mcp.tool("bridge_register", json!({"thread_id": "nope"}));
    assert!(
        refused && why.as_str().unwrap().contains("CODEX_THREAD_ID"),
        "{why}"
    );
    let (failed, registered) = mcp.tool(
        "bridge_register",
        json!({"thread_id": thread, "title": "Codex it", "workspace": "/work"}),
    );
    assert!(!failed, "{registered}");
    assert_eq!(registered["registered"], true);
    let binding = registered["binding_id"].as_str().unwrap().to_owned();

    let dm = call(
        "POST",
        &format!("{}/api/direct-conversations", served.url),
        &owner,
        Some(json!({"binding_id": binding})),
    );
    let conversation = dm["id"].as_str().unwrap().to_owned();
    call(
        "POST",
        &format!("{}/api/conversations/{conversation}/messages", served.url),
        &owner,
        Some(json!({"client_message_id": "c-1", "text": "hello mcp"})),
    );

    let woke = codex.parent().unwrap().join("woke.txt");
    let deadline = Instant::now() + Duration::from_secs(20);
    // Registration asked for `--version` first; the wake is the line that queues.
    while !std::fs::read_to_string(&woke).is_ok_and(|text| text.contains("queue")) {
        assert!(Instant::now() < deadline, "codex queue was never run");
        std::thread::sleep(Duration::from_millis(100));
    }
    let args = std::fs::read_to_string(&woke).unwrap();
    assert!(
        args.contains("queue --thread")
            && args.contains(thread)
            && args.contains("[duplex-chat] You've got mail"),
        "{args}"
    );
    assert!(
        args.contains("--resume") && !args.contains("hello mcp"),
        "{args}"
    );

    let (failed, received) = mcp.tool("bridge_receive", json!({}));
    assert!(!failed, "{received}");
    let message = &received["messages"][0];
    assert_eq!(message["text"], "hello mcp");
    assert!(
        received["note"]
            .as_str()
            .unwrap()
            .contains("the user (Sam)")
    );
    let sent = mcp.tool("bridge_send", json!({"conversation_id": message["conversation"]["id"], "text": "hi back", "reply_to": message["message_id"]}));
    assert!(!sent.0, "{}", sent.1);
    assert_eq!(sent.1["answered"], true);
    let events = call(
        "GET",
        &format!("{}/api/conversations/{conversation}/events", served.url),
        &owner,
        None,
    );
    assert!(
        events
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == "message.accepted" && e["payload"]["text"] == "hi back")
    );
    let (_, members) = mcp.tool(
        "bridge_members",
        json!({"conversation_id": message["conversation"]["id"]}),
    );
    assert!(
        members["rules"]
            .as_str()
            .unwrap()
            .starts_with("A direct conversation with Sam:"),
        "{members}"
    );
    let (_, tasks) = mcp.tool("bridge_task_status", json!({}));
    assert_eq!(tasks["total"], 0);
}

/// One `duplex` command in a fresh profile folder: whether it succeeded, stdout and stderr.
fn duplex(args: &[&str], home: &Path, extra: &[(&str, String)]) -> (bool, String, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_duplex"));
    command.args(args).stdin(Stdio::null());
    for (name, _) in std::env::vars() {
        if name.starts_with("CLAUDE") || name.starts_with("CODEX") || name.starts_with("DUPLEX") {
            command.env_remove(name);
        }
    }
    command.env("USERPROFILE", home).env("HOME", home);
    for (k, v) in extra {
        command.env(k, v);
    }
    let done = command.output().unwrap();
    (
        done.status.success(),
        String::from_utf8_lossy(&done.stdout).into_owned(),
        String::from_utf8_lossy(&done.stderr).into_owned(),
    )
}

#[test]
#[ignore = "needs the real broker: DUPLEX_BROKER_PYTHON"]
fn a_fresh_profile_goes_from_nothing_to_a_registered_session_and_the_owner_commands_see_it() {
    let scratch = Scratch::new("setup");
    let served = serve(&scratch.0);
    let home = scratch.0.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let adapter_file = home.join(".duplex-chat").join("adapter.json");

    let (ok, said, why) = duplex(
        &[
            "setup",
            "--broker",
            &served.url,
            "--no-claude",
            "--no-codex",
        ],
        &home,
        &[],
    );
    assert!(
        !ok && why.contains("enrollment token is missing"),
        "{said}{why}"
    );
    let (ok, said, why) = duplex(
        &[
            "setup",
            "--broker",
            &served.url,
            "--no-claude",
            "--no-codex",
        ],
        &home,
        &[("DUPLEX_ENROLLMENT_TOKEN", ENROLL.to_owned())],
    );
    assert!(ok, "{said}{why}");
    assert!(
        said.contains(&format!("broker {}", served.url)) && !said.contains(ENROLL),
        "{said}"
    );
    let written: Value =
        serde_json::from_str(&std::fs::read_to_string(&adapter_file).unwrap()).unwrap();
    assert_eq!(written["enrollment_token"], ENROLL);

    let (ok, said, why) = duplex(&["owner", "list"], &home, &[]);
    assert!(!ok && why.contains("duplex owner enroll"), "{said}{why}");
    let (ok, said, why) = duplex(
        &[
            "owner",
            "enroll",
            "--broker",
            &served.url,
            "--pairing-code",
            "wrong",
        ],
        &home,
        &[],
    );
    assert!(!ok && why.contains("HTTP"), "{said}{why}");
    let (ok, said, why) = duplex(
        &[
            "owner",
            "enroll",
            "--broker",
            &served.url,
            "--pairing-code",
            PAIRING,
            "--name",
            "it",
        ],
        &home,
        &[],
    );
    assert!(ok, "{said}{why}");
    let device =
        std::fs::read_to_string(home.join(".duplex-chat").join("owner-device.json")).unwrap();
    assert!(
        !said.contains(
            &serde_json::from_str::<Value>(&device).unwrap()["token"]
                .as_str()
                .unwrap()
                .to_owned()
        ),
        "the token is never printed"
    );

    // A Codex task joins with the config setup wrote: no environment, no flags.
    let codex = fake_codex(&scratch.0);
    let mut mcp = Mcp::start(&[
        ("USERPROFILE", home.display().to_string()),
        ("HOME", home.display().to_string()),
        ("DUPLEX_CODEX", codex.display().to_string()),
    ]);
    mcp.rpc("initialize", json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "it", "version": "0"}}));
    let (failed, registered) = mcp.tool("bridge_register", json!({"thread_id": "01a00000-0000-7000-8000-00000000000b", "title": "Codex fresh", "workspace": "/work"}));
    assert!(!failed && registered["registered"] == true, "{registered}");

    let owner = |args: &[&str]| {
        let (ok, said, why) = duplex(args, &home, &[]);
        assert!(ok, "{args:?}: {said}{why}");
        said
    };
    let sessions = owner(&["owner", "sessions"]);
    assert!(
        sessions.contains("codex Codex fresh active adapter 0.11.0"),
        "{sessions}"
    );
    assert!(owner(&["owner", "sessions", "--json"]).contains("\"title\": \"Codex fresh\""));
    let group = owner(&["owner", "group", "Review", "Codex fresh"]);
    let detail: Value = serde_json::from_str(&group).unwrap();
    let conversation = detail["id"].as_str().unwrap().to_owned();
    assert!(owner(&["owner", "list"]).contains("Review"));
    let posted = owner(&["owner", "post", &conversation, "please look", "Codex fresh"]);
    assert!(posted.contains('[') && posted.len() > 40, "{posted}");
    assert!(owner(&["owner", "events", &conversation]).contains("please look"));
    assert!(owner(&["owner", "deliveries", &conversation]).contains("Codex fresh please look"));
    assert!(owner(&["owner", "detail", &conversation]).contains("\"title\": \"Review\""));
    assert!(owner(&["owner", "mode", &conversation, "consult"]).contains("consult"));
    assert!(owner(&["owner", "archive", &conversation]).contains(&conversation));
    for quiet in ["refusals", "tasks", "limits", "waker", "subscriptions"] {
        owner(&["owner", quiet]);
    }
    let (ok, _, why) = duplex(&["owner", "retire", "no such session"], &home, &[]);
    assert!(!ok && why.contains("matches 0 active sessions"), "{why}");
    let (ok, _, why) = duplex(&["owner", "bogus"], &home, &[]);
    assert!(!ok && why.contains("duplex owner sessions"), "{why}");
}
