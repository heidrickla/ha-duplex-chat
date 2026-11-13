//! The real `duplex-waker.exe` against a local broker: it reads `waker.json`, lists the
//! broker's wake requests and reports what became of each. The listed request is one the
//! waker refuses before it starts anything (its ids are not plain), so no session is touched.

#![cfg(windows)]

mod common;

use common::{FakeBroker, Scratch};
use serde_json::json;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn the_installed_binary_lists_a_request_and_reports_it() {
    let scratch = Scratch::new("exe");
    let broker = FakeBroker::start();
    broker.list(&[json!({
        "id": "req-1",
        "binding_id": "b1",
        "provider": "claude",
        "native_id": "not a plain id",
        "workspace": scratch.path().to_string_lossy(),
        "reason": "limit_resume",
        "message_id": "m1"
    })]);
    std::fs::write(
        scratch.path().join("waker.json"),
        json!({"broker_url": broker.url, "token": common::TOKEN}).to_string(),
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_duplex-waker"))
        .env("DUPLEX_WAKER_HOME", scratch.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let log = scratch.path().join("waker.log");
    let deadline = Instant::now() + Duration::from_secs(20);
    // Its own home gives it its own instance name, so an installed waker does not stop it.
    let text = loop {
        let text = std::fs::read_to_string(&log).unwrap_or_default();
        if !broker.started().is_empty() && text.contains("report taken") {
            break text;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("the waker never reported: {text}");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let _ = child.kill();
    let _ = child.wait();
    assert!(text.contains(&format!("broker {}", broker.url)), "{text}");
    let started = broker.started();
    assert_eq!(started[0].0, "req-1");
    assert_eq!(started[0].1["ok"], false);
    assert_eq!(
        started[0].1["detail"],
        "request native_id is not a plain id"
    );
    assert!(
        started[0].1["report_id"]
            .as_str()
            .unwrap()
            .starts_with("req-1:")
    );
    assert!(
        !text.contains(common::TOKEN),
        "the token never reaches the log"
    );
}
