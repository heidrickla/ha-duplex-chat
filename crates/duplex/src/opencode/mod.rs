//! `duplex opencode`: the adapter for OpenCode sessions the bridge holds, one per model, in the
//! OpenCode desktop app's own server.
//!
//! It finds the app's server from the file the server writes at each launch, keeps the sessions
//! named in `opencode.json` (creating each once), registers each with the broker as provider
//! `opencode`, and turns each message the owner sends one of them into an input of that session.
//! The reply is the final assistant message of the round that took the input in. Peer messages
//! are refused, as by the Codex app-server adapter: peer text reaches a session only as tool data.
//!
//! It reads OpenCode's durable records (the running-session list, the message log, the pending
//! permission requests) on every pass. Its event stream drops events while a client is
//! disconnected, so it is not used. Nothing answers while the app is closed: messages wait in the
//! broker.
//!
//! Files, in `~/.duplex-chat`:
//!   adapter.json           broker_url, enrollment_token (what `duplex setup` writes)
//!   opencode.json          {"sessions": [{"title", "cwd", "model": "<provider>/<model>"}]}
//!   opencode-sessions.json title -> OpenCode session id, so a restart holds the same sessions
//!   bindings/opencode-*.token  each binding's current token
//!   opencode.log           one UTC line per event; moved to opencode.log.1 past 1 MB

pub mod config;
pub mod replies;
pub mod server;
pub mod session;

use crate::config::{AdapterConfig, config_dir, default_config_path};
use crate::redact::redact;
use duplex_proto::BrokerClient;
use session::{Managed, Shared};
use std::fmt::Display;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const LOG_LIMIT: u64 = 1 << 20;

/// Lines to stderr and, when a file is given, to it with a UTC time. Every line is redacted, so
/// no credential shape reaches either.
pub struct Log {
    file: Option<PathBuf>,
    lock: Mutex<()>,
}

impl Log {
    pub fn new(file: Option<PathBuf>) -> Self {
        Self {
            file,
            lock: Mutex::new(()),
        }
    }

    pub fn info(&self, message: impl Display) {
        self.line(&message.to_string());
    }

    pub fn warn(&self, message: impl Display) {
        self.line(&format!("warning: {message}"));
    }

    fn line(&self, message: &str) {
        let message = redact(message);
        let _held = self
            .lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        eprintln!("[duplex opencode] {message}");
        let Some(path) = &self.file else {
            return;
        };
        if std::fs::metadata(path).is_ok_and(|m| m.len() > LOG_LIMIT) {
            let _ = std::fs::rename(path, path.with_extension("log.1"));
        }
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = writeln!(file, "{} {message}", jiff::Timestamp::now());
        }
    }
}

/// How long a starting adapter waits for the one before it to let go of `opencode.lock`: a task
/// restarted at once can start while the stopped process is still exiting.
const LOCK_WAIT: Duration = Duration::from_secs(20);

/// Holds `opencode.lock` for the life of the process; a second adapter cannot open it (Windows).
fn single_instance(path: &PathBuf) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(false).write(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.share_mode(0);
    }
    let deadline = std::time::Instant::now() + LOCK_WAIT;
    loop {
        match options.open(path) {
            Err(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(250));
            }
            other => return other,
        }
    }
}

/// Runs the adapter until every session is retired. An error is why it could not start.
pub fn run() -> Result<(), String> {
    let dir = config_dir();
    let _ = std::fs::create_dir_all(&dir);
    let log = Log::new(Some(dir.join("opencode.log")));
    let _lock = single_instance(&dir.join("opencode.lock")).map_err(|e| {
        format!(
            "another duplex opencode holds opencode.lock after {} s ({e})",
            LOCK_WAIT.as_secs()
        )
    })?;
    let adapter_path = default_config_path();
    let adapter = AdapterConfig::load(&adapter_path)?.ok_or_else(|| {
        format!(
            "{} is missing: run duplex setup first",
            adapter_path.display()
        )
    })?;
    let specs = config::load(&dir.join("opencode.json"))?;
    let shared = Arc::new(Shared {
        broker: BrokerClient::new(&adapter.broker_url, Duration::from_secs(20)),
        enrollment_token: adapter.enrollment_token.clone(),
        opencode: server::OpenCode::new(
            server::Discovery::default_paths(),
            Duration::from_secs(30),
        ),
        sessions: config::SessionMap::new(dir.join("opencode-sessions.json")),
        token_dir: adapter.token_dir.clone(),
        log,
        poll: Duration::from_secs_f64(adapter.poll_seconds.max(0.5)),
        stop_poll: Duration::from_millis(250),
        turn_timeout: session::TURN_TIMEOUT,
    });
    shared.log.info(format!(
        "duplex opencode {} serving {}",
        env!("CARGO_PKG_VERSION"),
        specs
            .iter()
            .map(|s| format!("{} ({})", s.title, s.model()))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    let threads: Vec<_> = specs
        .into_iter()
        .map(|spec| {
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || shared.serve(Managed::new(spec)))
        })
        .collect();
    for thread in threads {
        let _ = thread.join();
    }
    Ok(())
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn a_restarted_adapter_waits_for_the_stopping_one_to_let_go_of_the_lock() {
        let dir = std::env::temp_dir().join(format!("duplex-oc-lock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("opencode.lock");
        let held = single_instance(&path).unwrap();
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(600));
            drop(held);
        });
        let started = std::time::Instant::now();
        assert!(single_instance(&path).is_ok());
        assert!(started.elapsed() >= Duration::from_millis(500));
        releaser.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
