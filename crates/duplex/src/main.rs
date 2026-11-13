use duplex_adapter::adapter::Adapter;
use duplex_adapter::compat;
use duplex_adapter::config::{AdapterConfig, default_config_path};
use duplex_adapter::identity::{Participant, SessionIdentity};
use duplex_adapter::log;
use duplex_adapter::mcp::{self, tools::Server};
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;
use std::process::ExitCode;
use std::sync::Arc;

const USAGE: &str = "\
duplex: joins a Claude Code session, a Codex task or OpenCode sessions to a duplex broker

Usage:
  duplex setup ...      write adapter.json and register the MCP server with Claude Code and Codex
  duplex owner ...      owner commands: enroll this machine, then sessions, groups, tasks and more
  duplex mcp            run the MCP server on stdin and stdout (what `duplex setup` registers)
  duplex opencode       hold the OpenCode sessions in ~/.duplex-chat/opencode.json for the broker
  duplex compat check   compare the Claude Code and Codex releases with the ones this build was reviewed against
  duplex --version

`duplex setup` and `duplex owner` with no arguments print their own usage.";

fn run_setup(args: &[String]) -> ExitCode {
    let token = std::env::var("DUPLEX_ENROLLMENT_TOKEN").ok();
    let options = match duplex_adapter::setup::parse(args, token, default_config_path()) {
        Ok(options) => options,
        Err(why) => {
            eprintln!("{why}\n\n{}", duplex_adapter::setup::USAGE);
            return ExitCode::from(2);
        }
    };
    let exe = std::env::current_exe().unwrap_or_else(|_| "duplex".into());
    match duplex_adapter::setup::run(
        &options,
        &duplex_adapter::setup::System,
        &exe,
        &mut std::io::stdout().lock(),
    ) {
        Ok(()) => ExitCode::SUCCESS,
        Err(why) => {
            eprintln!("{why}");
            ExitCode::FAILURE
        }
    }
}

fn run_owner(args: &[String]) -> ExitCode {
    use duplex_adapter::owner::{Failure, USAGE, device_path, run};
    match run(args, &mut std::io::stdout().lock(), &device_path()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure::Usage) => {
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
        Err(Failure::Said(why)) => {
            eprintln!("{why}");
            ExitCode::FAILURE
        }
    }
}

/// Whether this server runs inside a Claude Code session (with its inbox), or not. Codex
/// passes no CODEX_* variables to MCP servers, so `other` covers Codex tasks.
fn detect_host(session: Option<&SessionIdentity>) -> serde_json::Value {
    match session {
        Some(s) => {
            json!({"host": "claude", "id_prefix": s.session_id.chars().take(8).collect::<String>()})
        }
        None => json!({"host": "other"}),
    }
}

fn run_mcp() -> ExitCode {
    let env: HashMap<String, String> = std::env::vars().collect();
    let session = SessionIdentity::from_env(&env);
    let server = Server::new(detect_host(session.as_ref()));
    if let Some(identity) = session {
        let path = default_config_path();
        match AdapterConfig::load(&path) {
            Ok(Some(config)) => {
                let adapter = Arc::new(Adapter::new(config, Participant::Claude(identity)));
                adapter.spawn();
                server.set(adapter);
            }
            Ok(None) => log::info(format!(
                "{} is missing: duplex-chat is not active in this session",
                path.display()
            )),
            Err(why) => log::warn(why),
        }
    }
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    match mcp::serve(&server, stdin.lock(), &mut stdout) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            log::warn(format!("stdio closed: {err}"));
            ExitCode::FAILURE
        }
    }
}

/// Reads a URL with `curl`: the release check is a maintainer's command, and the adapter
/// itself carries no TLS stack.
fn fetch(url: &str) -> Result<String, String> {
    let done = std::process::Command::new("curl")
        .args([
            "--fail",
            "--silent",
            "--show-error",
            "--location",
            "--max-time",
            "20",
        ])
        .args([
            "--user-agent",
            "duplex compat check",
            "--header",
            "accept: application/json, text/plain",
        ])
        .arg(url)
        .output()
        .map_err(|e| format!("cannot run curl: {e}"))?;
    if !done.status.success() {
        return Err(String::from_utf8_lossy(&done.stderr).trim().to_owned());
    }
    String::from_utf8(done.stdout).map_err(|e| e.to_string())
}

fn run_compat() -> ExitCode {
    let mut stdout = std::io::stdout().lock();
    let status = compat::check(&fetch, &compat::manifest(), &mut |line| {
        let _ = writeln!(stdout, "{line}");
    });
    ExitCode::from(u8::try_from(status).unwrap_or(2))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["--version" | "-V"] => {
            println!("duplex {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        ["mcp"] => run_mcp(),
        ["opencode"] => match duplex_adapter::opencode::run() {
            Ok(()) => ExitCode::SUCCESS,
            Err(why) => {
                eprintln!("{why}");
                ExitCode::FAILURE
            }
        },
        ["compat", "check"] => run_compat(),
        ["setup", ..] => run_setup(&args[1..]),
        ["owner", ..] => run_owner(&args[1..]),
        _ => {
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}
