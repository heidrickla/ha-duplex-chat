//! `duplex setup`: takes a machine from nothing to a registered adapter. It writes
//! `~/.duplex-chat/adapter.json` and registers `duplex mcp` as an MCP server with Claude Code
//! and Codex, whichever are installed.

use crate::identity::default_codex_argv;
use duplex_proto::BrokerClient;
use serde_json::json;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

pub const USAGE: &str = "\
duplex setup --broker <url> [--enrollment-token <token>] [--poll-seconds <n>] [--no-claude] [--no-codex]
             [--no-check] [--config <adapter.json>]

The enrollment token is the broker's `enrollment_token` option. Prefer the DUPLEX_ENROLLMENT_TOKEN
environment variable to the flag: a flag shows in the process list.";

#[derive(Debug, PartialEq)]
pub struct Options {
    pub broker: String,
    pub enrollment_token: String,
    pub poll_seconds: Option<f64>,
    pub claude: bool,
    pub codex: bool,
    pub check: bool,
    pub config: PathBuf,
}

/// Reads the arguments; `env_token` is `DUPLEX_ENROLLMENT_TOKEN`.
pub fn parse(
    args: &[String],
    env_token: Option<String>,
    default_config: PathBuf,
) -> Result<Options, String> {
    let mut options = Options {
        broker: String::new(),
        enrollment_token: env_token.unwrap_or_default(),
        poll_seconds: None,
        claude: true,
        codex: true,
        check: true,
        config: default_config,
    };
    let mut rest = args.iter();
    while let Some(flag) = rest.next() {
        let mut value = || {
            rest.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match flag.as_str() {
            "--broker" => options.broker = value()?,
            "--enrollment-token" => options.enrollment_token = value()?,
            "--poll-seconds" => {
                options.poll_seconds = Some(
                    value()?
                        .parse()
                        .map_err(|_| "--poll-seconds is a number".to_owned())?,
                )
            }
            "--config" => options.config = PathBuf::from(value()?),
            "--no-claude" => options.claude = false,
            "--no-codex" => options.codex = false,
            "--no-check" => options.check = false,
            other => return Err(format!("unknown option {other}")),
        }
    }
    // The adapter has no TLS stack, as the waker has none: the broker is plain HTTP.
    if !options.broker.starts_with("http://") {
        return Err(
            "--broker is the broker's http:// address, e.g. http://homeassistant.local:8790"
                .to_owned(),
        );
    }
    if options.enrollment_token.trim().is_empty() {
        return Err("the enrollment token is missing: pass --enrollment-token or set DUPLEX_ENROLLMENT_TOKEN".to_owned());
    }
    Ok(options)
}

/// How setup runs the Claude Code and Codex command lines.
pub trait Runner {
    /// The program to run for `name` when it is installed.
    fn find(&self, name: &str) -> Option<String>;
    /// Runs it; whether it succeeded, and what it said.
    fn run(&self, program: &str, args: &[String]) -> io::Result<(bool, String)>;
}

pub struct System;

impl Runner for System {
    fn find(&self, name: &str) -> Option<String> {
        let mut candidates = vec![name.to_owned()];
        if cfg!(windows) {
            // npm installs a `.cmd` shim, which Command does not find by the bare name.
            candidates.push(format!("{name}.cmd"));
        }
        if name == "codex" {
            candidates.splice(0..0, default_codex_argv());
        }
        candidates.into_iter().find(|c| {
            Command::new(c)
                .arg("--version")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success())
        })
    }

    fn run(&self, program: &str, args: &[String]) -> io::Result<(bool, String)> {
        let done = Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .output()?;
        let said = format!(
            "{}{}",
            String::from_utf8_lossy(&done.stdout),
            String::from_utf8_lossy(&done.stderr)
        );
        Ok((done.status.success(), said.trim().to_owned()))
    }
}

fn write_config(options: &Options) -> Result<(), String> {
    let mut doc = json!({"broker_url": options.broker.trim_end_matches('/'), "enrollment_token": options.enrollment_token});
    if let Some(poll) = options.poll_seconds {
        doc["poll_seconds"] = json!(poll);
    }
    let path: &Path = &options.config;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("cannot create {}: {}", dir.display(), e.kind()))?;
    }
    std::fs::write(
        path,
        serde_json::to_string_pretty(&doc).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("cannot write {}: {}", path.display(), e.kind()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// Whether the address answers as a duplex broker; the note says if it is still starting.
fn check_broker(broker: &str) -> Result<&'static str, String> {
    let health = BrokerClient::new(broker, Duration::from_secs(10))
        .get("/health", "")
        .map_err(|e| format!("the broker at {broker} did not answer /health: {e}"))?;
    if health["ok"] != true {
        return Err(format!(
            "the broker at {broker} answered /health with {health}, not a duplex broker's"
        ));
    }
    Ok(if health["ready"] == true {
        "ready"
    } else {
        "answers, still starting"
    })
}

/// Registers `duplex mcp` with one client: removes an earlier registration, then adds it.
fn register(
    runner: &dyn Runner,
    out: &mut dyn Write,
    name: &str,
    remove: &[&str],
    add: &[&str],
    exe: &Path,
) -> Result<(), String> {
    let Some(program) = runner.find(name) else {
        let _ = writeln!(out, "{name}: not found on PATH; skipped");
        return Ok(());
    };
    let remove: Vec<String> = remove.iter().map(|s| (*s).to_owned()).collect();
    let _ = runner.run(&program, &remove); // absent the first time
    let mut add: Vec<String> = add.iter().map(|s| (*s).to_owned()).collect();
    add.extend(["--".to_owned(), exe.display().to_string(), "mcp".to_owned()]);
    match runner.run(&program, &add) {
        Ok((true, _)) => {
            let _ = writeln!(out, "{name}: registered the duplex MCP server");
            Ok(())
        }
        Ok((false, said)) => Err(format!("{name} could not add the MCP server: {said}")),
        Err(e) => Err(format!("{name} could not be run: {e}")),
    }
}

/// Runs setup. `exe` is the `duplex` binary the clients will start.
pub fn run(
    options: &Options,
    runner: &dyn Runner,
    exe: &Path,
    out: &mut dyn Write,
) -> Result<(), String> {
    if options.check {
        let state = check_broker(&options.broker)?;
        let _ = writeln!(out, "broker {}: {state}", options.broker);
    }
    write_config(options)?;
    let _ = writeln!(
        out,
        "wrote {} (the enrollment token is not shown)",
        options.config.display()
    );
    if options.claude {
        register(
            runner,
            out,
            "claude",
            &["mcp", "remove", "--scope", "user", "duplex"],
            &["mcp", "add", "--scope", "user", "duplex"],
            exe,
        )?;
    }
    if options.codex {
        register(
            runner,
            out,
            "codex",
            &["mcp", "remove", "duplex"],
            &["mcp", "add", "duplex"],
            exe,
        )?;
    }
    let _ = writeln!(
        out,
        "Start a Claude Code session or a Codex task: a Claude session joins by itself, and a Codex task calls bridge_register with its thread id."
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    struct Fake {
        installed: Vec<&'static str>,
        calls: RefCell<Vec<(String, Vec<String>)>>,
        fail_add: bool,
    }

    impl Runner for Fake {
        fn find(&self, name: &str) -> Option<String> {
            self.installed.contains(&name).then(|| name.to_owned())
        }
        fn run(&self, program: &str, args: &[String]) -> io::Result<(bool, String)> {
            self.calls
                .borrow_mut()
                .push((program.to_owned(), args.to_vec()));
            let is_add = args.contains(&"add".to_owned());
            Ok((
                !(is_add && self.fail_add),
                if is_add && self.fail_add {
                    "boom".into()
                } else {
                    String::new()
                },
            ))
        }
    }

    fn args(text: &str) -> Vec<String> {
        text.split_whitespace().map(str::to_owned).collect()
    }

    fn options(dir: &Path) -> Options {
        parse(
            &args("--broker http://b:8790/ --enrollment-token tok-123 --no-check"),
            None,
            dir.join("adapter.json"),
        )
        .unwrap()
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("duplex-setup-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn the_arguments_need_a_broker_and_a_token() {
        let config = PathBuf::from("a.json");
        let parsed = parse(
            &args("--broker http://b:8790 --poll-seconds 5 --no-codex"),
            Some("from-env".into()),
            config.clone(),
        )
        .unwrap();
        assert_eq!(
            parsed,
            Options {
                broker: "http://b:8790".into(),
                enrollment_token: "from-env".into(),
                poll_seconds: Some(5.0),
                claude: true,
                codex: false,
                check: true,
                config
            }
        );
        assert!(
            parse(&args("--broker nope"), Some("t".into()), PathBuf::new())
                .unwrap_err()
                .contains("broker's http:// address")
        );
        assert!(
            parse(
                &args("--broker https://b"),
                Some("t".into()),
                PathBuf::new()
            )
            .unwrap_err()
            .contains("http:// address"),
            "no TLS stack"
        );
        assert!(
            parse(&args("--broker http://b"), None, PathBuf::new())
                .unwrap_err()
                .contains("enrollment token is missing")
        );
        assert!(
            parse(
                &args("--broker http://b --bogus"),
                Some("t".into()),
                PathBuf::new()
            )
            .unwrap_err()
            .contains("unknown option --bogus")
        );
        assert!(
            parse(&args("--broker"), Some("t".into()), PathBuf::new())
                .unwrap_err()
                .contains("needs a value")
        );
        let flag_wins = parse(
            &args("--broker http://b --enrollment-token flag"),
            Some("env".into()),
            PathBuf::new(),
        )
        .unwrap();
        assert_eq!(flag_wins.enrollment_token, "flag");
    }

    #[test]
    fn setup_writes_the_config_and_registers_each_installed_client_once() {
        let dir = scratch("both");
        let fake = Fake {
            installed: vec!["claude", "codex"],
            calls: RefCell::new(Vec::new()),
            fail_add: false,
        };
        let mut out = Vec::new();
        run(&options(&dir), &fake, Path::new("/opt/duplex"), &mut out).unwrap();
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.join("adapter.json")).unwrap())
                .unwrap();
        assert_eq!(
            written,
            json!({"broker_url": "http://b:8790", "enrollment_token": "tok-123"})
        );
        let said = String::from_utf8(out).unwrap();
        assert!(
            !said.contains("tok-123"),
            "the token is never shown: {said}"
        );
        assert!(
            said.contains("claude: registered") && said.contains("codex: registered"),
            "{said}"
        );
        let calls = fake.calls.borrow();
        let line = |i: usize| format!("{} {}", calls[i].0, calls[i].1.join(" "));
        assert_eq!(calls.len(), 4);
        assert_eq!(line(0), "claude mcp remove --scope user duplex");
        assert_eq!(
            line(1),
            "claude mcp add --scope user duplex -- /opt/duplex mcp"
        );
        assert_eq!(line(2), "codex mcp remove duplex");
        assert_eq!(line(3), "codex mcp add duplex -- /opt/duplex mcp");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_client_that_is_not_installed_is_skipped_and_a_refusal_is_reported() {
        let dir = scratch("one");
        let only_codex = Fake {
            installed: vec!["codex"],
            calls: RefCell::new(Vec::new()),
            fail_add: false,
        };
        let mut out = Vec::new();
        run(
            &options(&dir),
            &only_codex,
            Path::new("/x/duplex"),
            &mut out,
        )
        .unwrap();
        assert!(
            String::from_utf8(out)
                .unwrap()
                .contains("claude: not found on PATH; skipped")
        );
        assert_eq!(only_codex.calls.borrow().len(), 2);
        let broken = Fake {
            installed: vec!["claude"],
            calls: RefCell::new(Vec::new()),
            fail_add: true,
        };
        let err = run(
            &options(&dir),
            &broken,
            Path::new("/x/duplex"),
            &mut Vec::new(),
        )
        .unwrap_err();
        assert_eq!(err, "claude could not add the MCP server: boom");
        let skipped = parse(
            &args("--broker http://b --enrollment-token t --no-check --no-claude --no-codex"),
            None,
            dir.join("c.json"),
        )
        .unwrap();
        let none = Fake {
            installed: vec!["claude", "codex"],
            calls: RefCell::new(Vec::new()),
            fail_add: false,
        };
        run(&skipped, &none, Path::new("/x"), &mut Vec::new()).unwrap();
        assert!(none.calls.borrow().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_unreachable_broker_stops_setup_before_it_writes_anything() {
        let dir = scratch("down");
        let mut opts = options(&dir);
        opts.check = true;
        opts.broker = "http://127.0.0.1:9".into();
        let fake = Fake {
            installed: vec![],
            calls: RefCell::new(Vec::new()),
            fail_add: false,
        };
        let err = run(&opts, &fake, Path::new("/x"), &mut Vec::new()).unwrap_err();
        assert!(err.contains("did not answer /health"), "{err}");
        assert!(!dir.join("adapter.json").exists());
    }
}
