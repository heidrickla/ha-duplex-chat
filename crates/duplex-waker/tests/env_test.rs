#![cfg(windows)]

mod common;

use common::{Scratch, claude_request, request};
use duplex_waker::launch::{CHILD_ENV, Tools, command, passed, plan};
use serde_json::json;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;

/// Both routes resolve to cmd.exe, so a real child prints the environment it got.
struct CmdTools;

impl Tools for CmdTools {
    fn claude(&self) -> Option<PathBuf> {
        Some(PathBuf::from("cmd.exe"))
    }
    fn codex(&self) -> Option<PathBuf> {
        Some(PathBuf::from("cmd.exe"))
    }
    fn claude_model(&self, _session: &str) -> Option<String> {
        None
    }
}

fn parent_with_credentials() -> Vec<(OsString, OsString)> {
    let mut parent: Vec<(OsString, OsString)> = std::env::vars_os().collect();
    for (name, value) in [
        ("ANTHROPIC_API_KEY", "sk-test"),
        ("anthropic_auth_token", "lower"),
        ("CLAUDE_CODE_OAUTH_TOKEN", "oauth"),
        ("CLAUDECODE", "1"),
        ("MCP_TIMEOUT", "5"),
        ("OPENAI_API_KEY", "sk-openai"),
        ("CODEX_API_KEY", "codex"),
        ("GITHUB_TOKEN", "ghp"),
        ("AGENT_MEMORY_TOKEN_CLAUDE", "memory"),
        ("AWS_SECRET_ACCESS_KEY", "aws"),
        ("SOME_UNNAMED_SECRET", "x"),
    ] {
        parent.push((name.into(), value.into()));
    }
    parent
}

/// For each route, the command built at the one spawn boundary gives a real child only
/// allowlisted variables, whatever credentials the parent holds.
#[test]
fn only_allowlisted_variables_reach_the_child_on_every_route() {
    let scratch = Scratch::new("env");
    let workspace = scratch.dir("workspace");
    let codex = json!({"id": "d2", "binding_id": "b2", "provider": "codex",
        "native_id": "019a3f2e-7c41-7d05-b8e2-4f1a6c9d0e37", "title": "probe",
        "workspace": null, "reason": "limit_resume", "message_id": "m2", "due_at": null});
    for listed in [claude_request("d1", &workspace), codex] {
        let mut route = plan(&request(&listed), &CmdTools).expect("a plan");
        // The route's own program, folder and environment; arguments that print it.
        route.args = vec!["/d".into(), "/c".into(), "set".into()];
        let mut child = command(&route, parent_with_credentials());
        child.stdout(Stdio::piped());
        let output = child.output().expect("cmd.exe runs");
        let text = String::from_utf8_lossy(&output.stdout).to_ascii_uppercase();
        for line in text.lines() {
            let name = line.split('=').next().unwrap_or("");
            // cmd.exe sets PROMPT itself when it is missing.
            assert!(
                CHILD_ENV.contains(&name) || name == "PROMPT",
                "{}: {name} reached the child",
                route.route
            );
        }
        for needed in [
            "USERPROFILE=",
            "APPDATA=",
            "LOCALAPPDATA=",
            "PATH=",
            "SYSTEMROOT=",
        ] {
            assert!(text.contains(needed), "{}: {needed} is passed", route.route);
        }
    }
}

#[test]
fn the_allowlist_matches_names_in_any_case_and_holds_no_credential() {
    assert!(passed("Path".as_ref()));
    assert!(passed("userprofile".as_ref()));
    for name in [
        "ANTHROPIC_API_KEY",
        "CLAUDE_CONFIG_DIR",
        "MCP_TIMEOUT",
        "OPENAI_API_KEY",
        "AGENT_MEMORY_TOKEN_CLAUDE",
    ] {
        assert!(!passed(name.as_ref()), "{name}");
    }
    for name in CHILD_ENV {
        for word in [
            "KEY",
            "TOKEN",
            "SECRET",
            "PASSWORD",
            "ANTHROPIC",
            "OPENAI",
            "CLAUDE",
        ] {
            assert!(!name.contains(word), "{name} looks like a credential");
        }
    }
}
