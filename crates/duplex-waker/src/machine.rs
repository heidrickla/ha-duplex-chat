//! Where Claude Code and Codex live on this machine.

use crate::config::Paths;
use crate::launch::Tools;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub struct MachineTools {
    pub paths: Paths,
}

/// The first folder of a PATH-style list that holds the file `name`.
pub fn find_on_path(name: &str, path: &std::ffi::OsStr) -> Option<PathBuf> {
    std::env::split_paths(path)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// Subfolders of `dir` (one level), sorted; none when it cannot be read.
fn subdirs(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.is_dir())
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

impl MachineTools {
    /// `codex()` with the PATH given.
    pub fn codex_on(&self, path: &std::ffi::OsStr) -> Option<PathBuf> {
        let vendored = self.paths.app_data.join(
            "npm/node_modules/@openai/codex/node_modules/@openai/codex-win32-x64/vendor/x86_64-pc-windows-msvc/bin/codex.exe",
        );
        vendored
            .is_file()
            .then_some(vendored)
            .or_else(|| find_on_path("codex.exe", path))
    }

    /// `%LOCALAPPDATA%\Claude-Data\claude_<package id>`: the desktop app's data folders.
    fn claude_data(&self) -> Vec<PathBuf> {
        subdirs(&self.paths.local_app_data.join("Claude-Data"))
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("claude_"))
            })
            .collect()
    }
}

impl Tools for MachineTools {
    /// The Claude Code the desktop app installed last (`claude-code\<version>\<build>\`),
    /// so a resumed transcript is written by the version the app runs; else ~\.local\bin.
    fn claude(&self) -> Option<PathBuf> {
        let mut newest: Option<(SystemTime, PathBuf)> = None;
        for data in self.claude_data() {
            for version in subdirs(&data.join("claude-code")) {
                for build in subdirs(&version) {
                    let exe = build.join("claude.exe");
                    let Ok(modified) = std::fs::metadata(&exe).and_then(|m| m.modified()) else {
                        continue;
                    };
                    if newest.as_ref().is_none_or(|(t, _)| modified > *t) {
                        newest = Some((modified, exe));
                    }
                }
            }
        }
        newest.map(|(_, exe)| exe).or_else(|| {
            let installed = self
                .paths
                .profile
                .join(".local")
                .join("bin")
                .join("claude.exe");
            let path = std::env::var_os("PATH").unwrap_or_default();
            [Some(installed), find_on_path("claude.exe", &path)]
                .into_iter()
                .flatten()
                .find(|p| p.is_file())
        })
    }

    /// The npm package's own codex.exe, as the session adapter uses; else `codex.exe` on the
    /// PATH.
    fn codex(&self) -> Option<PathBuf> {
        self.codex_on(&std::env::var_os("PATH").unwrap_or_default())
    }

    /// The desktop record (`claude-code-sessions\<account>\<org>\local_*.json`) whose
    /// `cliSessionId` is this session gives its model, so a resume costs what the session did.
    fn claude_model(&self, session: &str) -> Option<String> {
        #[derive(Deserialize)]
        struct DesktopRecord {
            #[serde(rename = "cliSessionId")]
            cli_session_id: Option<String>,
            model: Option<String>,
        }
        for data in self.claude_data() {
            for account in subdirs(&data.join("claude-code-sessions")) {
                for org in subdirs(&account) {
                    let Ok(entries) = std::fs::read_dir(&org) else {
                        continue;
                    };
                    for entry in entries.filter_map(Result::ok) {
                        let path = entry.path();
                        let name = entry.file_name().to_string_lossy().into_owned();
                        if !(name.starts_with("local_") && name.ends_with(".json")) {
                            continue;
                        }
                        let Some(record) = std::fs::read_to_string(&path)
                            .ok()
                            .and_then(|t| serde_json::from_str::<DesktopRecord>(&t).ok())
                        else {
                            continue;
                        };
                        if record.cli_session_id.as_deref() == Some(session) {
                            return record.model;
                        }
                    }
                }
            }
        }
        None
    }
}
