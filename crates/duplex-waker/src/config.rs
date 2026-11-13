//! Where the waker's files are and what `waker.json` holds.

use serde::Deserialize;
use std::fmt;
use std::path::{Path, PathBuf};

/// `%USERPROFILE%\.duplex-chat\waker.json`: the broker address and the waker's own token.
#[derive(Deserialize)]
pub struct WakerConfig {
    pub broker_url: String,
    pub token: String,
}

// The token never reaches a log line, even through a debug print.
impl fmt::Debug for WakerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WakerConfig")
            .field("broker_url", &self.broker_url)
            .field("token", &format_args!("<{} characters>", self.token.len()))
            .finish()
    }
}

impl WakerConfig {
    /// Reads and checks the file; the error names the problem, never a value from it.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {}", path.display(), e.kind()))?;
        let config: Self = serde_json::from_str(&text)
            .map_err(|_| format!("{} is not {{broker_url, token}} JSON", path.display()))?;
        if !config.broker_url.starts_with("http://") {
            return Err(format!(
                "{}: broker_url must be an http:// address",
                path.display()
            ));
        }
        if config.token.trim().is_empty() {
            return Err(format!("{}: token is empty", path.display()));
        }
        Ok(config)
    }
}

/// `Local\duplex-waker`, or with a `DUPLEX_WAKER_HOME` a name that also holds a hash of it
/// (FNV-1a over the folder's text, case-folded as Windows paths are).
pub fn instance_name(home_override: Option<&std::ffi::OsStr>) -> String {
    match home_override {
        None => "Local\\duplex-waker".to_owned(),
        Some(home) => {
            let hash = home
                .to_string_lossy()
                .to_lowercase()
                .bytes()
                .fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
                    (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
                });
            format!("Local\\duplex-waker-{hash:016x}")
        }
    }
}

/// The folders the waker reads, from the user's environment.
#[derive(Clone, Debug)]
pub struct Paths {
    pub profile: PathBuf,
    pub local_app_data: PathBuf,
    pub app_data: PathBuf,
}

impl Paths {
    pub fn from_env() -> Result<Self, String> {
        let var = |name: &str| {
            std::env::var_os(name)
                .map(PathBuf::from)
                .ok_or_else(|| format!("{name} is not set"))
        };
        Ok(Self {
            profile: var("USERPROFILE")?,
            local_app_data: var("LOCALAPPDATA")?,
            app_data: var("APPDATA")?,
        })
    }

    /// `%USERPROFILE%\.duplex-chat`, or `DUPLEX_WAKER_HOME` for a waker run beside the
    /// installed one (its own waker.json and log, the same machine otherwise).
    pub fn duplex_dir(&self) -> PathBuf {
        std::env::var_os("DUPLEX_WAKER_HOME")
            .map_or_else(|| self.profile.join(".duplex-chat"), PathBuf::from)
    }

    /// The name of the mutex that keeps one waker per home: the installed one's is fixed, and
    /// a waker run with `DUPLEX_WAKER_HOME` has its own, so it can run beside the installed one.
    pub fn instance_name(&self) -> String {
        instance_name(std::env::var_os("DUPLEX_WAKER_HOME").as_deref())
    }

    pub fn config_file(&self) -> PathBuf {
        self.duplex_dir().join("waker.json")
    }

    pub fn log_file(&self) -> PathBuf {
        self.duplex_dir().join("waker.log")
    }

    /// Claude Code's live-process records, `<pid>.json` each.
    pub fn claude_sessions(&self) -> PathBuf {
        self.profile.join(".claude").join("sessions")
    }
}
