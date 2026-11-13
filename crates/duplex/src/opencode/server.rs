//! The OpenCode server the desktop app runs, found again on every call: the app picks a port
//! per launch and writes it, with its password, to `service.json` in OpenCode's state folder.

use serde_json::Value;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The user OpenCode's Basic auth expects.
pub const USER: &str = "opencode";
/// What an answer body may hold: one page of a session's messages fits many times over.
const BODY_LIMIT: u64 = 64 << 20;
const MESSAGE_CHARS: usize = 300;

/// Why a call to OpenCode failed. No variant carries a header, so none carries the password.
#[derive(Clone, Debug, PartialEq)]
pub enum ServerError {
    /// No server answers: no service file, or nothing listens at its address.
    NotRunning(String),
    /// The server refused the password: the file was rewritten for a new launch.
    Unauthorized,
    /// An answer other than 2xx; `message` is OpenCode's own reason.
    Refused { code: u16, message: String },
    /// An answer that is not the JSON the route promises.
    Invalid(String),
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRunning(why) => write!(f, "OpenCode is not running ({why})"),
            Self::Unauthorized => f.write_str("OpenCode refused the password"),
            Self::Refused { code, message } => {
                write!(f, "OpenCode answered HTTP {code}: {message}")
            }
            Self::Invalid(why) => write!(f, "OpenCode answer not understood: {why}"),
        }
    }
}

impl std::error::Error for ServerError {}

impl ServerError {
    pub fn code(&self) -> Option<u16> {
        match self {
            Self::Refused { code, .. } => Some(*code),
            Self::Unauthorized => Some(401),
            _ => None,
        }
    }
}

/// Where the running server is described: `<state>/opencode/service.json` holds `url`, `pid`,
/// `version` and `password`; `<config>/opencode/service.json` holds the password too.
#[derive(Clone, Debug)]
pub struct Discovery {
    pub state_file: PathBuf,
    pub password_file: PathBuf,
}

impl Discovery {
    /// OpenCode's own folders: `XDG_STATE_HOME` and `XDG_CONFIG_HOME`, else `~/.local/state` and
    /// `~/.config`, on Windows as elsewhere.
    pub fn default_paths() -> Self {
        let home = crate::config::profile_dir();
        let base = |var: &str, fallback: &[&str]| {
            std::env::var_os(var)
                .filter(|v| !v.is_empty())
                .map_or_else(
                    || fallback.iter().fold(home.clone(), |p, part| p.join(part)),
                    PathBuf::from,
                )
                .join("opencode")
                .join("service.json")
        };
        Self {
            state_file: base("XDG_STATE_HOME", &[".local", "state"]),
            password_file: base("XDG_CONFIG_HOME", &[".config"]),
        }
    }

    /// The server as its files describe it now. An error names the file, never a value in it.
    pub fn read(&self) -> Result<Endpoint, ServerError> {
        let state = read_json(&self.state_file)?;
        let url = state
            .get("url")
            .and_then(Value::as_str)
            .filter(|u| u.starts_with("http://"))
            .ok_or_else(|| {
                ServerError::NotRunning(format!("{} names no http url", self.state_file.display()))
            })?
            .trim_end_matches('/')
            .to_owned();
        let password = match state.get("password").and_then(Value::as_str) {
            Some(p) if !p.is_empty() => p.to_owned(),
            _ => read_json(&self.password_file)?
                .get("password")
                .and_then(Value::as_str)
                .filter(|p| !p.is_empty())
                .ok_or_else(|| {
                    ServerError::NotRunning(format!(
                        "{} holds no password",
                        self.password_file.display()
                    ))
                })?
                .to_owned(),
        };
        Ok(Endpoint {
            auth: format!("Basic {}", base64(format!("{USER}:{password}").as_bytes())),
            url,
        })
    }
}

fn read_json(path: &Path) -> Result<Value, ServerError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| ServerError::NotRunning(format!("{}: {}", path.display(), e.kind())))?;
    serde_json::from_str(&text)
        .map_err(|_| ServerError::NotRunning(format!("{} is not JSON", path.display())))
}

/// One launch's address and its Basic auth header.
#[derive(Clone, PartialEq)]
pub struct Endpoint {
    pub url: String,
    auth: String,
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Endpoint")
            .field("url", &self.url)
            .field("auth", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verb {
    Get,
    Post,
    Patch,
}

/// A blocking client for the OpenCode server. Every call reads the service file first, so a
/// relaunched app (new port, new password) is followed from the next call on.
pub struct OpenCode {
    discovery: Discovery,
    agent: ureq::Agent,
}

impl OpenCode {
    pub fn new(discovery: Discovery, timeout: Duration) -> Self {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            .http_status_as_error(false)
            .build()
            .into();
        Self { discovery, agent }
    }

    pub fn call(&self, verb: Verb, path: &str, body: Option<&Value>) -> Result<Value, ServerError> {
        let endpoint = self.discovery.read()?;
        let url = format!("{}{path}", endpoint.url);
        let answered = match verb {
            Verb::Get => self
                .agent
                .get(&url)
                .header("Authorization", &endpoint.auth)
                .call(),
            Verb::Post | Verb::Patch => {
                let request = if verb == Verb::Post {
                    self.agent.post(&url)
                } else {
                    self.agent.patch(&url)
                }
                .header("Authorization", &endpoint.auth);
                match body {
                    Some(body) => request.send_json(body),
                    None => request.send_empty(),
                }
            }
        };
        // The transport error's text names the cause (refused, timed out), never a header.
        let mut response = answered.map_err(|e| ServerError::NotRunning(short(&e.to_string())))?;
        let code = response.status().as_u16();
        let text = response
            .body_mut()
            .with_config()
            .limit(BODY_LIMIT)
            .read_to_string()
            .map_err(|e| ServerError::Invalid(short(&e.to_string())))?;
        if code == 401 {
            return Err(ServerError::Unauthorized);
        }
        if !(200..300).contains(&code) {
            return Err(ServerError::Refused {
                code,
                message: message_of(&text),
            });
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| ServerError::Invalid(short(&e.to_string())))
    }

    pub fn get(&self, path: &str) -> Result<Value, ServerError> {
        self.call(Verb::Get, path, None)
    }

    pub fn post(&self, path: &str, body: &Value) -> Result<Value, ServerError> {
        self.call(Verb::Post, path, Some(body))
    }
}

fn short(text: &str) -> String {
    text.chars().take(MESSAGE_CHARS).collect()
}

/// OpenCode's error body is `{"_tag": ..., "message": ...}`; anything else is kept short.
fn message_of(body: &str) -> String {
    let found = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_owned));
    short(found.as_deref().unwrap_or(body).trim())
}

/// Standard base64 with padding, for the Basic auth header.
pub fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | u32::from(*b) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(TABLE[(n >> (18 - 6 * i) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_the_standard_alphabet_and_padding() {
        for (input, want) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("opencode:pw", "b3BlbmNvZGU6cHc="),
        ] {
            assert_eq!(base64(input.as_bytes()), want, "{input}");
        }
        assert_eq!(base64(&[0xff, 0xfe]), "//4=");
    }

    #[test]
    fn the_state_file_gives_the_address_and_the_password_stays_hidden() {
        let dir = std::env::temp_dir().join(format!("duplex-oc-server-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let state = dir.join("state.json");
        let config = dir.join("config.json");
        let discovery = Discovery {
            state_file: state.clone(),
            password_file: config.clone(),
        };
        assert!(matches!(discovery.read(), Err(ServerError::NotRunning(_))));
        std::fs::write(&state, r#"{"url": "http://127.0.0.1:5/", "pid": 1}"#).unwrap();
        std::fs::write(&config, r#"{"password": "pw-from-config"}"#).unwrap();
        let endpoint = discovery.read().unwrap();
        assert_eq!(endpoint.url, "http://127.0.0.1:5");
        assert_eq!(
            endpoint.auth,
            format!("Basic {}", base64(b"opencode:pw-from-config"))
        );
        std::fs::write(
            &state,
            r#"{"url": "http://127.0.0.1:6", "password": "pw-from-state"}"#,
        )
        .unwrap();
        let endpoint = discovery.read().unwrap();
        assert_eq!(endpoint.url, "http://127.0.0.1:6");
        assert!(!format!("{endpoint:?}").contains("cHctZnJvbS1zdGF0ZQ"));
        assert!(!format!("{endpoint:?}").contains(&base64(b"opencode:pw-from-state")));
        std::fs::write(
            &state,
            r#"{"url": "file:///x", "password": "pw-from-state"}"#,
        )
        .unwrap();
        let err = discovery.read().unwrap_err().to_string();
        assert!(
            err.contains("names no http url") && !err.contains("pw-from"),
            "{err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_error_body_gives_its_message() {
        assert_eq!(
            message_of(r#"{"_tag":"SessionNotFoundError","message":"Session not found: ses_x"}"#),
            "Session not found: ses_x"
        );
        assert_eq!(message_of("plain"), "plain");
    }
}
