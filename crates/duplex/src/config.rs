//! Where the adapter's files are and what `adapter.json` holds.

use serde::Deserialize;
use std::fmt;
use std::path::{Path, PathBuf};

/// The broker address and the enrollment token, in `~/.duplex-chat/adapter.json` (or the
/// file `DUPLEX_ADAPTER_CONFIG` names). Each binding's current token is kept in `bindings/`
/// beside it: the broker lets an active session register again only with that token, so a
/// restarted server keeps its binding.
#[derive(Clone)]
pub struct AdapterConfig {
    pub broker_url: String,
    pub enrollment_token: String,
    pub poll_seconds: f64,
    /// Where binding tokens are kept; None keeps them in memory.
    pub token_dir: Option<PathBuf>,
}

impl fmt::Debug for AdapterConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdapterConfig")
            .field("broker_url", &self.broker_url)
            .field("enrollment_token", &"<redacted>")
            .field("poll_seconds", &self.poll_seconds)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct Raw {
    broker_url: String,
    enrollment_token: String,
    #[serde(default)]
    poll_seconds: Option<f64>,
}

impl AdapterConfig {
    /// None when there is no file: without it nothing starts. An error names the problem,
    /// never a value from the file.
    pub fn load(path: &Path) -> Result<Option<Self>, String> {
        if !path.is_file() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {}", path.display(), e.kind()))?;
        let raw: Raw = serde_json::from_str(&text).map_err(|_| {
            format!(
                "{} is not {{broker_url, enrollment_token}} JSON",
                path.display()
            )
        })?;
        Ok(Some(Self {
            broker_url: raw.broker_url.trim_end_matches('/').to_owned(),
            enrollment_token: raw.enrollment_token,
            poll_seconds: raw.poll_seconds.unwrap_or(2.0),
            token_dir: path.parent().map(|p| p.join("bindings")),
        }))
    }
}

#[cfg(windows)]
fn known_folder(id: &windows_sys::core::GUID) -> Option<PathBuf> {
    use windows_sys::Win32::System::Com::CoTaskMemFree;
    use windows_sys::Win32::UI::Shell::SHGetKnownFolderPath;
    let mut path: *mut u16 = std::ptr::null_mut();
    // SAFETY: an out-pointer to a local; on success the shell hands back a NUL-terminated
    // string it allocated, which is copied and freed here.
    unsafe {
        if SHGetKnownFolderPath(id, 0, std::ptr::null_mut(), &mut path) != 0 || path.is_null() {
            return None;
        }
        let mut len = 0;
        while *path.add(len) != 0 {
            len += 1;
        }
        let text = String::from_utf16_lossy(std::slice::from_raw_parts(path, len));
        CoTaskMemFree(path.cast());
        Some(PathBuf::from(text))
    }
}

/// The user's profile folder. On Windows it comes from the shell when the environment has
/// none: Codex starts MCP servers with a reduced environment (no USERPROFILE).
pub fn profile_dir() -> PathBuf {
    if let Some(value) = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME")) {
        return PathBuf::from(value);
    }
    #[cfg(windows)]
    if let Some(found) = known_folder(&windows_sys::Win32::UI::Shell::FOLDERID_Profile) {
        return found;
    }
    PathBuf::from(".")
}

/// The roaming application data folder (Windows), from the environment or the shell.
pub fn roaming_app_data() -> Option<PathBuf> {
    if let Some(value) = std::env::var_os("APPDATA") {
        return Some(PathBuf::from(value));
    }
    #[cfg(windows)]
    return known_folder(&windows_sys::Win32::UI::Shell::FOLDERID_RoamingAppData);
    #[cfg(not(windows))]
    None
}

/// `~/.duplex-chat`: the profile folder itself, not AppData. The Claude and Codex desktop
/// apps are packaged, and a package's AppData writes land in its own private store.
pub fn config_dir() -> PathBuf {
    profile_dir().join(".duplex-chat")
}

pub fn default_config_path() -> PathBuf {
    std::env::var_os("DUPLEX_ADAPTER_CONFIG")
        .filter(|v| !v.is_empty())
        .map_or_else(|| config_dir().join("adapter.json"), PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_file_gives_the_broker_and_hides_its_token() {
        let dir = std::env::temp_dir().join(format!("duplex-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("adapter.json");
        assert!(AdapterConfig::load(&path).unwrap().is_none());
        std::fs::write(
            &path,
            r#"{"broker_url": "http://broker.test:8790/", "enrollment_token": "secret-token-1"}"#,
        )
        .unwrap();
        let config = AdapterConfig::load(&path).unwrap().unwrap();
        assert_eq!(config.broker_url, "http://broker.test:8790");
        assert_eq!(config.poll_seconds, 2.0);
        assert_eq!(config.token_dir, Some(dir.join("bindings")));
        assert!(!format!("{config:?}").contains("secret-token-1"));
        std::fs::write(&path, r#"{"broker_url": "x"}"#).unwrap();
        let err = AdapterConfig::load(&path).unwrap_err();
        assert!(
            err.contains("is not {broker_url, enrollment_token} JSON") && !err.contains("\"x\""),
            "{err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
