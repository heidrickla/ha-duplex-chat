//! `~/.duplex-chat/opencode.json`: the OpenCode sessions the adapter holds, one per model.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One managed session: its title in OpenCode and the broker, its folder and its model.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionSpec {
    pub title: String,
    pub cwd: String,
    pub provider_id: String,
    pub model_id: String,
}

impl SessionSpec {
    /// `providerID/modelID`, as the config names it.
    pub fn model(&self) -> String {
        format!("{}/{}", self.provider_id, self.model_id)
    }
}

#[derive(Deserialize)]
struct Raw {
    sessions: Vec<RawSession>,
}

#[derive(Deserialize)]
struct RawSession {
    title: String,
    cwd: String,
    model: String,
}

/// The sessions in `path`: `{"sessions": [{"title", "cwd", "model": "<provider>/<model>"}]}`.
/// An error names the problem and the entry, never anything else from the file.
pub fn load(path: &Path) -> Result<Vec<SessionSpec>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {}", path.display(), e.kind()))?;
    let raw: Raw = serde_json::from_str(&text).map_err(|e| {
        format!(
            "{} is not {{\"sessions\": [{{\"title\", \"cwd\", \"model\"}}]}}: {e}",
            path.display()
        )
    })?;
    let mut specs: Vec<SessionSpec> = Vec::new();
    for (n, entry) in raw.sessions.into_iter().enumerate() {
        let title = entry.title.trim().to_owned();
        if title.is_empty() || title.chars().count() > 200 {
            return Err(format!(
                "session {} needs a title of 1 to 200 characters",
                n + 1
            ));
        }
        if specs.iter().any(|s| s.title == title) {
            return Err(format!("the title {title:?} is used twice"));
        }
        let Some((provider_id, model_id)) = entry
            .model
            .split_once('/')
            .filter(|(p, m)| !p.is_empty() && !m.is_empty())
        else {
            return Err(format!("{title:?}: model must be <provider>/<model>"));
        };
        if entry.cwd.trim().is_empty() {
            return Err(format!("{title:?}: cwd is empty"));
        }
        specs.push(SessionSpec {
            title,
            cwd: entry.cwd,
            provider_id: provider_id.to_owned(),
            model_id: model_id.to_owned(),
        });
    }
    if specs.is_empty() {
        return Err(format!("{} names no session", path.display()));
    }
    Ok(specs)
}

/// Title to OpenCode session id, kept so a restarted adapter holds the same sessions.
pub struct SessionMap {
    path: PathBuf,
}

impl SessionMap {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn get(&self, title: &str) -> Option<String> {
        self.read().remove(title)
    }

    pub fn set(&self, title: &str, session_id: &str) -> std::io::Result<()> {
        let mut map = self.read();
        map.insert(title.to_owned(), session_id.to_owned());
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let text = serde_json::to_string_pretty(&map).map_err(std::io::Error::other)?;
        std::fs::write(&self.path, text)
    }

    fn read(&self) -> BTreeMap<String, String> {
        std::fs::read_to_string(&self.path)
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_model_line_names_its_provider_and_model() {
        let dir = std::env::temp_dir().join(format!("duplex-oc-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("opencode.json");
        std::fs::write(
            &path,
            r#"{"sessions": [
                {"title": "DeepSeek", "cwd": "C:\\w", "model": "opencode/deepseek-v4.1-flash-free"},
                {"title": "GLM", "cwd": "C:\\w", "model": "opencode/glm-5.3-flash-free"}
            ]}"#,
        )
        .unwrap();
        let specs = load(&path).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(
            (specs[0].provider_id.as_str(), specs[0].model_id.as_str()),
            ("opencode", "deepseek-v4.1-flash-free")
        );
        assert_eq!(specs[1].model(), "opencode/glm-5.3-flash-free");
        for bad in [
            r#"{"sessions": [{"title": "A", "cwd": "w", "model": "no-slash"}]}"#,
            r#"{"sessions": [{"title": "A", "cwd": "w", "model": "p/m"}, {"title": "A", "cwd": "w", "model": "p/m"}]}"#,
            r#"{"sessions": []}"#,
            r#"{"threads": []}"#,
        ] {
            std::fs::write(&path, bad).unwrap();
            assert!(load(&path).is_err(), "{bad}");
        }
        let map = SessionMap::new(dir.join("sessions.json"));
        assert_eq!(map.get("DeepSeek"), None);
        map.set("DeepSeek", "ses_1").unwrap();
        map.set("GLM", "ses_2").unwrap();
        assert_eq!(map.get("DeepSeek").as_deref(), Some("ses_1"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
