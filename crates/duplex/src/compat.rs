//! The adapter's version, and the Claude Code and Codex releases it was built against.
//!
//! `duplex-compat.json` beside the crate is the one source. The adapter sends it to the broker
//! at registration with the runtime versions it found, `bridge_status` warns when a runtime is
//! newer than what the adapter was reviewed against, and `duplex compat check` reads the latest
//! published releases and prints the changelog entries a review should read. Exit 0: nothing
//! due; 1: a review is due; 2: a source could not be read.

use regex::Regex;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::OnceLock;

pub const UNKNOWN: &str = "unknown";
const MANIFEST: &str = include_str!("../duplex-compat.json");

pub fn manifest() -> Value {
    serde_json::from_str(MANIFEST).expect("duplex-compat.json is JSON")
}

fn version_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^v?(\d+(?:\.\d+)*)").expect("a fixed pattern"))
}

/// A release number as integers with trailing zeros dropped, so 1.0 equals 1.0.0; a suffix
/// such as -alpha.1 is ignored. None when it is not a version ("unknown").
pub fn version_key(version: &str) -> Option<Vec<u64>> {
    let found = version_re().captures(version.trim())?;
    let mut parts: Vec<u64> = found[1]
        .split('.')
        .map(|p| p.parse().unwrap_or(0))
        .collect();
    while parts.len() > 1 && parts.last() == Some(&0) {
        parts.pop();
    }
    Some(parts)
}

/// True only when both are versions and `candidate` is the later one.
pub fn newer(candidate: &str, base: &str) -> bool {
    match (version_key(candidate), version_key(base)) {
        (Some(a), Some(b)) => a > b,
        _ => false,
    }
}

/// One line per runtime newer than the release the adapter was reviewed against.
pub fn review_due(runtime: &HashMap<String, String>, built_against: &Value) -> Vec<String> {
    let mut names: Vec<&String> = runtime.keys().collect();
    names.sort();
    names
        .into_iter()
        .filter_map(|name| {
            let built = built_against.get(name)?.get("version")?.as_str().unwrap_or("");
            newer(&runtime[name], built).then(|| {
                format!(
                    "{name} {} is newer than the {built} this adapter was built against: run `duplex compat check` and review it",
                    runtime[name]
                )
            })
        })
        .collect()
}

/// What the adapter tells the broker about itself at registration. The commit is set when
/// the binary is built (`DUPLEX_COMMIT`); a build without it says so.
pub fn adapter_info(runtime: &HashMap<String, String>) -> Value {
    json!({
        "version": env!("CARGO_PKG_VERSION"),
        "commit": option_env!("DUPLEX_COMMIT").unwrap_or(UNKNOWN),
        "built_against": manifest()["built_against"],
        "runtime": runtime,
    })
}

// The release check. Entries are kept when they mention what the adapter rests on: for
// Claude Code the cross-session inbox and the session lifecycle around it, for Codex the
// queue, the app-server daemon and thread ownership.
fn claude_terms() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)\binbox|SendMessage|ListAgents|cross-session|remote[ -]control|\bidle\b|\bwak|notification|\bsocket|\bMCP servers?\b.*\b(start|restart|stop|exit|spawn|probe|handshake|reconnect|lifecycle|die)",
        )
        .expect("a fixed pattern")
    })
}

fn codex_terms() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)\bqueue|app-server|app server|daemon|\bthread|\bresume|\bwak|message board|\bMCP\b").expect("a fixed pattern"))
}

pub const NPM: &str = "https://registry.npmjs.org/{package}/latest";
pub const CLAUDE_PACKAGE: &str = "@anthropic-ai%2fclaude-code";
pub const CODEX_PACKAGE: &str = "@openai%2fcodex";
pub const CLAUDE_CHANGELOG: &str =
    "https://raw.githubusercontent.com/anthropics/claude-code/main/CHANGELOG.md";
pub const CODEX_RELEASES: &str = "https://api.github.com/repos/openai/codex/releases?per_page=100";

pub type Section = (String, Vec<String>);

/// `## <version>` headings with their bullet entries, newest first as published.
pub fn claude_sections(changelog: &str) -> Vec<Section> {
    let mut sections: Vec<Section> = Vec::new();
    for line in changelog.lines() {
        if let Some(rest) = line.strip_prefix("##") {
            let version = rest.split_whitespace().next();
            if let (Some(version), true) = (version, rest.starts_with(char::is_whitespace)) {
                sections.push((version.to_owned(), Vec::new()));
                continue;
            }
        }
        let Some((_, entries)) = sections.last_mut() else {
            continue;
        };
        if let Some(entry) = line.strip_prefix("- ") {
            entries.push(entry.trim().to_owned());
        } else if line.starts_with("  ") && !line.trim().is_empty() && !entries.is_empty() {
            let last = entries.len() - 1;
            entries[last] = format!("{} {}", entries[last], line.trim());
        }
    }
    sections
}

/// Stable `rust-v<version>` releases with the bullet lines of their notes.
pub fn codex_sections(releases: &Value) -> Vec<Section> {
    let tag = Regex::new(r"^rust-v(\d+\.\d+\.\d+)$").expect("a fixed pattern");
    releases
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|release| {
            let found = tag.captures(
                release
                    .get("tag_name")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
            )?;
            let truthy = |key: &str| release.get(key).and_then(Value::as_bool).unwrap_or(false);
            if truthy("prerelease") || truthy("draft") {
                return None;
            }
            let lines = release
                .get("body")
                .and_then(Value::as_str)
                .unwrap_or("")
                .lines()
                .map(str::trim)
                .filter_map(|l| l.strip_prefix("- ").or_else(|| l.strip_prefix("* ")))
                .map(|l| l.trim().to_owned())
                .collect();
            Some((found[1].to_owned(), lines))
        })
        .collect()
}

/// Sections later than `base`, up to and including `latest`.
pub fn between(sections: &[Section], base: &str, latest: &str) -> Vec<Section> {
    sections
        .iter()
        .filter(|(v, _)| newer(v, base) && !newer(v, latest))
        .cloned()
        .collect()
}

/// The entries of each section that mention `terms`; sections with none are dropped.
pub fn matching(sections: &[Section], terms: &Regex) -> Vec<Section> {
    sections
        .iter()
        .filter_map(|(v, lines)| {
            let hits: Vec<String> = lines
                .iter()
                .filter(|l| terms.is_match(l))
                .cloned()
                .collect();
            (!hits.is_empty()).then(|| (v.clone(), hits))
        })
        .collect()
}

/// Reads a URL as text: the release check's only network use.
pub type Fetch<'a> = &'a dyn Fn(&str) -> Result<String, String>;

/// The release sections from `base` (exclusive) to `latest`. An error when the source cannot
/// show the whole range, so a partial read is never a quiet pass.
fn notes(name: &str, base: &str, latest: &str, fetch: Fetch<'_>) -> Result<Vec<Section>, String> {
    let sections = if name == "claude_code" {
        claude_sections(&fetch(CLAUDE_CHANGELOG)?)
    } else {
        let doc: Value =
            serde_json::from_str(&fetch(CODEX_RELEASES)?).map_err(|e| e.to_string())?;
        codex_sections(&doc)
    };
    if !sections
        .iter()
        .any(|(v, _)| version_key(v) == version_key(latest))
    {
        return Err(format!("the {name} notes have no entry for {latest}"));
    }
    if !sections.iter().any(|(v, _)| !newer(v, base)) {
        return Err(format!("the {name} notes do not reach back to {base}"));
    }
    Ok(between(&sections, base, latest))
}

/// Compares the published releases with the ones the adapter was built against. Returns the
/// exit status: 0 nothing due, 1 a review is due, 2 a source could not be read.
pub fn check(fetch: Fetch<'_>, manifest: &Value, out: &mut dyn FnMut(&str)) -> i32 {
    let mut status = 0;
    for (name, package, terms) in [
        ("claude_code", CLAUDE_PACKAGE, claude_terms()),
        ("codex", CODEX_PACKAGE, codex_terms()),
    ] {
        let built = &manifest["built_against"][name];
        let base = built["version"].as_str().unwrap_or("");
        let latest = fetch(&NPM.replace("{package}", package))
            .and_then(|body| serde_json::from_str::<Value>(&body).map_err(|e| e.to_string()))
            .and_then(|doc| {
                doc["version"]
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| "no version in the answer".to_owned())
            });
        let latest = match latest {
            Ok(latest) => latest,
            Err(err) => {
                out(&format!(
                    "{name}: UNREADABLE latest release from npm: {err}"
                ));
                status = 2;
                continue;
            }
        };
        if version_key(&latest).is_none() {
            out(&format!(
                "{name}: UNREADABLE npm answered {latest:?}, not a version"
            ));
            status = 2;
            continue;
        }
        if !newer(&latest, base) {
            out(&format!(
                "{name}: latest {latest}, built against {base}: nothing to review"
            ));
            continue;
        }
        status = status.max(1);
        out(&format!(
            "{name}: REVIEW DUE: latest {latest}, built against {base} (reviewed {})",
            built["reviewed"].as_str().unwrap_or("")
        ));
        let sections = match notes(name, base, &latest, fetch) {
            Ok(sections) => sections,
            Err(err) => {
                out(&format!("{name}: UNREADABLE release notes: {err}"));
                status = 2;
                continue;
            }
        };
        out(&format!(
            "  releases since {base}: {}",
            sections
                .iter()
                .map(|(v, _)| v.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        let hits = matching(&sections, terms);
        if hits.is_empty() {
            out("  no entry mentions the messaging terms");
        }
        for (version, lines) in hits {
            out(&format!("  {version}"));
            for line in lines {
                out(&format!("    - {line}"));
            }
        }
    }
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_compare_as_numbers_and_unknown_is_never_newer() {
        assert!(newer("2.1.322", "2.1.321"));
        assert!(newer("0.167.1", "0.166.0"));
        assert!(newer("2.10.0", "2.9.9"));
        assert!(!newer("1.0", "1.0.0"));
        assert!(!newer("1.0.0-alpha.1", "1.0"));
        assert!(newer("v1.2", "1.1.9"));
        assert!(!newer("unknown", "1.0"));
        assert!(!newer("2.0", "unknown"));
        assert_eq!(version_key("0.155.0-alpha.16"), Some(vec![0, 155]));
    }

    #[test]
    fn a_runtime_newer_than_the_review_is_named() {
        let built = manifest()["built_against"].clone();
        let runtime = |claude: &str| {
            HashMap::from([
                ("claude_code".to_owned(), claude.to_owned()),
                ("codex".to_owned(), "0.167.1".to_owned()),
            ])
        };
        assert!(review_due(&runtime("2.1.321"), &built).is_empty());
        let due = review_due(&runtime("2.2.0"), &built);
        assert_eq!(due.len(), 1);
        assert!(due[0].starts_with(
            "claude_code 2.2.0 is newer than the 2.1.321 this adapter was built against"
        ));
        assert!(review_due(&runtime(UNKNOWN), &built).is_empty());
    }

    #[test]
    fn registration_carries_the_version_the_releases_and_the_runtime() {
        let info = adapter_info(&HashMap::from([("codex".to_owned(), "0.167.1".to_owned())]));
        assert_eq!(info["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(info["built_against"]["claude_code"]["version"], "2.1.321");
        assert_eq!(info["runtime"]["codex"], "0.167.1");
        assert!(info["commit"].is_string());
    }

    #[test]
    fn the_notes_are_cut_into_sections() {
        let changelog = "# Changelog\n\n## 2.1.322\n\n- Fixed the inbox closing early\n  on a slow client\n- Other\n\n## 2.1.321\n\n- Older\n";
        let sections = claude_sections(changelog);
        assert_eq!(sections.len(), 2);
        assert_eq!(
            sections[0],
            (
                "2.1.322".to_owned(),
                vec![
                    "Fixed the inbox closing early on a slow client".to_owned(),
                    "Other".to_owned()
                ]
            )
        );
        let releases = json!([
            {"tag_name": "rust-v0.168.0", "body": "- queue now resumes\n* other\nplain"},
            {"tag_name": "rust-v0.169.0-alpha.1", "prerelease": true, "body": "- x"},
            {"tag_name": "rust-v0.167.1", "draft": true, "body": "- y"},
            {"tag_name": "not-a-release", "body": "- z"},
        ]);
        assert_eq!(
            codex_sections(&releases),
            [(
                "0.168.0".to_owned(),
                vec!["queue now resumes".to_owned(), "other".to_owned()]
            )]
        );
        assert_eq!(between(&sections, "2.1.321", "2.1.322").len(), 1);
        let hits = matching(&sections, claude_terms());
        assert_eq!(hits.len(), 1, "only the entry that names the inbox");
    }

    fn feed<'a>(
        npm_claude: &'a str,
        npm_codex: &'a str,
        changelog: &'a str,
        releases: &'a str,
    ) -> impl Fn(&str) -> Result<String, String> + 'a {
        move |url: &str| match url {
            u if u.contains("claude-code/latest")
                || u.contains("claude-code%2f") && u.contains("latest") =>
            {
                Ok(npm_claude.to_owned())
            }
            u if u.contains("codex") && u.contains("latest") => Ok(npm_codex.to_owned()),
            CLAUDE_CHANGELOG => Ok(changelog.to_owned()),
            CODEX_RELEASES => Ok(releases.to_owned()),
            _ => Err(format!("no such url {url}")),
        }
    }

    #[test]
    fn the_check_exits_by_what_it_finds() {
        let manifest = manifest();
        let changelog = "## 2.1.322\n- The inbox changed\n## 2.1.321\n- old\n";
        let releases = r#"[{"tag_name": "rust-v0.168.0", "body": "- queue changed"}, {"tag_name": "rust-v0.167.1", "body": "- old"}]"#;
        let mut lines: Vec<String> = Vec::new();
        let same = feed(
            r#"{"version": "2.1.321"}"#,
            r#"{"version": "0.167.1"}"#,
            changelog,
            releases,
        );
        assert_eq!(
            check(&same, &manifest, &mut |l| lines.push(l.to_owned())),
            0
        );
        assert!(lines[0].ends_with("nothing to review"));
        lines.clear();
        let newer_both = feed(
            r#"{"version": "2.1.322"}"#,
            r#"{"version": "0.168.0"}"#,
            changelog,
            releases,
        );
        assert_eq!(
            check(&newer_both, &manifest, &mut |l| lines.push(l.to_owned())),
            1
        );
        assert!(
            lines
                .iter()
                .any(|l| l.starts_with("claude_code: REVIEW DUE"))
        );
        assert!(lines.iter().any(|l| l == "    - The inbox changed"));
        assert!(lines.iter().any(|l| l == "    - queue changed"));
        lines.clear();
        let unreadable = feed("not json", r#"{"version": "latest"}"#, changelog, releases);
        assert_eq!(
            check(&unreadable, &manifest, &mut |l| lines.push(l.to_owned())),
            2
        );
        lines.clear();
        let short = feed(
            r#"{"version": "2.1.323"}"#,
            r#"{"version": "0.167.1"}"#,
            changelog,
            releases,
        );
        assert_eq!(
            check(&short, &manifest, &mut |l| lines.push(l.to_owned())),
            2,
            "notes that stop short of the latest release"
        );
        assert!(lines.iter().any(|l| l.contains("no entry for 2.1.323")));
    }
}
