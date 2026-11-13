//! Redaction of a message's text and of every tool result: the shapes a credential usually
//! has, and any run of 32 or more hex digits. Broker IDs that address a reply stay readable.

use regex::Regex;
use serde_json::{Map, Value};
use std::sync::OnceLock;

struct Pattern {
    label: &'static str,
    re: Regex,
}

fn patterns() -> &'static [Pattern] {
    static PATTERNS: OnceLock<Vec<Pattern>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        let shapes = [
            (
                "vault token",
                r"\b(?:hvs|hvb|hvr)\.[A-Za-z0-9_\-]{20,}\b|\bs\.[A-Za-z0-9]{24}\b",
            ),
            ("private key block", r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
            ("aws access key", r"\bAKIA[0-9A-Z]{16}\b"),
            ("github token", r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),
            ("bearer token", r"(?i)\bbearer\s+[A-Za-z0-9\-_.]{30,}"),
            // NAME = value where the name says secret and the value is long and not a
            // placeholder (`<...>`, `${...}` and `[redacted...]` are not in the value class).
            (
                "credential assignment",
                r#"(?i)\b[A-Z0-9_]*(?:PASSWORD|PASSWD|SECRET|TOKEN|API[_-]?KEY)\b\s*[:=]\s*["']?[A-Za-z0-9+/=_\-]{16,}"#,
            ),
        ];
        shapes
            .into_iter()
            .map(|(label, re)| Pattern {
                label,
                re: Regex::new(re).expect("a fixed pattern"),
            })
            .collect()
    })
}

fn blob() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"[A-Za-z0-9+/]{48,}={0,2}").expect("a fixed pattern"))
}

fn hex_run() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"[0-9a-fA-F]{32,}").expect("a fixed pattern"))
}

/// Replaces anything shaped like a credential with `[redacted:<label>]`.
pub fn redact(text: &str) -> String {
    let mut text = text.to_owned();
    for pattern in patterns() {
        text = pattern
            .re
            .replace_all(&text, format!("[redacted:{}]", pattern.label).as_str())
            .into_owned();
    }
    // 48 or more base64-like characters that are not all hex (a git sha is 40 hex): a match
    // starts at the start of its run, so what precedes it is not in the class.
    text = blob()
        .replace_all(&text, |found: &regex::Captures<'_>| {
            let run = &found[0];
            if run.len() >= 48 && run[..48].chars().all(|c| c.is_ascii_hexdigit()) {
                run.to_owned()
            } else {
                "[redacted:long opaque blob]".to_owned()
            }
        })
        .into_owned();
    hex_run()
        .replace_all(&text, "[redacted:hex run]")
        .into_owned()
}

const ID_KEYS: [&str; 6] = [
    "id",
    "binding_id",
    "conversation_id",
    "delivery_id",
    "message_id",
    "reply_to",
];

fn is_stored_id(value: &str) -> bool {
    value.len() == 32 && value.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'))
}

/// Redacts every string inside a JSON value, but keeps the broker IDs needed for replies.
pub fn redact_duplex_result(value: &Value) -> Value {
    match value {
        Value::String(text) => Value::String(redact(text)),
        Value::Object(map) => {
            let mut out = Map::new();
            for (key, item) in map {
                let kept =
                    ID_KEYS.contains(&key.as_str()) && item.as_str().is_some_and(is_stored_id);
                out.insert(
                    key.clone(),
                    if kept {
                        item.clone()
                    } else {
                        redact_duplex_result(item)
                    },
                );
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(redact_duplex_result).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn credential_shapes_are_replaced_by_their_label() {
        let cases = [
            (
                "token hvs.CAESIabcdefghijklmnopqrstuvwxyz1234",
                "vault token",
            ),
            ("-----BEGIN RSA PRIVATE KEY-----", "private key block"),
            ("key AKIAABCDEFGHIJKLMNOP here", "aws access key"),
            ("ghp_abcdefghijklmnopqrstuvwxyz0123456789", "github token"),
            (
                "Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789",
                "bearer token",
            ),
            (
                "API_KEY = \"abcdefghijklmnop1234\"",
                "credential assignment",
            ),
            (
                "DB_PASSWORD: hunter2hunter2hunter2",
                "credential assignment",
            ),
            (&"Zq9/".repeat(14), "long opaque blob"),
            (&"0123456789abcdef".repeat(2), "hex run"),
        ];
        for (text, label) in cases {
            let out = redact(text);
            assert!(
                out.contains(&format!("[redacted:{label}]")),
                "{text} -> {out}"
            );
        }
    }

    #[test]
    fn placeholders_words_and_short_values_stay() {
        for text in [
            "password",
            "TOKEN=<your token>",
            "API_KEY=${KEY}",
            "SECRET: [redacted:x]",
            "a git sha 0123456789abcdef0123456789abcdef01234567 is 40 hex",
            "short ghp_abc",
            "Bearer short",
        ] {
            let kept = redact(text);
            // The 40-digit sha is a hex run too (32 or more); only the rest must stay.
            if text.contains("0123456789abcdef0123") {
                assert!(kept.contains("[redacted:hex run]"));
            } else {
                assert_eq!(kept, text);
            }
        }
        // 48 hex digits are a hex run, not an opaque blob.
        assert_eq!(redact(&"ab".repeat(24)), "[redacted:hex run]");
    }

    #[test]
    fn reply_ids_survive_in_their_keys_and_nowhere_else() {
        let id = "6058b30915944089b93e6d61672f5134";
        let out = redact_duplex_result(&json!({
            "message_id": id,
            "reply_to": id,
            "text": format!("see {id}"),
            "other": id,
            "nested": [{"conversation_id": id}],
            "upper": {"id": id.to_uppercase()},
            "n": 3,
        }));
        assert_eq!(out["message_id"], id);
        assert_eq!(out["reply_to"], id);
        assert_eq!(out["nested"][0]["conversation_id"], id);
        assert_eq!(out["text"], "see [redacted:hex run]");
        assert_eq!(out["other"], "[redacted:hex run]");
        assert_eq!(out["upper"]["id"], "[redacted:hex run]");
        assert_eq!(out["n"], 3);
    }
}
