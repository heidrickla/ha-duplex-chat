//! `duplex owner`: owner-side broker calls from this machine, as an enrolled owner device.
//! Never prints the token.

use crate::cap::dumps;
use crate::config::config_dir;
use duplex_proto::client::{Method, quote};
use duplex_proto::ids::match_sessions;
use duplex_proto::{BrokerClient, BrokerError};
use jiff::Timestamp;
use serde_json::{Value, json};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const USAGE: &str = "\
duplex owner enroll --broker <url> --pairing-code <code> [--name <device name>]
                                                   enroll this machine as an owner device
duplex owner sessions [--json]                     each with its adapter version and \"behind\"
duplex owner release <session>                     its next registration takes the binding
duplex owner retire <session>                      retire a binding, named by id or title
duplex owner group <title> <session>...            create a group with those sessions
duplex owner detail <conversation>
duplex owner mode <conversation> user_directed|consult
duplex owner collaborate <conversation> <objective> [turns] [minutes]
duplex owner stop <conversation>
duplex owner archive|restore <conversation>        remove a group from the lists, or bring it back
duplex owner refusals                              what the broker refused in the last day, and why
duplex owner subscriptions                         connector subscriptions, renewals, last event
duplex owner tasks [--all]                         delegated tasks: open ones, or every one
duplex owner task <id>                             one task with its questions and events
duplex owner task-cancel <id>                      cancel a task and every task below it
duplex owner limits                                usage-limit resumes and what became of each
duplex owner waker                                 the waker's recent starts of closed sessions
duplex owner list                                  conversations the owner sees
duplex owner post <conversation> <text> <session>...
duplex owner events <conversation> [after]
duplex owner deliveries <conversation>             each delivery's latest state and message start
duplex owner redirect <delivery> <state> <session> the owner's message again, to the right session

Sessions are named by title or id. The device file is ~/.duplex-chat/owner-device.json.";

/// Why a command did not finish: the text goes to stderr.
#[derive(Debug, PartialEq)]
pub enum Failure {
    /// The arguments are not a command: print the usage.
    Usage,
    Said(String),
}

impl From<String> for Failure {
    fn from(text: String) -> Self {
        Self::Said(text)
    }
}

type Done = Result<(), Failure>;

pub fn device_path() -> PathBuf {
    config_dir().join("owner-device.json")
}

/// The broker and the token of an enrolled owner device.
struct Device {
    client: BrokerClient,
    token: String,
}

impl Device {
    fn load(path: &Path) -> Result<Self, Failure> {
        let text = std::fs::read_to_string(path).map_err(|_| {
            Failure::Said(format!(
                "no owner device at {}: run `duplex owner enroll` first",
                path.display()
            ))
        })?;
        let doc: Value = serde_json::from_str(&text)
            .map_err(|_| Failure::Said(format!("{} is not JSON", path.display())))?;
        let (Some(url), Some(token)) = (doc["broker_url"].as_str(), doc["token"].as_str()) else {
            return Err(Failure::Said(format!(
                "{} has no broker_url and token",
                path.display()
            )));
        };
        Ok(Self {
            client: BrokerClient::new(url, Duration::from_secs(15)),
            token: token.to_owned(),
        })
    }

    fn call(&self, method: Method, path: &str, body: Option<&Value>) -> Result<Value, Failure> {
        self.client
            .call(method, path, body, &self.token)
            .map_err(|e| match e {
                BrokerError::Status { code, detail } => Failure::Said(format!(
                    "HTTP {code}: {}",
                    detail.chars().take(300).collect::<String>()
                )),
                other => Failure::Said(other.to_string()),
            })
    }

    fn get(&self, path: &str) -> Result<Value, Failure> {
        self.call(Method::Get, path, None)
    }

    fn post(&self, path: &str, body: Option<&Value>) -> Result<Value, Failure> {
        self.call(Method::Post, path, body)
    }

    /// The id of the one active session a typed name picks out, by the broker's rule.
    fn resolve(&self, sessions: &Value, name: &str) -> Result<String, Failure> {
        let active: Vec<&Value> = sessions
            .as_array()
            .into_iter()
            .flatten()
            .filter(|s| s.get("active").is_none_or(|a| a != &Value::Bool(false)))
            .collect();
        let pairs: Vec<(String, String)> = active
            .iter()
            .map(|s| (text(&s["id"]), text(&s["title"])))
            .collect();
        let found = match_sessions(&pairs, name);
        match found.as_slice() {
            [one] => Ok(pairs[*one].0.clone()),
            many => Err(Failure::Said(format!(
                "{} matches {} active sessions",
                quote_py(name),
                many.len()
            ))),
        }
    }

    fn session_ids(&self, names: &[String]) -> Result<Vec<String>, Failure> {
        let sessions = self.get("/api/sessions")?;
        names.iter().map(|n| self.resolve(&sessions, n)).collect()
    }
}

/// A JSON value as Python's `print` shows it: strings bare, null as None.
fn text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "None".to_owned(),
        Value::Bool(b) => if *b { "True" } else { "False" }.to_owned(),
        other => other.to_string(),
    }
}

fn quote_py(name: &str) -> String {
    format!("'{}'", name.replace('\'', "\\'"))
}

/// A list of strings as Python's `print` shows it.
fn list_repr(items: &[String]) -> String {
    format!(
        "[{}]",
        items
            .iter()
            .map(|i| quote_py(i))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn utc(at: &Value) -> String {
    at.as_f64()
        .and_then(|secs| Timestamp::from_second(secs.floor() as i64).ok())
        .map_or_else(
            || "-".to_owned(),
            |t| t.strftime("%Y-%m-%dT%H:%M:%SZ").to_string(),
        )
}

fn or_dash(value: &Value) -> String {
    if value.is_null() || value.as_str() == Some("") {
        "-".to_owned()
    } else {
        text(value)
    }
}

fn rows(value: Value) -> Vec<Value> {
    value.as_array().cloned().unwrap_or_default()
}

fn first_n(text: &str, n: usize) -> String {
    text.chars().take(n).collect()
}

fn arg(args: &[String], at: usize) -> Result<&str, Failure> {
    args.get(at).map(String::as_str).ok_or(Failure::Usage)
}

fn show(out: &mut dyn Write, value: &Value) {
    let _ = writeln!(out, "{}", dumps(value, Some(1)));
}

fn line(out: &mut dyn Write, parts: &[String]) {
    let _ = writeln!(out, "{}", parts.join(" "));
}

fn enroll(args: &[String], out: &mut dyn Write, path: &Path) -> Done {
    let (mut broker, mut code, mut name) = (None, None, "owner".to_owned());
    let mut rest = args.iter();
    while let Some(flag) = rest.next() {
        let value = rest.next().ok_or(Failure::Usage)?;
        match flag.as_str() {
            "--broker" => broker = Some(value.clone()),
            "--pairing-code" => code = Some(value.clone()),
            "--name" => name = value.clone(),
            _ => return Err(Failure::Usage),
        }
    }
    let (Some(broker), Some(code)) = (broker, code) else {
        return Err(Failure::Usage);
    };
    let client = BrokerClient::new(&broker, Duration::from_secs(15));
    let answer = client
        .post(
            "/api/device/enroll",
            Some(&json!({"pairing_code": code, "device_name": name})),
            "",
        )
        .map_err(|e| {
            Failure::Said(match e {
                BrokerError::Status { code, detail } => format!("HTTP {code}: {detail}"),
                other => other.to_string(),
            })
        })?;
    let token = answer["token"]
        .as_str()
        .ok_or_else(|| Failure::Said("the broker sent no device token".into()))?;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| Failure::Said(format!("cannot create {}: {}", dir.display(), e.kind())))?;
    }
    let doc = json!({"broker_url": client.base_url(), "token": token});
    std::fs::write(path, doc.to_string())
        .map_err(|e| Failure::Said(format!("cannot write {}: {}", path.display(), e.kind())))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    let _ = writeln!(
        out,
        "enrolled {name} with {}; the device token is in {}",
        client.base_url(),
        path.display()
    );
    Ok(())
}

fn sessions(device: &Device, args: &[String], out: &mut dyn Write) -> Done {
    let all = device.get("/api/sessions")?;
    if args.first().map(String::as_str) == Some("--json") {
        show(out, &all);
        return Ok(());
    }
    for s in rows(all) {
        // A built-in member has no adapter ("-"); an adapter that reports nothing is version 0.
        let built_in = s.get("adapter_behind").is_none_or(Value::is_null);
        let version = s["adapter"]["version"]
            .as_str()
            .filter(|v| !v.is_empty())
            .map_or_else(
                || if built_in { "-" } else { "0" }.to_owned(),
                str::to_owned,
            );
        let state = if s["active"].as_bool().unwrap_or(false) {
            "active"
        } else {
            "retired"
        };
        let behind = if s["adapter_behind"].as_bool() == Some(true) {
            " behind"
        } else {
            ""
        };
        line(
            out,
            &[
                text(&s["id"]),
                text(&s["provider"]),
                text(&s["title"]),
                state.to_owned(),
                format!("adapter {version}{behind}"),
            ],
        );
    }
    Ok(())
}

fn task_view(device: &Device, id: &str, out: &mut dyn Write) -> Done {
    let t = device.get(&format!("/api/tasks/{}", quote(id, "")))?;
    line(
        out,
        &[
            text(&t["id"]),
            text(&t["status"]),
            or_dash(&t["outcome"]),
            format!(
                "{} -> {}",
                text(&t["requester"]["title"]),
                text(&t["assignee"]["title"])
            ),
        ],
    );
    line(out, &["task:".into(), text(&t["task"])]);
    line(out, &["done when:".into(), text(&t["done_when"])]);
    line(
        out,
        &[
            "conversation:".into(),
            text(&t["conversation_id"]),
            "parent:".into(),
            or_dash(&t["parent_task_id"]),
        ],
    );
    line(
        out,
        &[
            "created".into(),
            text(&t["created_at"]),
            "ended".into(),
            or_dash(&t["ended_at"]),
            "result read".into(),
            or_dash(&t["delivered_at"]),
        ],
    );
    for child in rows(t["children"].clone()) {
        line(
            out,
            &["child".into(), text(&child["id"]), text(&child["status"])],
        );
    }
    for q in rows(t["questions"].clone()) {
        line(
            out,
            &[
                "open".into(),
                text(&q["kind"]),
                "question".into(),
                text(&q["id"]),
                text(&q["text"]),
            ],
        );
    }
    if !t["result"].is_null() {
        line(out, &["result:".into(), text(&t["result"])]);
    }
    for e in rows(t["events"].clone()) {
        line(
            out,
            &[
                text(&e["seq"]),
                text(&e["created_at"]),
                text(&e["type"]),
                first_n(&text(&e["actor"]["id"]), 8),
                dumps(&e["payload"], None),
            ],
        );
    }
    Ok(())
}

fn deliveries(device: &Device, conversation: &str, out: &mut dyn Write) -> Done {
    let mut texts: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut latest: Vec<(String, (String, String, String))> = Vec::new();
    let events = device.get(&format!(
        "/api/conversations/{}/events?after=0&limit=1000",
        quote(conversation, "")
    ))?;
    for e in rows(events) {
        let p = &e["payload"];
        match e["type"].as_str() {
            Some("message.accepted") => {
                texts.insert(
                    text(&e["message_id"]),
                    p["text"].as_str().unwrap_or("").to_owned(),
                );
            }
            Some("delivery.changed") => {
                let entry = (
                    text(&e["message_id"]),
                    text(&p["binding_id"]),
                    text(&p["state"]),
                );
                let id = text(&p["delivery_id"]);
                match latest.iter_mut().find(|(d, _)| *d == id) {
                    Some(slot) => slot.1 = entry,
                    None => latest.push((id, entry)),
                }
            }
            _ => {}
        }
    }
    let titles: std::collections::HashMap<String, String> = rows(device.get("/api/sessions")?)
        .iter()
        .map(|s| (text(&s["id"]), text(&s["title"])))
        .collect();
    for (delivery, (message, binding, state)) in latest {
        let snippet = first_n(
            &texts
                .get(&message)
                .cloned()
                .unwrap_or_default()
                .replace('\n', " "),
            60,
        );
        let who = titles
            .get(&binding)
            .cloned()
            .unwrap_or_else(|| first_n(&binding, 8));
        line(out, &[delivery, state, who, snippet]);
    }
    Ok(())
}

fn events(device: &Device, args: &[String], out: &mut dyn Write) -> Done {
    let after: u64 = args
        .get(1)
        .map_or(Ok(0), |a| a.parse())
        .map_err(|_| Failure::Usage)?;
    let list = device.get(&format!(
        "/api/conversations/{}/events?after={after}&limit=500",
        quote(arg(args, 0)?, "")
    ))?;
    for e in rows(list) {
        let p = &e["payload"];
        let summary = ["text", "state", "reason", "mode", "objective"]
            .iter()
            .map(|k| &p[*k])
            .find(|v| !v.is_null() && v.as_str() != Some(""))
            .map_or_else(String::new, text);
        let to = match (&p["recipients"], &p["binding_id"]) {
            (Value::Array(r), _) if !r.is_empty() => {
                list_repr(&r.iter().map(text).collect::<Vec<_>>())
            }
            (_, b) if !b.is_null() => text(b),
            _ => String::new(),
        };
        line(
            out,
            &[
                text(&e["seq"]),
                text(&e["type"]),
                text(&e["actor"]["kind"]),
                first_n(&text(&e["actor"]["id"]), 8),
                if e["message_id"].is_null() {
                    String::new()
                } else {
                    text(&e["message_id"])
                },
                to,
                first_n(&summary, 120),
            ],
        );
    }
    Ok(())
}

fn collaborate(device: &Device, args: &[String], out: &mut dyn Write) -> Done {
    let turns: i64 = args
        .get(2)
        .map_or(Ok(6), |t| t.parse())
        .map_err(|_| Failure::Usage)?;
    let minutes: f64 = args
        .get(3)
        .map_or(Ok(10.0), |m| m.parse())
        .map_err(|_| Failure::Usage)?;
    let body = json!({"objective": arg(args, 1)?, "max_turns": turns, "minutes": minutes});
    show(
        out,
        &device.post(
            &format!(
                "/api/conversations/{}/collaboration",
                quote(arg(args, 0)?, "")
            ),
            Some(&body),
        )?,
    );
    Ok(())
}

fn post(device: &Device, args: &[String], out: &mut dyn Write) -> Done {
    let conversation = arg(args, 0)?;
    let body = json!({"client_message_id": client_message_id(), "text": arg(args, 1)?, "recipients": device.session_ids(&args[2..])?});
    let result = device.post(
        &format!("/api/conversations/{}/messages", quote(conversation, "")),
        Some(&body),
    )?;
    let bindings: Vec<String> = rows(result["deliveries"].clone())
        .iter()
        .map(|d| text(&d["binding_id"]))
        .collect();
    line(out, &[text(&result["message"]["id"]), list_repr(&bindings)]);
    Ok(())
}

fn client_message_id() -> String {
    use sha2::{Digest, Sha256};
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let seed = format!("owner-{nanos}-{}", std::process::id());
    Sha256::digest(seed.as_bytes())
        .iter()
        .take(16)
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Runs one owner command. `path` is the device file.
pub fn run(args: &[String], out: &mut dyn Write, path: &Path) -> Done {
    let Some((verb, rest)) = args.split_first() else {
        return Err(Failure::Usage);
    };
    if verb == "enroll" {
        return enroll(rest, out, path);
    }
    let device = Device::load(path)?;
    let conversation = |i: usize| -> Result<String, Failure> { Ok(quote(arg(rest, i)?, "")) };
    match verb.as_str() {
        "sessions" => sessions(&device, rest, out)?,
        "release" | "retire" => {
            let ids = device.session_ids(&rest[..rest.len().min(1)])?;
            let id = ids.first().ok_or(Failure::Usage)?;
            let done = device.post(&format!("/api/sessions/{id}/{verb}"), None)?;
            let past = if verb == "release" {
                "released"
            } else {
                "retired"
            };
            line(out, &[past.into(), text(&done["id"]), text(&done["title"])]);
        }
        "group" => {
            let created =
                device.post("/api/conversations", Some(&json!({"title": arg(rest, 0)?})))?;
            let id = text(&created["id"]);
            for binding in device.session_ids(&rest[1..])? {
                device.post(
                    &format!("/api/conversations/{id}/members"),
                    Some(&json!({"binding_id": binding})),
                )?;
            }
            show(out, &device.get(&format!("/api/conversations/{id}"))?);
        }
        "detail" => show(
            out,
            &device.get(&format!("/api/conversations/{}", conversation(0)?))?,
        ),
        "mode" => show(
            out,
            &device.post(
                &format!("/api/conversations/{}/mode", conversation(0)?),
                Some(&json!({"mode": arg(rest, 1)?})),
            )?,
        ),
        "collaborate" => collaborate(&device, rest, out)?,
        "stop" => show(
            out,
            &device.call(
                Method::Delete,
                &format!("/api/conversations/{}/collaboration", conversation(0)?),
                None,
            )?,
        ),
        "archive" | "restore" => show(
            out,
            &device.post(
                &format!("/api/conversations/{}/{verb}", conversation(0)?),
                None,
            )?,
        ),
        "refusals" => {
            for r in rows(device.get("/api/refusals")?) {
                line(
                    out,
                    &[
                        utc(&r["at"]),
                        text(&r["source"]),
                        text(&r["what"]),
                        "-".into(),
                        text(&r["reason"]),
                    ],
                );
            }
        }
        "subscriptions" => {
            for r in rows(device.get("/api/subscriptions")?) {
                let status = if r["last_status"].is_null() {
                    "-".to_owned()
                } else {
                    text(&r["last_status"])
                };
                line(
                    out,
                    &[
                        text(&r["id"]),
                        text(&r["host"]),
                        "created".into(),
                        utc(&r["created_at"]),
                        "renewals".into(),
                        text(&r["renewals"]),
                        "last".into(),
                        utc(&r["renewed_at"]),
                        "until".into(),
                        utc(&r["refresh_before"]),
                        "| event".into(),
                        or_dash(&r["last_event_id"]),
                        utc(&r["last_event_at"]),
                        status,
                    ],
                );
            }
        }
        "tasks" => {
            let open = if rest.first().map(String::as_str) == Some("--all") {
                "0"
            } else {
                "1"
            };
            for t in rows(device.get(&format!("/api/tasks?open={open}"))?) {
                let snippet = first_n(
                    &text(&t["task"])
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" "),
                    60,
                );
                line(
                    out,
                    &[
                        text(&t["id"]),
                        text(&t["status"]),
                        format!(
                            "{} -> {}",
                            text(&t["requester"]["title"]),
                            text(&t["assignee"]["title"])
                        ),
                        snippet,
                    ],
                );
            }
        }
        "task" => task_view(&device, arg(rest, 0)?, out)?,
        "task-cancel" => {
            let t = device.post(
                &format!("/api/tasks/{}/cancel", quote(arg(rest, 0)?, "")),
                Some(&json!({})),
            )?;
            line(
                out,
                &["cancelled".into(), text(&t["id"]), text(&t["status"])],
            );
        }
        "limits" => {
            for r in rows(device.get("/api/limits")?) {
                let reason = if r["reason"].is_null() {
                    String::new()
                } else {
                    text(&r["reason"])
                };
                line(
                    out,
                    &[
                        text(&r["id"]),
                        text(&r["state"]),
                        text(&r["source"]),
                        "failed".into(),
                        utc(&r["failed_at"]),
                        "reset".into(),
                        utc(&r["reset_at"]),
                        reason,
                    ],
                );
            }
        }
        "waker" => {
            for w in rows(device.get("/api/waker/starts")?) {
                let outcome = if w["ok"].as_bool().unwrap_or(false) {
                    "ok"
                } else {
                    "failed"
                };
                line(
                    out,
                    &[
                        text(&w["at"]),
                        outcome.into(),
                        text(&w["route"]),
                        text(&w["title"]),
                        text(&w["request_id"]),
                        text(&w["detail"]),
                    ],
                );
            }
        }
        "list" => {
            for c in rows(device.get("/api/conversations")?) {
                line(
                    out,
                    &[
                        text(&c["id"]),
                        text(&c["kind"]),
                        text(&c["mode"]),
                        text(&c["title"]),
                    ],
                );
            }
        }
        "deliveries" => deliveries(&device, arg(rest, 0)?, out)?,
        "redirect" => {
            let to = device.resolve(&device.get("/api/sessions")?, arg(rest, 2)?)?;
            let body = json!({"to": to, "expected_state": arg(rest, 1)?});
            let r = device.post(
                &format!("/api/deliveries/{}/redirect", quote(arg(rest, 0)?, "")),
                Some(&body),
            )?;
            line(
                out,
                &[
                    text(&r["disposition"]),
                    text(&r["message"]["conversation_id"]),
                    text(&r["delivery"]["id"]),
                ],
            );
        }
        "post" => post(&device, rest, out)?,
        "events" => events(&device, rest, out)?,
        _ => return Err(Failure::Usage),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_style_printing_of_values() {
        assert_eq!(text(&json!(null)), "None");
        assert_eq!(text(&json!(true)), "True");
        assert_eq!(text(&json!("a b")), "a b");
        assert_eq!(text(&json!(3)), "3");
        assert_eq!(list_repr(&["a".into(), "it's".into()]), "['a', 'it\\'s']");
        assert_eq!(list_repr(&[]), "[]");
        assert_eq!(utc(&json!(1_788_560_861.9)), "2026-09-04T22:27:41Z");
        assert_eq!(utc(&Value::Null), "-");
        assert_eq!(or_dash(&json!("")), "-");
    }

    #[test]
    fn no_arguments_and_unknown_verbs_are_usage() {
        let mut out = Vec::new();
        let missing = Path::new("does-not-exist.json");
        assert_eq!(run(&[], &mut out, missing), Err(Failure::Usage));
        let err = run(&["list".into()], &mut out, missing).unwrap_err();
        assert!(
            matches!(&err, Failure::Said(t) if t.contains("run `duplex owner enroll` first")),
            "{err:?}"
        );
        assert_eq!(
            run(
                &["enroll".into(), "--bogus".into(), "x".into()],
                &mut out,
                missing
            ),
            Err(Failure::Usage)
        );
        assert_eq!(
            run(&["enroll".into(), "--broker".into()], &mut out, missing),
            Err(Failure::Usage)
        );
    }
}
