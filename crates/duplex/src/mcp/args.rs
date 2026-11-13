//! A tool's argument table: every supplied argument, an unknown name included, is checked
//! before the handler runs, and the handler gets every declared name, an absent or null one as
//! its default.

use serde_json::{Map, Value, json};

type Check = Box<dyn Fn(&str, &Value) -> Result<Value, String> + Send + Sync>;

pub struct Arg {
    pub schema: Value,
    check: Check,
    pub required: bool,
    default: Value,
}

/// A string checked as it was typed and handed on unchanged: a string, not blank, within
/// `limit` characters. Nothing is stripped or coerced.
pub fn string(limit: usize, required: bool) -> Arg {
    Arg {
        schema: json!({"type": "string"}),
        check: Box::new(move |key, value| {
            let Some(text) = value.as_str() else {
                return Err(format!("{key} must be a string."));
            };
            if text.trim().is_empty() {
                return Err(if required {
                    format!("{key} is required.")
                } else {
                    format!("{key} is blank.")
                });
            }
            if text.chars().count() > limit {
                return Err(format!("{key} is over {limit} characters."));
            }
            Ok(value.clone())
        }),
        required,
        default: Value::Null,
    }
}

/// An integer within its range, never clamped; a bool is not one.
pub fn integer(default: i64, low: i64, high: Option<i64>) -> Arg {
    let mut schema = json!({"type": "integer", "minimum": low});
    if let Some(high) = high {
        schema["maximum"] = json!(high);
    }
    Arg {
        schema,
        check: Box::new(move |key, value| {
            let Some(n) = value.as_i64() else {
                return Err(format!("{key} must be an integer."));
            };
            if n < low || high.is_some_and(|h| n > h) {
                let bound = high.map_or_else(
                    || format!("at least {low}"),
                    |h| format!("from {low} to {h}"),
                );
                return Err(format!("{key} must be {bound}."));
            }
            Ok(json!(n))
        }),
        required: false,
        default: json!(default),
    }
}

pub fn choice(values: &'static [&'static str], required: bool) -> Arg {
    Arg {
        schema: json!({"type": "string", "enum": values}),
        check: Box::new(move |key, value| match value.as_str() {
            Some(v) if values.contains(&v) => Ok(value.clone()),
            _ => Err(format!("{key} is one of: {}.", values.join(", "))),
        }),
        required,
        default: Value::Null,
    }
}

fn is_thread_id(text: &str) -> bool {
    let parts: Vec<&str> = text.split('-').collect();
    parts.len() == 5
        && parts
            .iter()
            .zip([8, 4, 4, 4, 12])
            .all(|(p, n)| p.len() == n)
        && text
            .chars()
            .all(|c| c == '-' || matches!(c, '0'..='9' | 'a'..='f'))
}

/// This Codex task's own thread id.
pub fn thread_id() -> Arg {
    Arg {
        schema: json!({"type": "string"}),
        check: Box::new(|key, value| match value.as_str() {
            Some(text) if is_thread_id(text) => Ok(value.clone()),
            _ => Err(format!(
                "{key} must be this task's CODEX_THREAD_ID (read it in your shell)."
            )),
        }),
        required: true,
        default: Value::Null,
    }
}

/// A non-empty list of non-blank names. A Claude Code client keeps a respawned server's old
/// schema, which has no `to`, and then sends it as a string: a JSON list or comma-separated
/// names. Absent or null is a group reply; a false or empty value is refused, never omitted.
pub fn recipients() -> Arg {
    Arg {
        schema: json!({"type": "array", "items": {"type": "string"}}),
        check: Box::new(|key, value| {
            let list: Value = match value {
                Value::String(text) => match serde_json::from_str::<Value>(text) {
                    Ok(parsed @ Value::Array(_)) => parsed,
                    _ => Value::Array(
                        text.split(',')
                            .map(str::trim)
                            .filter(|n| !n.is_empty())
                            .map(|n| json!(n))
                            .collect(),
                    ),
                },
                other => other.clone(),
            };
            let ok = list.as_array().is_some_and(|names| {
                !names.is_empty()
                    && names
                        .iter()
                        .all(|n| n.as_str().is_some_and(|s| !s.trim().is_empty()))
            });
            if ok {
                Ok(list)
            } else {
                Err(format!(
                    "{key} is a non-empty list of session titles or ids."
                ))
            }
        }),
        required: false,
        default: Value::Null,
    }
}

/// The arguments a tool takes, in the order they are checked and listed.
pub struct Table(pub Vec<(&'static str, Arg)>);

impl Table {
    pub fn schema(&self) -> Value {
        let props: Map<String, Value> = self
            .0
            .iter()
            .map(|(n, a)| ((*n).to_owned(), a.schema.clone()))
            .collect();
        let required: Vec<&str> = self
            .0
            .iter()
            .filter(|(_, a)| a.required)
            .map(|(n, _)| *n)
            .collect();
        let mut out = json!({"type": "object", "properties": props, "additionalProperties": false});
        if !required.is_empty() {
            out["required"] = json!(required);
        }
        out
    }

    pub fn normalized(&self, given: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        let mut unknown: Vec<&str> = given
            .keys()
            .map(String::as_str)
            .filter(|k| !self.0.iter().any(|(n, _)| n == k))
            .collect();
        unknown.sort_unstable();
        if !unknown.is_empty() {
            return Err(format!(
                "{}: not an argument of this tool.",
                unknown.join(", ")
            ));
        }
        let mut out = Map::new();
        for (name, arg) in &self.0 {
            match given.get(*name).filter(|v| !v.is_null()) {
                Some(value) => {
                    out.insert((*name).to_owned(), (arg.check)(name, value)?);
                }
                None if arg.required => return Err(format!("{name} is required.")),
                None => {
                    out.insert((*name).to_owned(), arg.default.clone());
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn given(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    fn table() -> Table {
        Table(vec![
            ("id", string(10, true)),
            ("note", string(5, false)),
            ("n", integer(7, 0, Some(9))),
            ("mode", choice(&["auto", "queue"], false)),
            ("to", recipients()),
        ])
    }

    #[test]
    fn absent_and_null_values_take_their_defaults() {
        let out = table()
            .normalized(&given(json!({"id": "x", "note": null})))
            .unwrap();
        assert_eq!(out["note"], Value::Null);
        assert_eq!(out["n"], 7);
        assert_eq!(out["mode"], Value::Null);
        assert_eq!(out["to"], Value::Null);
    }

    #[test]
    fn each_check_names_the_argument_it_refuses() {
        let t = table();
        let refuse = |v: Value| t.normalized(&given(v)).unwrap_err();
        assert_eq!(refuse(json!({})), "id is required.");
        assert_eq!(refuse(json!({"id": "  "})), "id is required.");
        assert_eq!(refuse(json!({"id": 5})), "id must be a string.");
        assert_eq!(refuse(json!({"id": "x", "note": " "})), "note is blank.");
        assert_eq!(
            refuse(json!({"id": "x", "note": "toolong"})),
            "note is over 5 characters."
        );
        assert_eq!(
            refuse(json!({"id": "x", "n": true})),
            "n must be an integer."
        );
        assert_eq!(
            refuse(json!({"id": "x", "n": 1.5})),
            "n must be an integer."
        );
        assert_eq!(
            refuse(json!({"id": "x", "n": 10})),
            "n must be from 0 to 9."
        );
        assert_eq!(
            refuse(json!({"id": "x", "mode": "restart"})),
            "mode is one of: auto, queue."
        );
        assert_eq!(
            refuse(json!({"id": "x", "zzz": 1, "aaa": 2})),
            "aaa, zzz: not an argument of this tool."
        );
        assert_eq!(
            refuse(json!({"id": "x", "to": []})),
            "to is a non-empty list of session titles or ids."
        );
        assert_eq!(
            refuse(json!({"id": "x", "to": false})),
            "to is a non-empty list of session titles or ids."
        );
        assert_eq!(
            refuse(json!({"id": "x", "to": ["a", " "]})),
            "to is a non-empty list of session titles or ids."
        );
    }

    #[test]
    fn a_string_is_handed_on_unchanged() {
        let out = table().normalized(&given(json!({"id": "  x  "}))).unwrap();
        assert_eq!(out["id"], "  x  ");
    }

    #[test]
    fn recipients_come_as_a_list_a_json_string_or_commas() {
        let t = table();
        for to in [json!(["A", "B"]), json!("[\"A\", \"B\"]"), json!("A, B")] {
            assert_eq!(
                t.normalized(&given(json!({"id": "x", "to": to}))).unwrap()["to"],
                json!(["A", "B"])
            );
        }
        assert!(
            t.normalized(&given(json!({"id": "x", "to": ", ,"})))
                .is_err()
        );
    }

    #[test]
    fn a_thread_id_is_a_lowercase_uuid() {
        let t = Table(vec![("thread_id", thread_id())]);
        assert!(
            t.normalized(&given(
                json!({"thread_id": "01a00000-0000-7000-8000-000000000009"})
            ))
            .is_ok()
        );
        assert!(
            t.normalized(&given(
                json!({"thread_id": "01A00000-0000-7000-8000-000000000009"})
            ))
            .is_err()
        );
        assert!(t.normalized(&given(json!({"thread_id": "abc"}))).is_err());
        assert!(t.normalized(&given(json!({}))).is_err());
    }

    #[test]
    fn the_schema_lists_what_is_required() {
        let schema = table().schema();
        assert_eq!(schema["required"], json!(["id"]));
        assert_eq!(schema["properties"]["n"]["maximum"], 9);
        assert_eq!(
            schema["properties"]["mode"]["enum"],
            json!(["auto", "queue"])
        );
    }
}
