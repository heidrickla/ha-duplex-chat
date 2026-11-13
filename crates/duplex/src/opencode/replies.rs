//! Which round of an OpenCode session answered an input, read from the session's message log.
//!
//! The log is durable and ordered as delivered: a user message sits where the agent loop took
//! it in. A round runs from an input to the assistant message that stops (`finish: "stop"`), or
//! to the `idle` record that ends the execution with its outcome. A steered input joins the
//! round it was taken into; a queued one starts its own round after the running one stops.

use serde_json::Value;

/// What the log says about one input.
#[derive(Clone, Debug, PartialEq)]
pub enum Answer {
    /// Not in the log yet (still in the inbox), or its round is still running.
    Pending,
    /// Its round ended. `round` is the id of the message that ended it, the same for every
    /// input that rode the round; `text` is the round's last assistant text, if any.
    Ended {
        round: String,
        outcome: String,
        text: Option<String>,
    },
}

fn kind(message: &Value) -> &str {
    message.get("type").and_then(Value::as_str).unwrap_or("")
}

/// The text parts of an assistant message, joined; None when it has none.
fn text_of(message: &Value) -> Option<String> {
    let parts: Vec<&str> = message
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|p| p.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|p| p.get("text").and_then(Value::as_str))
        .filter(|t| !t.trim().is_empty())
        .collect();
    (!parts.is_empty()).then(|| parts.join("\n\n"))
}

/// The answer to `input_id` in `log`, which is in delivery order (oldest first).
pub fn answer_for(log: &[Value], input_id: &str) -> Answer {
    let Some(start) = log
        .iter()
        .position(|m| kind(m) == "user" && m.get("id").and_then(Value::as_str) == Some(input_id))
    else {
        return Answer::Pending;
    };
    let mut text = None;
    for message in &log[start + 1..] {
        let id = message.get("id").and_then(Value::as_str).unwrap_or("");
        match kind(message) {
            "assistant" => {
                if let Some(found) = text_of(message) {
                    text = Some(found);
                }
                if message.get("finish").and_then(Value::as_str) == Some("stop") {
                    return Answer::Ended {
                        round: id.to_owned(),
                        outcome: "succeeded".to_owned(),
                        text,
                    };
                }
            }
            "idle" => {
                let outcome = message
                    .get("outcome")
                    .and_then(Value::as_str)
                    .unwrap_or("succeeded");
                return Answer::Ended {
                    round: id.to_owned(),
                    outcome: outcome.to_owned(),
                    text,
                };
            }
            _ => {}
        }
    }
    Answer::Pending
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn user(id: &str) -> Value {
        json!({"id": id, "type": "user", "text": "..."})
    }

    fn assistant(id: &str, finish: &str, text: Option<&str>) -> Value {
        let mut content = vec![json!({"type": "reasoning", "text": "thinking"})];
        if let Some(text) = text {
            content.push(json!({"type": "text", "text": text}));
        }
        json!({"id": id, "type": "assistant", "finish": finish, "content": content})
    }

    fn idle(id: &str, outcome: &str) -> Value {
        json!({"id": id, "type": "idle", "outcome": outcome})
    }

    fn ended(round: &str, outcome: &str, text: Option<&str>) -> Answer {
        Answer::Ended {
            round: round.to_owned(),
            outcome: outcome.to_owned(),
            text: text.map(str::to_owned),
        }
    }

    // The shapes measured on OpenCode 2.3.0 (2026-11-13): a steer taken at the next step, a
    // queue taken after the running round stops, an interrupt.
    #[test]
    fn a_steered_input_shares_the_round_it_joined() {
        let log = [
            user("u1"),
            assistant("a1", "tool-calls", None),
            user("u2"),
            assistant("a2", "stop", Some("DONE\nSTEERED")),
            idle("i1", "succeeded"),
        ];
        assert_eq!(
            answer_for(&log, "u1"),
            ended("a2", "succeeded", Some("DONE\nSTEERED"))
        );
        assert_eq!(answer_for(&log, "u2"), answer_for(&log, "u1"));
    }

    #[test]
    fn a_queued_input_gets_its_own_round_in_the_same_execution() {
        let log = [
            user("u1"),
            assistant("a1", "tool-calls", None),
            assistant("a2", "stop", Some("FIRST")),
            user("u2"),
            assistant("a3", "stop", Some("SECOND")),
            idle("i1", "succeeded"),
        ];
        assert_eq!(
            answer_for(&log, "u1"),
            ended("a2", "succeeded", Some("FIRST"))
        );
        assert_eq!(
            answer_for(&log, "u2"),
            ended("a3", "succeeded", Some("SECOND"))
        );
    }

    #[test]
    fn an_interrupted_round_ends_at_its_idle_record_with_the_outcome() {
        let log = [
            user("u1"),
            assistant("a1", "error", None),
            idle("i1", "interrupted"),
            user("u2"),
            assistant("a2", "stop", Some("RESTARTED")),
            idle("i2", "succeeded"),
        ];
        assert_eq!(answer_for(&log, "u1"), ended("i1", "interrupted", None));
        assert_eq!(
            answer_for(&log, "u2"),
            ended("a2", "succeeded", Some("RESTARTED"))
        );
    }

    #[test]
    fn a_running_round_or_an_input_not_yet_taken_is_pending() {
        let log = [user("u1"), assistant("a1", "tool-calls", Some("working"))];
        assert_eq!(answer_for(&log, "u1"), Answer::Pending);
        assert_eq!(answer_for(&log, "u9"), Answer::Pending);
        // An assistant message is not a user input, whatever its id.
        assert_eq!(answer_for(&log, "a1"), Answer::Pending);
    }

    #[test]
    fn a_round_that_stops_without_text_keeps_the_last_text_it_had() {
        let log = [
            user("u1"),
            assistant("a1", "tool-calls", Some("partial")),
            assistant("a2", "stop", None),
        ];
        assert_eq!(
            answer_for(&log, "u1"),
            ended("a2", "succeeded", Some("partial"))
        );
        let silent = [user("u1"), assistant("a1", "stop", None)];
        assert_eq!(answer_for(&silent, "u1"), ended("a1", "succeeded", None));
    }
}
