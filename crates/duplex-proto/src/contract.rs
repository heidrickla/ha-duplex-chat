//! The adapter side of the broker's contract (`docs/adapter-contract.md`): the closed
//! sets, the registration and claim shapes and the refusals an adapter tells apart.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

/// The turn states an adapter may report.
pub const TURN_STATES: [&str; 4] = ["busy", "idle", "waiting", "unknown"];

/// The only kinds of wait that cross to the broker.
pub const WAIT_KINDS: [&str; 7] = [
    "permission_prompt",
    "input_needed",
    "worker_request",
    "sandbox_request",
    "dialog_open",
    "blocked",
    "user_input",
];

/// What the broker answers for a limit or skip report it will never accept.
pub const FINAL_STATUSES: [u16; 4] = [404, 405, 409, 422];

/// A turn reading as the broker takes it: `seq` runs from 1 per registration.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TurnBody {
    pub state: String,
    pub waiting_for: Option<String>,
    pub seq: u64,
}

impl TurnBody {
    /// Whatever built the turn, no state or wait outside the closed sets leaves here.
    pub fn new(state: &str, waiting_for: Option<&str>, seq: u64) -> Self {
        let state = if TURN_STATES.contains(&state) {
            state
        } else {
            "unknown"
        };
        let waiting_for = waiting_for.map(|w| {
            if WAIT_KINDS.contains(&w) {
                w
            } else {
                "user_input"
            }
        });
        Self {
            state: state.to_owned(),
            waiting_for: waiting_for.map(str::to_owned),
            seq,
        }
    }
}

/// Where a delivery stands, in the broker's words.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryState {
    Queued,
    Dispatching,
    Accepted,
    Consumed,
    Responded,
    Uncertain,
    Failed,
    Cancelled,
}

impl DeliveryState {
    pub const ALL: [Self; 8] = [
        Self::Queued,
        Self::Dispatching,
        Self::Accepted,
        Self::Consumed,
        Self::Responded,
        Self::Uncertain,
        Self::Failed,
        Self::Cancelled,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Dispatching => "dispatching",
            Self::Accepted => "accepted",
            Self::Consumed => "consumed",
            Self::Responded => "responded",
            Self::Uncertain => "uncertain",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.as_str() == text)
    }

    /// What the state tells a sender, so nobody reads a recipient's thread to learn
    /// whether a message arrived.
    pub fn meaning(self) -> &'static str {
        match self {
            Self::Queued => "stored; the recipient's adapter has not taken it yet",
            Self::Dispatching => "handed to the recipient's adapter, which woke it; not read yet",
            Self::Accepted => "the recipient's runtime accepted the wake; not read yet",
            Self::Consumed => "read by the recipient (bridge_receive)",
            Self::Responded => "read, and the recipient replied to it",
            Self::Uncertain => "the wake's outcome is unknown; see reason",
            Self::Failed => "the wake failed; see reason",
            Self::Cancelled => "cancelled before it was read; see reason",
        }
    }
}

/// What the broker's 422 says to a state report, told apart by its text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The delivery ended without being read (uncertain, failed or cancelled).
    ClosedUnread,
    /// The delivery was read already; an earlier report landed and its answer was lost.
    AlreadyRead,
    /// The hand-over was never recorded.
    NotAccepted,
    /// The hand-over is recorded already.
    AlreadyAccepted,
}

fn goes(detail: &str, from: &[&str], to: &[&str]) -> bool {
    from.iter().any(|f| {
        to.iter()
            .any(|t| detail.contains(&format!("cannot go from {f} to {t}")))
    })
}

impl Refusal {
    pub fn of(detail: &str) -> Option<Self> {
        if goes(
            detail,
            &["uncertain", "failed", "cancelled"],
            &["accepted", "consumed"],
        ) {
            Some(Self::ClosedUnread)
        } else if goes(
            detail,
            &["consumed", "responded"],
            &["accepted", "consumed"],
        ) {
            Some(Self::AlreadyRead)
        } else if goes(detail, &["dispatching"], &["consumed"]) {
            Some(Self::NotAccepted)
        } else if goes(detail, &["accepted"], &["accepted"]) {
            Some(Self::AlreadyAccepted)
        } else {
            None
        }
    }
}

/// The capability map an adapter states at registration. The panel enables only
/// verified controls.
pub fn capabilities(send_modes: &[&str]) -> Value {
    json!({
        "receive_messages": "verified",
        "publish_replies": "verified",
        "stream_text": "unsupported",
        "observe_activity": "unsupported",
        "steer": "unsupported",
        "interrupt": "unsupported",
        "relay_approvals": "unsupported",
        "resume_history": "unsupported",
        "attach_live": "verified",
        "rewake": "verified",
        "turn_state": true,
        "limits": true,
        "tasks": true,
        "send_modes": send_modes,
    })
}

/// A registered binding; fields beyond the id and title are kept as the broker sent them.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Binding {
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

/// `POST /api/adapter/register`: the binding and its token.
#[derive(Clone, Deserialize)]
pub struct Registered {
    pub token: String,
    pub binding: Binding,
}

impl std::fmt::Debug for Registered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registered")
            .field("token", &"<redacted>")
            .field("binding", &self.binding)
            .finish()
    }
}

/// A delivery claimed for the session, with its message and conversation as sent.
#[derive(Clone, Debug, PartialEq)]
pub struct Claimed {
    pub delivery_id: String,
    pub message: Value,
    pub conversation: Value,
}

/// The broker asks for the wake line again while a delivery stays unread.
#[derive(Clone, Debug, PartialEq)]
pub struct Rewake {
    pub delivery_id: String,
    pub attempt: u32,
    pub conversation_kind: String,
}

/// What `POST /api/adapter/claim` answered.
#[derive(Clone, Debug, PartialEq)]
pub enum Claim {
    Nothing,
    Rewake(Rewake),
    Delivery(Box<Claimed>),
}

impl Claim {
    pub fn parse(value: &Value) -> Result<Self, String> {
        if value.is_null() || value.as_object().is_some_and(Map::is_empty) {
            return Ok(Self::Nothing);
        }
        if let Some(due) = value.get("rewake") {
            let delivery_id = text(due, "delivery_id").ok_or("rewake without a delivery_id")?;
            return Ok(Self::Rewake(Rewake {
                delivery_id,
                attempt: due
                    .get("attempt")
                    .and_then(Value::as_u64)
                    .map_or(1, |n| n as u32),
                conversation_kind: text(due, "conversation_kind")
                    .unwrap_or_else(|| "direct".to_owned()),
            }));
        }
        let delivery_id = value
            .get("delivery")
            .and_then(|d| text(d, "id"))
            .ok_or("claim without a delivery id")?;
        let message = value
            .get("message")
            .cloned()
            .ok_or("claim without a message")?;
        let conversation = value
            .get("conversation")
            .cloned()
            .ok_or("claim without a conversation")?;
        Ok(Self::Delivery(Box::new(Claimed {
            delivery_id,
            message,
            conversation,
        })))
    }
}

/// A string field of a JSON object.
pub fn text(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_turn_outside_the_closed_sets_is_cleaned_at_the_boundary() {
        let body = TurnBody::new("sleeping", Some("a user typed words"), 3);
        assert_eq!(
            (body.state.as_str(), body.waiting_for.as_deref(), body.seq),
            ("unknown", Some("user_input"), 3)
        );
        let known = TurnBody::new("waiting", Some("dialog_open"), 1);
        assert_eq!(
            (known.state.as_str(), known.waiting_for.as_deref()),
            ("waiting", Some("dialog_open"))
        );
        assert_eq!(TurnBody::new("idle", None, 2).waiting_for, None);
    }

    #[test]
    fn every_state_round_trips_and_has_a_meaning() {
        for state in DeliveryState::ALL {
            assert_eq!(DeliveryState::parse(state.as_str()), Some(state));
            assert!(!state.meaning().is_empty());
        }
        assert_eq!(DeliveryState::parse("lost"), None);
    }

    #[test]
    fn refusals_are_told_apart_by_the_brokers_text() {
        let cases = [
            (
                "cannot go from failed to consumed",
                Some(Refusal::ClosedUnread),
            ),
            (
                "delivery d: cannot go from uncertain to accepted",
                Some(Refusal::ClosedUnread),
            ),
            (
                "cannot go from responded to consumed",
                Some(Refusal::AlreadyRead),
            ),
            (
                "cannot go from dispatching to consumed",
                Some(Refusal::NotAccepted),
            ),
            (
                "cannot go from accepted to accepted",
                Some(Refusal::AlreadyAccepted),
            ),
            ("cannot go from queued to consumed", None),
            ("", None),
        ];
        for (detail, want) in cases {
            assert_eq!(Refusal::of(detail), want, "{detail}");
        }
    }

    #[test]
    fn a_claim_is_nothing_a_rewake_or_a_delivery() {
        assert_eq!(Claim::parse(&Value::Null), Ok(Claim::Nothing));
        assert_eq!(Claim::parse(&json!({})), Ok(Claim::Nothing));
        assert_eq!(
            Claim::parse(
                &json!({"rewake": {"delivery_id": "d1", "attempt": 2, "conversation_kind": "group"}})
            ),
            Ok(Claim::Rewake(Rewake {
                delivery_id: "d1".into(),
                attempt: 2,
                conversation_kind: "group".into()
            }))
        );
        let delivery = json!({"delivery": {"id": "d2"}, "message": {"id": "m"}, "conversation": {"kind": "direct"}});
        let Ok(Claim::Delivery(claimed)) = Claim::parse(&delivery) else {
            panic!("not a delivery");
        };
        assert_eq!(claimed.delivery_id, "d2");
        assert!(Claim::parse(&json!({"delivery": {"id": "d2"}})).is_err());
    }

    #[test]
    fn registration_declares_the_modes_and_what_the_adapter_reports_and_acts_on() {
        let declared = capabilities(&["auto", "queue"]);
        assert_eq!(declared["send_modes"], json!(["auto", "queue"]));
        for on in ["turn_state", "limits", "tasks"] {
            assert_eq!(declared[on], json!(true), "{on}");
        }
        assert_eq!(declared["rewake"], "verified");
        // This adapter hands over through a wake, so it cannot steer or interrupt a turn itself.
        assert_eq!(declared["steer"], "unsupported");
        assert_eq!(declared["interrupt"], "unsupported");
    }

    #[test]
    fn registration_debug_never_shows_the_token() {
        let registered: Registered = serde_json::from_value(
            json!({"token": "secret-1", "binding": {"id": "b", "title": "t"}}),
        )
        .unwrap();
        assert!(!format!("{registered:?}").contains("secret-1"));
    }
}
