//! One managed OpenCode session and the step its thread repeats: open the session, register it,
//! square and answer what it holds, allow what it asks, and claim the next delivery with the
//! turn read just before. One thread owns a session's readings, sequence numbers and waiters,
//! so every reading is numbered in the order it was taken and none is cached.

use super::Log;
use super::config::{SessionMap, SessionSpec};
use super::replies::{Answer, answer_for};
use super::server::{OpenCode, ServerError, Verb};
use crate::compat::adapter_info;
use duplex_proto::client::{Method, quote};
use duplex_proto::contract::{Claim, Claimed, Registered, TurnBody};
use duplex_proto::{BrokerClient, BrokerError};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const SEND_MODES: [&str; 4] = ["auto", "queue", "steer", "restart"];
/// A delivery whose round has not ended after this long fails as timed out.
pub const TURN_TIMEOUT: Duration = Duration::from_secs(1800);
/// How long a restart waits for the interrupted turn to stop.
pub const STOP_TIMEOUT: Duration = Duration::from_secs(60);
/// The broker's notices member (`duplex_broker.store.BROKER_BINDING_ID`): limit resumes come from it.
pub const BROKER_ID: &str = "broker";
pub const ENDPOINT_REF: &str = "opencode-server";
const LOG_PAGE: usize = 100;
const LOG_PAGES: usize = 20;
/// A log page older than the oldest input by this much holds none of the inputs: a queued input
/// is stamped when it is taken in, never before it was sent.
const LOG_MARGIN_MS: f64 = 60_000.0;

/// What a managed session declares: all four send modes and its turn state; no task tools, no
/// usage-limit reports.
pub fn capabilities() -> Value {
    json!({
        "receive_messages": "verified",
        "publish_replies": "verified",
        "stream_text": "unsupported",
        "observe_activity": "unsupported",
        "steer": "supported",
        "interrupt": "supported",
        "relay_approvals": "unsupported",
        "resume_history": "verified",
        "attach_live": "unsupported",
        "send_modes": SEND_MODES,
        "turn_state": true,
    })
}

/// The session's own permission rules, after the agent's: every tool runs without a prompt, and
/// the question tool is refused, since nobody answers a form in a duplex-woken turn.
pub fn permissions() -> Value {
    json!([
        {"action": "*", "resource": "*", "effect": "allow"},
        {"action": "question", "resource": "*", "effect": "deny"},
    ])
}

fn supports() -> String {
    format!("(supports: {})", SEND_MODES.join(", "))
}

fn wall_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
}

/// Why a step stopped. The loop tells them apart: a broker refusal of the token re-registers, an
/// OpenCode failure opens the session again once OpenCode answers.
#[derive(Debug)]
pub enum StepError {
    Broker(BrokerError),
    Server(ServerError),
    Invalid(String),
}

impl std::fmt::Display for StepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Broker(e) => write!(f, "{e}"),
            Self::Server(e) => write!(f, "{e}"),
            Self::Invalid(e) => f.write_str(e),
        }
    }
}

impl From<BrokerError> for StepError {
    fn from(e: BrokerError) -> Self {
        Self::Broker(e)
    }
}

impl From<ServerError> for StepError {
    fn from(e: ServerError) -> Self {
        Self::Server(e)
    }
}

/// A delivery put into the session, waiting for the round that answers it.
#[derive(Clone, Debug)]
struct Waiter {
    delivery_id: String,
    conversation_id: String,
    message_id: String,
    /// The OpenCode input (its user message id) the delivery became.
    input_id: String,
    /// The broker has it as accepted: its answer must not overtake that.
    accepted: bool,
    since: Instant,
    sent_ms: f64,
}

/// One managed session as its thread holds it.
#[derive(Debug)]
pub struct Managed {
    pub spec: SessionSpec,
    pub session_id: String,
    pub binding_id: String,
    token: String,
    /// The last reading's number; the broker applies only a newer one.
    seq: u64,
    waiters: Vec<Waiter>,
    /// The session was checked against OpenCode since OpenCode last failed.
    opened: bool,
    /// The broker's open deliveries are to be squared with the waiters.
    unsettled: bool,
    pub retired: bool,
}

impl Managed {
    pub fn new(spec: SessionSpec) -> Self {
        Self {
            spec,
            session_id: String::new(),
            binding_id: String::new(),
            token: String::new(),
            seq: 0,
            waiters: Vec::new(),
            opened: false,
            unsettled: true,
            retired: false,
        }
    }

    pub fn waiting(&self) -> usize {
        self.waiters.len()
    }

    /// The delivery ids held, in claim order (tests).
    pub fn held(&self) -> Vec<String> {
        self.waiters.iter().map(|w| w.delivery_id.clone()).collect()
    }
}

/// What every managed session shares: the broker, OpenCode, the session map and the log.
pub struct Shared {
    pub broker: BrokerClient,
    pub enrollment_token: String,
    pub opencode: OpenCode,
    pub sessions: SessionMap,
    pub token_dir: Option<PathBuf>,
    pub log: Log,
    pub poll: Duration,
    /// How often a restart looks whether the interrupted turn stopped.
    pub stop_poll: Duration,
    pub turn_timeout: Duration,
}

/// The deliveries one ended round answers in one conversation, in claim order.
struct Round {
    key: (String, String),
    outcome: String,
    text: Option<String>,
    ids: Vec<String>,
}

/// A broker answer to a report: taken, refused for good (with the reason), or try again.
enum Reported {
    Taken,
    Refused(String),
}

impl Shared {
    /// One pass. True when a delivery was claimed, so the caller claims again at once. A failure
    /// leaves the session ready for the next pass: an OpenCode failure opens the session again
    /// (and tells the broker the turn is unknown), a refused token registers again, and a lost
    /// broker has its in-flight deliveries squared with the waiters.
    pub fn step(&self, s: &mut Managed) -> Result<bool, StepError> {
        let result = self.pass(s);
        match &result {
            Err(StepError::Server(_)) => {
                s.opened = false;
                // Nothing is claimed while OpenCode is away, so the broker hears it here.
                if !s.token.is_empty() {
                    self.send_reading(s, "unknown");
                }
            }
            Err(StepError::Broker(e)) if matches!(e.status(), Some(401 | 403)) => s.token.clear(),
            Err(StepError::Broker(e)) if e.status() != Some(410) => s.unsettled = true,
            _ => {}
        }
        result
    }

    fn pass(&self, s: &mut Managed) -> Result<bool, StepError> {
        if !s.opened {
            self.open(s)?;
        }
        if s.token.is_empty() {
            self.register(s)?;
        }
        if s.unsettled {
            self.reconcile(s)?;
            s.unsettled = false;
        }
        self.acknowledge(s)?;
        self.answer(s)?;
        self.allow_permissions(s)?;
        let reading = self.reading(s)?;
        let body = json!({"turn": reading});
        let claimed =
            self.broker
                .call(Method::Post, "/api/adapter/claim", Some(&body), &s.token)?;
        match Claim::parse(&claimed).map_err(StepError::Invalid)? {
            Claim::Nothing => Ok(false),
            // Never declared (`rewake`), so never sent; nothing to wake again.
            Claim::Rewake(_) => Ok(true),
            Claim::Delivery(claimed) => {
                self.deliver(s, &claimed)?;
                self.report_turn(s);
                Ok(true)
            }
        }
    }

    /// Repeats `step` until the owner retires the session.
    pub fn serve(&self, mut s: Managed) {
        const BACKOFF: [u64; 5] = [2, 5, 15, 30, 60];
        let mut attempt = 0usize;
        let mut problem = String::new();
        while !s.retired {
            match self.step(&mut s) {
                Ok(took) => {
                    attempt = 0;
                    if !problem.is_empty() {
                        self.log.info(format!("{}: working again", s.spec.title));
                        problem.clear();
                    }
                    if !took {
                        std::thread::sleep(self.poll);
                    }
                }
                Err(err) => {
                    let wait = match &err {
                        StepError::Broker(e) if e.status() == Some(410) => {
                            self.log
                                .info(format!("{}: the owner retired this session", s.spec.title));
                            s.retired = true;
                            continue;
                        }
                        StepError::Server(_) => 5,
                        _ => BACKOFF[attempt.min(BACKOFF.len() - 1)],
                    };
                    let said = err.to_string();
                    if said != problem {
                        self.log.warn(format!("{}: {said}", s.spec.title));
                        problem = said;
                    }
                    attempt += 1;
                    std::thread::sleep(Duration::from_secs(wait));
                }
            }
        }
    }

    /// Finds the session OpenCode holds for this title, or creates it, and keeps its model and
    /// permission rules as configured.
    fn open(&self, s: &mut Managed) -> Result<(), StepError> {
        let known = Some(s.session_id.clone())
            .filter(|id| !id.is_empty())
            .or_else(|| self.sessions.get(&s.spec.title));
        let found = match known {
            Some(id) => match self
                .opencode
                .get(&format!("/api/session/{}", quote(&id, "")))
            {
                Ok(info) => Some(info["data"].clone()),
                Err(ServerError::Refused {
                    code: 400 | 404, ..
                }) => None,
                Err(e) => return Err(e.into()),
            },
            None => None,
        };
        let id = match found {
            Some(info) => {
                let id = info["id"].as_str().unwrap_or_default().to_owned();
                self.keep_configured(s, &id, &info)?;
                id
            }
            None => self.create(s)?,
        };
        if id.is_empty() {
            return Err(StepError::Invalid("OpenCode gave the session no id".into()));
        }
        if id != s.session_id {
            if !s.session_id.is_empty() {
                self.log.info(format!(
                    "{}: session {} is gone; holding {id}",
                    s.spec.title, s.session_id
                ));
            }
            // Another native session is another binding.
            s.session_id = id;
            s.token.clear();
            s.waiters.clear();
        }
        self.sessions
            .set(&s.spec.title, &s.session_id)
            .map_err(|e| StepError::Invalid(format!("cannot keep the session id: {e}")))?;
        s.opened = true;
        Ok(())
    }

    fn create(&self, s: &Managed) -> Result<String, StepError> {
        let body = json!({
            "title": s.spec.title,
            "model": {"providerID": s.spec.provider_id, "id": s.spec.model_id},
            "location": {"directory": s.spec.cwd},
            "permissions": permissions(),
        });
        let made = self.opencode.post("/api/session", &body)?;
        let id = made["data"]["id"].as_str().unwrap_or_default().to_owned();
        self.log.info(format!(
            "{}: created OpenCode session {id} on {}",
            s.spec.title,
            s.spec.model()
        ));
        Ok(id)
    }

    fn keep_configured(&self, s: &Managed, id: &str, info: &Value) -> Result<(), StepError> {
        let path = format!("/api/session/{}", quote(id, ""));
        let model = &info["model"];
        if model["providerID"].as_str() != Some(&s.spec.provider_id)
            || model["id"].as_str() != Some(&s.spec.model_id)
        {
            let body = json!({"model": {"providerID": s.spec.provider_id, "id": s.spec.model_id}});
            self.opencode.post(&format!("{path}/model"), &body)?;
            self.log
                .info(format!("{}: model set to {}", s.spec.title, s.spec.model()));
        }
        if info["permissions"] != permissions() {
            let body = json!({"permissions": permissions()});
            self.opencode.call(Verb::Patch, &path, Some(&body))?;
            self.log
                .info(format!("{}: permission rules set", s.spec.title));
        }
        Ok(())
    }

    fn token_file(&self, s: &Managed) -> Option<PathBuf> {
        let dir = self.token_dir.as_ref()?;
        let digest = Sha256::digest(s.session_id.as_bytes());
        let key: String = digest.iter().take(12).map(|b| format!("{b:02x}")).collect();
        Some(dir.join(format!("opencode-{key}.token")))
    }

    /// Registers the session. The broker resets its reading count, and so does this thread; the
    /// first reading after it is sent at once.
    fn register(&self, s: &mut Managed) -> Result<(), StepError> {
        let held = self
            .token_file(s)
            .and_then(|p| std::fs::read_to_string(p).ok())
            .map_or_else(String::new, |t| t.trim().to_owned());
        let version = self
            .opencode
            .get("/api/info")
            .ok()
            .and_then(|i| i["version"].as_str().map(str::to_owned))
            .unwrap_or_else(|| "unknown".to_owned());
        let runtime = HashMap::from([("opencode".to_owned(), version)]);
        let body = json!({
            "provider": "opencode",
            "native_id": s.session_id,
            "title": s.spec.title,
            "workspace": s.spec.cwd,
            "endpoint_ref": ENDPOINT_REF,
            "capabilities": capabilities(),
            "current_token": held,
            "adapter": adapter_info(&runtime),
        });
        let answer = self.broker.call(
            Method::Post,
            "/api/adapter/register",
            Some(&body),
            &self.enrollment_token,
        )?;
        let registered: Registered = serde_json::from_value(answer)
            .map_err(|e| StepError::Invalid(format!("registration answer: {e}")))?;
        s.token = registered.token;
        s.binding_id = registered.binding.id;
        s.seq = 0;
        s.unsettled = true;
        if let Some(path) = self.token_file(s) {
            if let Some(dir) = path.parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            let _ = std::fs::write(&path, &s.token);
        }
        self.log.info(format!(
            "{}: registered as {} (OpenCode session {})",
            s.spec.title, s.binding_id, s.session_id
        ));
        self.report_turn(s);
        Ok(())
    }

    /// The turn as OpenCode's list of running sessions shows it now, with the next number.
    fn reading(&self, s: &mut Managed) -> Result<TurnBody, StepError> {
        let state = if self.busy(s)? { "busy" } else { "idle" };
        s.seq += 1;
        Ok(TurnBody::new(state, None, s.seq))
    }

    fn busy(&self, s: &Managed) -> Result<bool, ServerError> {
        let active = self.opencode.get("/api/session/active")?;
        Ok(active["data"].get(&s.session_id).is_some())
    }

    /// Sends a fresh reading; a failure is left to the next claim, which carries one too.
    fn report_turn(&self, s: &mut Managed) {
        let state = match self.busy(s) {
            Ok(true) => "busy",
            Ok(false) => "idle",
            Err(_) => "unknown",
        };
        self.send_reading(s, state);
    }

    fn send_reading(&self, s: &mut Managed, state: &str) {
        s.seq += 1;
        let body = serde_json::to_value(TurnBody::new(state, None, s.seq)).unwrap_or(Value::Null);
        let _ = self
            .broker
            .call(Method::Post, "/api/adapter/turn", Some(&body), &s.token);
    }

    /// Posts a delivery state. A client error no repeat changes is a refusal; anything else is
    /// tried again by a later step.
    fn report(
        &self,
        s: &Managed,
        delivery_id: &str,
        state: &str,
        native_turn_id: Option<&str>,
        reason: Option<&str>,
    ) -> Result<Reported, StepError> {
        let mut body = json!({"state": state});
        if let Some(turn) = native_turn_id {
            body["native_turn_id"] = json!(turn);
        }
        if let Some(reason) = reason {
            body["reason"] = json!(reason.chars().take(300).collect::<String>());
        }
        let path = format!("/api/adapter/deliveries/{delivery_id}/state");
        self.acknowledged(s, &path, &body)
    }

    fn acknowledged(&self, s: &Managed, path: &str, body: &Value) -> Result<Reported, StepError> {
        match self.broker.call(Method::Post, path, Some(body), &s.token) {
            Ok(_) => Ok(Reported::Taken),
            Err(e) if is_final(&e) => {
                self.log
                    .info(format!("{}: {path} refused: {}", s.spec.title, e.detail()));
                Ok(Reported::Refused(e.detail().to_owned()))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Squares the broker's in-flight deliveries with the waiters: one this thread holds is
    /// acknowledged again, any other can no longer be answered and ends failed.
    fn reconcile(&self, s: &mut Managed) -> Result<(), StepError> {
        let listed = self.broker.call(
            Method::Get,
            "/api/adapter/deliveries?state=dispatching,accepted",
            None,
            &s.token,
        )?;
        for delivery in listed.as_array().into_iter().flatten() {
            let id = delivery["id"].as_str().unwrap_or_default();
            match s.waiters.iter().position(|w| w.delivery_id == id) {
                Some(i) => {
                    if delivery["state"] == "dispatching" {
                        s.waiters[i].accepted = false;
                    }
                }
                None => {
                    self.report(
                        s,
                        id,
                        "failed",
                        None,
                        Some("the adapter no longer holds this turn"),
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Tells the broker each new waiter is accepted, after it is held.
    fn acknowledge(&self, s: &mut Managed) -> Result<(), StepError> {
        let mut i = 0;
        while i < s.waiters.len() {
            if s.waiters[i].accepted {
                i += 1;
                continue;
            }
            let w = s.waiters[i].clone();
            match self.report(s, &w.delivery_id, "accepted", Some(&w.input_id), None)? {
                Reported::Taken => {
                    s.waiters[i].accepted = true;
                    i += 1;
                }
                // Not this lease's any more (registered again, cancelled): nothing to answer.
                Reported::Refused(_) => {
                    s.waiters.remove(i);
                }
            }
        }
        Ok(())
    }

    /// Answers every waiter whose round ended: one reply per round and conversation, under the
    /// first delivery that rode it there.
    fn answer(&self, s: &mut Managed) -> Result<(), StepError> {
        let ready: Vec<&Waiter> = s.waiters.iter().filter(|w| w.accepted).collect();
        if ready.is_empty() {
            return Ok(());
        }
        let oldest = ready
            .iter()
            .map(|w| w.sent_ms)
            .fold(f64::INFINITY, f64::min);
        let log = self.read_log(&s.session_id, oldest - LOG_MARGIN_MS)?;
        let mut groups: Vec<Round> = Vec::new();
        for w in &ready {
            let answer = match answer_for(&log, &w.input_id) {
                Answer::Pending if w.since.elapsed() >= self.turn_timeout => Answer::Ended {
                    round: format!("timeout:{}", w.delivery_id),
                    outcome: "timeout".to_owned(),
                    text: None,
                },
                other => other,
            };
            if let Answer::Ended {
                round,
                outcome,
                text,
            } = answer
            {
                let key = (round, w.conversation_id.clone());
                match groups.iter_mut().find(|g| g.key == key) {
                    Some(group) => group.ids.push(w.delivery_id.clone()),
                    None => groups.push(Round {
                        key,
                        outcome,
                        text,
                        ids: vec![w.delivery_id.clone()],
                    }),
                }
            }
        }
        for group in groups {
            let members: Vec<Waiter> = group
                .ids
                .iter()
                .filter_map(|id| s.waiters.iter().find(|w| &w.delivery_id == id).cloned())
                .collect();
            self.close(s, &members, &group.outcome, group.text.as_deref())?;
            s.waiters.retain(|w| !group.ids.contains(&w.delivery_id));
        }
        Ok(())
    }

    fn close(
        &self,
        s: &Managed,
        group: &[Waiter],
        outcome: &str,
        text: Option<&str>,
    ) -> Result<(), StepError> {
        let first = &group[0];
        match text {
            Some(text) if outcome == "succeeded" => {
                let posted = self.post_reply(s, first, text)?;
                for w in group {
                    match &posted {
                        Reported::Taken => {
                            self.report(s, &w.delivery_id, "responded", None, None)?
                        }
                        // No reply stands in the broker, so the delivery was not answered.
                        Reported::Refused(why) => {
                            let reason = format!("the reply was refused: {why}");
                            self.report(s, &w.delivery_id, "failed", None, Some(&reason))?
                        }
                    };
                }
            }
            _ => {
                let reason = if outcome == "succeeded" {
                    "the turn ended without a reply".to_owned()
                } else {
                    format!("the turn ended {outcome}")
                };
                for w in group {
                    self.report(s, &w.delivery_id, "failed", None, Some(&reason))?;
                }
                if let Some(text) = text {
                    self.post_reply(s, first, text)?;
                }
            }
        }
        Ok(())
    }

    /// The delivery's reply; its id comes from the delivery, so a repeat is not a second reply.
    fn post_reply(&self, s: &Managed, w: &Waiter, text: &str) -> Result<Reported, StepError> {
        let body = json!({
            "conversation_id": w.conversation_id,
            "client_message_id": format!("reply-{}", w.delivery_id),
            "text": text,
            "parent_id": w.message_id,
        });
        self.acknowledged(s, "/api/adapter/messages", &body)
    }

    /// The session's messages, oldest first, back to the first page that starts before `since_ms`.
    fn read_log(&self, session_id: &str, since_ms: f64) -> Result<Vec<Value>, StepError> {
        let base = format!("/api/session/{}/message", quote(session_id, ""));
        let mut log: Vec<Value> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..LOG_PAGES {
            let path = match &cursor {
                None => format!("{base}?order=desc&limit={LOG_PAGE}"),
                Some(c) => format!("{base}?limit={LOG_PAGE}&cursor={}", quote(c, "")),
            };
            let page = self.opencode.get(&path)?;
            let messages = page["data"].as_array().cloned().unwrap_or_default();
            let older = messages
                .last()
                .and_then(|m| m["time"]["created"].as_f64())
                .is_some_and(|t| t < since_ms);
            log.extend(messages);
            cursor = page["cursor"]["next"].as_str().map(str::to_owned);
            if older || cursor.is_none() {
                break;
            }
        }
        log.reverse();
        Ok(log)
    }

    /// Answers every permission request of the session with allow, once: a duplex-woken turn
    /// never waits on a prompt. The session's rules allow everything already; this is the net.
    fn allow_permissions(&self, s: &Managed) -> Result<(), StepError> {
        let base = format!("/api/session/{}/permission", quote(&s.session_id, ""));
        let pending = self.opencode.get(&base)?;
        for request in pending["data"].as_array().into_iter().flatten() {
            let Some(id) = request["id"].as_str() else {
                continue;
            };
            let path = format!("{base}/{}/reply", quote(id, ""));
            match self.opencode.post(&path, &json!({"decision": "once"})) {
                Ok(_) => self.log.info(format!(
                    "{}: allowed {} ({id})",
                    s.spec.title,
                    request["action"].as_str().unwrap_or("?")
                )),
                // Answered meanwhile, by the app or the owner.
                Err(ServerError::Refused { code: 404, .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn is_peer(message: &Value) -> bool {
        let actor = &message["actor"];
        !(actor["kind"] == "user" || (actor["kind"] == "binding" && actor["id"] == BROKER_ID))
    }

    /// Puts one claimed message into the session as its send mode says.
    fn deliver(&self, s: &mut Managed, claimed: &Claimed) -> Result<(), StepError> {
        let message = &claimed.message;
        if Self::is_peer(message) {
            self.log.warn(format!(
                "{}: refused a peer message: peer delivery needs the bridge tool path",
                s.spec.title
            ));
            self.report(
                s,
                &claimed.delivery_id,
                "failed",
                None,
                Some("peer messages need the bridge tool path"),
            )?;
            return Ok(());
        }
        let mode = message["mode"].as_str().unwrap_or("auto");
        let text = message["text"].as_str().unwrap_or_default();
        let sent_ms = wall_ms();
        let input = match self.route(s, mode, text) {
            Ok(input) => input,
            Err(reason) => {
                self.report(s, &claimed.delivery_id, "failed", None, Some(&reason))?;
                return Ok(());
            }
        };
        // Held before the broker hears "accepted", so a lost answer cannot orphan it.
        s.waiters.push(Waiter {
            delivery_id: claimed.delivery_id.clone(),
            conversation_id: claimed.conversation["id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            message_id: message["id"].as_str().unwrap_or_default().to_owned(),
            input_id: input,
            accepted: false,
            since: Instant::now(),
            sent_ms,
        });
        self.acknowledge(s)
    }

    /// The mode's route; the input id it became, or why it cannot be done.
    fn route(&self, s: &Managed, mode: &str, text: &str) -> Result<String, String> {
        let refused = |e: ServerError| format!("OpenCode did not take it: {e}");
        match mode {
            // OpenCode's steer starts an idle session and joins a running one at its next step.
            "auto" => self.prompt(s, text, "steer").map_err(refused),
            "steer" => {
                if !self.busy(s).map_err(refused)? {
                    return Err(format!(
                        "steer needs a running turn, and none runs {}",
                        supports()
                    ));
                }
                self.prompt(s, text, "steer").map_err(refused)
            }
            // OpenCode's queue starts an idle session and waits for a running round to stop.
            "queue" => self.prompt(s, text, "queue").map_err(refused),
            "restart" => {
                if self.busy(s).map_err(refused)? {
                    self.stop(s).map_err(|e| e.to_string())?;
                }
                self.prompt(s, text, "steer").map_err(refused)
            }
            other => Err(format!("mode {other} is not supported {}", supports())),
        }
    }

    fn prompt(&self, s: &Managed, text: &str, delivery: &str) -> Result<String, ServerError> {
        let path = format!("/api/session/{}/prompt", quote(&s.session_id, ""));
        let taken = self
            .opencode
            .post(&path, &json!({"text": text, "delivery": delivery}))?;
        taken["data"]["id"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| ServerError::Invalid("the prompt answer has no input id".into()))
    }

    /// Interrupts the running turn and waits until it stopped. A refused interrupt is OpenCode
    /// saying the turn already ended; the wait decides.
    fn stop(&self, s: &Managed) -> Result<(), StepError> {
        let path = format!(
            "/api/session/{}/interrupt?resume=false",
            quote(&s.session_id, "")
        );
        match self.opencode.call(Verb::Post, &path, None) {
            Ok(_) | Err(ServerError::Refused { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        let deadline = Instant::now() + STOP_TIMEOUT;
        while self.busy(s)? {
            if Instant::now() >= deadline {
                return Err(StepError::Invalid(format!(
                    "the running turn did not stop within {} s",
                    STOP_TIMEOUT.as_secs()
                )));
            }
            std::thread::sleep(self.stop_poll);
        }
        Ok(())
    }
}

/// A refusal the broker will repeat: a client error other than a timeout or a rate limit.
fn is_final(e: &BrokerError) -> bool {
    e.status()
        .is_some_and(|c| (400..500).contains(&c) && c != 408 && c != 429)
}
