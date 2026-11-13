//! The broker's adapter side for one session: registers it, claims each delivery and wakes the
//! session with the fixed line, hands the text over through `receive`, and reports the
//! session's turn state and usage limits beside.
//!
//! The message text never goes into the wake line: it is read with `bridge_receive`, so it
//! arrives as tool data, not as a prompt.

mod chat;
mod serve;

use crate::compat::{adapter_info, review_due};
use crate::config::AdapterConfig;
use crate::identity::{Participant, default_home};
use crate::log;
use crate::paging::PageError;
use crate::policy::mode_refusal;
use crate::watch::{Limit, Source, Turn};
use duplex_proto::client::Method;
use duplex_proto::contract::{
    Binding, Claim, DeliveryState, Refusal, Registered, Rewake, TurnBody,
};
use duplex_proto::wake::wake_line;
use duplex_proto::{BrokerClient, BrokerError};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const BACKOFF: [f64; 5] = [2.0, 5.0, 15.0, 30.0, 60.0];
/// A failed wake is tried once more after this many seconds, then reported failed: 2 s covers
/// a transient pipe or queue error.
pub const WAKE_RETRY: f64 = 2.0;
/// With the broker's adapter stream open, a claim runs on each nudge and, as a net under a
/// lost nudge, after this many quiet seconds.
pub const STREAM_FALLBACK: Duration = Duration::from_secs(60);
/// A broker without the stream is polled; the stream is tried again after this long.
pub const STREAM_RETRY: Duration = Duration::from_secs(600);
/// How often the session's turn state and transcript are read.
pub const WATCH_SECONDS: f64 = 2.0;
/// How soon a failed turn-state report is tried again (a broker without the route: after
/// `TURN_UNSUPPORTED`).
pub const TURN_RETRY: Duration = Duration::from_secs(15);
pub const TURN_UNSUPPORTED: Duration = Duration::from_secs(600);

pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// Why an adapter call failed.
#[derive(Debug)]
pub enum AdapterError {
    Broker(BrokerError),
    /// A wake or a file failed.
    Io(String),
    /// A refusal for the session: bad names, a page that cannot be made.
    Invalid(String),
}

impl AdapterError {
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Broker(e) => e.status(),
            _ => None,
        }
    }

    pub fn detail(&self) -> String {
        match self {
            Self::Broker(e) if e.status().is_some() => e.detail().to_owned(),
            other => other.to_string(),
        }
    }
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Broker(e) => write!(f, "{e}"),
            Self::Io(s) | Self::Invalid(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for AdapterError {}

impl From<BrokerError> for AdapterError {
    fn from(e: BrokerError) -> Self {
        Self::Broker(e)
    }
}

impl From<io::Error> for AdapterError {
    fn from(e: io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

impl From<PageError> for AdapterError {
    fn from(e: PageError) -> Self {
        Self::Invalid(e.0)
    }
}

/// A delivery claimed and woken, waiting for `receive`.
#[derive(Clone)]
pub(crate) struct Pending {
    pub delivery_id: String,
    pub message: Value,
    pub conversation: Value,
    /// The broker has the hand-over as accepted.
    pub accepted: bool,
}

#[derive(Default)]
pub(crate) struct State {
    pub binding: Option<Binding>,
    pub token: String,
    pub owner_name: String,
    /// In claim order; `keep` puts deliveries back ahead of newer ones.
    pub pending: Vec<Pending>,
    /// message id -> (delivery id, author id)
    pub consumed: HashMap<String, (String, String)>,
    pub stream: &'static str,
    pub stream_retry_at: Option<Instant>,
    pub last_error: String,
    pub retired: bool,
    pub adapter: Option<Value>,
    pub turn_read: Option<Turn>,
    pub watch_error: String,
    pub turn_retry_at: Option<Instant>,
    pub receipts_held: Vec<Value>,
    pub entries_held: Vec<Value>,
    pub limit_runs: HashSet<String>,
    pub limits_pending: Vec<Limit>,
    pub limits_armed: Vec<String>,
    pub limits_skip: Vec<String>,
}

type Wake = Box<dyn Fn(&str) -> io::Result<()> + Send + Sync>;

pub struct Adapter {
    pub config: AdapterConfig,
    pub identity: Participant,
    pub(crate) client: BrokerClient,
    pub(crate) wake: Wake,
    pub wake_retry: Duration,
    pub home: PathBuf,
    pub(crate) state: Mutex<State>,
    /// Held across a hand-over: a queued Codex wake can run, and call `bridge_receive`,
    /// before the wake call returns.
    pub(crate) handover: Mutex<()>,
    /// A reading and its number, together: the next sequence number.
    pub(crate) reading: Mutex<u64>,
    pub(crate) source: Mutex<Box<dyn Source>>,
}

impl Adapter {
    pub fn new(config: AdapterConfig, identity: Participant) -> Self {
        let home = default_home();
        let source = identity.source(&home, now());
        let wake_identity = identity.clone();
        Self::with_parts(
            config,
            identity,
            home,
            source,
            Box::new(move |text| wake_identity.wake(text)),
        )
    }

    /// The adapter with its wake and its session reader given, for tests.
    pub fn with_parts(
        config: AdapterConfig,
        identity: Participant,
        home: PathBuf,
        source: Box<dyn Source>,
        wake: Wake,
    ) -> Self {
        let client = BrokerClient::new(&config.broker_url, Duration::from_secs(15));
        Self {
            config,
            identity,
            client,
            wake,
            wake_retry: Duration::from_secs_f64(WAKE_RETRY),
            home,
            state: Mutex::new(State {
                stream: "closed",
                owner_name: "the owner".to_owned(),
                ..State::default()
            }),
            handover: Mutex::new(()),
            reading: Mutex::new(0),
            source: Mutex::new(source),
        }
    }

    pub(crate) fn st(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn reading(&self) -> std::sync::MutexGuard<'_, u64> {
        self.reading
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn peek_turn(&self) -> Turn {
        self.source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .peek_turn()
    }

    pub fn owner_name(&self) -> String {
        self.st().owner_name.clone()
    }

    pub fn binding_id(&self) -> String {
        self.st()
            .binding
            .as_ref()
            .map_or_else(String::new, |b| b.id.clone())
    }

    pub fn title(&self) -> String {
        let bound = self.st().binding.as_ref().map(|b| b.title.clone());
        bound.unwrap_or_else(|| self.identity.title(&self.home))
    }

    pub fn is_registered(&self) -> bool {
        let st = self.st();
        st.binding.is_some() && !st.retired
    }

    pub fn status(&self) -> Value {
        let st = self.st();
        let mut out = json!({
            "broker": self.config.broker_url,
            "registered": st.binding.is_some() && !st.retired,
            "retired": st.retired,
            "binding_id": st.binding.as_ref().map(|b| &b.id),
            "title": st.binding.as_ref().map(|b| &b.title),
            "pending": st.pending.len(),
            "stream": st.stream,
            "last_error": st.last_error,
            "adapter": st.adapter,
            "turn": st.turn_read.map(|t| t.state.as_str()),
            "watch_error": st.watch_error,
        });
        if let Some(adapter) = &st.adapter {
            let runtime: HashMap<String, String> = adapter["runtime"]
                .as_object()
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_owned()))
                        .collect()
                })
                .unwrap_or_default();
            let due = review_due(&runtime, &adapter["built_against"]);
            if !due.is_empty() {
                out["review_due"] = json!(due);
            }
        }
        out
    }

    /// Whether the broker counts this binding's adapter behind the newest one among active
    /// sessions. A broker before adapter versions has no such answer.
    pub fn standing(&self) -> Value {
        match self.call(Method::Get, "/api/adapter/binding", None) {
            Ok(binding) => json!({"adapter_behind": binding.get("adapter_behind")}),
            Err(e) if e.status() == Some(404) => {
                json!({"adapter_behind": "unknown: the broker does not report it"})
            }
            Err(e) => json!({"adapter_behind": format!("unknown: {e}")}),
        }
    }

    /// A call as this session's binding.
    pub(crate) fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, BrokerError> {
        let token = self.st().token.clone();
        self.client.call(method, path, body, &token)
    }

    pub(crate) fn post(&self, path: &str, body: Option<&Value>) -> Result<Value, BrokerError> {
        self.call(Method::Post, path, body)
    }

    pub(crate) fn get(&self, path: &str) -> Result<Value, BrokerError> {
        self.call(Method::Get, path, None)
    }

    fn token_file(&self) -> Option<PathBuf> {
        let dir = self.config.token_dir.as_ref()?;
        let digest = Sha256::digest(self.identity.native_id().as_bytes());
        let key: String = digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
            .chars()
            .take(24)
            .collect();
        Some(dir.join(format!("{}-{key}.token", self.identity.provider())))
    }

    /// This session's last binding token, kept across restarts.
    fn held_token(&self) -> String {
        let held = self.st().token.clone();
        if !held.is_empty() {
            return held;
        }
        self.token_file()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .map_or_else(String::new, |t| t.trim().to_owned())
    }

    fn keep_token(&self, token: &str) {
        let Some(path) = self.token_file() else {
            return;
        };
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        if std::fs::write(&path, token).is_ok() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
            }
        }
    }

    /// This adapter's version, commit, built-against and runtime versions. Never blocks
    /// registration: without them the broker reads the adapter as old.
    fn describe_self(&self) -> Value {
        adapter_info(&self.identity.runtime(&self.home))
    }

    pub fn register(&self) -> Result<(), AdapterError> {
        let have = self.st().adapter.is_some();
        if !have {
            let info = self.describe_self();
            self.st().adapter = Some(info);
        }
        let adapter = self.st().adapter.clone();
        let mut body = json!({
            "provider": self.identity.provider(),
            "native_id": self.identity.native_id(),
            "title": self.identity.title(&self.home),
            "workspace": self.identity.workspace(&self.home),
            "endpoint_ref": self.identity.endpoint_ref(),
            "capabilities": duplex_proto::contract::capabilities(self.identity.send_modes()),
            "current_token": self.held_token(),
        });
        if let Some(adapter) = adapter {
            body["adapter"] = adapter;
        }
        // The broker resets its reading count when it registers; under the reading lock no
        // reading is numbered between its reset and this one.
        let mut seq = self.reading();
        let answer = self.client.call(
            Method::Post,
            "/api/adapter/register",
            Some(&body),
            &self.config.enrollment_token,
        )?;
        let registered: Registered = serde_json::from_value(answer.clone())
            .map_err(|e| AdapterError::Invalid(format!("registration answer: {e}")))?;
        self.keep_token(&registered.token);
        let mut st = self.st();
        st.token = registered.token;
        st.binding = Some(registered.binding);
        if let Some(name) = answer
            .get("owner_name")
            .and_then(Value::as_str)
            .filter(|n| !n.is_empty())
        {
            st.owner_name = name.to_owned();
        }
        st.pending.clear();
        st.consumed.clear();
        st.turn_retry_at = None;
        *seq = 0; // from 1 again, as the broker counts
        Ok(())
    }

    /// A turn just read, as the broker takes it, with the next number. Called under the
    /// reading lock, so the numbers run in the order the readings were taken.
    pub(crate) fn numbered(&self, seq: &mut u64, turn: Turn) -> TurnBody {
        *seq += 1;
        self.st().turn_read = Some(turn);
        TurnBody::new(turn.state.as_str(), turn.waiting_for, *seq)
    }

    /// Claims at most one delivery and wakes the session for it; true if one came.
    pub fn poll_once(&self) -> Result<bool, AdapterError> {
        let reading = {
            let mut seq = self.reading();
            let turn = self.peek_turn();
            self.numbered(&mut seq, turn)
        };
        let claimed = self.post("/api/adapter/claim", Some(&json!({"turn": reading})))?;
        let claim = Claim::parse(&claimed).map_err(AdapterError::Invalid)?;
        let claimed = match claim {
            Claim::Nothing => return Ok(false),
            Claim::Rewake(due) => {
                self.rewake(&due)?;
                return Ok(true);
            }
            Claim::Delivery(claimed) => *claimed,
        };
        let mode = claimed
            .message
            .get("mode")
            .and_then(Value::as_str)
            .unwrap_or("auto")
            .to_owned();
        if let Some(refusal) = self.mode_refusal(&mode) {
            self.state(
                &claimed.delivery_id,
                DeliveryState::Failed,
                Some(&refusal.chars().take(300).collect::<String>()),
            )?;
            return Ok(true);
        }
        let text = wake_line(
            self.identity.native_id(),
            claimed.message["id"].as_str().unwrap_or(""),
            claimed.conversation["kind"].as_str().unwrap_or("direct"),
        );
        // Held until the handover is recorded.
        let _hand = self
            .handover
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let delivery_id = claimed.delivery_id.clone();
        self.st().pending.push(Pending {
            delivery_id: delivery_id.clone(),
            message: claimed.message,
            conversation: claimed.conversation,
            accepted: false,
        });
        if let Err(err) = self.hand_over(&text) {
            self.st().pending.retain(|p| p.delivery_id != delivery_id);
            let reason = format!("wake failed: {err}");
            self.st().last_error = reason.clone();
            self.state(
                &delivery_id,
                DeliveryState::Failed,
                Some(&reason.chars().take(300).collect::<String>()),
            )?;
            return Ok(true);
        }
        if self.identity.wake_confirms() {
            self.state(&delivery_id, DeliveryState::Accepted, None)?;
            if let Some(p) = self
                .st()
                .pending
                .iter_mut()
                .find(|p| p.delivery_id == delivery_id)
            {
                p.accepted = true;
            }
        }
        Ok(true)
    }

    /// Why this delivery's send mode cannot be done now. Steer is checked against the
    /// session's turn as it is now, not as last read.
    fn mode_refusal(&self, mode: &str) -> Option<String> {
        let turn = if mode == "steer" {
            self.peek_turn()
        } else {
            Turn::UNKNOWN
        };
        mode_refusal(self.identity.send_modes(), &self.title(), mode, turn.state)
    }

    /// Sends the fixed wake line again for an unread delivery and reports the outcome. The
    /// check and the wake run under the hand-over lock: a read cannot slip between them.
    fn rewake(&self, due: &Rewake) -> Result<(), AdapterError> {
        let (mut ok, mut error) = (true, None);
        {
            let _hand = self
                .handover
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let message = self
                .st()
                .pending
                .iter()
                .find(|p| p.delivery_id == due.delivery_id)
                .map(|p| p.message["id"].as_str().unwrap_or("").to_owned());
            match message {
                None => {
                    (ok, error) = (false, Some("the adapter holds no such delivery".to_owned()))
                }
                Some(message) => {
                    let text =
                        wake_line(self.identity.native_id(), &message, &due.conversation_kind);
                    if let Err(err) = self.hand_over(&text) {
                        let reason = format!("wake failed: {err}");
                        self.st().last_error = reason.clone();
                        (ok, error) = (false, Some(reason));
                    }
                }
            }
        }
        let mut body = json!({"attempt": due.attempt, "ok": ok});
        if let Some(error) = error {
            body["error"] = json!(error.chars().take(300).collect::<String>());
        }
        match self.post(
            &format!("/api/adapter/deliveries/{}/wake", due.delivery_id),
            Some(&body),
        ) {
            Err(e) if matches!(e.status(), Some(409 | 422)) => {
                // The delivery left flight while the line was sent: nothing to count.
                log::info(format!(
                    "re-wake report for {} refused: {}",
                    due.delivery_id,
                    e.detail()
                ));
                Ok(())
            }
            Err(e) => Err(e.into()),
            Ok(_) => Ok(()),
        }
    }

    /// Sends the wake line; on a failure, once more after `wake_retry`. The second failure is
    /// returned for the caller to report.
    fn hand_over(&self, text: &str) -> io::Result<()> {
        if let Err(err) = (self.wake)(text) {
            log::info(format!(
                "wake failed ({err}); trying once more in {:.0} s",
                self.wake_retry.as_secs_f64()
            ));
            std::thread::sleep(self.wake_retry);
            (self.wake)(text)?;
        }
        Ok(())
    }

    /// Reports a delivery state.
    pub(crate) fn state(
        &self,
        delivery_id: &str,
        state: DeliveryState,
        reason: Option<&str>,
    ) -> Result<(), BrokerError> {
        let mut body = json!({"state": state.as_str()});
        if let Some(reason) = reason.filter(|r| !r.is_empty()) {
            body["reason"] = json!(reason);
        }
        self.post(
            &format!("/api/adapter/deliveries/{delivery_id}/state"),
            Some(&body),
        )
        .map(|_| ())
    }

    /// Reports the delivery consumed, after reporting it accepted when the broker does not
    /// have that yet: a Claude wake is unconfirmed until this read. `Ok(None)`: the broker has
    /// it as read. `Ok(Some(err))`: the broker confirms the delivery ended unread. Any other
    /// failure leaves the read unconfirmed and the delivery open.
    pub(crate) fn report_read(
        &self,
        delivery_id: &str,
        accepted: &mut bool,
    ) -> Result<Option<BrokerError>, BrokerError> {
        let mut resent = false;
        loop {
            let step = if *accepted {
                DeliveryState::Consumed
            } else {
                DeliveryState::Accepted
            };
            match self.state(delivery_id, step, None) {
                Ok(()) => {
                    if step == DeliveryState::Consumed {
                        return Ok(None);
                    }
                    *accepted = true;
                }
                Err(err) if err.status() == Some(422) => match Refusal::of(err.detail()) {
                    // An earlier report landed and its answer was lost.
                    Some(Refusal::AlreadyRead) => return Ok(None),
                    Some(Refusal::ClosedUnread) => return Ok(Some(err)),
                    Some(Refusal::AlreadyAccepted) => *accepted = true,
                    // The hand-over report was lost: send it again, then the read.
                    Some(Refusal::NotAccepted) if !resent => {
                        *accepted = false;
                        resent = true;
                    }
                    _ => return Err(err),
                },
                Err(err) => return Err(err),
            }
        }
    }

    pub fn waiting(&self) -> usize {
        let st = self.st();
        st.pending.len() + st.entries_held.len()
    }

    pub fn conversation(&self, conversation_id: &str) -> Result<Value, BrokerError> {
        self.get(&format!("/api/adapter/conversations/{conversation_id}"))
    }
}
