//! The loops: serve the broker (push stream with a polling net) and watch the session.

use super::{
    Adapter, AdapterError, BACKOFF, STREAM_FALLBACK, STREAM_RETRY, TURN_RETRY, TURN_UNSUPPORTED,
    WATCH_SECONDS,
};
use crate::log;
use crate::watch::{Event, Limit};
use duplex_proto::BrokerError;
use duplex_proto::client::quote;
use duplex_proto::contract::FINAL_STATUSES;
use serde_json::{Value, json};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tungstenite::client::IntoClientRequest;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Error as WsError, Message};

fn failed_message(what: &str, err: &AdapterError) -> String {
    format!("{what}: {err}")
}

impl Adapter {
    /// Serves the broker on a thread and watches the session on another; both end with the
    /// process.
    pub fn spawn(self: &Arc<Self>) {
        let watcher = Arc::clone(self);
        std::thread::spawn(move || {
            loop {
                watcher.watch_once();
                std::thread::sleep(Duration::from_secs_f64(WATCH_SECONDS));
            }
        });
        let server = Arc::clone(self);
        std::thread::spawn(move || server.serve());
    }

    /// Reads the session once and reports what changed. A failed read or report is noted in
    /// `watch_error` and tried again on the next call: it never ends the loop.
    pub fn watch_once(&self) {
        if !self.is_registered() {
            return;
        }
        let reading = {
            let mut seq = self.reading();
            let (turn, events) = self
                .source
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .sample();
            let reading = self.numbered(&mut seq, turn);
            self.take(events);
            reading
        };
        let mut failed = false;
        for result in [self.report_turn(&reading), self.report_limits()] {
            if let Err(err) = result {
                failed = true;
                self.watch_failed("report", &err);
            }
        }
        if !failed {
            self.st().watch_error.clear();
        }
    }

    fn watch_failed(&self, what: &str, err: &AdapterError) {
        let message = failed_message(what, err);
        let mut st = self.st();
        if st.watch_error != message {
            log::warn(format!("session watch {message}"));
        }
        st.watch_error = message;
    }

    /// Keeps each limit failure for reporting, once per run. A new turn of the session drops
    /// the failures not yet reported and schedules the reported ones for a skip.
    fn take(&self, events: Vec<Event>) {
        let mut st = self.st();
        for event in events {
            match event {
                Event::Limit(limit) => {
                    if st.limit_runs.insert(limit.run_id.clone()) {
                        st.limits_pending.push(limit);
                    }
                }
                Event::Answered => {
                    st.limits_pending.clear();
                    let armed = std::mem::take(&mut st.limits_armed);
                    st.limits_skip.extend(armed);
                }
            }
        }
    }

    /// Sends a reading, whatever was sent before: the claim sends readings too, so only the
    /// broker, ordering them by seq, knows which one it holds.
    fn report_turn(&self, reading: &duplex_proto::contract::TurnBody) -> Result<(), AdapterError> {
        if self
            .st()
            .turn_retry_at
            .is_some_and(|at| Instant::now() < at)
        {
            return Ok(());
        }
        let body =
            serde_json::to_value(reading).map_err(|e| AdapterError::Invalid(e.to_string()))?;
        if let Err(err) = self.post("/api/adapter/turn", Some(&body)) {
            let wait = if matches!(err.status(), Some(404 | 405)) {
                TURN_UNSUPPORTED
            } else {
                TURN_RETRY
            };
            self.st().turn_retry_at = Some(Instant::now() + wait);
            return Err(err.into());
        }
        Ok(())
    }

    /// Skips the resumes the session answered itself, then reports new failures. A refusal the
    /// broker will repeat drops the item; any other error keeps it.
    fn report_limits(&self) -> Result<(), AdapterError> {
        loop {
            let Some(first) = self.st().limits_skip.first().cloned() else {
                break;
            };
            let path = format!("/api/adapter/limits/{}/skip", quote(&first, ":"));
            self.final_call(&path, &json!({"reason": "answered"}))?;
            self.st().limits_skip.remove(0);
        }
        loop {
            let Some(limit) = self.st().limits_pending.first().cloned() else {
                break;
            };
            let row = self.final_call("/api/adapter/limits", &limit_body(&limit))?;
            self.st().limits_pending.remove(0);
            if let Some(row) = row
                && row.get("state").and_then(Value::as_str) == Some("armed")
                && let Some(id) = row.get("id").and_then(Value::as_str)
            {
                self.st().limits_armed.push(id.to_owned());
            }
        }
        Ok(())
    }

    /// A POST whose refusal the broker will repeat is logged and dropped: `Ok(None)`.
    fn final_call(&self, path: &str, body: &Value) -> Result<Option<Value>, AdapterError> {
        match self.post(path, Some(body)) {
            Ok(value) => Ok(Some(value)),
            Err(err) if err.status().is_some_and(|s| FINAL_STATUSES.contains(&s)) => {
                log::info(format!("POST {path} refused: {}", err.detail()));
                Ok(None)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Serves the broker until the owner retires the session.
    pub fn serve(&self) {
        let mut attempt = 0usize;
        loop {
            match self.serve_step(&mut attempt) {
                Ok(()) => {}
                Err(AdapterError::Broker(err)) if err.status().is_some() => {
                    self.st().last_error = err.to_string();
                    if err.status() == Some(410) {
                        // The owner retired this session: stop, never register again.
                        self.st().retired = true;
                        return;
                    }
                    if matches!(err.status(), Some(401 | 403)) {
                        // The token no longer names a live lease: register again.
                        self.st().binding = None;
                    }
                    backoff(&mut attempt);
                }
                Err(err) => {
                    self.st().last_error = err.to_string();
                    backoff(&mut attempt);
                }
            }
        }
    }

    fn serve_step(&self, attempt: &mut usize) -> Result<(), AdapterError> {
        if self.st().binding.is_none() {
            self.register()?;
            log::info(format!(
                "registered with the broker as {}",
                self.binding_id()
            ));
        }
        let due = self
            .st()
            .stream_retry_at
            .is_none_or(|at| Instant::now() >= at);
        if due {
            return self.stream(attempt);
        }
        let took = self.poll_once()?;
        *attempt = 0;
        self.st().last_error.clear();
        if !took {
            std::thread::sleep(Duration::from_secs_f64(self.config.poll_seconds));
        }
        Ok(())
    }

    fn drain(&self) -> Result<(), AdapterError> {
        while self.poll_once()? {}
        Ok(())
    }

    /// Claims on each nudge from the broker's adapter stream instead of polling. Returns when
    /// the broker has no stream, so the loop polls; fails when the stream does.
    fn stream(&self, attempt: &mut usize) -> Result<(), AdapterError> {
        let token = self.st().token.clone();
        let mut request = self
            .client
            .ws_url("/api/adapter/stream")
            .into_client_request()
            .map_err(|e| AdapterError::Io(e.to_string()))?;
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {token}")
                .parse()
                .map_err(|_| AdapterError::Invalid("the token is not a header".into()))?,
        );
        let mut socket = match tungstenite::connect(request) {
            Ok((socket, _)) => socket,
            Err(WsError::Http(answer)) => {
                let mut st = self.st();
                st.stream = "unsupported";
                st.stream_retry_at = Some(Instant::now() + STREAM_RETRY);
                log::info(format!(
                    "the broker has no adapter stream (HTTP {}); polling",
                    answer.status()
                ));
                return Ok(());
            }
            Err(err) => return Err(AdapterError::Io(format!("adapter stream: {err}"))),
        };
        if let MaybeTlsStream::Plain(tcp) = socket.get_mut() {
            tcp.set_read_timeout(Some(STREAM_FALLBACK))
                .map_err(|e| AdapterError::Io(e.to_string()))?;
        }
        self.st().stream = "open";
        *attempt = 0;
        let ended = self.stream_frames(&mut socket);
        let mut st = self.st();
        if st.stream == "open" {
            st.stream = "closed";
        }
        drop(st);
        ended
    }

    fn stream_frames<S: io::Read + io::Write>(
        &self,
        socket: &mut tungstenite::WebSocket<S>,
    ) -> Result<(), AdapterError> {
        loop {
            let frame = match socket.read() {
                Ok(Message::Text(text)) => {
                    serde_json::from_str::<Value>(&text).unwrap_or(Value::Null)
                }
                Ok(Message::Close(_)) => {
                    return Err(AdapterError::Io("adapter stream closed".into()));
                }
                Ok(_) => continue,
                // A quiet stream: claim anyway, as a net under a lost nudge.
                Err(WsError::Io(e))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    json!({"type": "nudge"})
                }
                Err(e) => return Err(AdapterError::Io(format!("adapter stream closed: {e}"))),
            };
            match frame.get("type").and_then(Value::as_str) {
                Some("error") => {
                    return Err(AdapterError::Broker(BrokerError::Status {
                        code: 401,
                        detail: frame
                            .get("detail")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_owned(),
                    }));
                }
                Some("nudge") => {
                    self.drain()?;
                    self.st().last_error.clear();
                }
                _ => {}
            }
        }
    }
}

fn backoff(attempt: &mut usize) {
    std::thread::sleep(Duration::from_secs_f64(
        BACKOFF[(*attempt).min(BACKOFF.len() - 1)],
    ));
    *attempt += 1;
}

fn limit_body(limit: &Limit) -> Value {
    json!({
        "run_id": limit.run_id,
        "source": limit.source,
        "matched": limit.matched,
        "failed_at": limit.failed_at,
        "reset_at": limit.reset_at,
    })
}
