//! A blocking HTTP client for the broker. It holds no token: each call names its bearer,
//! so one client serves a session adapter, the waker and the owner tool alike.

use serde::de::DeserializeOwned;
use serde_json::Value;
use std::fmt;
use std::time::Duration;

/// The most of a broker's error body a `detail` keeps.
const DETAIL_CHARS: usize = 300;

/// Why a broker call failed. No variant carries a header, so none carries a token.
#[derive(Debug, PartialEq)]
pub enum BrokerError {
    /// No answer: refused, timed out, DNS.
    Unreachable(String),
    /// An answer other than 2xx; `detail` is the broker's `detail` text.
    Status { code: u16, detail: String },
    /// An answer that is not the JSON the route promises.
    Invalid(String),
}

impl BrokerError {
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Status { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// The broker's reason for a refusal, empty for the other kinds.
    pub fn detail(&self) -> &str {
        match self {
            Self::Status { detail, .. } => detail,
            _ => "",
        }
    }
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreachable(why) => write!(f, "broker unreachable: {why}"),
            Self::Status { code, detail } if detail.is_empty() => {
                write!(f, "broker answered HTTP {code}")
            }
            Self::Status { code, detail } => write!(f, "broker answered HTTP {code}: {detail}"),
            Self::Invalid(why) => write!(f, "broker answer not understood: {why}"),
        }
    }
}

impl std::error::Error for BrokerError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Delete,
}

#[derive(Clone)]
pub struct BrokerClient {
    base: String,
    agent: ureq::Agent,
    timeout: Duration,
}

impl BrokerClient {
    pub fn new(broker_url: &str, timeout: Duration) -> Self {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            .http_status_as_error(false)
            .build()
            .into();
        Self {
            base: broker_url.trim_end_matches('/').to_owned(),
            agent,
            timeout,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// The `ws://` address of a broker route, for the adapter stream.
    pub fn ws_url(&self, path: &str) -> String {
        format!(
            "ws{}{path}",
            self.base.strip_prefix("http").unwrap_or(&self.base)
        )
    }

    /// One call. The answer's JSON, or `Null` for an empty body (a 204).
    pub fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        bearer: &str,
    ) -> Result<Value, BrokerError> {
        let url = format!("{}{path}", self.base);
        let auth = format!("Bearer {bearer}");
        let answered = match method {
            Method::Get => {
                let mut request = self.agent.get(&url);
                if !bearer.is_empty() {
                    request = request.header("Authorization", &auth);
                }
                request.call()
            }
            Method::Delete => {
                let mut request = self.agent.delete(&url);
                if !bearer.is_empty() {
                    request = request.header("Authorization", &auth);
                }
                request.call()
            }
            Method::Post => {
                let mut request = self.agent.post(&url);
                if !bearer.is_empty() {
                    request = request.header("Authorization", &auth);
                }
                match body {
                    Some(body) => request.send_json(body),
                    None => request.send_empty(),
                }
            }
        };
        let mut response = answered.map_err(unreachable)?;
        let code = response.status().as_u16();
        let text = response
            .body_mut()
            .read_to_string()
            .map_err(|e| BrokerError::Invalid(short(&e.to_string())))?;
        if !(200..300).contains(&code) {
            return Err(BrokerError::Status {
                code,
                detail: detail_of(&text),
            });
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| BrokerError::Invalid(short(&e.to_string())))
    }

    pub fn get(&self, path: &str, bearer: &str) -> Result<Value, BrokerError> {
        self.call(Method::Get, path, None, bearer)
    }

    pub fn post(
        &self,
        path: &str,
        body: Option<&Value>,
        bearer: &str,
    ) -> Result<Value, BrokerError> {
        self.call(Method::Post, path, body, bearer)
    }

    /// `call`, with the answer read as a `T`.
    pub fn call_as<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        bearer: &str,
    ) -> Result<T, BrokerError> {
        let value = self.call(method, path, body, bearer)?;
        serde_json::from_value(value).map_err(|e| BrokerError::Invalid(short(&e.to_string())))
    }
}

fn short(text: &str) -> String {
    text.chars().take(200).collect()
}

// The transport error's text names the cause (refused, timed out, DNS), never a header.
fn unreachable(err: ureq::Error) -> BrokerError {
    BrokerError::Unreachable(short(&err.to_string()))
}

/// The `detail` of a FastAPI error body, else the start of the body.
fn detail_of(body: &str) -> String {
    let text = match serde_json::from_str::<Value>(body) {
        Ok(Value::Object(map)) => match map.get("detail") {
            Some(Value::String(s)) if !s.is_empty() => s.clone(),
            Some(Value::Null) | None => body.to_owned(),
            Some(Value::String(_)) => body.to_owned(),
            Some(other) => other.to_string(),
        },
        _ => body.to_owned(),
    };
    text.chars().take(DETAIL_CHARS).collect()
}

/// Percent-encodes `text` for a URL path or query, leaving letters, digits, `_.-~` and
/// the characters in `safe` as they are.
pub fn quote(text: &str, safe: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(byte, b'_' | b'.' | b'-' | b'~')
            || safe.as_bytes().contains(&byte)
        {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_detail_is_the_brokers_reason() {
        assert_eq!(
            detail_of(r#"{"detail": "cannot go from failed to consumed"}"#),
            "cannot go from failed to consumed"
        );
        assert_eq!(detail_of("plain text"), "plain text");
        assert_eq!(detail_of(r#"{"detail": [1]}"#), "[1]");
        assert_eq!(detail_of(r#"{"other": 1}"#), r#"{"other": 1}"#);
        assert_eq!(detail_of(&"x".repeat(900)).len(), DETAIL_CHARS);
    }

    #[test]
    fn quoting_keeps_the_safe_set() {
        assert_eq!(quote("a b/c:d", ""), "a%20b%2Fc%3Ad");
        assert_eq!(quote("a:b", ":"), "a:b");
        assert_eq!(quote("é", ""), "%C3%A9");
    }

    #[test]
    fn the_stream_address_follows_the_scheme() {
        let plain = BrokerClient::new("http://broker.test:8790/", Duration::from_secs(1));
        assert_eq!(
            plain.ws_url("/api/adapter/stream"),
            "ws://broker.test:8790/api/adapter/stream"
        );
        let secure = BrokerClient::new("https://broker.test", Duration::from_secs(1));
        assert_eq!(secure.ws_url("/x"), "wss://broker.test/x");
    }
}
