//! The broker's waker routes: list the wake requests, report each start.

use crate::client::{BrokerClient, BrokerError};
use serde::{Deserialize, Serialize};

/// One listed wake request (contract: Waker, `GET /api/waker/requests`).
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Request {
    pub id: String,
    pub binding_id: String,
    pub provider: String,
    pub native_id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub workspace: Option<String>,
    pub reason: String,
    pub message_id: String,
    #[serde(default)]
    pub due_at: Option<String>,
}

/// The body of `POST /api/waker/requests/{id}/started`.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Report {
    pub ok: bool,
    pub route: String,
    pub detail: String,
    /// `<request id>:<started ms>:<n>`, the same on every retry: the broker keeps one
    /// record per id and answers a repeat as taken (contract: Waker report ids). Set
    /// when the report is queued; empty until then.
    pub report_id: String,
}

impl Report {
    pub fn new(ok: bool, route: &str, detail: &str) -> Self {
        // The broker keeps at most 300 characters of detail.
        Self {
            ok,
            route: route.to_owned(),
            detail: detail.chars().take(300).collect(),
            report_id: String::new(),
        }
    }
}

/// A report's id (contract: Waker report ids): its request, the waker process's start in
/// UTC milliseconds, and its 1-based number among that request's reports in the process.
pub fn report_id(request_id: &str, started_ms: u64, n: u32) -> String {
    format!("{request_id}:{started_ms}:{n}")
}

/// `GET /api/waker/requests`.
pub fn requests(client: &BrokerClient, token: &str) -> Result<Vec<Request>, BrokerError> {
    client.call_as(
        crate::client::Method::Get,
        "/api/waker/requests",
        None,
        token,
    )
}

/// `POST /api/waker/requests/{id}/started`.
pub fn started(
    client: &BrokerClient,
    token: &str,
    request_id: &str,
    report: &Report,
) -> Result<(), BrokerError> {
    let body = serde_json::to_value(report).map_err(|e| BrokerError::Invalid(e.to_string()))?;
    client.post(
        &format!("/api/waker/requests/{request_id}/started"),
        Some(&body),
        token,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_report_keeps_300_characters_and_an_id_names_its_run() {
        assert_eq!(Report::new(true, "r", &"x".repeat(500)).detail.len(), 300);
        assert_eq!(
            report_id("req", 1_700_000_000_000, 2),
            "req:1700000000000:2"
        );
    }
}
