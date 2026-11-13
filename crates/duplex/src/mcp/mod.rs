//! A stdio MCP server: one JSON-RPC message per line in, one per line out. stdout carries
//! nothing else; logs go to stderr.

pub mod args;
pub mod tools;

use crate::cap::dumps;
use crate::redact::{redact, redact_duplex_result};
use serde_json::{Map, Value, json};
use std::io::{BufRead, Write};
use tools::{Server, Tool, ToolError, tools};

const PROTOCOLS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];

fn reply(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn failure(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

/// A tool result: the redacted payload as indented JSON text.
fn content(text: &str, is_error: bool) -> Value {
    json!({"content": [{"type": "text", "text": text}], "isError": is_error})
}

fn call(server: &Server, listed: &[Tool], params: &Value) -> Value {
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    let Some(tool) = listed.iter().find(|t| t.name == name) else {
        return content(&format!("no tool named {name}"), true);
    };
    let given: Map<String, Value> = params
        .get("arguments")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let outcome = tool
        .table
        .normalized(&given)
        .map_err(ToolError::Refusal)
        .and_then(|args| (tool.handler)(server, &args));
    match outcome {
        Ok(payload) => content(&dumps(&redact_duplex_result(&payload), Some(1)), false),
        Err(ToolError::Refusal(text)) => content(&redact(&text), true),
        Err(ToolError::Failed(text)) => content(&redact(&format!("error: {text}")), true),
    }
}

/// The response to one request, or None for a notification.
pub fn handle(server: &Server, listed: &[Tool], request: &Value) -> Option<Value> {
    let id = request.get("id")?;
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    let params = request.get("params").cloned().unwrap_or(Value::Null);
    Some(match method {
        "initialize" => {
            let asked = params
                .get("protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or("");
            let version = PROTOCOLS
                .iter()
                .find(|p| **p == asked)
                .unwrap_or(&PROTOCOLS[0]);
            reply(
                id,
                json!({
                    "protocolVersion": version,
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {"name": "duplex", "version": env!("CARGO_PKG_VERSION")},
                }),
            )
        }
        "ping" => reply(id, json!({})),
        "tools/list" => reply(
            id,
            json!({"tools": listed.iter().map(Tool::listing).collect::<Vec<_>>()}),
        ),
        "tools/call" => reply(id, call(server, listed, &params)),
        other => failure(id, -32601, &format!("method not found: {other}")),
    })
}

/// Reads requests until the input ends and writes each response.
pub fn serve(server: &Server, input: impl BufRead, output: &mut impl Write) -> std::io::Result<()> {
    let listed = tools();
    for line in input.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => handle(server, &listed, &request),
            Err(_) => Some(failure(&Value::Null, -32700, "parse error")),
        };
        if let Some(response) = response {
            writeln!(output, "{response}")?;
            output.flush()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(server: &Server, lines: &[Value]) -> Vec<Value> {
        let input = lines
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        let mut out = Vec::new();
        serve(server, input.as_bytes(), &mut out).unwrap();
        String::from_utf8(out)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn the_handshake_lists_the_tools_and_ignores_notifications() {
        let server = Server::new(json!({"host": "other"}));
        let out = run(
            &server,
            &[
                json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05"}}),
                json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
                json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
                json!({"jsonrpc": "2.0", "id": 3, "method": "ping"}),
                json!({"jsonrpc": "2.0", "id": 4, "method": "nope"}),
            ],
        );
        assert_eq!(out.len(), 4, "a notification gets no answer");
        assert_eq!(out[0]["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(out[0]["result"]["serverInfo"]["name"], "duplex");
        assert_eq!(out[1]["result"]["tools"].as_array().unwrap().len(), 14);
        assert_eq!(out[2]["result"], json!({}));
        assert_eq!(out[3]["error"]["code"], -32601);
    }

    #[test]
    fn an_unknown_protocol_gets_the_newest_this_server_speaks() {
        let server = Server::new(json!({"host": "other"}));
        let out = run(
            &server,
            &[
                json!({"jsonrpc": "2.0", "id": "a", "method": "initialize", "params": {"protocolVersion": "1999-01-01"}}),
            ],
        );
        assert_eq!(out[0]["result"]["protocolVersion"], PROTOCOLS[0]);
        assert_eq!(out[0]["id"], "a");
    }

    #[test]
    fn a_tool_call_returns_redacted_text_and_refusals_are_errors_for_the_model() {
        let server = Server::new(json!({"host": "other"}));
        let out = run(
            &server,
            &[
                json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "bridge_status", "arguments": {}}}),
                json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "bridge_receive", "arguments": {}}}),
                json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "bridge_send", "arguments": {"text": "x"}}}),
                json!({"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "bridge_send", "arguments": {"conversation_id": "c", "text": "x", "bogus": 1}}}),
                json!({"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "nothing"}}),
                json!({"jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": {"name": "bridge_register", "arguments": {"thread_id": "nope"}}}),
            ],
        );
        assert_eq!(out[0]["result"]["isError"], false);
        let status: Value =
            serde_json::from_str(out[0]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(status, json!({"active": false, "host": {"host": "other"}}));
        assert_eq!(out[1]["result"]["isError"], true);
        assert!(
            out[1]["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with("duplex-chat is not active here.")
        );
        assert_eq!(
            out[2]["result"]["content"][0]["text"],
            "conversation_id is required."
        );
        assert_eq!(
            out[3]["result"]["content"][0]["text"],
            "bogus: not an argument of this tool."
        );
        assert_eq!(
            out[4]["result"]["content"][0]["text"],
            "no tool named nothing"
        );
        assert!(
            out[5]["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("CODEX_THREAD_ID")
        );
    }

    #[test]
    fn a_line_that_is_not_json_is_a_parse_error_and_the_loop_goes_on() {
        let server = Server::new(json!({"host": "other"}));
        let mut out = Vec::new();
        serve(
            &server,
            "{broken\n\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n".as_bytes(),
            &mut out,
        )
        .unwrap();
        let lines: Vec<Value> = String::from_utf8(out)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines[0]["error"]["code"], -32700);
        assert_eq!(lines[1]["result"], json!({}));
    }
}
