//! The size cap on a tool result, applied where the result is built so nothing is cut after
//! the fact: a client spills an oversized result to a file and loses the locators in it.
//!
//! The serialized `CallToolResult` (the content text and the envelope around it) is at most
//! `RESULT_CAP_BYTES`. A tool that hands over data it consumes pages inside this budget (see
//! `paging`) instead of letting the data be cut.

use serde_json::Value;
use std::fmt::Write as _;

pub const RESULT_CAP_BYTES: usize = 20_480;
/// What the JSON-RPC envelope around a result adds on the wire (the id, the version, the
/// server's stamp).
pub const ENVELOPE_BYTES: usize = 256;
/// `{"content":[{"type":"text","text":` and `}],"isError":false}` around the text.
const RESULT_WRAPPER_BYTES: usize = 34 + 19;

fn escape(text: &str, out: &mut String) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 || (c as u32) > 0x7e && c != '\u{7f}' => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    let _ = write!(out, "\\u{unit:04x}");
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn write_value(value: &Value, indent: Option<usize>, level: usize, out: &mut String) {
    let pad =
        |level: usize| indent.map_or_else(String::new, |n| format!("\n{}", " ".repeat(n * level)));
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => escape(s, out),
        Value::Array(items) if items.is_empty() => out.push_str("[]"),
        Value::Object(map) if map.is_empty() => out.push_str("{}"),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(if indent.is_some() { "," } else { ", " });
                }
                out.push_str(&pad(level + 1));
                write_value(item, indent, level + 1, out);
            }
            out.push_str(&pad(level));
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (i, (key, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push_str(if indent.is_some() { "," } else { ", " });
                }
                out.push_str(&pad(level + 1));
                escape(key, out);
                out.push_str(": ");
                write_value(item, indent, level + 1, out);
            }
            out.push_str(&pad(level));
            out.push('}');
        }
    }
}

/// The value as Python's `json.dumps(value, ensure_ascii=True)` writes it, with `indent`
/// spaces per level when given. Sizes are measured in this form.
pub fn dumps(value: &Value, indent: Option<usize>) -> String {
    let mut out = String::new();
    write_value(value, indent, 0, &mut out);
    out
}

/// The bytes the result text takes inside the result: JSON-escaped once more.
fn encoded_bytes(text: &str) -> usize {
    2 + text
        .chars()
        .map(|c| match c {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
            c if (c as u32) < 0x20 => 6,
            c => c.len_utf8(),
        })
        .sum::<usize>()
}

/// Whether a payload goes out as it is: the serialized result is within the cap.
pub fn fits_whole(payload: &Value) -> bool {
    encoded_bytes(&dumps(payload, Some(1))) + RESULT_WRAPPER_BYTES + ENVELOPE_BYTES
        <= RESULT_CAP_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn dumps_matches_pythons_layout() {
        let value = json!({"a": [1, 2], "b": {}, "c": [], "d": "é\n\"", "e": null, "f": true});
        assert_eq!(
            dumps(&value, Some(1)),
            "{\n \"a\": [\n  1,\n  2\n ],\n \"b\": {},\n \"c\": [],\n \"d\": \"\\u00e9\\n\\\"\",\n \"e\": null,\n \"f\": true\n}"
        );
        assert_eq!(
            dumps(&value, None),
            "{\"a\": [1, 2], \"b\": {}, \"c\": [], \"d\": \"\\u00e9\\n\\\"\", \"e\": null, \"f\": true}"
        );
        assert_eq!(dumps(&json!("😀"), None), "\"\\ud83d\\ude00\"");
        assert_eq!(dumps(&json!("\u{1}\u{7f}"), None), "\"\\u0001\u{7f}\"");
    }

    #[test]
    fn a_payload_fits_until_its_wire_size_passes_the_cap() {
        assert!(fits_whole(&json!({"text": "x".repeat(19_000)})));
        assert!(!fits_whole(&json!({"text": "x".repeat(20_200)})));
        // Newlines and quotes cost two bytes on the wire.
        assert!(!fits_whole(&json!({"text": "\"".repeat(9_000)})));
        // The edge: the dump of {"t": "x…"} is n + 12 characters with two newlines and four
        // quotes that cost two bytes each, so n + 20 bytes inside the result string with its own
        // two quotes, plus the wrapper and the envelope.
        let edge = RESULT_CAP_BYTES - RESULT_WRAPPER_BYTES - ENVELOPE_BYTES - 20;
        assert!(fits_whole(&json!({"t": "x".repeat(edge)})));
        assert!(!fits_whole(&json!({"t": "x".repeat(edge + 1)})));
    }
}
