//! Logging to stderr: stdout is the MCP protocol.

use std::fmt::Display;

pub fn info(message: impl Display) {
    eprintln!("[duplex] {message}");
}

pub fn warn(message: impl Display) {
    eprintln!("[duplex] warning: {message}");
}
