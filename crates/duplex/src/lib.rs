//! The `duplex` adapter: joins a Claude Code session or a Codex task to a duplex broker.

pub mod adapter;
pub mod cap;
pub mod compat;
pub mod config;
pub mod identity;
pub mod log;
pub mod mcp;
pub mod owner;
pub mod paging;
pub mod policy;
pub mod redact;
pub mod setup;
pub mod watch;
