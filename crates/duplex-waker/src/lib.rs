//! The duplex waker: polls the broker for wake requests and starts the closed Claude
//! session or Codex thread each one names, so a queued broker message reaches it. It
//! starts Windows processes, so the crate is empty on other systems.
#![cfg(windows)]

pub mod broker;
pub mod config;
pub mod launch;
pub mod log;
pub mod machine;
pub mod registry;
pub mod waker;
