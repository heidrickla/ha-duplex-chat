//! What the broker, the session adapter (`duplex`) and the waker share: message and
//! conversation IDs, the wake line, the broker's wire types and a blocking HTTP client.

pub mod client;
pub mod contract;
pub mod ids;
pub mod wake;
pub mod waker;

pub use client::{BrokerClient, BrokerError};
