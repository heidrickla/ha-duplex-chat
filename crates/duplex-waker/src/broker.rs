//! The broker's waker routes, over the shared client.

use duplex_proto::{BrokerClient, waker};
use std::time::Duration;

pub use duplex_proto::BrokerError;
pub use duplex_proto::waker::{Report, Request};

pub trait Broker {
    fn requests(&self) -> Result<Vec<Request>, BrokerError>;
    fn started(&self, request_id: &str, report: &Report) -> Result<(), BrokerError>;
}

pub struct HttpBroker {
    client: BrokerClient,
    token: String,
}

impl HttpBroker {
    pub fn new(broker_url: &str, token: &str) -> Self {
        Self {
            client: BrokerClient::new(broker_url, Duration::from_secs(15)),
            token: token.to_owned(),
        }
    }
}

impl Broker for HttpBroker {
    fn requests(&self) -> Result<Vec<Request>, BrokerError> {
        waker::requests(&self.client, &self.token)
    }

    fn started(&self, request_id: &str, report: &Report) -> Result<(), BrokerError> {
        waker::started(&self.client, &self.token, request_id, report)
    }
}
