use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use parking_lot::RwLock;
use tracing::info;

use crate::protocol::messages::MetadataResponseBroker;

#[derive(Debug, Default)]
pub struct BrokerTopology {
    /// Brokers keyed by broker ID
    topology: RwLock<HashMap<i32, Broker>>,
}

#[derive(Debug, Clone)]
pub struct Broker {
    /// broker ID from the topology metadata
    pub id: i32,
    host: String,
    port: i32,
}

impl Display for Broker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl<'a> From<&'a MetadataResponseBroker> for Broker {
    fn from(b: &'a MetadataResponseBroker) -> Self {
        Self {
            id: b.node_id,
            host: b.host.clone(),
            port: b.port,
        }
    }
}

impl BrokerTopology {
    pub fn is_empty(&self) -> bool {
        self.topology.read().is_empty()
    }

    /// Returns the broker for the provided broker ID
    pub async fn get_broker(&self, broker_id: i32) -> Option<Broker> {
        self.topology.read().get(&broker_id).cloned()
    }

    /// Returns a list of all brokers
    pub fn get_brokers(&self) -> Vec<Broker> {
        self.topology.read().values().cloned().collect()
    }

    /// Updates with the provided broker metadata
    pub fn update(&self, brokers: &[MetadataResponseBroker]) {
        let mut topology = self.topology.write();
        for broker in brokers {
            match topology.entry(broker.node_id) {
                Entry::Occupied(mut o) => {
                    let current = o.get_mut();
                    if current.host != broker.host || current.port != broker.port {
                        let new = Broker::from(broker);
                        info!(
                            broker=broker.node_id,
                            current=%current,
                            new=%new,
                            "Broker update",
                        );
                        *current = new;
                    }
                }
                Entry::Vacant(v) => {
                    let new = Broker::from(broker);
                    info!(
                        broker=broker.node_id,
                        new=%new,
                        "New broker",
                    );
                    v.insert(new);
                }
            }
        }
    }
}
