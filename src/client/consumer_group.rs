use std::sync::Arc;

use crate::backoff::BackoffConfig;
use crate::connection::BrokerConnector;

/// ConsumerGroup doesn't hold a global stats for offsets
pub struct ConsumerGroup {
    brokers: Arc<BrokerConnector>,

    backoff_config: BackoffConfig,

    group: String,
    topics: Vec<String>,
    member_id: String,
}

impl ConsumerGroup {

}
