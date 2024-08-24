use std::sync::Arc;

use thiserror::Error;

use crate::backoff::BackoffConfig;
use crate::build_info::DEFAULT_CLIENT_ID;
use crate::client::partition::PartitionClient;
use crate::connection::{Broker, BrokerConnector, MetadataLookupMode, TlsConfig};
use crate::topic::Topic;

pub mod consumer;
mod consumer_group;
pub mod controller;
pub mod error;
pub(crate) mod metadata_cache;
pub mod partition;
pub mod producer;

use self::{controller::ControllerClient, partition::UnknownTopicHandling};
use crate::client::consumer_group::ConsumerGroup;
use error::{Error, Result};

pub use crate::connection::SaslConfig;

#[derive(Debug, Error)]
pub enum ProduceError {
    #[error("Broker error: {0}")]
    BrokerError(#[from] crate::connection::Error),

    #[error("Request error: {0}")]
    RequestError(#[from] crate::messenger::RequestError),

    #[error("Got duplicate results for topic '{topic}' and partition {partition}")]
    DuplicateResult { topic: String, partition: i32 },

    #[error("No result for record {index}")]
    NoResult { index: usize },
}

/// Builder for [`Client`].
pub struct ClientBuilder {
    bootstrap_brokers: Vec<String>,
    client_id: Option<Arc<str>>,
    max_message_size: usize,
    socks5_proxy: Option<String>,
    tls_config: TlsConfig,
    sasl_config: Option<SaslConfig>,
    backoff_config: Arc<BackoffConfig>,
}

impl ClientBuilder {
    /// Create a new [`ClientBuilder`] with the list of bootstrap brokers
    pub fn new(bootstrap_brokers: Vec<String>) -> Self {
        Self {
            bootstrap_brokers,
            client_id: None,
            max_message_size: 100 * 1024 * 1024, // 100MB
            socks5_proxy: None,
            tls_config: TlsConfig::default(),
            sasl_config: None,
            backoff_config: Default::default(),
        }
    }

    /// Sets client ID.
    pub fn client_id(mut self, client_id: impl Into<Arc<str>>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Set maximum size (in bytes) of message frames that can be received from a broker.
    ///
    /// Setting this to larger sizes allows you to specify larger size limits in [`PartitionClient::fetch_records`],
    /// however it increases maximum memory consumption per broker connection. Settings this too small will result in
    /// failures all over the place since metadata requests cannot be handled any longer.
    pub fn max_message_size(mut self, max_message_size: usize) -> Self {
        self.max_message_size = max_message_size;
        self
    }

    /// Set up backoff configuration
    pub fn backoff_config(mut self, backoff_config: BackoffConfig) -> Self {
        self.backoff_config = Arc::from(backoff_config);
        self
    }

    /// Use SOCKS5 proxy.
    #[cfg(feature = "transport-socks5")]
    pub fn socks5_proxy(mut self, proxy: String) -> Self {
        self.socks5_proxy = Some(proxy);
        self
    }

    /// Setup TLS.
    #[cfg(feature = "transport-tls")]
    pub fn tls_config(mut self, tls_config: Arc<rustls::ClientConfig>) -> Self {
        self.tls_config = Some(tls_config);
        self
    }

    /// Setup SASL username and password. Mechanism is assumed to be PLAIN.
    pub fn sasl_config(mut self, sasl_config: SaslConfig) -> Self {
        self.sasl_config = Some(sasl_config);
        self
    }

    /// Build [`Client`].
    pub async fn build(self) -> Result<Client> {
        let brokers = Arc::new(BrokerConnector::new(
            self.bootstrap_brokers,
            self.client_id
                .unwrap_or_else(|| Arc::from(DEFAULT_CLIENT_ID)),
            self.tls_config,
            self.socks5_proxy,
            self.sasl_config,
            self.max_message_size,
            Arc::clone(&self.backoff_config),
        ));
        brokers.refresh_metadata().await?;

        Ok(Client {
            brokers,
            backoff_config: self.backoff_config,
        })
    }
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder").finish_non_exhaustive()
    }
}

/// Top-level cluster-wide client.
///
/// This client can be used to query some cluster-wide metadata and construct task-specific sub-clients like
/// [`ControllerClient`] and [`PartitionClient`].
///
/// Must be constructed using [`ClientBuilder`].
#[derive(Debug)]
pub struct Client {
    brokers: Arc<BrokerConnector>,
    backoff_config: Arc<BackoffConfig>,
}

impl Client {
    /// Returns a client for performing certain cluster-wide operations.
    pub fn controller_client(&self) -> Result<ControllerClient> {
        Ok(ControllerClient::new(
            Arc::clone(&self.brokers),
            Arc::clone(&self.backoff_config),
        ))
    }

    /// Returns a client for performing operations on a specific partition
    pub async fn partition_client(
        &self,
        topic: impl Into<String> + Send,
        partition: i32,
        unknown_topic_handling: UnknownTopicHandling,
    ) -> Result<PartitionClient> {
        PartitionClient::new(
            topic.into(),
            partition,
            Arc::clone(&self.brokers),
            unknown_topic_handling,
            Arc::clone(&self.backoff_config),
        )
        .await
    }

    /// Create a new consumer group the given topics
    pub async fn consumer_group(&self, group: String, topics: &[Topic]) -> Result<ConsumerGroup> {
        ConsumerGroup::new(
            self.brokers.clone(),
            Arc::clone(&self.backoff_config),
            group,
            topics,
        )
        .await
    }

    /// Returns a list of all brokers from cluster topology
    pub fn brokers(&self) -> Vec<Broker> {
        self.brokers.topology.get_brokers()
    }

    /// Returns a list of topics in the cluster, if None is provided, all topics returned
    pub async fn list_topics(&self) -> Result<Vec<Topic>> {
        // Do not used a cached metadata response to satisfy this request, in
        // order to prevent:
        //
        //  * Client creates a topic
        //  * Client calls list_topics() and does not see new topic
        //
        // Because this is an unconstrained metadata request (all topics) it
        // will update the cached metadata entry.
        let (response, _gen) = self
            .brokers
            .request_metadata(&MetadataLookupMode::ArbitraryBroker, None)
            .await?;

        Ok(response
            .topics
            .into_iter()
            .filter(|t| !matches!(t.is_internal, Some(true)))
            .map(|t| Topic {
                name: t.name,
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| p.partition_index)
                    .collect(),
            })
            .collect())
    }
}
