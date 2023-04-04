use std::ops::ControlFlow;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::protocol::messages::{
    heartbeat, Assignment, ConsumerGroupMemberMetadata, JoinGroupRequest, JoinGroupResponse,
    MetadataResponse, SyncGroupRequest, SyncGroupResponse,
};
use crate::protocol::traits::WriteType;
use crate::{
    backoff::{Backoff, BackoffConfig, ErrorOrThrottle},
    client::{Error, Result},
    connection::{
        BrokerCache, BrokerCacheGeneration, BrokerConnection, BrokerConnector, MessengerTransport,
        MetadataLookupMode,
    },
    messenger::RequestError,
    protocol::{
        error::Error as ProtocolError,
        messages::{
            find_coordinator, join_group, list_groups, CreateTopicRequest, CreateTopicsRequest,
            DeleteTopicsRequest, DescribeGroupsRequest, Group,
        },
    },
    throttle::maybe_throttle,
    validation::ExactlyOne,
};

use super::error::RequestContext;

/// DEFAULT_SESSION_TIMEOUT_MS contains the default interval the coordinator will wait
/// for a heartbeat before marking a consumer as dead.
const DEFAULT_SESSION_TIMEOUT_MS: i32 = 30 * 1000;

/// DEFAULT_REBALANCE_TIMEOUT_MS contains the amount of time the coordinator will wait
/// for consumers to issue a join group once a rebalance has been requested.
const DEFAULT_REBALANCE_TIMEOUT_MS: i32 = 30 * 1000;

#[derive(Debug)]
pub struct ControllerClient {
    brokers: Arc<BrokerConnector>,

    backoff_config: BackoffConfig,

    /// Current broker connection if any
    current_broker: Mutex<(Option<BrokerConnection>, BrokerCacheGeneration)>,
}

impl ControllerClient {
    pub(super) fn new(brokers: Arc<BrokerConnector>) -> Self {
        Self {
            brokers,
            backoff_config: Default::default(),
            current_broker: Mutex::new((None, BrokerCacheGeneration::START)),
        }
    }

    /// Create a topic
    pub async fn create_topic(
        &self,
        name: impl Into<String> + Send,
        num_partitions: i32,
        replication_factor: i16,
        timeout_ms: i32,
    ) -> Result<()> {
        let request = &CreateTopicsRequest {
            topics: vec![CreateTopicRequest {
                name: name.into(),
                num_partitions,
                replication_factor,
                assignments: vec![],
                configs: vec![],
                tagged_fields: None,
            }],
            timeout_ms,
            validate_only: false,
            tagged_fields: None,
        };

        maybe_retry(&self.backoff_config, self, "create_topic", || async move {
            let (broker, gen) = self
                .get()
                .await
                .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
            let response = broker
                .request(request)
                .await
                .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(gen))))?;

            maybe_throttle(Some(response.throttle_time_ms))?;

            let topic = response
                .topics
                .exactly_one()
                .map_err(|e| ErrorOrThrottle::Error((Error::exactly_one_topic(e), Some(gen))))?;

            match topic.error {
                None => Ok(()),
                Some(protocol_error) => Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: topic.error_message,
                        request: RequestContext::Topic(topic.name),
                        response: None,
                        is_virtual: false,
                    },
                    Some(gen),
                ))),
            }
        })
        .await?;

        // Refresh the cache now there is definitely a new topic to observe.
        let _ = self.brokers.refresh_metadata().await;

        Ok(())
    }

    /// Delete a topic
    pub async fn delete_topic(
        &self,
        name: impl Into<String> + Send,
        timeout_ms: i32,
    ) -> Result<()> {
        let request = &DeleteTopicsRequest {
            topic_names: vec![name.into()],
            timeout_ms,
            tagged_fields: None,
        };

        maybe_retry(&self.backoff_config, self, "delete_topic", || async move {
            let (broker, gen) = self
                .get()
                .await
                .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
            let response = broker
                .request(request)
                .await
                .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(gen))))?;

            maybe_throttle(response.throttle_time_ms)?;

            let topic = response
                .responses
                .exactly_one()
                .map_err(|e| ErrorOrThrottle::Error((Error::exactly_one_topic(e), Some(gen))))?;

            match topic.error {
                None => Ok(()),
                Some(protocol_error) => Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: topic.error_message,
                        request: RequestContext::Topic(topic.name),
                        response: None,
                        is_virtual: false,
                    },
                    Some(gen),
                ))),
            }
        })
        .await?;

        // Refresh the cache now there is definitely a new topic to observe.
        let _ = self.brokers.refresh_metadata().await;

        Ok(())
    }

    pub async fn describe_groups(&self, groups: &[String]) -> Result<Vec<Group>> {
        let request = &DescribeGroupsRequest {
            groups: groups.iter().map(|s| s.to_string()).collect(),
            include_authorized_operations: false,
        };

        maybe_retry(
            &self.backoff_config,
            self,
            "describe_groups",
            || async move {
                let (broker, gen) = self
                    .get()
                    .await
                    .map_err(|err| ErrorOrThrottle::Error((err, None)))?;
                let response = broker
                    .request(request)
                    .await
                    .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(gen))))?;

                Ok(response.groups)
            },
        )
        .await
    }

    pub async fn list_groups(&self) -> Result<Vec<list_groups::Group>> {
        let request = &list_groups::ListGroupsRequest {
            states_filter: vec![],
            tagged_fields: None,
        };

        maybe_retry(&self.backoff_config, self, "list_groups", || async move {
            let (broker, gen) = self
                .get()
                .await
                .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
            let resp = broker
                .request(request)
                .await
                .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(gen))))?;

            maybe_throttle(Some(resp.throttle_time_ms))?;

            Ok(resp.groups)
        })
        .await
    }

    pub async fn heartbeat(
        &self,
        req: &heartbeat::HeartbeatRequest,
    ) -> Result<heartbeat::HeartbeatResponse> {
        maybe_retry(&self.backoff_config, self, "heartbeat", || async {
            let coordinator_id = self.get_coordinator(&req.group_id).await?;

            let broker = self
                .brokers
                .connect(coordinator_id)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?
                .ok_or(ErrorOrThrottle::Error((
                    Error::InvalidResponse(format!("Coordinator {} not found", coordinator_id)),
                    None,
                )))?;

            let resp = broker
                .request(req)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

            maybe_throttle(Some(resp.throttle_time_ms))?;

            match resp.error_code {
                None => Ok(resp),
                Some(protocol_error) => Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: Some("heartbeat failed".to_string()),
                        request: RequestContext::Group(req.group_id.clone()),
                        response: None,
                        is_virtual: false,
                    },
                    None,
                ))),
            }
        })
        .await
    }

    /// join_group join the group and return member_id
    pub async fn join_group(
        &self,
        group_id: &str,
        topics: Vec<String>,
    ) -> Result<JoinGroupResponse> {
        let meta = ConsumerGroupMemberMetadata {
            version: 0,
            topics,
            user_data: vec![],
            owned_partitions: vec![],
        };
        let mut metadata = Vec::new();
        meta.write(&mut metadata).expect("encode success");

        let req = &JoinGroupRequest {
            group_id: group_id.to_string(),
            session_timeout_ms: DEFAULT_SESSION_TIMEOUT_MS,
            rebalance_timeout_ms: DEFAULT_REBALANCE_TIMEOUT_MS,
            member_id: String::new(),
            group_instance_id: None,
            protocol_type: "consumer".to_string(),
            protocols: vec![join_group::Protocol {
                name: "roundrobin".to_string(),
                metadata,
            }],
        };

        maybe_retry(&self.backoff_config, self, "join_group", || async move {
            let coordinator_id = self.get_coordinator(group_id).await?;

            let broker = self
                .brokers
                .connect(coordinator_id)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?
                .ok_or(ErrorOrThrottle::Error((
                    Error::InvalidResponse(format!("Coordinator {} not found", coordinator_id)),
                    None,
                )))?;

            let resp = broker
                .request(req)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

            maybe_throttle(resp.throttle_time_ms)?;

            match resp.error_code {
                None => Ok(resp),
                Some(ProtocolError::MemberIdRequired) => {
                    let mut new_req = req.clone();
                    new_req.member_id = resp.member_id;

                    let resp = broker
                        .request(&new_req)
                        .await
                        .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

                    maybe_throttle(resp.throttle_time_ms)?;

                    match resp.error_code {
                        None => Ok(resp),
                        Some(protocol_error) => Err(ErrorOrThrottle::Error((
                            Error::ServerError {
                                protocol_error,
                                error_message: Some("join group failed".to_string()),
                                request: RequestContext::Group(group_id.to_string()),
                                response: None,
                                is_virtual: false,
                            },
                            None,
                        ))),
                    }
                }
                Some(protocol_error) => Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: Some("join group failed".to_string()),
                        request: RequestContext::Group(group_id.to_string()),
                        response: None,
                        is_virtual: false,
                    },
                    None,
                ))),
            }
        })
        .await
    }

    pub async fn sync_group(
        &self,
        group_id: String,
        generation_id: i32,
        member_id: String,
        group_instance_id: Option<String>,
        assignments: Vec<Assignment>,
    ) -> Result<SyncGroupResponse> {
        info!("sync {member_id}");

        let req = &SyncGroupRequest {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type: Some("consumer".into()),
            protocol_name: Some("roundrobin".into()),
            assignments,
            tagged_fields: None,
        };

        maybe_retry(&self.backoff_config, self, "sync_group", || async move {
            let coordinator_id = self.get_coordinator(&req.group_id).await?;
            let broker = self
                .brokers
                .connect(coordinator_id)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?
                .ok_or(ErrorOrThrottle::Error((
                    Error::InvalidResponse(format!("Coordinator {} not found", coordinator_id)),
                    None,
                )))?;

            let resp = broker
                .request(req)
                .await
                .map_err(|err| {
                    println!("err: {:?}", err);
                    err
                })
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

            maybe_throttle(Some(resp.throttle_time_ms))?;

            match resp.error_code {
                None => Ok(resp),
                Some(protocol_error) => Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: Some("sync group failed".to_string()),
                        request: RequestContext::Group(req.group_id.clone()),
                        response: None,
                        is_virtual: false,
                    },
                    None,
                ))),
            }
        })
        .await
    }

    pub async fn get_coordinator(
        &self,
        key: &str,
    ) -> std::result::Result<i32, ErrorOrThrottle<(Error, Option<BrokerCacheGeneration>)>> {
        let (broker, gen) = self
            .get()
            .await
            .map_err(|err| ErrorOrThrottle::Error((err, None)))?;

        let req = &find_coordinator::FindCoordinatorRequest {
            key: key.to_string(),
            key_type: find_coordinator::CoordinatorType::Group,
            tagged_fields: None,
        };

        let resp = broker
            .request(req)
            .await
            .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(gen))))?;

        maybe_throttle(Some(resp.throttle_time_ms))?;

        Ok(resp.node_id)
    }

    pub async fn metadata(&self) -> Result<MetadataResponse> {
        let (metadata, _gen) = self
            .brokers
            .request_metadata(&MetadataLookupMode::ArbitraryBroker, Some(vec![]))
            .await?;

        Ok(metadata)
    }

    /// Retrieve the broker ID of the controller
    async fn get_controller_id(&self) -> Result<i32> {
        // Request an uncached, fresh copy of the metadata.
        let (metadata, _gen) = self
            .brokers
            .request_metadata(&MetadataLookupMode::ArbitraryBroker, Some(vec![]))
            .await?;

        let controller_id = metadata
            .controller_id
            .ok_or_else(|| Error::InvalidResponse("Leader is NULL".to_owned()))?;

        Ok(controller_id)
    }
}

/// Caches the cluster controller broker.
#[async_trait]
impl BrokerCache for &ControllerClient {
    type R = MessengerTransport;
    type E = Error;

    async fn get(&self) -> Result<(Arc<Self::R>, BrokerCacheGeneration)> {
        let mut current_broker = self.current_broker.lock().await;
        if let Some(broker) = &current_broker.0 {
            return Ok((Arc::clone(broker), current_broker.1));
        }

        info!("Creating new controller broker connection",);

        let controller_id = self.get_controller_id().await?;
        let broker = self.brokers.connect(controller_id).await?.ok_or_else(|| {
            Error::InvalidResponse(format!(
                "Controller {} not found in metadata response",
                controller_id
            ))
        })?;

        current_broker.0 = Some(Arc::clone(&broker));
        current_broker.1.bump();

        Ok((broker, current_broker.1))
    }

    async fn invalidate(&self, reason: &'static str, gen: BrokerCacheGeneration) {
        let mut guard = self.current_broker.lock().await;

        if guard.1 != gen {
            // stale request
            debug!(
                reason,
                current_gen = guard.1.get(),
                request_gen = gen.get(),
                "stale invalidation request for arbitrary broker cache",
            );
            return;
        }

        info!(reason, "Invalidating cached controller broker",);
        guard.0.take();
    }
}

/// Takes a `request_name` and a function yielding a fallible future
/// and handles certain classes of error
pub(crate) async fn maybe_retry<B, R, F, T>(
    backoff_config: &BackoffConfig,
    broker_cache: B,
    request_name: &str,
    f: R,
) -> Result<T>
where
    B: BrokerCache,
    R: (Fn() -> F) + Send + Sync,
    F: std::future::Future<
            Output = Result<T, ErrorOrThrottle<(Error, Option<BrokerCacheGeneration>)>>,
        > + Send,
{
    let mut backoff = Backoff::new(backoff_config);

    backoff
        .retry_with_backoff(request_name, || async {
            let (error, cache_gen) = match f().await {
                Ok(v) => {
                    return ControlFlow::Break(Ok(v));
                }
                Err(ErrorOrThrottle::Throttle(t)) => {
                    return ControlFlow::Continue(ErrorOrThrottle::Throttle(t));
                }
                Err(ErrorOrThrottle::Error(e)) => e,
            };

            match error {
                // broken connection
                Error::Request(RequestError::Poisoned(_) | RequestError::IO(_))
                | Error::Connection(_) => {
                    if let Some(cache_gen) = cache_gen {
                        broker_cache
                            .invalidate("controller client: connection broken", cache_gen)
                            .await
                    }
                }

                // our broker is actually not the controller
                Error::ServerError {
                    protocol_error: ProtocolError::NotController,
                    ..
                } => {
                    if let Some(cache_gen) = cache_gen {
                        broker_cache
                            .invalidate(
                                "controller client: server error: not controller",
                                cache_gen,
                            )
                            .await;
                    }
                }

                // fatal
                _ => {
                    error!(
                        e=%error,
                        request_name,
                        "request encountered fatal error",
                    );
                    return ControlFlow::Break(Err(error));
                }
            }
            ControlFlow::Continue(ErrorOrThrottle::Error(error))
        })
        .await
        .map_err(Error::RetryFailed)?
}
