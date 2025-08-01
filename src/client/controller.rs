use std::ops::ControlFlow;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use super::error::RequestContext;
use crate::backoff::{Backoff, BackoffConfig, ErrorOrThrottle};
use crate::client::{Error, Result};
use crate::connection::{
    BrokerCache, BrokerCacheGeneration, BrokerConnection, BrokerConnector, MessengerTransport,
    MetadataLookupMode,
};
use crate::messenger::RequestError;
use crate::protocol::messages::{
    CoordinatorType, DescribeGroupsRequest, DescribeGroupsResponseGroup, FindCoordinatorRequest,
    ListGroupsRequest, OffsetFetchRequest, OffsetFetchRequestTopic, OffsetFetchResponseTopic,
};
use crate::protocol::{
    error::Error as ProtocolError,
    messages::{
        CreateTopicRequest, CreateTopicsRequest, DeleteTopicsRequest, ListGroupsResponseGroup,
    },
};
use crate::throttle::maybe_throttle;
use crate::validation::ExactlyOne;

#[derive(Debug)]
pub struct ControllerClient {
    brokers: Arc<BrokerConnector>,

    backoff_config: Arc<BackoffConfig>,

    /// Current broker connection if any
    current_broker: Mutex<(Option<BrokerConnection>, BrokerCacheGeneration)>,
}

impl ControllerClient {
    pub(super) fn new(brokers: Arc<BrokerConnector>, backoff_config: Arc<BackoffConfig>) -> Self {
        Self {
            brokers,
            backoff_config,
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
            validate_only: None,
            tagged_fields: None,
        };

        maybe_retry(&self.backoff_config, self, "create_topic", || async move {
            let (broker, g) = self
                .get()
                .await
                .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
            let response = broker
                .request(request)
                .await
                .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(g))))?;

            maybe_throttle(response.throttle_time_ms)?;

            let topic = response
                .topics
                .exactly_one()
                .map_err(|e| ErrorOrThrottle::Error((Error::exactly_one_topic(e), Some(g))))?;

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
                    Some(g),
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
            let (broker, g) = self
                .get()
                .await
                .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
            let response = broker
                .request(request)
                .await
                .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(g))))?;

            maybe_throttle(response.throttle_time_ms)?;

            let topic = response
                .responses
                .exactly_one()
                .map_err(|e| ErrorOrThrottle::Error((Error::exactly_one_topic(e), Some(g))))?;

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
                    Some(g),
                ))),
            }
        })
        .await?;

        // Refresh the cache now there is definitely a new topic to observe.
        let _ = self.brokers.refresh_metadata().await;

        Ok(())
    }

    /// ListGroups return a list group response or error.
    pub async fn list_groups(&self) -> Result<Vec<ListGroupsResponseGroup>> {
        let (metadata, _gen) = self
            .brokers
            .request_metadata(&MetadataLookupMode::ArbitraryBroker, None)
            .await?;

        let mut tasks = futures::stream::FuturesUnordered::new();
        for broker in metadata.brokers {
            let broker_id = broker.node_id;
            let brokers = self.brokers.clone();
            let backoff = self.backoff_config.clone();

            tasks.push(async move {
                let broker = match brokers.connect(broker_id).await? {
                    Some(broker) => broker,
                    None => return Ok(vec![]),
                };
                let req = &ListGroupsRequest {
                    states_filter: vec![],
                    tagged_fields: None,
                };

                // the broker_cache(2nd arg) is useless, but `maybe_retry` need this
                maybe_retry(&backoff, brokers.as_ref(), "list_groups", || {
                    let broker = broker.clone();

                    async move {
                        let resp = broker
                            .request(req)
                            .await
                            .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

                        maybe_throttle(resp.throttle_time_ms)?;

                        if let Some(protocol_error) = resp.error_code {
                            return Err(ErrorOrThrottle::Error((
                                Error::ServerError {
                                    protocol_error,
                                    error_message: None,
                                    request: RequestContext::Group("".to_string()),
                                    response: None,
                                    is_virtual: false,
                                },
                                None,
                            )));
                        }

                        Ok(resp.groups)
                    }
                })
                .await
            });
        }

        let mut groups = vec![];
        while let Some(result) = tasks.next().await {
            if let Ok(partial) = result {
                groups.extend(partial);
            }
        }

        Ok(groups)
    }

    /// describe_groups return describe group response or error
    pub async fn describe_groups(
        &self,
        groups: Vec<String>,
    ) -> Result<Vec<DescribeGroupsResponseGroup>> {
        // get coordinators
        let mut coordinator_ids = vec![];
        for group in &groups {
            let req = &FindCoordinatorRequest {
                key: group.clone(),
                key_type: CoordinatorType::Group,
                tagged_fields: None,
            };

            let coordinator_id = maybe_retry(
                &self.backoff_config,
                self,
                "describe_groups",
                || async move {
                    let (broker, g) = self
                        .brokers
                        .as_ref()
                        .get()
                        .await
                        .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

                    let resp = broker
                        .request(req)
                        .await
                        .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(g))))?;

                    maybe_throttle(resp.throttle_time_ms)?;

                    Ok(resp.node_id)
                },
            )
            .await?;

            coordinator_ids.push(coordinator_id);
        }

        let req = &DescribeGroupsRequest {
            groups,
            include_authorized_operations: false,
        };

        let mut tasks = futures::stream::FuturesUnordered::new();
        for coordinator_id in coordinator_ids {
            let brokers = Arc::clone(&self.brokers);
            let backoff = Arc::clone(&self.backoff_config);

            tasks.push(async move {
                let broker = match brokers.connect(coordinator_id).await? {
                    Some(broker) => broker,
                    None => return Ok::<Vec<DescribeGroupsResponseGroup>, Error>(vec![]),
                };

                let groups = maybe_retry(&backoff, brokers.as_ref(), "describe_groups", || async {
                    let resp = broker
                        .request(req)
                        .await
                        .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

                    maybe_throttle(resp.throttle_time_ms)?;

                    Ok(resp.groups)
                })
                .await?;

                Ok(groups)
            });
        }

        let mut groups = vec![];
        while let Some(result) = tasks.next().await {
            if let Ok(partial) = result {
                groups.extend(partial);
            }
        }

        Ok(groups)
    }

    pub async fn consumer_group_offsets(
        &self,
        group: String,
        topics: Vec<OffsetFetchRequestTopic>,
    ) -> Result<Vec<OffsetFetchResponseTopic>> {
        let req = &FindCoordinatorRequest {
            key: group.clone(),
            key_type: CoordinatorType::Group,
            tagged_fields: None,
        };

        let (coordinator, g) = maybe_retry(
            &self.backoff_config,
            self,
            "find_coordinator",
            || async move {
                let (broker, g) = self
                    .get()
                    .await
                    .map_err(|err| ErrorOrThrottle::Error((err, None)))?;
                let resp = broker
                    .request(req)
                    .await
                    .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(g))))?;

                maybe_throttle(resp.throttle_time_ms)?;

                if let Some(protocol_error) = resp.error_code {
                    return Err(ErrorOrThrottle::Error((
                        Error::ServerError {
                            protocol_error,
                            error_message: resp.error_message,
                            request: RequestContext::Group(req.key.clone()),
                            response: None,
                            is_virtual: false,
                        },
                        Some(g),
                    )));
                }

                Ok((resp.node_id, g))
            },
        )
        .await?;

        let req = &OffsetFetchRequest {
            group_id: group,
            topics,
            require_stable: false,
            tagged_fields: None,
        };

        maybe_retry(&self.backoff_config, self, "offset_fetch", || async move {
            let broker = match self.brokers.connect(coordinator).await {
                Ok(Some(conn)) => conn,
                Ok(None) => {
                    self.brokers
                        .as_ref()
                        .invalidate("connect coordinator failed", g)
                        .await;
                    let err = Error::InvalidResponse(
                        "coordinator not found in metadata cache".to_string(),
                    );
                    return Err(ErrorOrThrottle::Error((err, None)));
                }
                Err(err) => {
                    return Err(ErrorOrThrottle::Error((Error::Connection(err), Some(g))));
                }
            };

            let resp = broker
                .request(req)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(g))))?;

            maybe_throttle(resp.throttle_time_ms)?;

            if let Some(protocol_error) = resp.error_code {
                return Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: None,
                        request: RequestContext::Group(req.group_id.clone()),
                        response: None,
                        is_virtual: false,
                    },
                    Some(g),
                )));
            }

            Ok(resp.topics)
        })
        .await
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
                "Controller {controller_id} not found in metadata response",
            ))
        })?;

        current_broker.0 = Some(Arc::clone(&broker));
        current_broker.1.bump();

        Ok((broker, current_broker.1))
    }

    async fn invalidate(&self, reason: &'static str, g: BrokerCacheGeneration) {
        let mut guard = self.current_broker.lock().await;

        if guard.1 != g {
            // stale request
            debug!(
                reason,
                current_gen = guard.1.get(),
                request_gen = g.get(),
                "stale invalidation request for arbitrary broker cache",
            );
            return;
        }

        info!(reason, "Invalidating cached controller broker");
        guard.0.take();
    }
}

/// Takes a `request_name` and a function yielding a fallible future
/// and handles certain classes of error
pub async fn maybe_retry<B, R, F, T>(
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
