use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::backoff::{BackoffConfig, ErrorOrThrottle};
use crate::client::controller::maybe_retry;
use crate::client::error::{Error, ProtocolError, RequestContext};
use crate::client::{DEFAULT_REBALANCE_TIMEOUT_MS, DEFAULT_SESSION_TIMEOUT_MS};
use crate::connection::{
    BrokerCache, BrokerCacheGeneration, BrokerConnection, BrokerConnector, MessengerTransport,
    MetadataLookupMode,
};
use crate::protocol::messages::find_coordinator::{CoordinatorType, FindCoordinatorRequest};
use crate::protocol::messages::offset_commit::{
    OffsetCommitRequest, PartitionCommit, TopicCommit, TopicCommitResult,
};
use crate::protocol::messages::offset_fetch::{OffsetFetchRequest, OffsetFetchTopicResult};
use crate::protocol::messages::{
    find_coordinator, heartbeat, join_group, leave_group, Assignment,
    ConsumerGroupMemberAssignment, ConsumerGroupMemberMetadata,
    PartitionAssignment, SyncGroupRequest,
};
use crate::protocol::traits::WriteType;
use crate::throttle::maybe_throttle;
use crate::topic::Topic;

/// ConsumerGroup is a helper for consuming topics, it doesn't consume topics,
/// you have to consume it by yourself with [`StreamConsumer`], commit offsets
/// should be done by yourself too.
///
/// N.B. you have to refresh metadata and check if you should restart it.
pub struct ConsumerGroup {
    group: String,
    generation_id: i32,
    group_instance_id: Option<String>,
    member_id: String,
    assignment: ConsumerGroupMemberAssignment,

    backoff: BackoffConfig,
    brokers: Arc<BrokerConnector>,
    /// Current broker connection if any
    current_broker: Mutex<(Option<BrokerConnection>, BrokerCacheGeneration)>,
}

impl ConsumerGroup {
    pub(crate) async fn new(
        brokers: Arc<BrokerConnector>,
        group: String,
        topics: &[Topic],
    ) -> Result<Self, Error> {
        let req = FindCoordinatorRequest {
            key: &group,
            key_type: CoordinatorType::Group,
            tagged_fields: None,
        };

        let coordinator = loop {
            let (broker, gen) = brokers.as_ref().get().await?;

            let resp = broker.request(&req).await?;

            if resp.throttle_time_ms != 0 {
                tokio::time::sleep(Duration::from_millis(resp.throttle_time_ms as u64)).await;
                continue;
            }

            match resp.error_code {
                None => match brokers.as_ref().connect(resp.node_id).await? {
                    Some(c) => break c,
                    None => {
                        brokers
                            .as_ref()
                            .invalidate("coordinator not found", gen)
                            .await;
                        continue;
                    }
                },
                Some(protocol_error) => {
                    return Err(Error::ServerError {
                        protocol_error,
                        error_message: Some("find coordinator failed".into()),
                        request: RequestContext::Group(group),
                        response: None,
                        is_virtual: false,
                    })
                }
            }
        };

        let meta = ConsumerGroupMemberMetadata {
            version: 0,
            topics: topics.iter().map(|t| t.name.clone()).collect::<Vec<_>>(),
            user_data: vec![],
            owned_partitions: vec![],
        };
        let mut metadata = Vec::new();
        meta.write(&mut metadata).expect("encode success");

        let mut req = join_group::JoinGroupRequest {
            group_id: group.clone(),
            session_timeout_ms: DEFAULT_SESSION_TIMEOUT_MS,
            rebalance_timeout_ms: DEFAULT_REBALANCE_TIMEOUT_MS,
            member_id: "".to_string(),
            group_instance_id: None,
            protocol_type: "consumer".to_string(),
            protocols: vec![join_group::Protocol {
                name: "roundrobin".to_string(),
                metadata,
            }],
        };

        let join = loop {
            let resp = coordinator.request(&req).await?;
            match resp.throttle_time_ms {
                Some(d) if d != 0 => {
                    tokio::time::sleep(Duration::from_millis(d as u64)).await;
                    continue;
                }
                _ => {}
            }

            match resp.error_code {
                None => break resp,
                Some(protocol_error) => {
                    match protocol_error {
                        ProtocolError::MemberIdRequired => {
                            req.member_id = resp.member_id;
                            continue;
                        }

                        // fatal
                        _ => {
                            return Err(Error::ServerError {
                                protocol_error,
                                error_message: Some("JoinGroup".into()),
                                request: RequestContext::Group(group),
                                response: None,
                                is_virtual: false,
                            })
                        }
                    }
                }
            }
        };

        let assignments = if join.leader == join.member_id {
            round_robin(&topics, join.members)
                .into_iter()
                .map(|(member_id, topics)| {
                    let assignment = ConsumerGroupMemberAssignment {
                        version: 0,
                        topics,
                        user_data: vec![],
                    };

                    let mut buf = Vec::new();
                    assignment
                        .write(&mut buf)
                        .expect("write ConsumerGroupMemberAssignment success");

                    Assignment {
                        member_id,
                        assignment: buf,
                        tagged_fields: None,
                    }
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let req = &SyncGroupRequest {
            group_id: group.clone(),
            generation_id: join.generation_id,
            member_id: join.member_id.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".into()),
            protocol_name: Some("roundrobin".into()),
            assignments,
            tagged_fields: None,
        };

        let assignment = loop {
            let resp = coordinator.request(req).await?;
            if resp.throttle_time_ms != 0 {
                tokio::time::sleep(Duration::from_millis(resp.throttle_time_ms as u64)).await;
                continue;
            }

            match resp.error_code {
                None => break resp.assignment,
                Some(protocol_error) => {
                    return Err(Error::ServerError {
                        protocol_error,
                        error_message: Some("sync group failed".to_string()),
                        request: RequestContext::Group(req.group_id.clone()),
                        response: None,
                        is_virtual: false,
                    })
                }
            }
        };

        Ok(Self {
            group,
            generation_id: join.generation_id,
            group_instance_id: None,
            member_id: join.member_id,
            assignment,
            backoff: Default::default(),
            brokers,
            current_broker: Mutex::new((None, BrokerCacheGeneration::START)),
        })
    }

    pub async fn heartbeat(&self) -> Result<(), Error> {
        let req = &heartbeat::HeartbeatRequest {
            group_id: &self.group,
            generation_id: self.generation_id,
            member_id: &self.member_id,
            group_instance_id: self.group_instance_id.clone(),
        };

        maybe_retry(&self.backoff, self, "heartbeat", || async move {
            let (broker, gen) = self
                .get()
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

            let resp = broker
                .request(req)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(gen))))?;

            maybe_throttle(Some(resp.throttle_time_ms))?;

            match resp.error_code {
                None => Ok(()),
                Some(protocol_error) => Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: Some("heartbeat failed".to_string()),
                        request: RequestContext::Group(req.group_id.to_string()),
                        response: None,
                        is_virtual: false,
                    },
                    None,
                ))),
            }
        })
        .await
    }

    pub async fn leave(&self) -> Result<(), Error> {
        let req = &leave_group::LeaveGroupRequest {
            group_id: self.group.clone(),
            member_id: self.member_id.clone(),
            members: vec![leave_group::Member {
                member_id: self.member_id.clone(),
                group_instance_id: self.group_instance_id.clone(),
                reason: Some("blah".to_string()),
                tagged_fields: None,
            }],
            tagged_fields: None,
        };

        maybe_retry(&self.backoff, self, "leave_group", || async move {
            let (broker, gen) = self
                .get()
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

            let resp = broker
                .request(req)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(gen))))?;

            maybe_throttle(resp.throttle_time_ms)?;

            match resp.error_code {
                None => Ok(()),
                Some(protocol_error) => Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: None,
                        request: RequestContext::Group(req.group_id.to_string()),
                        response: None,
                        is_virtual: false,
                    },
                    None,
                ))),
            }
        })
        .await
    }

    pub async fn offsets(&self) -> Result<Vec<OffsetFetchTopicResult>, Error> {
        let req = &OffsetFetchRequest {
            group_id: self.group.clone(),
            topics: vec![],
        };

        maybe_retry(&self.backoff, self, "offset_fetch", || async move {
            let (broker, gen) = self
                .get()
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

            let resp = broker
                .request(req)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(gen))))?;

            maybe_throttle(resp.throttle_time_ms)?;

            match resp.error_code {
                Some(protocol_error) => Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: None,
                        request: RequestContext::Group(req.group_id.clone()),
                        response: None,
                        is_virtual: false,
                    },
                    None,
                ))),
                None => Ok(resp.topics),
            }
        })
        .await
    }

    pub async fn commit(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<Vec<TopicCommitResult>, Error> {
        let req = &OffsetCommitRequest {
            group_id: self.group.clone(),
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            retention_time_ms: 0,
            group_instance_id: self.group_instance_id.clone(),
            topics: vec![TopicCommit {
                name: topic.to_string(),
                partitions: vec![PartitionCommit {
                    partition_index: partition,
                    committed_offset: offset,
                    commit_timestamp: 0,
                    committed_leader_epoch: 0,
                    committed_metadata: None,
                }],
            }],
        };

        maybe_retry(&self.backoff, self, "offset_commit", || async move {
            let (broker, gen) = self
                .get()
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

            let resp = broker
                .request(req)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(gen))))?;

            maybe_throttle(Some(resp.throttle_time_ms))?;

            Ok(resp.topics)
        })
        .await
    }

    pub fn assignment(&self) -> &[PartitionAssignment] {
        &self.assignment.topics
    }
}

// round robin
// member_id -> topic -> partitions
fn round_robin(
    topics: &[Topic],
    mut members: Vec<join_group::Member>,
) -> BTreeMap<String, Vec<PartitionAssignment>> {
    let mut plan = BTreeMap::new();
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));

    let mut i = 0;
    let n = members.len();
    for topic in topics {
        for (partition, _) in &topic.partitions {
            let member = loop {
                let member = &members[i % n];
                i += 1;

                if member.metadata.topics.contains(&topic.name) {
                    break &member.member_id;
                }
            };

            let tp = match plan.get_mut(member) {
                Some(tp) => tp,
                None => {
                    plan.entry(member.clone()).or_insert_with(|| {
                        vec![PartitionAssignment {
                            topic: topic.name.clone(),
                            partitions: vec![*partition],
                        }]
                    });

                    continue;
                }
            };

            match tp.iter_mut().find(|a| a.topic == topic.name) {
                Some(assignment) => {
                    assignment.partitions.push(*partition);
                }
                None => tp.push(PartitionAssignment {
                    topic: topic.name.clone(),
                    partitions: vec![*partition],
                }),
            }
        }
    }

    plan
}

/// Caches the coordinator broker.
#[async_trait]
impl BrokerCache for &ConsumerGroup {
    type R = MessengerTransport;
    type E = Error;

    async fn get(
        &self,
    ) -> crate::connection::Result<(Arc<Self::R>, BrokerCacheGeneration), Self::E> {
        let mut current = self.current_broker.lock().await;
        if let Some(broker) = &current.0 {
            return Ok((Arc::clone(broker), current.1));
        }

        info!("Creating new coordinator broker connection");

        // get controller broker first
        let (meta, _gen) = self
            .brokers
            .request_metadata(&MetadataLookupMode::ArbitraryBroker, Some(vec![]))
            .await?;

        let controller_id = meta
            .controller_id
            .ok_or_else(|| Error::InvalidResponse("leader is null".to_string()))?;

        let broker = self.brokers.connect(controller_id).await?.ok_or_else(|| {
            Error::InvalidResponse(format!(
                "Controller {} not found in metadata response",
                controller_id
            ))
        })?;

        let req = &find_coordinator::FindCoordinatorRequest {
            key: &self.group,
            key_type: CoordinatorType::Group,
            tagged_fields: None,
        };

        let resp = broker.request(req).await?;
        if let Some(protocol_error) = resp.error_code {
            return Err(Error::ServerError {
                protocol_error,
                error_message: resp.error_message,
                request: RequestContext::Group(self.group.clone()),
                response: None,
                is_virtual: false,
            });
        }

        let broker = self.brokers.connect(resp.node_id).await?.ok_or_else(|| {
            Error::InvalidResponse(format!("Coordinator {} connect failed", resp.node_id))
        })?;

        current.0 = Some(Arc::clone(&broker));
        current.1.bump();

        Ok((broker, current.1))
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

        info!(reason, "invalidating cached coordinator broker");
        guard.0.take();
    }
}
