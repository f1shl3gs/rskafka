//! Kafka separates storage from compute. Storage is handled by the brokers and
//! compute is mainly handled by consumers or frameworks built on top of consumers
//! (Kafka Streams, ksqlDB). Consumer groups play a key role in the effectiveness
//! and scalability of Kafka consumers.
//!
//! https://developer.confluent.io/courses/architecture/consumer-group-protocol

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::backoff::ErrorOrThrottle;
use crate::client::controller::maybe_retry;
use crate::client::error::{Error, ProtocolError, RequestContext};
use crate::connection::{
    BrokerCache, BrokerCacheGeneration, BrokerConnection, BrokerConnector, MessengerTransport,
};
use crate::protocol::messages::{
    ConsumerGroupMemberAssignment, ConsumerGroupMemberMetadata, CoordinatorType,
    FindCoordinatorRequest, HeartbeatRequest, JoinGroupProtocol, JoinGroupRequest,
    JoinGroupResponseMember, LeaveGroupRequest, OffsetCommitRequest, OffsetCommitRequestTopic,
    OffsetFetchRequest, OffsetFetchResponseTopic, PartitionAssignment, SyncGroupRequest,
    SyncGroupRequestAssignment,
};
use crate::protocol::traits::WriteType;
use crate::throttle::maybe_throttle;
use crate::topic::Topic;
use crate::BackoffConfig;

/// DEFAULT_SESSION_TIMEOUT_MS contains the default interval the coordinator will wait
/// for a heartbeat before marking a consumer as dead.
pub const DEFAULT_SESSION_TIMEOUT_MS: i32 = 10 * 1000; // 10s

/// DEFAULT_REBALANCE_TIMEOUT_MS contains the amount of time the coordinator will wait
/// for consumers to issue a join group once a rebalance has been requested.
pub const DEFAULT_REBALANCE_TIMEOUT_MS: i32 = 60 * 1000; // 60s

pub const BALANCE_STRATEGY_RANGE: &str = "range";
pub const BALANCE_STRATEGY_ROUNDROBIN: &str = "roundrobin";

/// ConsumerGroup is a helper for consuming topics, it doesn't consume topics,
/// you have to consume and commit offsets it by yourself.
///
/// N.B. you have to refresh metadata and check if you should restart it
#[derive(Debug)]
pub struct ConsumerGroup {
    group: String,
    member_id: String,
    group_instance_id: Option<String>,

    generation_id: i32,
    backoff: BackoffConfig,

    assignment: ConsumerGroupMemberAssignment,

    brokers: Arc<BrokerConnector>,
    coordinator: Mutex<(Option<BrokerConnection>, BrokerCacheGeneration)>,
}

impl ConsumerGroup {
    pub(crate) async fn new(
        brokers: Arc<BrokerConnector>,
        group: String,
        topics: &[Topic],
    ) -> Result<Self, Error> {
        let backoff = BackoffConfig::default();

        let req = &FindCoordinatorRequest {
            key: group.clone(),
            key_type: CoordinatorType::Group,
            tagged_fields: None,
        };

        let coordinator_id =
            maybe_retry(&backoff, brokers.as_ref(), "find_coordinator", || async {
                let (broker, gen) = brokers
                    .as_ref()
                    .get()
                    .await
                    .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;
                let resp = broker
                    .request(req)
                    .await
                    .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(gen))))?;

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
                        Some(gen),
                    )));
                }

                Ok(resp.node_id)
            })
            .await?;

        let coordinator = match brokers.as_ref().connect(coordinator_id).await? {
            Some(broker) => broker,
            None => {
                return Err(Error::InvalidResponse(
                    "connect to coordinator failed".to_string(),
                ))
            }
        };

        let resp = maybe_retry(&backoff, brokers.as_ref(), "join_group", || {
            let coordinator = coordinator.clone();
            let group = group.clone();

            async move {
                let metadata = ConsumerGroupMemberMetadata {
                    version: 0,
                    topics: topics.iter().map(|t| t.name.clone()).collect(),
                    user_data: vec![],
                    owned_partitions: vec![],
                    generation_id: 0,
                    rack_id: None,
                };
                let mut buf = Vec::new();
                metadata.write(&mut buf).expect("encode success");
                let mut req = JoinGroupRequest {
                    group_id: group,
                    session_timeout_ms: DEFAULT_SESSION_TIMEOUT_MS,
                    rebalance_timeout_ms: DEFAULT_REBALANCE_TIMEOUT_MS,
                    member_id: "".to_string(),
                    group_instance_id: None,
                    protocol_type: "consumer".to_string(),
                    protocols: vec![JoinGroupProtocol {
                        name: BALANCE_STRATEGY_ROUNDROBIN.to_string(),
                        metadata: buf,
                    }],
                };

                let resp = loop {
                    let resp = coordinator
                        .request(&req)
                        .await
                        .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

                    maybe_throttle(resp.throttle_time_ms)?;

                    if let Some(protocol_error) = resp.error_code {
                        match protocol_error {
                            ProtocolError::MemberIdRequired => {
                                req.member_id = resp.member_id;
                                continue;
                            }
                            _ => {
                                return Err(ErrorOrThrottle::Error((
                                    Error::ServerError {
                                        protocol_error,
                                        error_message: None,
                                        request: RequestContext::Group(req.group_id.clone()),
                                        response: None,
                                        is_virtual: false,
                                    },
                                    None,
                                )))
                            }
                        }
                    } else {
                        break resp;
                    }
                };

                maybe_throttle(resp.throttle_time_ms)?;

                if let Some(err) = resp.error_code {
                    return Err(ErrorOrThrottle::Error((
                        Error::ServerError {
                            protocol_error: err,
                            error_message: None,
                            request: RequestContext::Group(req.group_id.clone()),
                            response: None,
                            is_virtual: false,
                        },
                        None,
                    )));
                }

                Ok(resp)
            }
        })
        .await?;

        let assignments = if resp.leader == resp.member_id {
            let plan = match resp.protocol_name.as_ref() {
                BALANCE_STRATEGY_ROUNDROBIN => round_robin(&topics, resp.members),
                BALANCE_STRATEGY_RANGE => range(&topics, resp.members),
                _ => {
                    return Err(Error::InvalidResponse(format!(
                        "invalid protocol {}",
                        resp.protocol_name
                    )))
                }
            };

            plan.into_iter()
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

                    SyncGroupRequestAssignment {
                        member_id,
                        assignment: buf,
                        tagged_fields: None,
                    }
                })
                .collect()
        } else {
            vec![]
        };

        let req = &SyncGroupRequest {
            group_id: group.clone(),
            generation_id: resp.generation_id,
            member_id: resp.member_id,
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some(BALANCE_STRATEGY_ROUNDROBIN.to_string()),
            assignments,
            tagged_fields: None,
        };
        let assignment = maybe_retry(&backoff, brokers.as_ref(), "sync_group", || {
            let coordinator = coordinator.clone();

            async move {
                let resp = coordinator
                    .request(req)
                    .await
                    .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

                maybe_throttle(resp.throttle_time_ms)?;

                match resp.error_code {
                    None => Ok(resp.assignments),
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
                }
            }
        })
        .await?;

        Ok(ConsumerGroup {
            group,
            member_id: req.member_id.clone(),
            group_instance_id: None,
            generation_id: resp.generation_id,
            backoff,
            brokers,
            assignment,
            coordinator: Mutex::new((Some(coordinator), BrokerCacheGeneration::START)),
        })
    }

    pub async fn heartbeat(&self) -> Result<(), Error> {
        let req = &HeartbeatRequest {
            group_id: &self.group,
            generation_id: self.generation_id,
            member_id: &self.member_id,
            group_instance_id: None,
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

            maybe_throttle(resp.throttle_time_ms)?;

            if let Some(protocol_error) = resp.error_code {
                return Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: None,
                        request: RequestContext::Group(req.group_id.to_string()),
                        response: None,
                        is_virtual: false,
                    },
                    Some(gen),
                )));
            }

            Ok(())
        })
        .await
    }

    pub async fn leave(&self) -> Result<(), Error> {
        let req = &LeaveGroupRequest {
            group_id: self.group.clone(),
            member_id: self.member_id.clone(),
            members: vec![],
            tagged_fields: None,
        };

        maybe_retry(&self.backoff, self, "leave_group", || async move {
            let (broker, gen) = self
                .get()
                .await
                .map_err(|err| ErrorOrThrottle::Error((err, None)))?;
            let resp = broker
                .request(req)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(gen))))?;

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
                    None,
                )));
            }

            Ok(())
        })
        .await
    }

    pub async fn offsets(&self) -> Result<Vec<OffsetFetchResponseTopic>, Error> {
        let req = &OffsetFetchRequest {
            group_id: self.group.to_string(),
            topics: vec![],
            require_stable: false,
            tagged_fields: None,
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

            if let Some(protocol_error) = resp.error_code {
                return Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: None,
                        request: RequestContext::Group(req.group_id.clone()),
                        response: None,
                        is_virtual: false,
                    },
                    None,
                )));
            }

            Ok(resp.topics)
        })
        .await
    }

    pub async fn commit(&self, topics: Vec<OffsetCommitRequestTopic>) -> Result<(), Error> {
        let req = &OffsetCommitRequest {
            group_id: self.group.clone(),
            generation_id_or_member_epoch: self.generation_id,
            member_id: self.member_id.clone(),
            retention_time_ms: 0,
            group_instance_id: self.group_instance_id.clone(),
            topics,
        };

        let (results, gen) = maybe_retry(&self.backoff, self, "offset_commit", || async move {
            let (broker, gen) = self
                .get()
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;
            let resp = broker
                .request(req)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(gen))))?;

            maybe_throttle(resp.throttle_time_ms)?;

            Ok((resp.topics, gen))
        })
        .await?;

        for topic in results {
            for partition in topic.partitions {
                if let Some(err) = partition.error_code {
                    match err {
                        ProtocolError::NotLeaderOrFollower
                        | ProtocolError::LeaderNotAvailable
                        | ProtocolError::CoordinatorNotAvailable
                        | ProtocolError::NotCoordinator => {
                            // not a critical error, we just need to redispatch
                            self.invalidate("", gen).await;
                        }
                        ProtocolError::OffsetMetadataTooLarge
                        | ProtocolError::InvalidCommitOffsetSize => {
                            // nothing we can do about this, just tell the user and carry on
                            error!(
                                message = "Offset metadata is too large",
                                topic = topic.name,
                                partition = partition.partition_index,
                                %err
                            )
                        }
                        ProtocolError::OffsetNotAvailable => {
                            // nothing wrong but we didn't commit, we'll get it next time round
                        }
                        ProtocolError::FencedInstanceId => {
                            // TODO: close the whole consumer for instance fenced...
                        }
                        ProtocolError::UnknownTopicOrPartition => {
                            // TODO: handle this error
                            self.invalidate("", gen).await;
                        }
                        _ => {
                            self.invalidate("", gen).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn assignment(&self) -> &[PartitionAssignment] {
        &self.assignment.topics
    }
}

/// Caches the coordinator broker
impl BrokerCache for &ConsumerGroup {
    type R = MessengerTransport;
    type E = Error;

    async fn get(&self) -> Result<(Arc<Self::R>, BrokerCacheGeneration), Self::E> {
        let mut current = self.coordinator.lock().await;
        if let Some(broker) = &current.0 {
            return Ok((Arc::clone(broker), current.1));
        }

        info!("creating a new coordinator broker connection");

        // find the coordinator
        let req = &FindCoordinatorRequest {
            key: self.group.clone(),
            key_type: CoordinatorType::Group,
            tagged_fields: None,
        };

        let coordinator_id = maybe_retry(&self.backoff, *self, "find_coordinator", || async move {
            // we don't need to connect to controller, every broker can handle this request.
            //
            // See: https://developer.confluent.io/courses/architecture/consumer-group-protocol/#step-1--find-group-coordinator
            let (broker, gen) = self
                .brokers
                .as_ref()
                .get()
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), None)))?;

            let resp = broker
                .request(req)
                .await
                .map_err(|err| ErrorOrThrottle::Error((err.into(), Some(gen))))?;

            maybe_throttle(resp.throttle_time_ms)?;

            Ok(resp.node_id)
        })
        .await?;

        let coordinator = self.brokers.connect(coordinator_id).await?.ok_or_else(|| {
            Error::InvalidResponse(format!(
                "coordinator {} not found in metadata response",
                coordinator_id
            ))
        })?;

        current.0 = Some(Arc::clone(&coordinator));
        current.1.bump();

        Ok((coordinator, current.1))
    }

    async fn invalidate(&self, reason: &'static str, gen: BrokerCacheGeneration) {
        let mut guard = self.coordinator.lock().await;

        if guard.1 != gen {
            // stale request
            debug!(
                message = "stale invalidation request for coordinator cache",
                reason,
                current_gen = guard.1.get(),
                request_gen = gen.get(),
            );

            return;
        }

        info!(message = "invalidating cached coordinator broker", reason);

        guard.0.take();
    }
}

fn round_robin(
    topics: &[Topic],
    members: Vec<JoinGroupResponseMember>,
) -> BTreeMap<String, Vec<PartitionAssignment>> {
    let mut plan = BTreeMap::default();

    let mut i = 0;
    let n = members.len();
    for topic in topics {
        for partition in &topic.partitions {
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
                            topic: topic.name.to_string(),
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

fn range(
    topics: &[Topic],
    members: Vec<JoinGroupResponseMember>,
) -> BTreeMap<String, Vec<PartitionAssignment>> {
    let mut plan = BTreeMap::default();

    // build members by topic map
    let mut mbt: BTreeMap<String, Vec<String>> = BTreeMap::default();
    for JoinGroupResponseMember {
        member_id,
        metadata,
        ..
    } in members
    {
        for topic in metadata.topics {
            mbt.entry(topic)
                .and_modify(|ids| ids.push(member_id.clone()))
                .or_insert(vec![member_id.clone()]);
        }
    }

    // assemble plan
    for (topic_name, mut ids) in mbt {
        ids.sort();
        ids.dedup();

        let topic = topics
            .into_iter()
            .find(|topic| topic.name == topic_name)
            .expect("topic must exist");
        let partitions_per_consumer = topic.partitions.len() / ids.len();
        let consumers_with_extra_partition = topic.partitions.len() % ids.len();

        for (index, id) in ids.into_iter().enumerate() {
            let min = index * partitions_per_consumer
                + std::cmp::min(consumers_with_extra_partition, index);
            let extra = if index < consumers_with_extra_partition {
                1
            } else {
                0
            };

            let max = min + partitions_per_consumer + extra;
            let mut part = topic.partitions.iter().cloned().collect::<Vec<_>>();
            let partitions = part.drain(min..max).collect::<Vec<_>>();
            if partitions.is_empty() {
                continue;
            }

            plan.entry(id)
                .and_modify(|pas: &mut Vec<PartitionAssignment>| {
                    pas.push(PartitionAssignment {
                        topic: topic.name.clone(),
                        partitions: partitions.clone(),
                    })
                })
                .or_insert(vec![PartitionAssignment {
                    topic: topic.name.clone(),
                    partitions,
                }]);
        }
    }

    plan
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn btree_set(items: impl Iterator<Item = i32>) -> BTreeSet<i32> {
        let mut set = BTreeSet::new();
        for item in items {
            set.insert(item);
        }
        set
    }

    #[test]
    fn balance_strategy_round_robin() {
        for (members, topics, want) in [
            (
                vec![
                    ("m1", vec!["t1", "t2", "t3"]),
                    ("m2", vec!["t1", "t2", "t3"]),
                ],
                vec![("t1", vec![0]), ("t2", vec![0]), ("t3", vec![0])],
                vec![
                    ("m1", vec![("t1", vec![0]), ("t3", vec![0])]),
                    ("m2", vec![("t2", vec![0])]),
                ],
            ),
            (
                vec![
                    ("m1", vec!["t1", "t2", "t3"]),
                    ("m2", vec!["t1", "t2", "t3"]),
                ],
                vec![
                    ("t1", vec![0]),
                    ("t2", vec![0, 1]),
                    ("t3", vec![0, 1, 2, 3]),
                ],
                vec![
                    (
                        "m1",
                        vec![("t1", vec![0]), ("t2", vec![1]), ("t3", vec![1, 3])],
                    ),
                    ("m2", vec![("t2", vec![0]), ("t3", vec![0, 2])]),
                ],
            ),
            (
                vec![("m1", vec!["t1"]), ("m2", vec!["t2"])],
                vec![("t1", vec![0])],
                vec![("m1", vec![("t1", vec![0])])],
            ),
            (
                vec![("m1", vec!["t1", "t2", "t3"])],
                vec![("t1", vec![0]), ("t2", vec![0]), ("t3", vec![0, 1, 2])],
                vec![(
                    "m1",
                    vec![("t1", vec![0]), ("t2", vec![0]), ("t3", vec![0, 1, 2])],
                )],
            ),
            (
                vec![("m1", vec!["t1", "t2", "t3"]), ("m2", vec!["t1"])],
                vec![("t1", vec![0]), ("t2", vec![0]), ("t3", vec![0])],
                vec![(
                    "m1",
                    vec![("t1", vec![0]), ("t2", vec![0]), ("t3", vec![0])],
                )],
            ),
            (
                vec![("m1", vec!["t1", "t2", "t3"]), ("m2", vec!["t1", "t3"])],
                vec![("t1", vec![0]), ("t2", vec![0]), ("t3", vec![0])],
                vec![
                    ("m1", vec![("t1", vec![0]), ("t2", vec![0])]),
                    ("m2", vec![("t3", vec![0])]),
                ],
            ),
            (
                vec![
                    ("m", vec!["t1", "t2", "tt2"]),
                    ("m2", vec!["t1", "t2", "tt2"]),
                    ("m3", vec!["t1", "t2", "tt2"]),
                ],
                vec![("t1", vec![0]), ("t2", vec![0]), ("tt2", vec![0])],
                vec![
                    ("m", vec![("t1", vec![0])]),
                    ("m2", vec![("t2", vec![0])]),
                    ("m3", vec![("tt2", vec![0])]),
                ],
            ),
            (
                vec![
                    ("m1", vec!["t1"]),
                    ("m2", vec!["t1"]),
                    ("m3", vec!["t1"]),
                    ("m4", vec!["t1"]),
                ],
                vec![("t1", vec![0, 1])],
                vec![("m1", vec![("t1", vec![0])]), ("m2", vec![("t1", vec![1])])],
            ),
        ] {
            let topics = topics
                .into_iter()
                .map(|(name, partitions)| Topic {
                    name: name.to_string(),
                    partitions: btree_set(partitions.into_iter()),
                })
                .collect::<Vec<Topic>>();
            let members = members
                .iter()
                .map(|(name, topics)| JoinGroupResponseMember {
                    member_id: name.to_string(),
                    group_instance_id: None,
                    metadata: ConsumerGroupMemberMetadata {
                        version: 0,
                        topics: topics.iter().map(|name| name.to_string()).collect(),
                        user_data: vec![],
                        owned_partitions: vec![],
                        generation_id: 0,
                        rack_id: None,
                    },
                })
                .collect();
            let expect = want
                .into_iter()
                .map(|(member, assign)| {
                    let assignment = assign
                        .into_iter()
                        .map(|(topic, partitions)| PartitionAssignment {
                            topic: topic.to_string(),
                            partitions,
                        })
                        .collect();

                    (member.to_string(), assignment)
                })
                .collect::<BTreeMap<String, Vec<PartitionAssignment>>>();
            let got = round_robin(&topics, members);

            assert_eq!(expect, got);
        }
    }

    #[test]
    fn balance_strategy_range() {
        for (name, members, topics, want) in [
            (
                "2 members, 2 topics, 4 partitions each",
                vec![("m1", vec!["t1", "t2"]), ("m2", vec!["t1", "t2"])],
                vec![("t1", vec![0, 1, 2, 3]), ("t2", vec![0, 1, 2, 3])],
                vec![
                    ("m1", vec![("t1", vec![0, 1]), ("t2", vec![0, 1])]),
                    ("m2", vec![("t1", vec![2, 3]), ("t2", vec![2, 3])]),
                ],
            ),
            (
                "2 members, 2 topics, 4 partitions each (different member ids)",
                vec![("m3", vec!["t1", "t2"]), ("m4", vec!["t1", "t2"])],
                vec![("t1", vec![0, 1, 2, 3]), ("t2", vec![0, 1, 2, 3])],
                vec![
                    ("m3", vec![("t1", vec![0, 1]), ("t2", vec![0, 1])]),
                    ("m4", vec![("t1", vec![2, 3]), ("t2", vec![2, 3])]),
                ],
            ),
            (
                "3 members, 1 topic, 1 partition each",
                vec![("m1", vec!["t1"]), ("m2", vec!["t1"]), ("m3", vec!["t1"])],
                vec![("t1", vec![0])],
                vec![("m1", vec![("t1", vec![0])])],
            ),
            (
                "2 members, 2 topics, 3 partitions each",
                vec![("m1", vec!["t1", "t2"]), ("m2", vec!["t1", "t2"])],
                vec![("t1", vec![0, 1, 2]), ("t2", vec![0, 1, 2])],
                vec![
                    ("m1", vec![("t1", vec![0, 1]), ("t2", vec![0, 1])]),
                    ("m2", vec![("t1", vec![2]), ("t2", vec![2])]),
                ],
            ),
            (
                "2 members, 2 topics, different subscriptions",
                vec![("m1", vec!["t1"]), ("m2", vec!["t1", "t2"])],
                vec![("t1", vec![0, 1]), ("t2", vec![0, 1])],
                vec![
                    ("m1", vec![("t1", vec![0])]),
                    ("m2", vec![("t1", vec![1]), ("t2", vec![0, 1])]),
                ],
            ),
            (
                "2 members, 1 topic with duplicate assignments, 8 partitions each",
                vec![
                    ("m1", vec!["t1", "t1", "t1", "t1", "t1", "t1", "t1", "t1"]),
                    (
                        "m2",
                        vec![
                            "t1", "t1", "t1", "t1", "t1", "t1", "t1", "t1", "t1", "t1", "t1", "t1",
                        ],
                    ),
                ],
                vec![("t1", vec![0, 1, 2, 3, 4, 5, 6, 7])],
                vec![
                    ("m1", vec![("t1", vec![0, 1, 2, 3])]),
                    ("m2", vec![("t1", vec![4, 5, 6, 7])]),
                ],
            ),
        ] {
            let topics = topics
                .into_iter()
                .map(|(name, partitions)| Topic {
                    name: name.to_string(),
                    partitions: btree_set(partitions.into_iter()),
                })
                .collect::<Vec<Topic>>();
            let members = members
                .iter()
                .map(|(name, topics)| JoinGroupResponseMember {
                    member_id: name.to_string(),
                    group_instance_id: None,
                    metadata: ConsumerGroupMemberMetadata {
                        version: 0,
                        topics: topics.iter().map(|name| name.to_string()).collect(),
                        user_data: vec![],
                        owned_partitions: vec![],
                        generation_id: 0,
                        rack_id: None,
                    },
                })
                .collect();
            let expect = want
                .into_iter()
                .map(|(member, assign)| {
                    let assignment = assign
                        .into_iter()
                        .map(|(topic, partitions)| PartitionAssignment {
                            topic: topic.to_string(),
                            partitions,
                        })
                        .collect();

                    (member.to_string(), assignment)
                })
                .collect::<BTreeMap<String, Vec<PartitionAssignment>>>();
            let got = range(&topics, members);

            assert_eq!(expect, got, "{name}");
        }
    }
}
