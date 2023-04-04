use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::error::{Error, ProtocolError};
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::protocol::messages::{
    heartbeat, Assignment, ConsumerGroupMemberAssignment, ConsumerGroupMemberMetadata,
    PartitionAssignment,
};
use rskafka::protocol::traits::{ReadType, WriteType};
use rskafka::record::{Record, RecordAndOffset};
use rskafka::topic::Topic;
use tracing::{error, info, warn};
use tracing_subscriber::FmtSubscriber;

use crate::test_helpers::start_logging;

#[ignore]
#[tokio::test]
async fn produce() {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_err| eprintln!("Unable to set global default subscriber"))
        .unwrap();

    let bootstrap_brokers = vec!["localhost:9010".to_string()];
    let client = ClientBuilder::new(bootstrap_brokers).build().await.unwrap();
    let controller_client = client.controller_client().unwrap();

    let topic = "test_00";
    if client
        .list_topics()
        .await
        .unwrap()
        .iter()
        .find(|t| t.name == topic)
        .is_none()
    {
        controller_client
            .create_topic(topic, 2, 2, 5_000)
            .await
            .unwrap();
    }

    let p1 = &client
        .partition_client(topic, 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let p2 = &client
        .partition_client(topic, 1, UnknownTopicHandling::Error)
        .await
        .unwrap();

    for i in 0..1000 {
        let pc = if i % 2 == 0 { p1 } else { p2 };

        let partition = pc.partition();
        pc.produce(
            vec![Record {
                key: None,
                value: Some(format!("msg {i} from partition {partition}").into_bytes()),
                headers: Default::default(),
                timestamp: Default::default(),
            }],
            Compression::NoCompression,
        )
        .await
        .unwrap();

        info!("produce done");

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

mod test_helpers;

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn consumer_group() {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_err| eprintln!("Unable to set global default subscriber"))
        .unwrap();

    let group = "demo";
    let bootstrap_brokers = vec!["localhost:9010".to_string()];

    let client = ClientBuilder::new(bootstrap_brokers).build().await.unwrap();
    let client = Arc::new(client);

    loop {
        // list all topics
        let topics = client.list_topics().await.unwrap();

        // filter
        let topics = topics
            .into_iter()
            .filter(|t| t.name.starts_with("test_"))
            .collect::<Vec<_>>();

        // session
        let names = topics.iter().map(|t| t.name.clone()).collect::<Vec<_>>();
        let control_client = client.controller_client().unwrap();
        let join = control_client.join_group(group, names).await.unwrap();

        // prepare distribution plan if we joined as the leader.
        let mut assignments = None;
        if join.leader == join.member_id {
            assignments = Some(
                balance(&topics, &join.members)
                    .into_iter()
                    .map(|(member_id, topics)| {
                        let assignment = ConsumerGroupMemberAssignment {
                            version: 0,
                            topics,
                            user_data: vec![],
                        };

                        let mut writer = Vec::new();
                        assignment
                            .write(&mut writer)
                            .expect("write ConsumerGroupMemberAssignment success");

                        Assignment {
                            member_id,
                            assignment: writer,
                            tagged_fields: None,
                        }
                    })
                    .collect::<Vec<_>>(),
            );
        }

        let resp = control_client
            .sync_group(
                group.to_string(),
                join.generation_id,
                join.member_id.clone(),
                None,
                assignments.unwrap_or_default(),
            )
            .await
            .unwrap();

        let (tx, rx) = futures::channel::oneshot::channel::<()>();
        let rx = rx.shared();

        // retrieve and sort claims
        if resp.assignment.len() > 0 {
            let mut reader = Cursor::new(&resp.assignment);
            let assignment = ConsumerGroupMemberAssignment::read(&mut reader).unwrap();
            for PartitionAssignment { topic, partitions } in assignment.topics {
                for partition in partitions {
                    let cli = client.clone();
                    let shutdown = rx.clone();
                    let topic = topic.clone();

                    // Once shutdown resolved, this task will exit too, therefore we can
                    // just spawn and forget.
                    tokio::spawn(async move {
                        let pc = match cli
                            .partition_client(&topic, partition, UnknownTopicHandling::Error)
                            .await
                        {
                            Ok(pc) => pc,
                            Err(err) => {
                                error!(
                                    "create partition client({topic}/{partition}) failed, {err}"
                                );
                                return;
                            }
                        };

                        info!("start consumer {}/{}", pc.topic(), pc.partition());
                        let mut consumer =
                            StreamConsumerBuilder::new(Arc::new(pc), StartOffset::Latest)
                                .build()
                                .take_until(shutdown);

                        while let Some(result) = consumer.next().await {
                            match result {
                                Ok((RecordAndOffset { record, offset }, _last_high_watermark)) => {
                                    let value = record.value.unwrap();
                                    let msg = String::from_utf8_lossy(&value);

                                    info!("{topic} {partition} {offset} {msg}");
                                }
                                Err(err) => {
                                    error!("consumer failed, {topic} {partition} {err}")
                                }
                            }
                        }
                    });
                }
            }
        }

        // heartbeat loop
        let mut shutdown = rx.clone();
        let req = heartbeat::HeartbeatRequest {
            group_id: group.to_string(),
            generation_id: join.generation_id,
            member_id: join.member_id,
            group_instance_id: None,
        };
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(3));

            info!("start heartbeat loop");
            loop {
                tokio::select! {
                    _ = ticker.tick() => {},
                    _ = &mut shutdown => return
                }

                match control_client.heartbeat(&req).await {
                    Ok(_resp) => {
                        info!("heartbeat success");
                    }
                    Err(err) => match err {
                        Error::ServerError { protocol_error, .. } => {
                            match protocol_error {
                                ProtocolError::RebalanceInProgress => {
                                    // partition check loop might call send too.
                                    let _ = tx.send(());
                                    return;
                                }

                                ProtocolError::UnknownMemberId
                                | ProtocolError::IllegalGeneration
                                | ProtocolError::FencedInstanceId => {
                                    warn!("heartbeat down");
                                    return;
                                }

                                err => warn!("unexpected protocol error {err}"),
                            }
                        }
                        err => {
                            warn!("heartbeat failed, {err}")
                        }
                    },
                }
            }
        });

        // check partition number
        // loop {}

        tokio::time::sleep(Duration::from_secs(600)).await;
    }
}

// round robin
// member_id -> topic -> partitions
fn balance(
    topics: &[Topic],
    members: &[rskafka::protocol::messages::join_group::Member],
) -> BTreeMap<String, Vec<PartitionAssignment>> {
    let mut plan = BTreeMap::new();
    let mut members = members
        .iter()
        .map(|m| {
            let mut c = Cursor::new(&m.metadata);
            let meta = ConsumerGroupMemberMetadata::read(&mut c).unwrap();

            (m.member_id.clone(), meta)
        })
        .collect::<Vec<_>>();
    members.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut i = 0;
    let n = members.len();
    for topic in topics {
        for (partition, _) in &topic.partitions {
            let member = loop {
                let (member, metadata) = &members[i % n];
                i += 1;

                if metadata.topics.contains(&topic.name) {
                    break member;
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

#[cfg(test)]
mod tests {
    use super::*;
    use rskafka::protocol::traits::WriteType;
    use rskafka::topic::Partition;

    #[test]
    fn round_robin() {
        fn new_topic(name: &str, partitions: i32) -> Topic {
            let partitions = (0..partitions)
                .into_iter()
                .map(|p| {
                    (
                        p,
                        Partition {
                            leader_id: 0,
                            replica_nodes: vec![],
                            isr_nodes: vec![],
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();

            Topic {
                name: name.to_string(),
                partitions,
            }
        }

        for (mut members, topics, want) in [
            (
                vec![
                    ("m1", vec!["t1", "t2", "t3"]),
                    ("m2", vec!["t1", "t2", "t3"]),
                ],
                vec![("t1", 1), ("t2", 1), ("t3", 1)],
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
                vec![("t1", 1), ("t2", 2), ("t3", 4)],
                vec![
                    (
                        "m1",
                        vec![("t1", vec![0]), ("t2", vec![1]), ("t3", vec![1, 3])],
                    ),
                    ("m2", vec![("t2", vec![0]), ("t3", vec![0, 2])]),
                ],
            ),
            (
                vec![("m1", vec!["t1"]), ("m2", vec!["t1"])],
                vec![("t1", 1)],
                vec![("m1", vec![("t1", vec![0])])],
            ),
            (
                vec![("m1", vec!["t1", "t2", "t3"])],
                vec![("t1", 1), ("t2", 1), ("t3", 3)],
                vec![(
                    "m1",
                    vec![("t1", vec![0]), ("t2", vec![0]), ("t3", vec![0, 1, 2])],
                )],
            ),
            (
                vec![("m1", vec!["t1", "t2", "t3"]), ("m2", vec!["t1"])],
                vec![("t1", 1), ("t2", 1), ("t3", 1)],
                vec![(
                    "m1",
                    vec![("t1", vec![0]), ("t2", vec![0]), ("t3", vec![0])],
                )],
            ),
            (
                vec![("m1", vec!["t1", "t2", "t3"]), ("m2", vec!["t1", "t3"])],
                vec![("t1", 1), ("t2", 1), ("t3", 1)],
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
                vec![("t1", 1), ("t2", 1), ("tt2", 1)],
                vec![
                    ("m", vec![("t1", vec![0])]),
                    ("m2", vec![("t2", vec![0])]),
                    ("m3", vec![("tt2", vec![0])]),
                ],
            ),
        ] {
            members.sort();
            let members = members
                .iter()
                .map(|(id, topics)| {
                    let meta = ConsumerGroupMemberMetadata {
                        version: 0,
                        topics: topics.iter().map(|t| t.to_string()).collect(),
                        user_data: vec![],
                        owned_partitions: vec![],
                    };

                    let mut metadata = Vec::new();
                    meta.write(&mut metadata).unwrap();

                    rskafka::protocol::messages::join_group::Member {
                        member_id: id.to_string(),
                        group_instance_id: None,
                        metadata,
                    }
                })
                .collect::<Vec<_>>();

            let topics = topics
                .iter()
                .map(|(name, total)| new_topic(name, *total))
                .collect::<Vec<_>>();

            let want = want
                .into_iter()
                .map(|(name, assignments)| {
                    let assignments = assignments
                        .into_iter()
                        .map(|(topic, partitions)| PartitionAssignment {
                            topic: topic.to_string(),
                            partitions,
                        })
                        .collect::<Vec<_>>();

                    (name.to_string(), assignments)
                })
                .collect::<BTreeMap<_, _>>();

            let got = balance(&topics, &members);

            assert_eq!(got, want)
        }
    }
}
