use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rskafka::client::error::ProtocolError;
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::protocol::messages::{
    OffsetCommitRequestTopic, OffsetCommitRequestTopicPartition, PartitionAssignment,
};
use rskafka::record::RecordAndOffset;
use tokio::signal::unix::SignalKind;
use tokio::sync::Notify;
use tracing::error;

const MAX_MESSAGE_LENGTH: i32 = 52428800;
const FETCH_TIMEOUT_MS: i32 = 500;

#[tokio::main]
async fn main() {
    let n = tokio::runtime::Handle::current().metrics().num_workers();
    println!("number of workers: {}", n);

    let brokers = vec!["0.0.0.0:9011".to_string()];
    let client = ClientBuilder::new(brokers).build().await.unwrap();
    let client = Arc::new(client);

    // watch signals
    let mut signal = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    let shutdown = Arc::new(Notify::new());
    let signal_shutdown = Arc::clone(&shutdown);
    tokio::spawn(async move {
        let _ = signal.recv().await;
        signal_shutdown.notify_waiters();
    });

    // list topics and find what we want
    let mut topics = client
        .list_topics()
        .await
        .unwrap()
        .into_iter()
        .filter(|t| t.name.starts_with("test"))
        .collect::<Vec<_>>();
    if topics.is_empty() {
        println!("no matched topic");
        return;
    }

    loop {
        let group = client
            .consumer_group("test".to_string(), &topics)
            .await
            .map(Arc::new)
            .unwrap();
        let committed_offsets = group.offsets().await.unwrap();
        let mut offsets = BTreeMap::<String, BTreeMap<i32, Arc<AtomicI64>>>::new();
        let notify = Arc::new(Notify::new());

        for PartitionAssignment { topic, partitions } in group.assignment() {
            let topic_committed_offsets = committed_offsets.iter().find(|t| &t.name == topic);

            let mut topic_offsets = BTreeMap::<i32, Arc<AtomicI64>>::new();
            for partition in partitions {
                let current_offset = Arc::new(AtomicI64::new(0));
                topic_offsets.insert(*partition, current_offset.clone());

                let pc = client
                    .partition_client(topic, *partition, UnknownTopicHandling::Error)
                    .await
                    .unwrap();

                let committed_offset = match topic_committed_offsets {
                    Some(topic) => topic
                        .partitions
                        .iter()
                        .find(|p| p.partition_index == *partition)
                        .map(|p| p.committed_offset),
                    None => None,
                };

                // consume one partition
                let signal = Arc::clone(&notify);
                tokio::spawn(async move {
                    // get correct start offset.
                    let start = match committed_offset {
                        Some(committed) => {
                            // the committed offset might be small than earliest offset.
                            // if this happened, OutOfRange error will be returned.
                            let earliest = match pc.get_offset(OffsetAt::Earliest).await {
                                Ok(o) => o,
                                Err(err) => {
                                    error!(message = "get earliest offset failed", %err);
                                    return;
                                }
                            };

                            committed.max(earliest)
                        }
                        None => match pc.get_offset(OffsetAt::Latest).await {
                            Ok(o) => o,
                            Err(err) => {
                                error!(message = "get start offset failed", %err);
                                return;
                            }
                        },
                    };

                    current_offset.store(start, Ordering::Relaxed);

                    println!("start consume records from partition, topic: {}, partition: {}, start: {start}", pc.topic(), pc.partition());

                    loop {
                        let start = current_offset.load(Ordering::Relaxed);
                        let result = tokio::select! {
                            result = pc.fetch_records(start, 1..MAX_MESSAGE_LENGTH, FETCH_TIMEOUT_MS) => result,
                            _ = signal.notified() => break,
                        };

                        match result {
                            Ok((records, _watermark)) => {
                                for RecordAndOffset { record, offset } in records {
                                    let msg = match &record.value {
                                        Some(value) => String::from_utf8_lossy(value),
                                        None => Cow::Borrowed(""),
                                    };

                                    println!(
                                        "recv record, {}/{}/{} -- {}",
                                        pc.topic(),
                                        pc.partition(),
                                        offset,
                                        msg
                                    );
                                    current_offset.store(offset + 1, Ordering::Relaxed);
                                }
                            }
                            Err(err) => {
                                println!("fetch records failed, {err}");
                                break;
                            }
                        }
                    }
                });
            }

            offsets.insert(topic.to_string(), topic_offsets);
        }

        // heartbeat loop
        let signal = Arc::clone(&notify);
        let hc = Arc::clone(&group);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(3));

            loop {
                tokio::select! {
                    _ = signal.notified() => break,
                    _ = ticker.tick() => {}
                }

                if let Err(err) = hc.heartbeat().await {
                    match err {
                        rskafka::client::error::Error::ServerError {
                            protocol_error: ProtocolError::RebalanceInProgress,
                            ..
                        } => {
                            println!("rebalancing triggered: {err}");
                            break;
                        }
                        _ => {
                            println!("heartbeat failed: {err}");
                            break;
                        }
                    }
                }
            }

            signal.notify_waiters();
        });

        // commit loop
        let signal = Arc::clone(&notify);
        let cc = Arc::clone(&group);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(3));

            loop {
                tokio::select! {
                    _ = signal.notified() => break,
                    _ = ticker.tick() => {}
                }

                let topics = offsets
                    .iter()
                    .map(|(topic, topic_offsets)| {
                        for (index, offset) in topic_offsets {
                            let offset = offset.load(Ordering::Relaxed);
                            println!("commit {topic} {index}/{offset}");
                        }

                        let partitions = topic_offsets
                            .into_iter()
                            .map(|(partition, offset)| OffsetCommitRequestTopicPartition {
                                partition_index: *partition,
                                committed_offset: offset.load(Ordering::Relaxed),
                                committed_timestamp: 0,
                                committed_leader_epoch: 0,
                                committed_metadata: None,
                            })
                            .collect::<Vec<_>>();

                        OffsetCommitRequestTopic {
                            name: topic.to_string(),
                            partitions,
                        }
                    })
                    .collect::<Vec<_>>();

                if let Err(err) = cc.commit(topics).await {
                    println!("error while committing offsets: {err}");
                    // still continue to committing
                }
            }
        });

        // topic check loop
        let mut ticker = tokio::time::interval(Duration::from_secs(10 * 60));
        loop {
            tokio::select! {
                _ = ticker.tick() => {},
                _ = notify.notified() => {
                    // rebalance, heartbeat or commit error
                    break
                },
                _ = shutdown.notified() => {
                    // SIGNAL received, stop consumer

                    // stop consume, heartbeat, commit offsets
                    notify.notify_waiters();

                    if let Err(err) = group.leave().await {
                        println!("error while leaving consumer group: {err}");
                    }

                    return
                },
            }

            let new_topics = match client.list_topics().await {
                Ok(topics) => topics
                    .into_iter()
                    .filter(|t| t.name.starts_with("test"))
                    .collect::<Vec<_>>(),
                Err(err) => {
                    println!("error while getting topics from client: {err}");
                    continue;
                }
            };

            if !new_topics.eq(&topics) {
                // topics or partitions changed, stop consuming,
                notify.notify_waiters();

                // update topics
                topics = new_topics;
                break;
            }
        }
    }
}
