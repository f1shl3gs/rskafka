mod test_helpers;

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::error::{Error, ProtocolError};
use rskafka::client::partition::{Compression, OffsetAt, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::protocol::messages::PartitionAssignment;
use rskafka::record::{Record, RecordAndOffset};
use rskafka::topic::Topic;
use tracing::{error, info, warn};

use crate::test_helpers::maybe_start_logging;

#[ignore]
#[tokio::test]
async fn produce() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();

    let topic = "test_00";
    if client
        .list_topics()
        .await
        .unwrap()
        .iter()
        .find(|t| t.name == topic)
        .is_none()
    {
        let controller_client = client.controller_client().unwrap();

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

        info!("produce done msg {partition}/{i}");

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_group() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let client = Arc::new(client);

    let group = "demo";
    let start_at = OffsetAt::Earliest;

    loop {
        // list all topics
        let topics = client.list_topics().await.unwrap();

        // filter
        let topics = topics
            .into_iter()
            .filter(|t| t.name.starts_with("test_"))
            .collect::<Vec<_>>();

        let consumer = client
            .consumer_group(group.to_string(), &topics)
            .await
            .unwrap();

        let consumer = Arc::new(consumer);
        let notify = Arc::new(tokio::sync::Notify::new());
        let mut offsets = Vec::new();

        let start_offsets = consumer.offsets().await.unwrap();

        info!("start offsets: {:?}", start_offsets);

        // consume topics
        for PartitionAssignment { topic, partitions } in consumer.assignment() {
            let starts = start_offsets.iter().find(|t| &t.name == topic);
            for partition in partitions {
                let topic = topic.to_string();
                let partition = *partition;
                let shutdown = notify.clone();
                let cli = client.clone();
                let current_offset = Arc::new(AtomicI64::new(0));
                offsets.push(current_offset.clone());

                let start = match starts {
                    Some(topic) => topic
                        .partitions
                        .iter()
                        .find(|p| p.partition_index == partition)
                        .map(|p| StartOffset::At(p.committed_offset))
                        .unwrap_or_else(|| StartOffset::Latest),
                    None => StartOffset::Latest,
                };

                // Once shutdown resolved, this task will exit too, therefore we can
                // just spawn and forget.
                tokio::spawn(async move {
                    let pc = match cli
                        .partition_client(&topic, partition, UnknownTopicHandling::Error)
                        .await
                    {
                        Ok(pc) => pc,
                        Err(err) => {
                            error!("create partition client({topic}/{partition}) failed, {err}");
                            return;
                        }
                    };

                    let start = match pc.get_offset(start_at).await {
                        Ok(current) => match start {
                            StartOffset::At(committed) => {
                                if committed < current {
                                    StartOffset::At(current)
                                } else {
                                    start
                                }
                            }
                            _ => start,
                        },
                        Err(err) => {
                            error!("get offset failed, err: {}", err);
                            return;
                        }
                    };

                    info!(
                        "start consumer {}/{} at {:?}",
                        pc.topic(),
                        pc.partition(),
                        start
                    );

                    let mut consumer = StreamConsumerBuilder::new(Arc::new(pc), start).build();

                    loop {
                        tokio::select! {
                            result = consumer.next() => {
                                if let Some(record) = result {
                                    match record {
                                        Ok((RecordAndOffset { record, offset }, _last_high_watermark)) => {
                                            let value = record.value.unwrap();
                                            let msg = String::from_utf8_lossy(&value);

                                            info!("consume -- {topic} {partition} {offset} {msg}");

                                            // update offset here
                                            current_offset.store(offset, Ordering::Relaxed);
                                        }
                                        Err(err) => {
                                            error!("consumer failed, {topic} {partition} {err}")
                                        }
                                    }
                                } else {
                                    // closed
                                    break;
                                }
                            },

                            _ = shutdown.notified() => {
                                break
                            }
                        }
                    }

                    info!("consumer {}/{} exit", topic, partition);
                });
            }
        }

        // heartbeat
        let shutdown = notify.clone();
        let hc = consumer.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(3));
            let mut retries = 3;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {},
                    _ = shutdown.notified() => return
                }

                if let Err(err) = hc.heartbeat().await {
                    match err {
                        Error::ServerError { protocol_error, .. }
                            if protocol_error == ProtocolError::RebalanceInProgress =>
                        {
                            info!("rebalancing triggered");
                            break;
                        }
                        _ => {
                            warn!("unexpected error when heartbeat, {}", err);
                            retries -= 1;
                            if retries <= 0 {
                                break;
                            }
                        }
                    }
                } else {
                    retries = 3;
                    info!("heartbeat success");
                }
            }

            // topic check loop might call this too, so send might failed,
            // but it's ok;
            shutdown.notify_waiters();

            info!("heartbeat loop exit");
        });

        // commit loop
        let shutdown = notify.clone();
        let cc = consumer.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));

            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    _ = ticker.tick() => {}
                }

                let mut i = 0;
                for PartitionAssignment { topic, partitions } in cc.assignment() {
                    for partition in partitions {
                        let offset = offsets[i].load(Ordering::Relaxed);
                        i += 1;

                        if offset == 0 {
                            // consume nothing till now
                            continue;
                        }

                        let topics = cc.commit(topic, *partition, offset).await.unwrap();
                        for topic in topics {
                            for p in topic.partitions {
                                info!(
                                    "commit offset of {}/{}/{} err:{:?}",
                                    topic.name, p.partition_index, offset, p.error_code
                                );
                            }
                        }
                    }
                }
            }
        });

        // topic check loop
        let mut ticker = tokio::time::interval(Duration::from_secs(60 * 10));
        loop {
            tokio::select! {
                _ = notify.notified() => break,
                _ = ticker.tick() => {}
            }

            let mut new_topics = client
                .list_topics()
                .await
                .unwrap()
                .into_iter()
                .filter(|t| t.name.starts_with("test_"))
                .collect::<Vec<_>>();
            new_topics.sort_by(|a, b| a.name.cmp(&b.name));

            if !compare_topics(&topics, &new_topics) {
                notify.notify_waiters();
                break;
            }
        }

        // leave the group
        // consumer.leave().await.unwrap();

        // don't wait the underground task
    }
}

// compare_topics compare topic count, name and partitions
//
// eq_by is not stable yet.
fn compare_topics(old: &[Topic], new: &[Topic]) -> bool {
    if old.len() != new.len() {
        return false;
    }

    for i in 0..old.len() {
        let o = &old[i];
        let n = &new[i];

        if o.name != n.name {
            return false;
        }

        if o.partitions.len() != n.partitions.len() {
            return false;
        }
    }

    return true;
}
