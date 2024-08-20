use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::producer::aggregator::RecordAggregator;
use rskafka::client::producer::BatchProducerBuilder;
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;

const TOPIC: &str = "test";

#[tokio::main]
async fn main() {
    let brokers = vec!["localhost:9010".to_string(), "localhost:9011".to_string()];
    let client = ClientBuilder::new(brokers).build().await.unwrap();

    let cc = client.controller_client().unwrap();
    // topic might be exist already
    let _ = cc.create_topic(TOPIC, 3, 2, 1000).await;

    let client = Arc::new(client);
    tokio::spawn(produce_per_partition(client.clone(), 0));
    tokio::spawn(produce_per_partition(client.clone(), 1));
    tokio::spawn(produce_per_partition(client.clone(), 2));

    tokio::time::sleep(Duration::from_secs(10000)).await;
}

async fn produce_per_partition(client: Arc<Client>, partition: i32) {
    let pc = client
        .partition_client(TOPIC, partition, UnknownTopicHandling::Retry)
        .await
        .unwrap();
    let producer = BatchProducerBuilder::new(Arc::new(pc))
        .with_linger(Duration::from_secs(5))
        .build(RecordAggregator::new(128));

    let mut index = 0;
    loop {
        let record = Record {
            key: None,
            value: Some(format!("hello: {partition} {index}").into()),
            headers: BTreeMap::from([("foo".to_owned(), b"bar".to_vec())]),
            timestamp: Utc::now(),
        };

        let offset = producer.produce(record).await.unwrap();
        index += 1;

        println!("partition: {}, offset: {}", partition, offset);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
