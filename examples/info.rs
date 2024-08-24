use rskafka::client::ClientBuilder;
use rskafka::protocol::messages::OffsetFetchRequestTopic;

#[tokio::main]
async fn main() {
    let brokers = vec!["0.0.0.0:9011".to_string()];
    let client = ClientBuilder::new(brokers).build().await.unwrap();

    let admin = client.controller_client().unwrap();
    let groups = admin.list_groups().await.unwrap();
    let topics = client.list_topics().await.unwrap();

    let groups = admin
        .describe_groups(groups.iter().map(|g| g.group_id.clone()).collect())
        .await
        .unwrap();
    if groups.is_empty() {
        println!("No groups available");
        return;
    }

    for group in groups {
        println!("GROUP_ID:       {}", group.group_id);
        println!("GROUP_STATE:    {:?}", group.group_state);
        println!("PROTOCOL_TYPE:  {}", group.protocol_type);
        println!("MEMBER:");
        for member in group.members {
            println!("  MEMBER_ID:           {}", member.member_id);
            println!("  GROUP_INSTANCE_ID:   {:?}", member.group_instance_id);
            println!("  CLIENT_ID:           {}", member.client_id);
            println!("  CLIENT_HOST:         {}", member.client_host);
            println!();
        }

        let topic_names = topics
            .iter()
            .map(|t| OffsetFetchRequestTopic {
                name: t.name.clone(),
                partition_indexes: t.partitions.keys().cloned().collect(),
                tagged_fields: None,
            })
            .collect::<Vec<_>>();
        let offsets = admin
            .consumer_group_offsets(group.group_id, topic_names)
            .await
            .unwrap();
        for topic in offsets {
            println!("TOPIC:        {}", topic.name);
            for partition in topic.partitions {
                println!(
                    "  COMMITS:    {}/{}",
                    partition.partition_index, partition.committed_offset
                );
            }
        }
        println!();
    }
}
