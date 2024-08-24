use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Topic {
    /// The name of this topic.
    pub name: String,

    /// Partition information
    pub partitions: BTreeMap<i32, Partition>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Partition {
    /// The ID of the leader broker.
    pub leader_id: i32,

    /// The set of all nodes that host this partition.
    pub replica_nodes: Vec<i32>,

    /// The set of all nodes that are in sync with the leader for this partition.
    pub isr_nodes: Vec<i32>,
}
