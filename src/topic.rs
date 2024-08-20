use std::collections::BTreeSet;

#[derive(Debug, Eq, PartialEq)]
pub struct Topic {
    pub name: String,
    pub partitions: BTreeSet<i32>,
}
