use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_versioned_array, write_versioned_array, ReadVersionedError, ReadVersionedType,
    RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::traits::{ReadType, WriteType};
use std::io::{Read, Write};

pub struct PartitionCommit {
    /// The partition index.
    pub partition_index: i32,

    /// the message offset to be committed.
    pub committed_offset: i64,

    /// The timestamp of the commit.
    ///
    /// Removed in version 2.
    pub commit_timestamp: i64,

    /// The leader epoch of this partition.
    ///
    /// Added in version 6
    pub committed_leader_epoch: i32,

    /// Any associated metadata the client wants to keep.
    pub committed_metadata: Option<String>,
}

impl<W: Write> WriteVersionedType<W> for PartitionCommit {
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;

        self.partition_index.write(writer)?;
        self.committed_offset.write(writer)?;

        if v < 2 {
            self.commit_timestamp.write(writer)?;
        }

        if v >= 6 {
            self.committed_leader_epoch.write(writer)?;
        }

        self.committed_metadata.write(writer)?;

        Ok(())
    }
}

pub struct TopicCommit {
    /// The topic name.
    pub name: String,

    /// Each partition to commit offsets for.
    pub partitions: Vec<PartitionCommit>,
}

impl<W: Write> WriteVersionedType<W> for TopicCommit {
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        self.name.write(writer)?;

        write_versioned_array(writer, version, Some(&self.partitions))?;

        Ok(())
    }
}

// v1 or later
pub struct OffsetCommitRequest {
    /// The unique group identifier.
    pub group_id: String,

    /// The generation of the group.
    pub generation_id: i32,

    /// The member ID assigned by the group coordinator.
    pub member_id: String,

    /// The time period in ms to retain the offset.
    ///
    /// Added in version 2.
    /// Removed in version 5.
    pub retention_time_ms: i64,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 7.
    pub group_instance_id: Option<String>,

    /// The topics to commit offsets for.
    pub topics: Vec<TopicCommit>,
}

impl<W: Write> WriteVersionedType<W> for OffsetCommitRequest {
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v >= 1 && v < 8);

        self.group_id.write(writer)?;
        self.generation_id.write(writer)?;
        self.member_id.write(writer)?;

        if v >= 2 && v < 5 {
            self.retention_time_ms.write(writer)?;
        }

        if v >= 7 {
            self.group_instance_id.write(writer)?;
        }

        write_versioned_array(writer, version, Some(&self.topics))?;

        Ok(())
    }
}

impl RequestBody for OffsetCommitRequest {
    type ResponseBody = OffsetCommitResponse;
    const API_KEY: ApiKey = ApiKey::OffsetCommit;
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(1, 7);
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(8);
}

pub struct PartitionCommitResult {
    /// The partition index.
    pub partition_index: i32,

    /// The error_code, or 0 if there was no error.
    pub error_code: Option<Error>,
}

impl<R: Read> ReadVersionedType<R> for PartitionCommitResult {
    fn read_versioned(reader: &mut R, _version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let partition_index = i32::read(reader)?;
        let error_code = Error::new(i16::read(reader)?);

        Ok(Self {
            partition_index,
            error_code,
        })
    }
}

pub struct TopicCommitResult {
    /// The topic name.
    pub name: String,

    /// The responses for each partition in the topic.
    pub partitions: Vec<PartitionCommitResult>,
}

impl<R: Read> ReadVersionedType<R> for TopicCommitResult {
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let name = String::read(reader)?;
        let partitions = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(Self { name, partitions })
    }
}

// v1 of later
pub struct OffsetCommitResponse {
    /// The duration in milliseconds for which the request was throttled due
    /// to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 3.
    pub throttle_time_ms: i32,

    /// The responses for each partition in the topic.
    pub topics: Vec<TopicCommitResult>,
}

impl<R: Read> ReadVersionedType<R> for OffsetCommitResponse {
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v >= 1 && v < 8);

        let throttle_time_ms = (v >= 3)
            .then(|| i32::read(reader))
            .transpose()?
            .unwrap_or_default();
        let topics = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

// Tests are ported from github.com/Shopify/sarama
//
// https://github.com/Shopify/sarama/blob/main/offset_commit_request_test.go
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_commit_request() {
        for (req, want) in [
            (
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id: 0,
                    member_id: "".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: None,
                    topics: vec![],
                },
                vec![
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', // group
                    0x00, 0x00, 0x00, 0x00, // generation id
                    0x00, 0x00, // member id
                    0x00, 0x00, 0x00, 0x00,
                ],
            ),
            (
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id: 0x1122,
                    member_id: "cons".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: None,
                    topics: vec![TopicCommit {
                        name: "topic".to_string(),
                        partitions: vec![PartitionCommit {
                            partition_index: 0x5221,
                            committed_offset: 0xDEADBEEF,
                            commit_timestamp: -1,
                            committed_leader_epoch: 0,
                            committed_metadata: Some("metadata".into()),
                        }],
                    }],
                },
                vec![
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', // group
                    0x00, 0x00, 0x11, 0x22, // generation id
                    0x00, 0x04, b'c', b'o', b'n', b's', // member id
                    0x00, 0x00, 0x00, 0x01, // topics
                    0x00, 0x05, b't', b'o', b'p', b'i', b'c', // topic name
                    0x00, 0x00, 0x00, 0x01, // partitions
                    0x00, 0x00, 0x52, 0x21, // partition index
                    0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // partition offset
                    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // timestamp
                    0x00, 0x08, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a', // metadata
                ],
            ),
        ] {
            let mut buf = Vec::new();
            req.write_versioned(&mut buf, ApiVersion::new(1)).unwrap();
            assert_eq!(buf, want);
        }
    }

    #[test]
    fn offset_commit_request_v2_v4() {
        let no_topic = vec![
            0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', // group
            0x00, 0x00, 0x11, 0x22, // generation id
            0x00, 0x04, b'c', b'o', b'n', b's', // member id
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x33, // retention time
            0x00, 0x00, 0x00, 0x00, // topics
        ];

        let one_topic = vec![
            0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', // group
            0x00, 0x00, 0x11, 0x22, // generation id
            0x00, 0x04, b'c', b'o', b'n', b's', // member id
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x33, // retention time
            0x00, 0x00, 0x00, 0x01, // topics
            0x00, 0x05, b't', b'o', b'p', b'i', b'c', // topic name
            0x00, 0x00, 0x00, 0x01, // partitions
            0x00, 0x00, 0x52, 0x21, // partition index
            0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // offset
            0x00, 0x08, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a', // metadata
        ];

        for v in 2..4 {
            let mut req = OffsetCommitRequest {
                group_id: "foobar".to_string(),
                generation_id: 0x1122,
                member_id: "cons".to_string(),
                retention_time_ms: 0x4433,
                group_instance_id: None,
                topics: vec![],
            };

            let mut buf = Vec::new();
            req.write_versioned(&mut buf, ApiVersion::new(v)).unwrap();
            assert_eq!(buf, no_topic);

            req.topics.push(TopicCommit {
                name: "topic".to_string(),
                partitions: vec![PartitionCommit {
                    partition_index: 0x5221,
                    committed_offset: 0xDEADBEEF,
                    commit_timestamp: 0,
                    committed_leader_epoch: 0,
                    committed_metadata: Some("metadata".into()),
                }],
            });
            let mut buf = Vec::new();
            req.write_versioned(&mut buf, ApiVersion::new(v)).unwrap();
            assert_eq!(buf, one_topic);
        }
    }

    #[test]
    fn offset_commit_request_v5_to_v7() {
        for (version, req, want) in [
            (
                5,
                OffsetCommitRequest {
                    group_id: "foo".to_string(),
                    generation_id: 1,
                    member_id: "mid".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: None,
                    topics: vec![TopicCommit {
                        name: "topic".to_string(),
                        partitions: vec![PartitionCommit {
                            partition_index: 1,
                            committed_offset: 2,
                            commit_timestamp: 0,
                            committed_leader_epoch: 0,
                            committed_metadata: Some("meta".into()),
                        }],
                    }],
                },
                vec![
                    0, 3, b'f', b'o', b'o', // GroupId
                    0x00, 0x00, 0x00, 0x01, // GenerationId
                    0, 3, b'm', b'i', b'd', // MemberId
                    0, 0, 0, 1, // One Topic
                    0, 5, b't', b'o', b'p', b'i', b'c', // Name
                    0, 0, 0, 1, // One Partition
                    0, 0, 0, 1, // PartitionIndex
                    0, 0, 0, 0, 0, 0, 0, 2, // CommittedOffset
                    0, 4, b'm', b'e', b't', b'a', // CommittedMetadata
                ],
            ),
            (
                6,
                OffsetCommitRequest {
                    group_id: "foo".to_string(),
                    generation_id: 1,
                    member_id: "mid".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: None,
                    topics: vec![TopicCommit {
                        name: "topic".to_string(),
                        partitions: vec![PartitionCommit {
                            partition_index: 1,
                            committed_offset: 2,
                            commit_timestamp: 0,
                            committed_leader_epoch: 3,
                            committed_metadata: Some("meta".into()),
                        }],
                    }],
                },
                vec![
                    0, 3, b'f', b'o', b'o', // GroupId
                    0x00, 0x00, 0x00, 0x01, // GenerationId
                    0, 3, b'm', b'i', b'd', // MemberId
                    0, 0, 0, 1, // One Topic
                    0, 5, b't', b'o', b'p', b'i', b'c', // Name
                    0, 0, 0, 1, // One Partition
                    0, 0, 0, 1, // PartitionIndex
                    0, 0, 0, 0, 0, 0, 0, 2, // CommittedOffset
                    0, 0, 0, 3, // CommittedEpoch
                    0, 4, b'm', b'e', b't', b'a', // CommittedMetadata
                ],
            ),
            (
                7,
                OffsetCommitRequest {
                    group_id: "foo".to_string(),
                    generation_id: 1,
                    member_id: "mid".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: Some("gid".into()),
                    topics: vec![TopicCommit {
                        name: "topic".to_string(),
                        partitions: vec![PartitionCommit {
                            partition_index: 1,
                            committed_offset: 12,
                            commit_timestamp: 0,
                            committed_leader_epoch: 4,
                            committed_metadata: Some("meta".into()),
                        }],
                    }],
                },
                vec![
                    0, 3, b'f', b'o', b'o', // GroupId
                    0x00, 0x00, 0x00, 0x01, // GenerationId
                    0, 3, b'm', b'i', b'd', // MemberId
                    0, 3, b'g', b'i', b'd', // MemberId
                    0, 0, 0, 1, // One Topic
                    0, 5, b't', b'o', b'p', b'i', b'c', // Name
                    0, 0, 0, 1, // One Partition
                    0, 0, 0, 1, // PartitionIndex
                    0, 0, 0, 0, 0, 0, 0, 12, // CommittedOffset
                    0, 0, 0, 4, // CommittedEpoch
                    0, 4, b'm', b'e', b't', b'a', // CommittedMetadata
                ],
            ),
        ] {
            let mut buf = Vec::new();
            req.write_versioned(&mut buf, ApiVersion::new(version))
                .unwrap();
            assert_eq!(buf, want, "version {version}");
        }
    }
}
