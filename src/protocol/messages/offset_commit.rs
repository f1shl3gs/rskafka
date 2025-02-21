use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
    read_versioned_array, write_versioned_array,
};
use crate::protocol::traits::{ReadType, WriteType};

#[derive(Debug)]
pub struct OffsetCommitRequestTopicPartition {
    /// The partition index.
    pub partition_index: i32,

    /// The message offset to be committed.
    pub committed_offset: i64,

    /// The timestamp of the commit.
    ///
    /// Added in version 1
    /// Removed in version 2
    pub committed_timestamp: i64,

    /// The leader epoch of this partition.
    ///
    /// Added in version 6
    pub committed_leader_epoch: i32,

    /// Any associated metadata the client wants to keep.
    pub committed_metadata: Option<String>,
}

impl<W> WriteVersionedType<W> for OffsetCommitRequestTopicPartition
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v < 8);

        self.partition_index.write(writer)?;
        self.committed_offset.write(writer)?;

        if v == 1 {
            self.committed_timestamp.write(writer)?;
        }

        if v > 5 {
            self.committed_leader_epoch.write(writer)?;
        }

        self.committed_metadata.write(writer)?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct OffsetCommitRequestTopic {
    /// The topic name.
    pub name: String,

    /// Each partition to commit offsets for.
    pub partitions: Vec<OffsetCommitRequestTopicPartition>,
}

impl<W> WriteVersionedType<W> for OffsetCommitRequestTopic
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v < 8);

        self.name.write(writer)?;
        write_versioned_array(writer, version, Some(&self.partitions))?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct OffsetCommitRequest {
    /// The unique group identifier.
    pub group_id: String,

    /// The generation of the group if using the classic group protocol
    /// or the member epoch if using the consumer protocol.
    ///
    /// Added in version 1
    pub generation_id_or_member_epoch: i32,

    /// The member ID assigned by the group coordinator.
    ///
    /// Added in version 1
    pub member_id: String,

    /// The time period in ms to retain the offset.
    ///
    /// Added in version 2
    /// Removed in version 5
    pub retention_time_ms: i64,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 7
    pub group_instance_id: Option<String>,

    /// The topics to commit offsets for.
    pub topics: Vec<OffsetCommitRequestTopic>,
}

impl<W> WriteVersionedType<W> for OffsetCommitRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v < 8);

        self.group_id.write(writer)?;

        if v >= 1 {
            self.generation_id_or_member_epoch.write(writer)?;
            self.member_id.write(writer)?;
        }

        if v >= 2 && v <= 4 {
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
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 7);
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(8);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
#[allow(missing_copy_implementations)]
pub struct OffsetCommitResponseTopicPartition {
    /// The partition index.
    pub partition_index: i32,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,
}

impl<R> ReadVersionedType<R> for OffsetCommitResponseTopicPartition
where
    R: Read,
{
    fn read_versioned(reader: &mut R, _version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let partition_index = i32::read(reader)?;
        let error_code = Error::new(i16::read(reader)?);

        Ok(OffsetCommitResponseTopicPartition {
            partition_index,
            error_code,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct OffsetCommitResponseTopic {
    /// The topic name.
    pub name: String,

    /// The responses for each partition in the topic.
    pub partitions: Vec<OffsetCommitResponseTopicPartition>,
}

impl<R> ReadVersionedType<R> for OffsetCommitResponseTopic
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let name = String::read(reader)?;
        let partitions = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(OffsetCommitResponseTopic { name, partitions })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct OffsetCommitResponse {
    /// The duration in milliseconds for which the request was throttled due to
    /// a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 3
    pub throttle_time_ms: Option<i32>,

    /// The responses for each topic.
    pub topics: Vec<OffsetCommitResponseTopic>,
}

impl<R> ReadVersionedType<R> for OffsetCommitResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v < 8);

        let throttle_time_ms = (v > 2).then(|| i32::read(reader)).transpose()?;
        let topics = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(OffsetCommitResponse {
            throttle_time_ms,
            topics,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn request() {
        for (name, version, req, want) in [
            (
                "no blocks",
                0,
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id_or_member_epoch: 0,
                    member_id: "".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: None,
                    topics: vec![],
                },
                [
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', 0x00, 0x00, 0x00, 0x00,
                ]
                .as_ref(),
            ),
            (
                "one block",
                0,
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id_or_member_epoch: 0,
                    member_id: "".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: None,
                    topics: vec![OffsetCommitRequestTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitRequestTopicPartition {
                            partition_index: 0x5221,
                            committed_offset: 0xdeadbeef,
                            committed_timestamp: 0,
                            committed_leader_epoch: 0,
                            committed_metadata: Some("metadata".to_string()),
                        }],
                    }],
                },
                [
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', 0x00, 0x00, 0x00, 0x01, 0x00,
                    0x05, b't', b'o', b'p', b'i', b'c', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x52,
                    0x21, 0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x08, b'm', b'e',
                    b't', b'a', b'd', b'a', b't', b'a',
                ]
                .as_ref(),
            ),
            (
                "no blocks",
                1,
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id_or_member_epoch: 0x1122,
                    member_id: "cons".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: None,
                    topics: vec![],
                },
                [
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', 0x00, 0x00, 0x11, 0x22, 0x00,
                    0x04, b'c', b'o', b'n', b's', 0x00, 0x00, 0x00, 0x00,
                ]
                .as_ref(),
            ),
            (
                "one block",
                1,
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id_or_member_epoch: 0x1122,
                    member_id: "cons".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: None,
                    topics: vec![OffsetCommitRequestTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitRequestTopicPartition {
                            partition_index: 0x5221,
                            committed_offset: 0xdeadbeef,
                            committed_timestamp: -1,
                            committed_leader_epoch: 0,
                            committed_metadata: Some("metadata".to_string()),
                        }],
                    }],
                },
                [
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', 0x00, 0x00, 0x11, 0x22, 0x00,
                    0x04, b'c', b'o', b'n', b's', 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, b't', b'o',
                    b'p', b'i', b'c', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x52, 0x21, 0x00, 0x00,
                    0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                    0xFF, 0x00, 0x08, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a',
                ]
                .as_ref(),
            ),
            (
                "no blocks",
                2,
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id_or_member_epoch: 0x1122,
                    member_id: "cons".to_string(),
                    retention_time_ms: 0x4433,
                    group_instance_id: None,
                    topics: vec![],
                },
                [
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', 0x00, 0x00, 0x11, 0x22, 0x00,
                    0x04, b'c', b'o', b'n', b's', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x33,
                    0x00, 0x00, 0x00, 0x00,
                ]
                .as_ref(),
            ),
            (
                "one block",
                2,
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id_or_member_epoch: 0x1122,
                    member_id: "cons".to_string(),
                    retention_time_ms: 0x4433,
                    group_instance_id: None,
                    topics: vec![OffsetCommitRequestTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitRequestTopicPartition {
                            partition_index: 0x5221,
                            committed_offset: 0xdeadbeef,
                            committed_timestamp: -1,
                            committed_leader_epoch: 0,
                            committed_metadata: Some("metadata".to_string()),
                        }],
                    }],
                },
                [
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', 0x00, 0x00, 0x11, 0x22, 0x00,
                    0x04, b'c', b'o', b'n', b's', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x33,
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x05, b't', b'o', b'p', b'i', b'c', 0x00, 0x00,
                    0x00, 0x01, 0x00, 0x00, 0x52, 0x21, 0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE,
                    0xEF, 0x00, 0x08, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a',
                ]
                .as_ref(),
            ),
            (
                "no blocks",
                3,
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id_or_member_epoch: 0x1122,
                    member_id: "cons".to_string(),
                    retention_time_ms: 0x4433,
                    group_instance_id: None,
                    topics: vec![],
                },
                [
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', 0x00, 0x00, 0x11, 0x22, 0x00,
                    0x04, b'c', b'o', b'n', b's', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x33,
                    0x00, 0x00, 0x00, 0x00,
                ]
                .as_ref(),
            ),
            (
                "one block",
                3,
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id_or_member_epoch: 0x1122,
                    member_id: "cons".to_string(),
                    retention_time_ms: 0x4433,
                    group_instance_id: None,
                    topics: vec![OffsetCommitRequestTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitRequestTopicPartition {
                            partition_index: 0x5221,
                            committed_offset: 0xdeadbeef,
                            committed_timestamp: -1,
                            committed_leader_epoch: 0,
                            committed_metadata: Some("metadata".to_string()),
                        }],
                    }],
                },
                [
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', 0x00, 0x00, 0x11, 0x22, 0x00,
                    0x04, b'c', b'o', b'n', b's', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x33,
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x05, b't', b'o', b'p', b'i', b'c', 0x00, 0x00,
                    0x00, 0x01, 0x00, 0x00, 0x52, 0x21, 0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE,
                    0xEF, 0x00, 0x08, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a',
                ]
                .as_ref(),
            ),
            (
                "no blocks",
                4,
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id_or_member_epoch: 0x1122,
                    member_id: "cons".to_string(),
                    retention_time_ms: 0x4433,
                    group_instance_id: None,
                    topics: vec![],
                },
                [
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', 0x00, 0x00, 0x11, 0x22, 0x00,
                    0x04, b'c', b'o', b'n', b's', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x33,
                    0x00, 0x00, 0x00, 0x00,
                ]
                .as_ref(),
            ),
            (
                "one block",
                4,
                OffsetCommitRequest {
                    group_id: "foobar".to_string(),
                    generation_id_or_member_epoch: 0x1122,
                    member_id: "cons".to_string(),
                    retention_time_ms: 0x4433,
                    group_instance_id: None,
                    topics: vec![OffsetCommitRequestTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitRequestTopicPartition {
                            partition_index: 0x5221,
                            committed_offset: 0xdeadbeef,
                            committed_timestamp: -1,
                            committed_leader_epoch: 0,
                            committed_metadata: Some("metadata".to_string()),
                        }],
                    }],
                },
                [
                    0x00, 0x06, b'f', b'o', b'o', b'b', b'a', b'r', 0x00, 0x00, 0x11, 0x22, 0x00,
                    0x04, b'c', b'o', b'n', b's', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x33,
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x05, b't', b'o', b'p', b'i', b'c', 0x00, 0x00,
                    0x00, 0x01, 0x00, 0x00, 0x52, 0x21, 0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE,
                    0xEF, 0x00, 0x08, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a',
                ]
                .as_ref(),
            ),
            (
                "one block",
                5,
                OffsetCommitRequest {
                    group_id: "foo".to_string(),
                    generation_id_or_member_epoch: 1,
                    member_id: "mid".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: None,
                    topics: vec![OffsetCommitRequestTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitRequestTopicPartition {
                            partition_index: 1,
                            committed_offset: 2,
                            committed_timestamp: 0,
                            committed_leader_epoch: 0,
                            committed_metadata: Some("meta".to_string()),
                        }],
                    }],
                },
                [
                    0, 3, b'f', b'o', b'o', // GroupId
                    0x00, 0x00, 0x00, 0x01, // GenerationId
                    0, 3, b'm', b'i', b'd', // MemberId
                    0, 0, 0, 1, // One Topic
                    0, 5, b't', b'o', b'p', b'i', b'c', // Name
                    0, 0, 0, 1, // One Partition
                    0, 0, 0, 1, // PartitionIndex
                    0, 0, 0, 0, 0, 0, 0, 2, // CommittedOffset
                    0, 4, b'm', b'e', b't', b'a', // CommittedMetadata
                ]
                .as_ref(),
            ),
            (
                "one block",
                6,
                OffsetCommitRequest {
                    group_id: "foo".to_string(),
                    generation_id_or_member_epoch: 1,
                    member_id: "mid".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: None,
                    topics: vec![OffsetCommitRequestTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitRequestTopicPartition {
                            partition_index: 1,
                            committed_offset: 2,
                            committed_timestamp: 0,
                            committed_leader_epoch: 3,
                            committed_metadata: Some("meta".to_string()),
                        }],
                    }],
                },
                [
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
                ]
                .as_ref(),
            ),
            (
                "one block",
                7,
                OffsetCommitRequest {
                    group_id: "foo".to_string(),
                    generation_id_or_member_epoch: 1,
                    member_id: "mid".to_string(),
                    retention_time_ms: 0,
                    group_instance_id: Some("gid".to_string()),
                    topics: vec![OffsetCommitRequestTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitRequestTopicPartition {
                            partition_index: 1,
                            committed_offset: 2,
                            committed_timestamp: 0,
                            committed_leader_epoch: 3,
                            committed_metadata: Some("meta".to_string()),
                        }],
                    }],
                },
                [
                    0, 3, b'f', b'o', b'o', // GroupId
                    0x00, 0x00, 0x00, 0x01, // GenerationId
                    0, 3, b'm', b'i', b'd', // MemberId
                    0, 3, b'g', b'i', b'd', // MemberId
                    0, 0, 0, 1, // One Topic
                    0, 5, b't', b'o', b'p', b'i', b'c', // Name
                    0, 0, 0, 1, // One Partition
                    0, 0, 0, 1, // PartitionIndex
                    0, 0, 0, 0, 0, 0, 0, 2, // CommittedOffset
                    0, 0, 0, 3, // CommittedEpoch
                    0, 4, b'm', b'e', b't', b'a', // CommittedMetadata
                ]
                .as_ref(),
            ),
        ] {
            let mut cursor = Cursor::new([0u8; 128]);
            req.write_versioned(&mut cursor, ApiVersion(version))
                .unwrap();
            let len = cursor.position() as usize;
            let got = &cursor.get_ref()[..len];
            assert_eq!(got, want, "{name}/{version}");
        }
    }

    #[test]
    fn response() {
        for (name, version, want, data) in [
            (
                "empty",
                0,
                OffsetCommitResponse {
                    throttle_time_ms: None,
                    topics: vec![],
                },
                [
                    0x00, 0x00, 0x00, 0x00, // Empty topic
                ]
                .as_ref(),
            ),
            (
                "two partition",
                0,
                OffsetCommitResponse {
                    throttle_time_ms: None,
                    topics: vec![OffsetCommitResponseTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitResponseTopicPartition {
                            partition_index: 3,
                            error_code: None,
                        }],
                    }],
                },
                [
                    0, 0, 0, 1, // Topic Len
                    0, 5, b't', b'o', b'p', b'i', b'c', // Name
                    0, 0, 0, 1, // Partition Len
                    0, 0, 0, 3, // PartitionIndex
                    0, 0, // ErrorCode
                ]
                .as_ref(),
            ),
            (
                "empty",
                3,
                OffsetCommitResponse {
                    throttle_time_ms: Some(100),
                    topics: vec![OffsetCommitResponseTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitResponseTopicPartition {
                            partition_index: 3,
                            error_code: None,
                        }],
                    }],
                },
                [
                    0, 0, 0, 100, // ThrottleTimeMs
                    0, 0, 0, 1, // Topic Len
                    0, 5, b't', b'o', b'p', b'i', b'c', // Name
                    0, 0, 0, 1, // Partition Len
                    0, 0, 0, 3, // PartitionIndex
                    0, 0, // ErrorCode
                ]
                .as_ref(),
            ),
            (
                "normal",
                0,
                OffsetCommitResponse {
                    throttle_time_ms: None,
                    topics: vec![OffsetCommitResponseTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitResponseTopicPartition {
                            partition_index: 3,
                            error_code: Some(Error::NotLeaderOrFollower),
                        }],
                    }],
                },
                [
                    0, 0, 0, 1, // Topic Len
                    0, 5, b't', b'o', b'p', b'i', b'c', // Name
                    0, 0, 0, 1, // Partition Len
                    0, 0, 0, 3, // PartitionIndex
                    0, 6, // ErrorCode
                ]
                .as_ref(),
            ),
            (
                "normal",
                3,
                OffsetCommitResponse {
                    throttle_time_ms: Some(123),
                    topics: vec![OffsetCommitResponseTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitResponseTopicPartition {
                            partition_index: 3,
                            error_code: Some(Error::NotLeaderOrFollower),
                        }],
                    }],
                },
                [
                    0, 0, 0, 123, // ThrottleTimeMs
                    0, 0, 0, 1, // Topic Len
                    0, 5, b't', b'o', b'p', b'i', b'c', // Name
                    0, 0, 0, 1, // Partition Len
                    0, 0, 0, 3, // PartitionIndex
                    0, 6, // ErrorCode
                ]
                .as_ref(),
            ),
            (
                "normal",
                4,
                OffsetCommitResponse {
                    throttle_time_ms: Some(123),
                    topics: vec![OffsetCommitResponseTopic {
                        name: "topic".to_string(),
                        partitions: vec![OffsetCommitResponseTopicPartition {
                            partition_index: 3,
                            error_code: Some(Error::NotLeaderOrFollower),
                        }],
                    }],
                },
                [
                    0, 0, 0, 123, // ThrottleTimeMs
                    0, 0, 0, 1, // Topic Len
                    0, 5, b't', b'o', b'p', b'i', b'c', // Name
                    0, 0, 0, 1, // Partition Len
                    0, 0, 0, 3, // PartitionIndex
                    0, 6, // ErrorCode
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let got =
                OffsetCommitResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(got, want, "{name}/{version}");
        }
    }
}
