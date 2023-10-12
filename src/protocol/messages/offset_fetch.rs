use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_versioned_array, write_versioned_array, ReadVersionedError, ReadVersionedType,
    RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::traits::{ReadType, WriteType};

pub struct OffsetFetchTopic {
    /// The topic name
    name: String,

    /// The partition indexes we would like to fetch offsets for.
    partition_indexes: Vec<i32>,
}

impl<W: Write> WriteVersionedType<W> for OffsetFetchTopic {
    fn write_versioned(
        &self,
        writer: &mut W,
        _version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        self.name.write(writer)?;
        self.partition_indexes.write(writer)?;

        Ok(())
    }
}

pub struct OffsetFetchRequest {
    /// The group to fetch offsets for.
    pub group_id: String,

    /// Each topic we would like to fetch offsets for, or null to
    /// fetch offsets for all topics.
    pub topics: Vec<OffsetFetchTopic>,
}

impl RequestBody for OffsetFetchRequest {
    type ResponseBody = OffsetFetchResponse;

    const API_KEY: ApiKey = ApiKey::OffsetFetch;

    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 5);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion::new(6);
}

impl<W: Write> WriteVersionedType<W> for OffsetFetchRequest {
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        self.group_id.write(writer)?;

        if self.topics.is_empty() && v >= 2 {
            write_versioned_array::<W, OffsetFetchTopic>(writer, version, None)?;
        } else {
            write_versioned_array(writer, version, Some(&self.topics))?;
        }

        Ok(())
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct OffsetFetchPartitionResult {
    /// The partition index.
    pub partition_index: i32,

    /// The committed message offset.
    pub committed_offset: i64,

    /// The leader epoch.
    ///
    /// Added in version 5.
    pub committed_leader_epoch: i32,

    /// The partition metadata
    pub metadata: Option<String>,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,
}

impl<R: Read> ReadVersionedType<R> for OffsetFetchPartitionResult {
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let partition_index = i32::read(reader)?;
        let committed_offset = i64::read(reader)?;
        let committed_leader_epoch = if v >= 5 { i32::read(reader)? } else { 0 };
        let metadata = ReadType::read(reader)?;
        let error_code = Error::new(i16::read(reader)?);

        Ok(Self {
            partition_index,
            committed_offset,
            committed_leader_epoch,
            metadata,
            error_code,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct OffsetFetchTopicResult {
    /// The topic name.
    pub name: String,

    /// The responses per partitions
    pub partitions: Vec<OffsetFetchPartitionResult>,
}

impl<R: Read> ReadVersionedType<R> for OffsetFetchTopicResult {
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let name = String::read(reader)?;
        let partitions = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(Self { name, partitions })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct OffsetFetchResponse {
    /// The duration in milliseconds for which the request was throttled due to
    /// a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 3.
    pub throttle_time_ms: Option<i32>,

    /// The response per topic.
    pub topics: Vec<OffsetFetchTopicResult>,

    /// The top-level error code, or 0 if there was no error.
    ///
    /// Added in version 2.
    pub error_code: Option<Error>,
}

impl<R: Read> ReadVersionedType<R> for OffsetFetchResponse {
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let throttle_time_ms = if v < 3 {
            None
        } else {
            Some(i32::read(reader)?)
        };

        let topics = read_versioned_array(reader, version)?.unwrap_or_default();
        let error_code = if v >= 2 {
            Error::new(i16::read(reader)?)
        } else {
            None
        };

        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::test_utils::{assert_read_versioned, assert_write_versioned};

    #[test]
    fn request() {
        let no_group_no_partition = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

        let no_partition = vec![0x00, 0x04, b'b', b'l', b'a', b'h', 0x00, 0x00, 0x00, 0x00];

        let one_partition = vec![
            0x00, 0x04, b'b', b'l', b'a', b'h', 0x00, 0x00, 0x00, 0x01, 0x00, 0x0D, b't', b'o',
            b'p', b'i', b'c', b'T', b'h', b'e', b'F', b'i', b'r', b's', b't', 0x00, 0x00, 0x00,
            0x01, 0x4F, 0x4F, 0x4F, 0x4F,
        ];

        for i in 0..2 {
            let mut req = OffsetFetchRequest {
                group_id: "".to_string(),
                topics: vec![],
            };
            assert_write_versioned!(req, i, no_group_no_partition);
            req.group_id = "blah".to_string();
            assert_write_versioned!(req, i, no_partition);
            req.topics.push(OffsetFetchTopic {
                name: "topicTheFirst".to_string(),
                partition_indexes: vec![0x4F4F4F4F],
            });
            assert_write_versioned!(req, i, one_partition);
        }

        // all topics
        let all_topics = vec![0x00, 0x04, b'b', b'l', b'a', b'h', 0xff, 0xff, 0xff, 0xff];
        for i in 2..5 {
            let req = OffsetFetchRequest {
                group_id: "blah".to_string(),
                topics: vec![],
            };

            assert_write_versioned!(req, i, all_topics);
        }
    }

    #[test]
    fn empty_response() {
        let empty_offsets: Vec<u8> = vec![0x00, 0x00, 0x00, 0x00];
        let want = OffsetFetchResponse {
            throttle_time_ms: None,
            topics: vec![],
            error_code: None,
        };

        assert_read_versioned!(empty_offsets.clone(), 0, want);
        assert_read_versioned!(empty_offsets, 1, want);
    }
}
