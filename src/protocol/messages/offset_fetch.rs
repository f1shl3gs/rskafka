use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_compact_versioned_array, read_versioned_array, write_compact_versioned_array,
    write_versioned_array, ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError,
    WriteVersionedType,
};
use crate::protocol::primitives::{TaggedFields, UnsignedVarint};
use crate::protocol::traits::{ReadCompactType, ReadType, WriteCompactType, WriteError, WriteType};

#[derive(Debug)]
pub struct OffsetFetchRequestTopic {
    /// The topic name.
    ///
    /// COMPACT_STRING > 5
    pub name: String,

    /// The partition indexes we would like to fetch offsets for.
    pub partition_indexes: Vec<i32>,

    /// The tagged fields
    ///
    /// Added in version 6
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for OffsetFetchRequestTopic
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

        if v > 5 {
            self.name.write_compact(writer)?;

            let len =
                u64::try_from(self.partition_indexes.len() + 1).map_err(WriteError::Overflow)?;
            UnsignedVarint(len).write(writer)?;
            for index in &self.partition_indexes {
                index.write(writer)?;
            }

            self.tagged_fields.write(writer)?;
        } else {
            self.name.write(writer)?;

            let len = i32::try_from(self.partition_indexes.len()).map_err(WriteError::Overflow)?;
            len.write(writer)?;
            for index in &self.partition_indexes {
                index.write(writer)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct OffsetFetchRequest {
    /// The group to fetch offsets for.
    pub group_id: String,

    /// Each topic we would like to fetch offsets for, or null to fetch
    /// offsets for all topics.
    pub topics: Vec<OffsetFetchRequestTopic>,

    /// Whether broker should hold on returning unstable offsets but set
    /// a retriable error code for the partitions.
    ///
    /// Added in version 7
    pub require_stable: bool,

    /// The tagged fields
    ///
    /// Added in version 6
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for OffsetFetchRequest
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

        if v > 5 {
            self.group_id.write_compact(writer)?;
            write_compact_versioned_array(writer, version, Some(&self.topics))?;
        } else {
            self.group_id.write(writer)?;
            write_versioned_array(writer, version, Some(&self.topics))?;
        }

        if v > 6 {
            self.require_stable.write(writer)?;
        }

        if v > 5 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

impl RequestBody for OffsetFetchRequest {
    type ResponseBody = OffsetFetchResponse;
    const API_KEY: ApiKey = ApiKey::OffsetFetch;
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 7);
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(6);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct OffsetFetchResponseTopicPartition {
    /// The partition index.
    pub partition_index: i32,

    /// The committed message offset.
    pub committed_offset: i64,

    /// The leader epoch.
    ///
    /// Added in version 5
    pub committed_leader_epoch: i32,

    /// The partition metadata.
    ///
    /// NULLABLE_STRING >= 0
    /// COMPACT_NULLABLE_STRING >= 6
    pub metadata: Option<String>,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// The tagged fields
    ///
    /// Added in version 6
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for OffsetFetchResponseTopicPartition
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v < 8);

        let partition_index = i32::read(reader)?;
        let committed_offset = i64::read(reader)?;
        let committed_leader_epoch = if v > 4 { i32::read(reader)? } else { 0 };
        let metadata = if v > 5 {
            Option::<String>::read_compact(reader)?
        } else {
            Option::<String>::read(reader)?
        };
        let error_code = Error::new(i16::read(reader)?);
        let tagged_fields = if v > 5 {
            Some(TaggedFields::read(reader)?)
        } else {
            None
        };

        Ok(OffsetFetchResponseTopicPartition {
            partition_index,
            committed_offset,
            committed_leader_epoch,
            metadata,
            error_code,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct OffsetFetchResponseTopic {
    /// The topic name.
    ///
    /// COMPACT_STRING > 5
    pub name: String,

    /// The responses per partition
    pub partitions: Vec<OffsetFetchResponseTopicPartition>,

    /// The tagged_fields
    ///
    /// Added in version 6
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for OffsetFetchResponseTopic
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v < 8);

        let name = if v > 5 {
            String::read_compact(reader)?
        } else {
            String::read(reader)?
        };
        let partitions = if v > 5 {
            read_compact_versioned_array(reader, version)?.unwrap_or_default()
        } else {
            read_versioned_array(reader, version)?.unwrap_or_default()
        };

        let tagged_fields = if v > 5 {
            Some(TaggedFields::read(reader)?)
        } else {
            None
        };

        Ok(OffsetFetchResponseTopic {
            name,
            partitions,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct OffsetFetchResponse {
    /// The duration in milliseconds for which the request was throttled due to
    /// a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 3
    pub throttle_time_ms: Option<i32>,

    /// The responses per topic.
    pub topics: Vec<OffsetFetchResponseTopic>,

    /// The top-level error code, or 0 if there was no error.
    ///
    /// Added in version 2
    pub error_code: Option<Error>,

    /// The tagged fields
    ///
    /// Added in version 6
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for OffsetFetchResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v < 8);

        let throttle_time_ms = if v > 2 {
            Some(i32::read(reader)?)
        } else {
            None
        };
        let topics = if v > 5 {
            read_compact_versioned_array(reader, version)?.unwrap_or_default()
        } else {
            read_versioned_array(reader, version)?.unwrap_or_default()
        };
        let error_code = if v > 1 {
            Error::new(i16::read(reader)?)
        } else {
            None
        };
        let tagged_fields = if v > 5 {
            Some(TaggedFields::read(reader)?)
        } else {
            None
        };

        Ok(OffsetFetchResponse {
            throttle_time_ms,
            topics,
            error_code,
            tagged_fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    const NO_GROUP_NO_PARTITIONS: [u8; 6] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

    const NO_PARTITIONS: [u8; 10] = [0x00, 0x04, b'b', b'l', b'a', b'h', 0x00, 0x00, 0x00, 0x00];

    const NO_PARTITIONS_V6: [u8; 7] = [0x05, b'b', b'l', b'a', b'h', 0x01, 0x00];

    const NO_PARTITIONS_V7: [u8; 8] = [0x05, b'b', b'l', b'a', b'h', 0x01, 0x01, 0x00];

    const ONE_PARTITION: [u8; 33] = [
        0x00, 0x04, b'b', b'l', b'a', b'h', 0x00, 0x00, 0x00, 0x01, 0x00, 0x0D, b't', b'o', b'p',
        b'i', b'c', b'T', b'h', b'e', b'F', b'i', b'r', b's', b't', 0x00, 0x00, 0x00, 0x01, 0x4F,
        0x4F, 0x4F, 0x4F,
    ];

    const ONE_PARTITION_V6: [u8; 27] = [
        0x05, b'b', b'l', b'a', b'h', 0x02, 0x0E, b't', b'o', b'p', b'i', b'c', b'T', b'h', b'e',
        b'F', b'i', b'r', b's', b't', 0x02, 0x4F, 0x4F, 0x4F, 0x4F, 0x00, 0x00,
    ];

    const ONE_PARTITION_V7: [u8; 28] = [
        0x05, b'b', b'l', b'a', b'h', 0x02, 0x0E, b't', b'o', b'p', b'i', b'c', b'T', b'h', b'e',
        b'F', b'i', b'r', b's', b't', 0x02, 0x4F, 0x4F, 0x4F, 0x4F, 0x00, 0x00, 0x00,
    ];

    #[test]
    fn request() {
        for (name, version, req, want) in [
            (
                "no group, no partitions",
                0,
                OffsetFetchRequest {
                    group_id: "".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_GROUP_NO_PARTITIONS.as_ref(),
            ),
            (
                "no partitions",
                0,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_PARTITIONS.as_ref(),
            ),
            (
                "one partition",
                0,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![OffsetFetchRequestTopic {
                        name: "topicTheFirst".to_string(),
                        partition_indexes: vec![0x4f4f4f4f],
                        tagged_fields: None,
                    }],
                    require_stable: false,
                    tagged_fields: None,
                },
                ONE_PARTITION.as_ref(),
            ),
            (
                "no group, no partition",
                1,
                OffsetFetchRequest {
                    group_id: "".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_GROUP_NO_PARTITIONS.as_ref(),
            ),
            (
                "no partitions",
                1,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_PARTITIONS.as_ref(),
            ),
            (
                "one partition",
                1,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![OffsetFetchRequestTopic {
                        name: "topicTheFirst".to_string(),
                        partition_indexes: vec![0x4f4f4f4f],
                        tagged_fields: None,
                    }],
                    require_stable: false,
                    tagged_fields: None,
                },
                ONE_PARTITION.as_ref(),
            ),
            (
                "no group, no partition",
                2,
                OffsetFetchRequest {
                    group_id: "".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_GROUP_NO_PARTITIONS.as_ref(),
            ),
            (
                "no partitions",
                2,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_PARTITIONS.as_ref(),
            ),
            (
                "one partition",
                2,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![OffsetFetchRequestTopic {
                        name: "topicTheFirst".to_string(),
                        partition_indexes: vec![0x4f4f4f4f],
                        tagged_fields: None,
                    }],
                    require_stable: false,
                    tagged_fields: None,
                },
                ONE_PARTITION.as_ref(),
            ),
            (
                "no group, no partition",
                3,
                OffsetFetchRequest {
                    group_id: "".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_GROUP_NO_PARTITIONS.as_ref(),
            ),
            (
                "no partitions",
                3,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_PARTITIONS.as_ref(),
            ),
            (
                "one partition",
                3,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![OffsetFetchRequestTopic {
                        name: "topicTheFirst".to_string(),
                        partition_indexes: vec![0x4f4f4f4f],
                        tagged_fields: None,
                    }],
                    require_stable: false,
                    tagged_fields: None,
                },
                ONE_PARTITION.as_ref(),
            ),
            (
                "no group, no partition",
                4,
                OffsetFetchRequest {
                    group_id: "".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_GROUP_NO_PARTITIONS.as_ref(),
            ),
            (
                "no partitions",
                4,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_PARTITIONS.as_ref(),
            ),
            (
                "one partition",
                4,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![OffsetFetchRequestTopic {
                        name: "topicTheFirst".to_string(),
                        partition_indexes: vec![0x4f4f4f4f],
                        tagged_fields: None,
                    }],
                    require_stable: false,
                    tagged_fields: None,
                },
                ONE_PARTITION.as_ref(),
            ),
            (
                "no group, no partition",
                5,
                OffsetFetchRequest {
                    group_id: "".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_GROUP_NO_PARTITIONS.as_ref(),
            ),
            (
                "one partition",
                5,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![OffsetFetchRequestTopic {
                        name: "topicTheFirst".to_string(),
                        partition_indexes: vec![0x4f4f4f4f],
                        tagged_fields: None,
                    }],
                    require_stable: false,
                    tagged_fields: None,
                },
                ONE_PARTITION.as_ref(),
            ),
            (
                "no partitions",
                5,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_PARTITIONS.as_ref(),
            ),
            (
                "no partitions",
                6,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![],
                    require_stable: false,
                    tagged_fields: None,
                },
                NO_PARTITIONS_V6.as_ref(),
            ),
            (
                "one partition",
                6,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![OffsetFetchRequestTopic {
                        name: "topicTheFirst".to_string(),
                        partition_indexes: vec![0x4f4f4f4f],
                        tagged_fields: None,
                    }],
                    require_stable: false,
                    tagged_fields: None,
                },
                ONE_PARTITION_V6.as_ref(),
            ),
            (
                "no partitions",
                7,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![],
                    require_stable: true,
                    tagged_fields: None,
                },
                NO_PARTITIONS_V7.as_ref(),
            ),
            (
                "one partition",
                7,
                OffsetFetchRequest {
                    group_id: "blah".to_string(),
                    topics: vec![OffsetFetchRequestTopic {
                        name: "topicTheFirst".to_string(),
                        partition_indexes: vec![0x4f4f4f4f],
                        tagged_fields: None,
                    }],
                    require_stable: false,
                    tagged_fields: None,
                },
                ONE_PARTITION_V7.as_ref(),
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
                OffsetFetchResponse {
                    throttle_time_ms: None,
                    topics: vec![],
                    error_code: None,
                    tagged_fields: None,
                },
                [0x00, 0x00, 0x00, 0x00].as_ref(),
            ),
            (
                "empty",
                1,
                OffsetFetchResponse {
                    throttle_time_ms: None,
                    topics: vec![],
                    error_code: None,
                    tagged_fields: None,
                },
                [0x00, 0x00, 0x00, 0x00].as_ref(),
            ),
            (
                "error",
                2,
                OffsetFetchResponse {
                    throttle_time_ms: None,
                    topics: vec![],
                    error_code: Some(Error::InvalidRequest),
                    tagged_fields: None,
                },
                [0x00, 0x00, 0x00, 0x00, 0x00, 0x2A].as_ref(),
            ),
            (
                "error",
                3,
                OffsetFetchResponse {
                    throttle_time_ms: Some(9),
                    topics: vec![],
                    error_code: Some(Error::InvalidRequest),
                    tagged_fields: None,
                },
                [0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A].as_ref(),
            ),
            (
                "error",
                4,
                OffsetFetchResponse {
                    throttle_time_ms: Some(9),
                    topics: vec![],
                    error_code: Some(Error::InvalidRequest),
                    tagged_fields: None,
                },
                [0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A].as_ref(),
            ),
            (
                "error",
                5,
                OffsetFetchResponse {
                    throttle_time_ms: Some(9),
                    topics: vec![],
                    error_code: Some(Error::InvalidRequest),
                    tagged_fields: None,
                },
                [0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A].as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let got =
                OffsetFetchResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(got, want, "{name}/{version}");
        }
    }
}
