//! `ListOffsets` request and response.
//!
//! # References
//! - [KIP-79](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65868090)
//! - [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error as ApiError,
    messages::{read_versioned_array, write_versioned_array, IsolationLevel},
    traits::{ReadType, WriteType},
};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};

#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub struct ListOffsetsRequestPartition {
    /// The partition index.
    pub partition_index: i32,

    /// The current timestamp.
    ///
    /// Depending on the version this will return:
    ///
    /// - **version 0:** `max_num_offsets` offsets that are smaller/equal than this timestamp.
    /// - **version 1 and later:** return timestamp and offset of the first/message greater/equal than this timestamp
    ///
    /// Per [KIP-79] this can have the following special values:
    ///
    /// - `-1`: latest offset
    /// - `-2`: earlist offset
    ///
    /// [KIP-79]: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65868090
    pub timestamp: i64,

    /// The maximum number of offsets to report.
    ///
    /// Defaults to 1.
    ///
    /// Removed in version 1.
    pub max_num_offsets: Option<i32>,
}

impl<W> WriteVersionedType<W> for ListOffsetsRequestPartition
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 3);

        self.partition_index.write(writer)?;
        self.timestamp.write(writer)?;

        if v == 0 {
            // Only fetch 1 offset by default.
            self.max_num_offsets.unwrap_or(1).write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ListOffsetsRequestTopic {
    /// The topic name.
    pub name: String,

    /// Each partition in the request.
    ///
    /// Note: A partition may only appear once within the request.
    pub partitions: Vec<ListOffsetsRequestPartition>,
}

impl<W> WriteVersionedType<W> for ListOffsetsRequestTopic
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 3);

        self.name.write(writer)?;
        write_versioned_array(writer, version, Some(&self.partitions))?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ListOffsetsRequest {
    /// The broker ID of the requestor, or -1 if this request is being made by a normal consumer.
    pub replica_id: i32,

    /// This setting controls the visibility of transactional records.
    ///
    /// Using `READ_UNCOMMITTED` (`isolation_level = 0`) makes all records visible. With `READ_COMMITTED`
    /// (`isolation_level = 1`), non-transactional and `COMMITTED` transactional records are visible. To be more
    /// concrete, `READ_COMMITTED` returns all data from offsets smaller than the current LSO (last stable offset), and
    /// enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard
    /// `ABORTED` transactional records.
    ///
    /// As per [KIP-98] the default is `READ_UNCOMMITTED`.
    ///
    /// Added in version 2.
    ///
    /// [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
    pub isolation_level: Option<IsolationLevel>,

    /// Each topic in the request.
    ///
    /// Note: A topic may only appear once within the request.
    pub topics: Vec<ListOffsetsRequestTopic>,
}

impl<W> WriteVersionedType<W> for ListOffsetsRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 3);

        self.replica_id.write(writer)?;

        if v >= 2 {
            // The default is `READ_UNCOMMITTED`.
            let level: i8 = self.isolation_level.unwrap_or_default().into();
            level.write(writer)?;
        }

        write_versioned_array(writer, version, Some(&self.topics))?;

        Ok(())
    }
}

impl RequestBody for ListOffsetsRequest {
    type ResponseBody = ListOffsetsResponse;

    const API_KEY: ApiKey = ApiKey::ListOffsets;

    /// At the time of writing this is the same subset supported by rdkafka
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 3);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(6);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ListOffsetsResponsePartition {
    /// The partition index.
    pub partition_index: i32,

    /// The partition error code, or 0 if there was no error.
    pub error_code: Option<ApiError>,

    /// The result offsets.
    ///
    /// Removed in version 1.
    pub old_style_offsets: Option<Vec<i64>>,

    /// The timestamp associated with the returned offset.
    ///
    /// Added in version 1.
    pub timestamp: Option<i64>,

    /// The returned offset.
    ///
    /// Added in version 1.
    pub offset: Option<i64>,
}

impl<R> ReadVersionedType<R> for ListOffsetsResponsePartition
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 3);

        Ok(Self {
            partition_index: i32::read(reader)?,
            error_code: ApiError::new(i16::read(reader)?),
            old_style_offsets: (v < 1).then(|| Vec::<i64>::read(reader)).transpose()?,
            timestamp: (v >= 1).then(|| i64::read(reader)).transpose()?,
            offset: (v >= 1).then(|| i64::read(reader)).transpose()?,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ListOffsetsResponseTopic {
    /// The topic name.
    pub name: String,

    /// Each partition in the response.
    pub partitions: Vec<ListOffsetsResponsePartition>,
}

impl<R> ReadVersionedType<R> for ListOffsetsResponseTopic
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 3);

        Ok(Self {
            name: String::read(reader)?,
            partitions: read_versioned_array(reader, version)?.unwrap_or_default(),
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ListOffsetsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 2.
    pub throttle_time_ms: Option<i32>,

    /// Each topic in the response.
    pub topics: Vec<ListOffsetsResponseTopic>,
}

impl<R> ReadVersionedType<R> for ListOffsetsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 3);

        Ok(Self {
            throttle_time_ms: (v >= 2).then(|| i32::read(reader)).transpose()?,
            topics: read_versioned_array(reader, version)?.unwrap_or_default(),
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
                ListOffsetsRequest {
                    replica_id: -1, // `-1` is the default replica ID
                    isolation_level: None,
                    topics: vec![],
                },
                [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00].as_ref(),
            ),
            (
                "one block",
                0,
                ListOffsetsRequest {
                    replica_id: -1,
                    isolation_level: None,
                    topics: vec![ListOffsetsRequestTopic {
                        name: "foo".to_string(),
                        partitions: vec![ListOffsetsRequestPartition {
                            partition_index: 4,
                            timestamp: 1,
                            max_num_offsets: Some(2),
                        }],
                    }],
                },
                [
                    0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, b'f', b'o', b'o',
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
                ]
                .as_ref(),
            ),
            (
                "no blocks",
                1,
                ListOffsetsRequest {
                    replica_id: -1,
                    isolation_level: None,
                    topics: vec![],
                },
                [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00].as_ref(),
            ),
            (
                "one block",
                1,
                ListOffsetsRequest {
                    replica_id: -1,
                    isolation_level: None,
                    topics: vec![ListOffsetsRequestTopic {
                        name: "bar".to_string(),
                        partitions: vec![ListOffsetsRequestPartition {
                            partition_index: 4,
                            timestamp: 1,
                            max_num_offsets: Some(2),
                        }],
                    }],
                },
                [
                    0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, b'b', b'a', b'r',
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x01,
                ]
                .as_ref(),
            ),
            (
                "no blocks",
                2,
                ListOffsetsRequest {
                    replica_id: -1,
                    isolation_level: None,
                    topics: vec![],
                },
                [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00].as_ref(),
            ),
            (
                "one block",
                2,
                ListOffsetsRequest {
                    replica_id: -1,
                    isolation_level: Some(IsolationLevel::ReadCommitted),
                    topics: vec![ListOffsetsRequestTopic {
                        name: "bar".to_string(),
                        partitions: vec![ListOffsetsRequestPartition {
                            partition_index: 4,
                            timestamp: 1,
                            max_num_offsets: Some(2),
                        }],
                    }],
                },
                [
                    0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, b'b', b'a',
                    b'r', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x01,
                ]
                .as_ref(),
            ),
            (
                "with replica id",
                0,
                ListOffsetsRequest {
                    replica_id: 42,
                    isolation_level: None,
                    topics: vec![],
                },
                [0x00, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x00].as_ref(),
            ),
        ] {
            let mut buf = Cursor::new([0u8; 128]);
            req.write_versioned(&mut buf, ApiVersion(version)).unwrap();
            let len = buf.position() as usize;
            let got = &buf.get_ref().as_slice()[..len];
            assert_eq!(got, want, "{name}/{version}");
        }
    }

    #[test]
    fn response() {
        for (name, version, want, data) in [
            (
                "empty",
                0,
                ListOffsetsResponse {
                    throttle_time_ms: None,
                    topics: vec![],
                },
                [0x00, 0x00, 0x00, 0x00].as_ref(),
            ),
            (
                "normal",
                0,
                ListOffsetsResponse {
                    throttle_time_ms: None,
                    topics: vec![
                        ListOffsetsResponseTopic {
                            name: "a".to_string(),
                            partitions: vec![],
                        },
                        ListOffsetsResponseTopic {
                            name: "z".to_string(),
                            partitions: vec![ListOffsetsResponsePartition {
                                partition_index: 2,
                                error_code: None,
                                old_style_offsets: Some(vec![5, 6]),
                                timestamp: None,
                                offset: None,
                            }],
                        },
                    ],
                },
                [
                    0x00, 0x00, 0x00, 0x02, 0x00, 0x01, b'a', 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                    b'z', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x06,
                ]
                .as_ref(),
            ),
            (
                "normal",
                1,
                ListOffsetsResponse {
                    throttle_time_ms: None,
                    topics: vec![
                        ListOffsetsResponseTopic {
                            name: "a".to_string(),
                            partitions: vec![],
                        },
                        ListOffsetsResponseTopic {
                            name: "z".to_string(),
                            partitions: vec![ListOffsetsResponsePartition {
                                partition_index: 2,
                                error_code: None,
                                old_style_offsets: None,
                                timestamp: Some(1477920049286),
                                offset: Some(6),
                            }],
                        },
                    ],
                },
                [
                    0x00, 0x00, 0x00, 0x02, 0x00, 0x01, b'a', 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                    b'z', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
                    0x01, 0x58, 0x1A, 0xE6, 0x48, 0x86, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x06,
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let resp =
                ListOffsetsResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(resp, want, "{name}/{version}");
        }
    }
}
