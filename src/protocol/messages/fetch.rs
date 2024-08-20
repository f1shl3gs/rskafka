use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error as ApiError,
    messages::{read_versioned_array, write_versioned_array, IsolationLevel},
    primitives::Records,
    traits::{ReadType, WriteType},
};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};

#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub struct FetchRequestPartition {
    /// The partition index.
    pub partition: i32,

    /// The message offset.
    pub fetch_offset: i64,

    /// The maximum bytes to fetch from this partition.
    ///
    /// See KIP-74 for cases where this limit may not be honored.
    pub partition_max_bytes: i32,
}

impl<W> WriteVersionedType<W> for FetchRequestPartition
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        self.partition.write(writer)?;
        self.fetch_offset.write(writer)?;
        self.partition_max_bytes.write(writer)?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct FetchRequestTopic {
    /// The name of the topic to fetch.
    pub topic: String,

    /// The partitions to fetch.
    pub partitions: Vec<FetchRequestPartition>,
}

impl<W> WriteVersionedType<W> for FetchRequestTopic
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        self.topic.write(writer)?;
        write_versioned_array(writer, version, Some(&self.partitions))?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct FetchRequest {
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    pub replica_id: i32,

    /// The maximum time in milliseconds to wait for the response.
    pub max_wait_ms: i32,

    /// The minimum bytes to accumulate in the response.
    pub min_bytes: i32,

    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    ///
    /// Defaults to "no limit / max".
    ///
    /// Added in version 3.
    pub max_bytes: Option<i32>,

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
    /// Added in version 4.
    ///
    /// [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
    pub isolation_level: Option<IsolationLevel>,

    /// The topics to fetch.
    pub topics: Vec<FetchRequestTopic>,
}

impl<W> WriteVersionedType<W> for FetchRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        self.replica_id.write(writer)?;
        self.max_wait_ms.write(writer)?;
        self.min_bytes.write(writer)?;

        if v >= 3 {
            // defaults to "no limit / max".
            self.max_bytes.unwrap_or(i32::MAX).write(writer)?;
        }

        if v >= 4 {
            // The default is `READ_UNCOMMITTED`.
            let level: i8 = self.isolation_level.unwrap_or_default().into();
            level.write(writer)?;
        }

        write_versioned_array(writer, version, Some(&self.topics))?;

        Ok(())
    }
}

impl RequestBody for FetchRequest {
    type ResponseBody = FetchResponse;

    const API_KEY: ApiKey = ApiKey::Fetch;

    /// That's enough for now.
    ///
    /// Note that we do not support fetch request prior to version 4, since this is the version when message version 2
    /// was introduced ([KIP-98]).
    ///
    /// [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(4, 4);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(12);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
#[allow(missing_copy_implementations)]
pub struct FetchResponseAbortedTransaction {
    /// The producer id associated with the aborted transaction.
    pub producer_id: i64,

    /// The first offset in the aborted transaction.
    pub first_offset: i64,
}

impl<R> ReadVersionedType<R> for FetchResponseAbortedTransaction
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(4 <= v && v <= 4);

        Ok(Self {
            producer_id: i64::read(reader)?,
            first_offset: i64::read(reader)?,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct FetchResponsePartition {
    /// The partition index.
    pub partition_index: i32,

    /// The error code, or 0 if there was no fetch error.
    pub error_code: Option<ApiError>,

    /// The current high water mark.
    pub high_watermark: i64,

    /// The last stable offset (or LSO) of the partition.
    ///
    /// This is the last offset such that the state of all transactional records prior to this offset have been decided
    /// (`ABORTED` or `COMMITTED`).
    ///
    /// Added in version 4.
    pub last_stable_offset: Option<i64>,

    /// The aborted transactions.
    ///
    /// Added in version 4.
    pub aborted_transactions: Vec<FetchResponseAbortedTransaction>,

    /// The record data.
    pub records: Records,
}

impl<R> ReadVersionedType<R> for FetchResponsePartition
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        Ok(Self {
            partition_index: i32::read(reader)?,
            error_code: ApiError::new(i16::read(reader)?),
            high_watermark: i64::read(reader)?,
            last_stable_offset: (v >= 4).then(|| i64::read(reader)).transpose()?,
            aborted_transactions: (v >= 4)
                .then(|| read_versioned_array(reader, version))
                .transpose()?
                .flatten()
                .unwrap_or_default(),
            records: Records::read(reader)?,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct FetchResponseTopic {
    /// The topic name.
    pub topic: String,

    /// The topic partitions.
    pub partitions: Vec<FetchResponsePartition>,
}

impl<R> ReadVersionedType<R> for FetchResponseTopic
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        Ok(Self {
            topic: String::read(reader)?,
            partitions: read_versioned_array(reader, version)?.unwrap_or_default(),
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct FetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1.
    pub throttle_time_ms: Option<i32>,

    /// The response topics.
    pub responses: Vec<FetchResponseTopic>,
}

impl<R> ReadVersionedType<R> for FetchResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        Ok(Self {
            throttle_time_ms: (v >= 1).then(|| i32::read(reader)).transpose()?,
            responses: read_versioned_array(reader, version)?.unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::record::{
        ControlBatchOrRecords, ControlBatchRecord, RecordBatch, RecordBatchCompression,
        RecordBatchTimestampType,
    };
    use std::io::Cursor;

    #[test]
    fn request() {
        for (name, version, req, want) in [(
            "one block",
            4,
            FetchRequest {
                replica_id: -1,
                max_wait_ms: 0,
                min_bytes: 0,
                max_bytes: Some(0xff),
                isolation_level: Some(IsolationLevel::ReadCommitted),
                topics: vec![FetchRequestTopic {
                    topic: "topic".to_string(),
                    partitions: vec![FetchRequestPartition {
                        partition: 0x12,
                        fetch_offset: 0x34,
                        partition_max_bytes: 0x56,
                    }],
                }],
            },
            [
                0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0xFF, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, b't', b'o', b'p', b'i', b'c',
                0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x34, 0x00, 0x00, 0x00, 0x56,
            ]
            .as_ref(),
        )] {
            let mut cursor = Cursor::new([0u8; 128]);
            req.write_versioned(&mut cursor, ApiVersion(version))
                .unwrap();
            let len = cursor.position() as usize;
            let got = &cursor.get_ref()[..len];
            assert_eq!(got, want, "{name}/{version}");
        }
    }

    // TODO: fix this test
    #[ignore]
    #[test]
    fn response() {
        for (name, version, want, data) in [(
            "one message",
            4,
            FetchResponse {
                throttle_time_ms: None,
                responses: vec![FetchResponseTopic {
                    topic: "topic".to_string(),
                    partitions: vec![FetchResponsePartition {
                        partition_index: 5,
                        error_code: None,
                        high_watermark: 0x10101010,
                        last_stable_offset: None,
                        aborted_transactions: vec![],
                        records: Records(vec![RecordBatch {
                            base_offset: 0,
                            partition_leader_epoch: 0,
                            last_offset_delta: 0,
                            first_timestamp: 0,
                            max_timestamp: 0,
                            producer_id: 0,
                            producer_epoch: 0,
                            base_sequence: 0,
                            records: ControlBatchOrRecords::ControlBatch(
                                ControlBatchRecord::Commit,
                            ),
                            compression: RecordBatchCompression::NoCompression,
                            is_transactional: false,
                            timestamp_type: RecordBatchTimestampType::CreateTime,
                        }]),
                    }],
                }],
            },
            [
                0x00, 0x00, 0x00, 0x00, // ThrottleTime
                0x00, 0x00, 0x00, 0x01, // Number of Topics
                0x00, 0x05, b't', b'o', b'p', b'i', b'c', // Topic
                0x00, 0x00, 0x00, 0x01, // Number of Partitions
                0x00, 0x00, 0x00, 0x05, // Partition
                0x00, 0x01, // Error
                0x00, 0x00, 0x00, 0x00, 0x10, 0x10, 0x10, 0x10, // High Watermark Offset
                0x00, 0x00, 0x00, 0x00, 0x10, 0x10, 0x10, 0x10, // Last Stable Offset
                0x00, 0x00, 0x00, 0x00, // Number of Aborted Transactions
                0x00, 0x00, 0x00, 0x1C, // messageSet
                0x00, 0x00, 0x00, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
                // message
                0x23, 0x96, 0x4a, 0xf7, // CRC
                0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x02, 0x00, 0xEE,
            ]
            .as_ref(),
        )] {
            let mut reader = Cursor::new(data);
            let got = FetchResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(got, want, "{name}/{version}");
        }
    }
}
