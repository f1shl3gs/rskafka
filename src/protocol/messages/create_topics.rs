use std::io::{Read, Write};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
    read_compact_versioned_array, read_versioned_array, write_compact_versioned_array,
    write_versioned_array,
};
use crate::protocol::api_version::ApiVersionRange;
use crate::protocol::error::Error;
use crate::protocol::traits::{ReadCompactType, WriteCompactType};
use crate::protocol::{
    api_key::ApiKey,
    api_version::ApiVersion,
    primitives::*,
    traits::{ReadType, WriteType},
};

#[derive(Debug)]
pub struct CreateTopicsRequest {
    /// The topics to create
    pub topics: Vec<CreateTopicRequest>,

    /// How long to wait in milliseconds before timing out the request.
    pub timeout_ms: i32,

    /// If true, check that the topics can be created as specified, but don't create anything.
    ///
    /// Added in version 1
    pub validate_only: Option<bool>,

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
}

impl RequestBody for CreateTopicsRequest {
    type ResponseBody = CreateTopicsResponse;

    const API_KEY: ApiKey = ApiKey::CreateTopics;

    /// Enough for now.
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 5);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(5);
}

impl<W> WriteVersionedType<W> for CreateTopicsRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        if self.validate_only.is_some() && v < 1 {
            return Err(WriteVersionedError::FieldNotAvailable {
                version,
                field: "validate_only".to_string(),
            });
        }

        if v >= 5 {
            write_compact_versioned_array(writer, version, Some(self.topics.as_slice()))?;
        } else {
            write_versioned_array(writer, version, Some(self.topics.as_slice()))?;
        }
        self.timeout_ms.write(writer)?;

        if v >= 1 {
            match self.validate_only {
                Some(b) => b.write(writer)?,
                None => false.write(writer)?,
            }
        }

        if v >= 5 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct CreateTopicRequest {
    /// The topic name
    pub name: String,

    /// The number of partitions to create in the topic, or -1 if we are either
    /// specifying a manual partition assignment or using the default partitions.
    ///
    /// Note: default partition count requires broker version >= 2.4.0 (KIP-464)
    pub num_partitions: i32,

    /// The number of replicas to create for each partition in the topic, or -1 if we are either
    /// specifying a manual partition assignment or using the default replication factor.
    ///
    /// Note: default replication factor requires broker version >= 2.4.0 (KIP-464)
    pub replication_factor: i16,

    /// The manual partition assignment, or the empty array if we are using automatic assignment.
    pub assignments: Vec<CreateTopicAssignment>,

    /// The custom topic configurations to set.
    pub configs: Vec<CreateTopicConfig>,

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for CreateTopicRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        if v >= 5 {
            self.name.write_compact(writer)?
        } else {
            self.name.write(writer)?;
        }

        self.num_partitions.write(writer)?;
        self.replication_factor.write(writer)?;

        if v >= 5 {
            write_compact_versioned_array(writer, version, Some(&self.assignments))?;
        } else {
            write_versioned_array(writer, version, Some(&self.assignments))?;
        }

        if v >= 5 {
            write_compact_versioned_array(writer, version, Some(&self.configs))?;
        } else {
            write_versioned_array(writer, version, Some(&self.configs))?;
        }

        if v >= 5 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct CreateTopicAssignment {
    /// The partition index
    pub partition_index: i32,

    /// The brokers to place the partition on
    pub broker_ids: Vec<i32>,

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for CreateTopicAssignment
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        self.partition_index.write(writer)?;

        if v >= 5 {
            self.broker_ids.write_compact(writer)?;
        } else {
            self.broker_ids.write(writer)?;
        }

        if v >= 5 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct CreateTopicConfig {
    /// The configuration name.
    pub name: String,

    /// The configuration value.
    pub value: Option<String>,

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for CreateTopicConfig
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        if v >= 5 {
            self.name.write_compact(writer)?;
        } else {
            self.name.write(writer)?;
        }

        if v >= 5 {
            self.value.write_compact(writer)?;
        } else {
            self.value.write(writer)?;
        }

        if v >= 5 {
            self.tagged_fields.write(writer)?
        }

        Ok(())
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct CreateTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota
    /// violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 2
    pub throttle_time_ms: Option<i32>,

    /// Results for each topic we tried to create.
    pub topics: Vec<CreateTopicResponse>,

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for CreateTopicsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let throttle_time_ms = (v >= 2).then(|| i32::read(reader)).transpose()?;
        let topics = if v >= 5 {
            read_compact_versioned_array(reader, version)?.unwrap_or_default()
        } else {
            read_versioned_array(reader, version)?.unwrap_or_default()
        };
        let tagged_fields = (v >= 5).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            throttle_time_ms,
            topics,
            tagged_fields,
        })
    }
}

/// Configuration of the topic.
///
/// Added in version 5
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct CreateTopicResponseConfig {
    /// The configuration name.
    pub name: String,

    /// The configuration value.
    pub value: Option<String>,

    /// True if the configuration is read-only.
    pub read_only: bool,

    /// The configuration source.
    pub config_source: i8,

    /// True if this configuration is sensitive.
    pub is_sensitive: bool,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<R> ReadVersionedType<R> for CreateTopicResponseConfig
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v == 5);

        Ok(Self {
            name: String::read_compact(reader)?,
            value: Option::<String>::read_compact(reader)?,
            read_only: bool::read(reader)?,
            config_source: i8::read(reader)?,
            is_sensitive: bool::read(reader)?,
            tagged_fields: TaggedFields::read(reader)?,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct CreateTopicResponse {
    /// The topic name.
    pub name: String,

    /// The error code, or 0 if there was no error.
    pub error: Option<Error>,

    /// The error message
    ///
    /// Added in version 1
    pub error_message: Option<String>,

    /// Number of partitions of the topic.
    ///
    /// Added in version 5
    pub num_partitions: Option<i32>,

    /// Replication factor of the topic.
    ///
    /// Added in version 5.
    pub replication_factor: Option<i16>,

    /// Configuration of the topic.
    ///
    /// Added in version 5
    pub configs: Vec<CreateTopicResponseConfig>,

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for CreateTopicResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let name = if v >= 5 {
            String::read_compact(reader)?
        } else {
            String::read(reader)?
        };
        let error = Error::new(i16::read(reader)?);
        let error_message = if v > 4 {
            Option::<String>::read_compact(reader)?
        } else if v > 0 {
            Option::<String>::read(reader)?
        } else {
            None
        };
        let num_partitions = (v >= 5).then(|| i32::read(reader)).transpose()?;
        let replication_factor = (v >= 5).then(|| i16::read(reader)).transpose()?;
        let configs = (v >= 5)
            .then(|| read_compact_versioned_array(reader, version))
            .transpose()?
            .flatten()
            .unwrap_or_default();
        let tagged_fields = (v >= 5).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            name,
            error,
            error_message,
            num_partitions,
            replication_factor,
            configs,
            tagged_fields,
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
                "one with retention.ms",
                0,
                CreateTopicsRequest {
                    topics: vec![CreateTopicRequest {
                        name: "topic".to_string(),
                        num_partitions: -1,
                        replication_factor: -1,
                        assignments: vec![CreateTopicAssignment {
                            partition_index: 0,
                            broker_ids: vec![0, 1, 2],
                            tagged_fields: None,
                        }],
                        configs: vec![CreateTopicConfig {
                            name: "retention.ms".to_string(),
                            value: Some("-1".into()),
                            tagged_fields: None,
                        }],
                        tagged_fields: None,
                    }],
                    timeout_ms: 100,
                    validate_only: None,
                    tagged_fields: None,
                },
                [
                    0, 0, 0, 1, 0, 5, b't', b'o', b'p', b'i', b'c', 255, 255, 255, 255, 255, 255,
                    0, 0, 0, 1, // 1 replica assignment
                    0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0,
                    1, // 1 config
                    0, 12, b'r', b'e', b't', b'e', b'n', b't', b'i', b'o', b'n', b'.', b'm', b's',
                    0, 2, b'-', b'1', 0, 0, 0, 100,
                ]
                .as_ref(),
            ),
            (
                "one with retention.ms",
                1,
                CreateTopicsRequest {
                    topics: vec![CreateTopicRequest {
                        name: "topic".to_string(),
                        num_partitions: -1,
                        replication_factor: -1,
                        assignments: vec![CreateTopicAssignment {
                            partition_index: 0,
                            broker_ids: vec![0, 1, 2],
                            tagged_fields: None,
                        }],
                        configs: vec![CreateTopicConfig {
                            name: "retention.ms".to_string(),
                            value: Some("-1".into()),
                            tagged_fields: None,
                        }],
                        tagged_fields: None,
                    }],
                    timeout_ms: 100,
                    validate_only: Some(true),
                    tagged_fields: None,
                },
                [
                    0, 0, 0, 1, // topic length
                    0, 5, b't', b'o', b'p', b'i', b'c', // topic name
                    255, 255, 255, 255, // partitions
                    255, 255, // replication factor
                    0, 0, 0, 1, // 1 replica assignment
                    0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0,
                    1, // 1 config
                    0, 12, b'r', b'e', b't', b'e', b'n', b't', b'i', b'o', b'n', b'.', b'm',
                    b's', // config key
                    0, 2, b'-', b'1', // config value
                    0, 0, 0, 100, // timeout ms
                    1,   // validate only
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
                "invalid request",
                0,
                CreateTopicsResponse {
                    throttle_time_ms: None,
                    topics: vec![CreateTopicResponse {
                        name: "topic".to_string(),
                        error: Some(Error::InvalidRequest),
                        error_message: None,
                        num_partitions: None,
                        replication_factor: None,
                        configs: vec![],
                        tagged_fields: None,
                    }],
                    tagged_fields: None,
                },
                [0, 0, 0, 1, 0, 5, b't', b'o', b'p', b'i', b'c', 0, 42].as_ref(),
            ),
            (
                "invalid request with error message",
                1,
                CreateTopicsResponse {
                    throttle_time_ms: None,
                    topics: vec![CreateTopicResponse {
                        name: "topic".to_string(),
                        error: Some(Error::InvalidRequest),
                        error_message: Some("msg".to_string()),
                        num_partitions: None,
                        replication_factor: None,
                        configs: vec![],
                        tagged_fields: None,
                    }],
                    tagged_fields: None,
                },
                [
                    0, 0, 0, 1, 0, 5, b't', b'o', b'p', b'i', b'c', 0, 42, 0, 3, b'm', b's', b'g',
                ]
                .as_ref(),
            ),
            (
                "error",
                2,
                CreateTopicsResponse {
                    throttle_time_ms: Some(100),
                    topics: vec![CreateTopicResponse {
                        name: "topic".to_string(),
                        error: Some(Error::InvalidRequest),
                        error_message: Some("msg".to_string()),
                        num_partitions: None,
                        replication_factor: None,
                        configs: vec![],
                        tagged_fields: None,
                    }],
                    tagged_fields: None,
                },
                [
                    0, 0, 0, 100, // throttle time ms
                    0, 0, 0, 1, // topics length
                    0, 5, b't', b'o', b'p', b'i', b'c', // topic name
                    0, 42, // error
                    0, 3, b'm', b's', b'g', // error message
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let got =
                CreateTopicsResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(got, want, "{name}/{version}");
        }
    }
}
