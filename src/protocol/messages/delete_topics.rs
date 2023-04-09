use std::io::{Read, Write};

use crate::protocol::traits::{ReadCompactType, WriteCompactType};
use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error,
    messages::{
        read_compact_versioned_array, read_versioned_array, ReadVersionedError, ReadVersionedType,
        RequestBody, WriteVersionedError, WriteVersionedType,
    },
    primitives::TaggedFields,
    traits::{ReadType, WriteType},
};

#[derive(Debug)]
pub struct DeleteTopicsRequest {
    /// The names of the topics to delete.
    pub topic_names: Vec<String>,

    /// The length of time in milliseconds to wait for the deletions to complete.
    pub timeout_ms: i32,

    /// The tagged fields.
    ///
    /// Added in version 4.
    pub tagged_fields: Option<TaggedFields>,
}

impl RequestBody for DeleteTopicsRequest {
    type ResponseBody = DeleteTopicsResponse;

    const API_KEY: ApiKey = ApiKey::DeleteTopics;

    /// Enough for now.
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 5);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion::new(4);
}

impl<W> WriteVersionedType<W> for DeleteTopicsRequest
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

        if v >= 4 {
            self.topic_names.write_compact(writer)?;
        } else {
            self.topic_names.write(writer)?;
        };

        self.timeout_ms.write(writer)?;

        if v >= 4 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct DeleteTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the
    /// request did not violate any quota.
    ///
    /// Added in version 1.
    pub throttle_time_ms: Option<i32>,

    /// The results for each topic we tried to delete.
    pub responses: Vec<DeleteTopicsResponseTopic>,

    /// The tagged fields.
    ///
    /// Added in version 4.
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for DeleteTopicsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let throttle_time_ms = (v >= 1).then(|| i32::read(reader)).transpose()?;
        let responses = if v >= 4 {
            read_compact_versioned_array(reader, version)?.unwrap_or_default()
        } else {
            read_versioned_array(reader, version)?.unwrap_or_default()
        };
        let tagged_fields = (v >= 4).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            throttle_time_ms,
            responses,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
pub struct DeleteTopicsResponseTopic {
    /// The topic name.
    pub name: String,

    /// The error code, or 0 if there was no error.
    pub error: Option<Error>,

    /// The error message, or null if there was no error.
    ///
    /// Added in version 5.
    pub error_message: Option<String>,

    /// The tagged fields.
    ///
    /// Added in version 4.
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for DeleteTopicsResponseTopic
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let name = if v >= 4 {
            String::read_compact(reader)?
        } else {
            String::read(reader)?
        };
        let error = Error::new(i16::read(reader)?);
        let error_message = if v >= 5 {
            ReadCompactType::read_compact(reader)?
        } else {
            None
        };

        let tagged_fields = (v >= 4).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            name,
            error,
            error_message,
            tagged_fields,
        })
    }
}
