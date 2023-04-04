use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_versioned_array, ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError,
    WriteVersionedType,
};
use crate::protocol::primitives::TaggedFields;
use crate::protocol::traits::{ReadCompactType, ReadType, WriteCompactType, WriteType};

pub struct DeleteGroupsRequest {
    /// The group names to delete.
    pub group_names: Vec<String>,

    /// The tagged fields.
    ///
    /// Added in version 2.
    pub tagged_fields: Option<TaggedFields>,
}

impl RequestBody for DeleteGroupsRequest {
    type ResponseBody = DeleteGroupsResponse;

    const API_KEY: ApiKey = ApiKey::DeleteGroups;

    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 2);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion::new(2);
}

impl<W> WriteVersionedType<W> for DeleteGroupsRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 2);

        if v >= 2 {
            self.group_names.write_compact(writer)?;
        } else {
            self.group_names.write(writer)?;
        }

        // handle tagged fields
        if v >= 2 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct DeleteGroupResult {
    /// The group id.
    ///
    /// STRING < 2
    /// COMPACT_STRING >= 2
    pub group_id: String,

    /// The deletion error, or 0 if the deletion succeeded.
    pub error_code: Option<Error>,

    /// The tagged fields.
    ///
    /// Added in version 2.
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for DeleteGroupResult
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 2);

        let group_id = if v < 2 {
            String::read(reader)?
        } else {
            String::read_compact(reader)?
        };
        let error_code = Error::new(i16::read(reader)?);

        let tagged_fields = (v >= 2).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            group_id,
            error_code,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
pub struct DeleteGroupsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota
    /// violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,

    /// The deletion results.
    pub results: Vec<DeleteGroupResult>,

    /// The tagged fields.
    ///
    /// Added in version 4.
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for DeleteGroupsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 2);

        let throttle_time_ms = i32::read(reader)?;
        let results = read_versioned_array(reader, version)?.unwrap_or_default();
        let tagged_fields = (v >= 2).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            throttle_time_ms,
            results,
            tagged_fields,
        })
    }
}
