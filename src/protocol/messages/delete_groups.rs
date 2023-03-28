use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_versioned_array, ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError,
    WriteVersionedType,
};
use crate::protocol::primitives::{
    Array, CompactArrayRef, CompactStringRef, Int16, Int32, String_, TaggedFields,
};
use crate::protocol::traits::{ReadType, WriteType};

pub struct DeleteGroupsRequest {
    /// The group names to delete.
    pub group_names: Array<String_>,

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
            if let Some(names) = self.group_names.0.as_ref() {
                let names: Vec<_> = names
                    .iter()
                    .map(|name| CompactStringRef(name.0.as_str()))
                    .collect();

                CompactArrayRef(Some(&names)).write(writer)?;
            } else {
                CompactArrayRef::<CompactStringRef<'_>>(None).write(writer)?;
            }
        } else {
            self.group_names.write(writer)?;
        }

        // handle tagged fields
        if v >= 2 {
            match self.tagged_fields.as_ref() {
                Some(tagged_fields) => {
                    tagged_fields.write(writer)?;
                }
                None => TaggedFields::default().write(writer)?,
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct DeleteGroupResult {
    /// The group id
    pub group_id: String_,

    /// The deletion error, or 0 if the deletion succeeded.
    pub error_code: Option<Error>,
}

impl<R> ReadVersionedType<R> for DeleteGroupResult
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 2);

        let group_id = String_::read(reader)?;
        let error_code = Error::new(Int16::read(reader)?.0);

        Ok(Self {
            group_id,
            error_code,
        })
    }
}

#[derive(Debug)]
pub struct DeleteGroupsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota
    /// violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: Int32,

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

        let throttle_time_ms = Int32::read(reader)?;
        let results = read_versioned_array(reader, version)?.unwrap_or_default();
        let tagged_fields = (v >= 2).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            throttle_time_ms,
            results,
            tagged_fields,
        })
    }
}
