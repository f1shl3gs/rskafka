use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_versioned_array, ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError,
    WriteVersionedType,
};
use crate::protocol::primitives::{Array, CompactString, Int16, Int32, String_, TaggedFields};
use crate::protocol::traits::{ReadType, WriteType};

#[derive(Debug)]
pub struct ListGroupsRequest {
    /// The states of the groups we want to list. If empty all groups
    /// are returned with their state.
    ///
    /// Added in version 4
    pub states_filter: Array<CompactString>,

    /// The tagged fields.
    ///
    /// Added in version 3
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for ListGroupsRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 4);

        if v >= 4 {
            self.states_filter.write(writer)?;
        }

        if v >= 3 {
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

impl RequestBody for ListGroupsRequest {
    type ResponseBody = ListGroupsResponse;

    const API_KEY: ApiKey = ApiKey::ListGroups;

    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(0)), ApiVersion(Int16(0)));

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(3));
}

#[derive(Debug)]
pub struct Group {
    /// The group ID.
    ///
    /// v < 4  STRING
    /// v >= 4 COMPACT_STRING
    pub group_id: String_,

    /// The group protocol type.
    ///
    /// v < 4  STRING
    /// v >= 4 COMPACT_STRING
    pub protocol_type: String_,

    /// The group state name.
    ///
    /// Added in version 4
    pub group_state: Option<CompactString>,
}

impl<R> ReadVersionedType<R> for Group
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 4);

        let group_id = if v < 4 {
            String_::read(reader)?
        } else {
            let n = CompactString::read(reader)?;
            String_(n.0)
        };

        let protocol_type = if v < 4 {
            String_::read(reader)?
        } else {
            let n = CompactString::read(reader)?;
            String_(n.0)
        };

        let group_state = (v >= 4).then(|| CompactString::read(reader)).transpose()?;

        Ok(Self {
            group_id,
            protocol_type,
            group_state,
        })
    }
}

#[derive(Debug)]
pub struct ListGroupsResponse {
    /// The duration in milliseconds for which the request was throttled due to a
    /// quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1
    pub throttle_time_ms: Option<Int32>,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// Each group in the response.
    pub groups: Vec<Group>,

    /// The tagged fields.
    ///
    /// Added in version 3
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ListGroupsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 4);

        let throttle_time_ms = (v >= 1).then(|| Int32::read(reader)).transpose()?;
        let error_code = Error::new(Int16::read(reader)?.0);
        let groups = read_versioned_array(reader, version)?.unwrap_or_default();
        let tagged_fields = (v >= 3).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
            tagged_fields,
        })
    }
}
