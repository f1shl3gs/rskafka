use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_versioned_array, write_versioned_array, ReadVersionedError, ReadVersionedType,
    RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::primitives::TaggedFields;
use crate::protocol::traits::{ReadCompactType, ReadType, WriteCompactType, WriteType};

/// Added in version 3
#[derive(Debug)]
pub struct Member {
    /// The member ID to remove from the group.
    ///
    /// v == 3 STRING
    /// v >= 4 COMPACT_STRING
    pub member_id: String,

    /// The group instance ID to remove from the group.
    ///
    /// v == 3 NULLABLE_STRING
    /// v >= 4 COMPACT_NULLABLE_STRING
    pub group_instance_id: Option<String>,

    /// The reason why the member left the group.
    ///
    /// Added in version 5. COMPACT_NULLABLE_STRING
    pub reason: Option<String>,

    /// The tagged fields.
    ///
    /// Added in version 4
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for Member
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v >= 3);

        if v == 3 {
            self.member_id.write(writer)?;
        } else {
            self.member_id.write_compact(writer)?;
        }

        if v == 3 {
            self.group_instance_id.write(writer)?;
        } else {
            self.group_instance_id.write_compact(writer)?;
        }

        if v >= 5 {
            self.reason.write_compact(writer)?;
        }

        if v >= 4 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct LeaveGroupRequest {
    /// The ID of the group to leave.
    pub group_id: String,

    /// The member ID to remove from the group.
    ///
    /// Added in version 0
    /// Removed in version 3
    pub member_id: String,

    /// List of leaving member identities.
    ///
    /// Added in version 3
    pub members: Vec<Member>,

    /// The tagged fields.
    ///
    /// Added in version 4
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for LeaveGroupRequest
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
            self.group_id.write_compact(writer)?;
        } else {
            self.group_id.write(writer)?;
        }

        if v < 3 {
            self.member_id.write(writer)?;
        }

        if v >= 3 {
            write_versioned_array(writer, version, Some(&self.members))?;
        }

        if v >= 4 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

impl RequestBody for LeaveGroupRequest {
    type ResponseBody = LeaveGroupResponse;

    const API_KEY: ApiKey = ApiKey::LeaveGroup;

    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 2);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion::new(4);
}

/// Leaving member.
///
/// Added in version 3.
#[derive(Debug)]
pub struct LeaveGroupResponseMember {
    /// The member ID to remove from the group.
    ///
    /// version == 3: STRING
    /// version >= 4: COMPACT_STRING
    pub member_id: String,

    /// The group instance ID to remove from the group.
    ///
    /// version == 3: NULLABLE_STRING
    /// version >= 4: COMPACT_NULLABLE_STRING
    pub group_instance_id: Option<String>,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// The tagged fields.
    ///
    /// Added in version 4
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for LeaveGroupResponseMember
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5 && v >= 3);

        let member_id = if v < 4 {
            String::read(reader)?
        } else {
            String::read_compact(reader)?
        };

        let group_instance_id = if v < 4 {
            ReadType::read(reader)?
        } else {
            ReadCompactType::read_compact(reader)?
        };

        let error_code = Error::new(i16::read(reader)?);

        let tagged_fields = if v >= 4 {
            Some(TaggedFields::read(reader)?)
        } else {
            None
        };

        Ok(Self {
            member_id,
            group_instance_id,
            error_code,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
pub struct LeaveGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a
    /// quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1.
    pub throttle_time_ms: Option<i32>,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// List of leaving member responses.
    ///
    /// Added in version 3.
    pub members: Vec<LeaveGroupResponseMember>,

    /// The tagged fields.
    ///
    /// Added in version 4
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for LeaveGroupResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let throttle_time_ms = (v >= 1).then(|| i32::read(reader)).transpose()?;
        let error_code = Error::new(i16::read(reader)?);

        let members = if v >= 3 {
            read_versioned_array(reader, version)?.unwrap_or_default()
        } else {
            vec![]
        };

        let tagged_fields = if v >= 4 {
            Some(TaggedFields::read(reader)?)
        } else {
            None
        };

        Ok(Self {
            throttle_time_ms,
            error_code,
            members,
            tagged_fields,
        })
    }
}
