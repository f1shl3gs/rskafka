use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_versioned_array, write_versioned_array, ReadVersionedError, ReadVersionedType,
    RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::primitives::{
    CompactNullableString, CompactNullableStringRef, CompactString, CompactStringRef, Int16, Int32,
    NullableString, String_, TaggedFields,
};
use crate::protocol::traits::{ReadType, WriteType};

/// Added in version 3
#[derive(Debug)]
pub struct Member {
    /// The member ID to remove from the group.
    ///
    /// v == 3 STRING
    /// v >= 4 COMPACT_STRING
    pub member_id: String_,

    /// The group instance ID to remove from the group.
    ///
    /// v == 3 NULLABLE_STRING
    /// v >= 4 COMPACT_NULLABLE_STRING
    pub group_instance_id: Option<NullableString>,

    /// The reason why the member left the group.
    ///
    /// Added in version 5.
    pub reason: Option<CompactNullableString>,
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
        let v = version.0 .0;
        assert!(v >= 3);

        if v == 3 {
            self.member_id.write(writer)?;
        } else {
            CompactStringRef(self.member_id.0.as_str()).write(writer)?;
        }

        if v == 3 {
            match self.group_instance_id.as_ref() {
                Some(s) => s.write(writer)?,
                None => NullableString::default().write(writer)?,
            }
        } else {
            match self.group_instance_id.as_ref() {
                Some(s) => {
                    CompactNullableStringRef(s.0.as_deref()).write(writer)?;
                }
                None => {
                    CompactNullableString(None).write(writer)?;
                }
            }
        }

        if v >= 5 {
            match self.reason.as_ref() {
                Some(s) => s.write(writer)?,
                None => {
                    CompactNullableString(None).write(writer)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct LeaveGroupRequest {
    /// The ID of the group to leave.
    pub group_id: String_,

    /// The member ID to remove from the group.
    ///
    /// Added in version 0
    /// Removed in version 3
    pub member_id: String_,

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
        let v = version.0 .0;
        assert!(v <= 5);

        self.group_id.write(writer)?;
        if v < 3 {
            self.member_id.write(writer)?;
        }

        if v >= 3 {
            write_versioned_array(writer, version, Some(&self.members))?;

            match self.tagged_fields.as_ref() {
                Some(tagged_fields) => tagged_fields.write(writer)?,
                None => TaggedFields::default().write(writer)?,
            }
        }

        Ok(())
    }
}

impl RequestBody for LeaveGroupRequest {
    type ResponseBody = LeaveGroupResponse;

    const API_KEY: ApiKey = ApiKey::LeaveGroup;

    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(0)), ApiVersion(Int16(5)));

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(4));
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
    pub member_id: String_,

    /// The group instance ID to remove from the group.
    ///
    /// version == 3: NULLABLE_STRING
    /// version >= 4: COMPACT_NULLABLE_STRING
    pub group_instance_id: String_,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,
}

impl<R> ReadVersionedType<R> for LeaveGroupResponseMember
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 5 && v >= 3);

        let member_id = if v < 4 {
            String_::read(reader)?
        } else {
            let n = CompactString::read(reader)?;
            String_(n.0)
        };

        let group_instance_id = if v < 4 {
            String_::read(reader)?
        } else {
            let n = CompactString::read(reader)?;
            String_(n.0)
        };

        let error_code = Error::new(Int16::read(reader)?.0);

        Ok(Self {
            member_id,
            group_instance_id,
            error_code,
        })
    }
}

#[derive(Debug)]
pub struct LeaveGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a
    /// quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1.
    pub throttle_time_ms: Option<Int32>,

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
        let v = version.0 .0;
        assert!(v <= 5);

        let throttle_time_ms = (v >= 1).then(|| Int32::read(reader)).transpose()?;
        let error_code = Error::new(Int16::read(reader)?.0);

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
