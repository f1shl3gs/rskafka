use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    write_compact_versioned_array, write_versioned_array, ReadVersionedError, ReadVersionedType,
    RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::primitives::{
    Bytes, CompactBytes, CompactNullableString, CompactNullableStringRef, CompactStringRef, Int16,
    Int32, NullableString, String_, TaggedFields,
};
use crate::protocol::traits::{ReadType, WriteType};

pub struct Assignment {
    /// The ID of the member to assign.
    ///
    /// String < 4
    /// CompactString >= 4
    pub member_id: String_,

    /// The member assignment.
    ///
    /// Bytes < 4
    /// CompactBytes >= 4
    pub assignment: Vec<u8>,
}

impl<W> WriteVersionedType<W> for Assignment
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

        if v < 4 {
            self.member_id.write(writer)?;
        } else {
            CompactStringRef(self.member_id.0.as_str()).write(writer)?;
        }

        if v < 4 {
            Bytes(self.assignment.clone()).write(writer)?;
        } else {
            CompactBytes(self.assignment.clone()).write(writer)?;
        }

        Ok(())
    }
}

pub struct SyncGroupRequest {
    /// The unique group identifier.
    ///
    /// String < 4
    /// CompactString >= 4
    pub group_id: String_,

    /// The generation of the group.
    pub generation_id: Int32,

    /// The member ID assigned by the group.
    ///
    /// String < 4
    /// CompactString >= 4
    pub member_id: String_,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 3.
    /// NullableString == 3
    /// CompactNullableString >= 4
    pub group_instance_id: NullableString,

    /// The group protocol type.
    ///
    /// Added in version 5.
    pub protocol_type: CompactNullableString,

    /// The group proto name.
    ///
    /// Added in version 5.
    pub protocol_name: CompactNullableString,

    /// Each Assignment.
    pub assignments: Vec<Assignment>,

    /// The tagged fields.
    ///
    /// Added in version 4.
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for SyncGroupRequest
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

        if v < 4 {
            self.group_id.write(writer)?;
        } else {
            CompactStringRef(&self.group_id.0).write(writer)?;
        }

        self.generation_id.write(writer)?;

        if v < 4 {
            self.member_id.write(writer)?;
        } else {
            CompactStringRef(&self.member_id.0).write(writer)?;
        }

        if v == 3 {
            self.group_instance_id.write(writer)?
        } else {
            CompactNullableStringRef(self.group_instance_id.0.as_deref()).write(writer)?;
        }

        if v >= 5 {
            self.protocol_type.write(writer)?;
            self.protocol_name.write(writer)?;
        }

        if v >= 4 {
            write_compact_versioned_array(writer, version, Some(&self.assignments))?;
        } else {
            write_versioned_array(writer, version, Some(&self.assignments))?;
        }

        if v >= 4 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

impl RequestBody for SyncGroupRequest {
    type ResponseBody = SyncGroupResponse;

    const API_KEY: ApiKey = ApiKey::SyncGroup;

    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 5);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion::new(4);
}

pub struct SyncGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a
    /// quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1.
    pub throttle_time_ms: Int32,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// The group protocol type.
    ///
    /// Added in version 5.
    pub protocol_type: CompactNullableString,

    /// The group protocol name.
    ///
    /// Added in version 5.
    pub protocol_name: CompactNullableString,

    /// The member assignment.
    ///
    /// Bytes <= 3
    /// CompactBytes >= 4
    pub assignment: Vec<u8>,

    /// The tagged fields.
    ///
    /// Added in version 4.
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for SyncGroupResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let throttle_time_ms = if v >= 1 {
            Int32::read(reader)?
        } else {
            Int32(0)
        };

        let error_code = Error::new(Int16::read(reader)?.0);

        let protocol_type = if v >= 5 {
            CompactNullableString::read(reader)?
        } else {
            CompactNullableString(None)
        };

        let protocol_name = if v >= 5 {
            CompactNullableString::read(reader)?
        } else {
            CompactNullableString(None)
        };

        let assignment = if v >= 4 {
            Bytes::read(reader)?.0
        } else {
            CompactBytes::read(reader)?.0
        };

        let tagged_fields = (v >= 4).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            protocol_type,
            protocol_name,
            assignment,
            tagged_fields,
        })
    }
}
