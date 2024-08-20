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

#[derive(Debug)]
pub struct LeaveGroupRequestMember {
    /// The member ID to remove from the group.
    ///
    /// STRING == 3
    /// COMPACT_STRING >= 4
    pub member_id: String,

    /// The group instance ID to remove from group
    ///
    /// NULLABLE_STRING == 3
    /// COMPACT_NULLABLE_STARING >= 4
    pub group_instance_id: Option<String>,

    /// The reason why the member left the group.
    ///
    /// COMPACT_NULLABLE_STRING >= 5
    pub reason: Option<String>,

    /// The tagged fields.
    ///
    /// Added in version 4
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for LeaveGroupRequestMember
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
    /// Removed in version 3
    pub member_id: String,

    /// List of leaving member identities.
    ///
    /// Added in version 3
    pub members: Vec<LeaveGroupRequestMember>,

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

/// Leaving member
///
/// Added in version 3
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct LeaveGroupResponseMember {
    /// The member ID to remove from the group
    ///
    /// STRING == 3
    /// COMPACT_STRING >= 4
    pub member_id: String,

    /// The group instance ID to remove from the group.
    ///
    /// NULLABLE_STRING == 3
    /// COMPACT_NULLABLE_STRING >= 4
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

impl RequestBody for LeaveGroupRequest {
    type ResponseBody = LeaveGroupResponse;
    const API_KEY: ApiKey = ApiKey::LeaveGroup;
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 2);
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(4);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct LeaveGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a
    /// quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1
    pub throttle_time_ms: Option<i32>,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// List of leaving member responses.
    ///
    /// Added in version 3
    pub members: Vec<LeaveGroupResponseMember>,

    /// The tagged fields
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn request() {
        for (name, version, req, want) in [
            (
                "v0",
                0,
                LeaveGroupRequest {
                    group_id: "foo".to_string(),
                    member_id: "bar".to_string(),
                    members: vec![],
                    tagged_fields: None,
                },
                [0, 3, b'f', b'o', b'o', 0, 3, b'b', b'a', b'r'].as_ref(),
            ),
            (
                "v3",
                3,
                LeaveGroupRequest {
                    group_id: "foo".to_string(),
                    member_id: "".to_string(),
                    members: vec![
                        LeaveGroupRequestMember {
                            member_id: "mid1".to_string(),
                            group_instance_id: None,
                            reason: None,
                            tagged_fields: None,
                        },
                        LeaveGroupRequestMember {
                            member_id: "mid2".to_string(),
                            group_instance_id: Some("gid".into()),
                            reason: None,
                            tagged_fields: None,
                        },
                    ],
                    tagged_fields: None,
                },
                [
                    0, 3, b'f', b'o', b'o', 0, 0, 0, 2, // Two Member
                    0, 4, b'm', b'i', b'd', b'1', // MemberId
                    255, 255, // GroupInstanceId  nil
                    0, 4, b'm', b'i', b'd', b'2', // MemberId
                    0, 3, b'g', b'i', b'd', // GroupInstanceId
                ]
                .as_ref(),
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
                "no error",
                0,
                LeaveGroupResponse {
                    throttle_time_ms: None,
                    error_code: None,
                    members: vec![],
                    tagged_fields: None,
                },
                [0x00, 0x00].as_ref(),
            ),
            (
                "error",
                0,
                LeaveGroupResponse {
                    throttle_time_ms: None,
                    error_code: Some(Error::UnknownMemberId),
                    members: vec![],
                    tagged_fields: None,
                },
                [0, 25].as_ref(),
            ),
            (
                "no err",
                1,
                LeaveGroupResponse {
                    throttle_time_ms: Some(100),
                    error_code: None,
                    members: vec![],
                    tagged_fields: None,
                },
                [
                    0, 0, 0, 100, // ThrottleTime
                    0x00, 0x00, // Err
                ]
                .as_ref(),
            ),
            (
                "v3",
                3,
                LeaveGroupResponse {
                    throttle_time_ms: Some(100),
                    error_code: None,
                    members: vec![
                        LeaveGroupResponseMember {
                            member_id: "mid1".to_string(),
                            group_instance_id: None,
                            error_code: None,
                            tagged_fields: None,
                        },
                        LeaveGroupResponseMember {
                            member_id: "mid2".to_string(),
                            group_instance_id: Some("gid".into()),
                            error_code: Some(Error::UnknownMemberId),
                            tagged_fields: None,
                        },
                    ],
                    tagged_fields: None,
                },
                [
                    0, 0, 0, 100, // ThrottleTime
                    0x00, 0x00, // Err
                    0, 0, 0, 2, // Two Members
                    0, 4, b'm', b'i', b'd', b'1', // MemberId
                    255, 255, // GroupInstanceId
                    0, 0, // Err
                    0, 4, b'm', b'i', b'd', b'2', // MemberId
                    0, 3, b'g', b'i', b'd', // GroupInstanceId
                    0, 25, // Err
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let resp =
                LeaveGroupResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(resp, want, "{name}/{version}")
        }
    }
}
