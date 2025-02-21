use std::io::{Cursor, Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::consumer_group::ConsumerGroupMemberAssignment;
use crate::protocol::messages::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
    write_compact_versioned_array, write_versioned_array,
};
use crate::protocol::primitives::TaggedFields;
use crate::protocol::traits::{ReadCompactType, ReadType, WriteCompactType, WriteType};

#[derive(Debug)]
pub struct SyncGroupRequestAssignment {
    /// The ID of the member to assign.
    ///
    /// STRING < 4
    /// COMPACT_STRING >= 4
    pub member_id: String,

    /// The member assignment.
    ///
    /// BYTES < 4
    /// COMPACT_BYTES >= 4
    pub assignment: Vec<u8>,

    /// The tagged fields
    ///
    /// Added in version 4
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for SyncGroupRequestAssignment
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
            self.member_id.write_compact(writer)?;
        }

        if v < 4 {
            self.assignment.write(writer)?;
        } else {
            self.assignment.write_compact(writer)?;
        }

        if v >= 4 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct SyncGroupRequest {
    /// The unique group identifier.
    ///
    /// STRING < 4
    /// COMPACT_STRING >= 4
    pub group_id: String,

    /// The generation of the group
    pub generation_id: i32,

    /// The member ID assigned by the group.
    ///
    /// STRING < 4
    /// COMPACT_STRING >= 4
    pub member_id: String,

    /// the unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 3.
    /// NULLABLE_STRING == 3
    /// COMPACT_NULLABLE_STRING >= 4
    pub group_instance_id: Option<String>,

    /// The group protocol type.
    ///
    /// Added in version 5.
    /// COMPACT_NULLABLE_STRING >= 5
    pub protocol_type: Option<String>,

    /// The group proto name
    ///
    /// Added in version 5.
    /// COMPACT_NULLABLE_STRING >= 5
    pub protocol_name: Option<String>,

    /// Each assignment
    pub assignments: Vec<SyncGroupRequestAssignment>,

    /// The tagged fields.
    ///
    /// Added in version 4
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
            self.group_id.write_compact(writer)?;
        }

        self.generation_id.write(writer)?;

        if v < 4 {
            self.member_id.write(writer)?;
        } else {
            self.member_id.write_compact(writer)?;
        }

        if v == 3 {
            self.group_instance_id.write(writer)?;
        } else if v >= 4 {
            self.group_instance_id.write_compact(writer)?;
        }

        if v >= 5 {
            self.protocol_type.write_compact(writer)?;
            self.protocol_name.write_compact(writer)?;
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
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(4);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct SyncGroupResponse {
    /// The duration in milliseconds for which the request was throttled due
    /// to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1
    pub throttle_time_ms: Option<i32>,

    /// The error code, or 0 if there was no error
    pub error_code: Option<Error>,

    /// The group protocol type.
    ///
    /// Added in version 5.
    pub protocol_type: Option<String>,

    /// The group protocol name
    ///
    /// Added in version 5
    pub protocol_name: Option<String>,

    /// The member assignment
    ///
    /// BYTES <= 3
    /// COMPACT_BYTES >= 4
    pub assignments: ConsumerGroupMemberAssignment,

    /// The tagged fields.
    ///
    /// Added in version 4
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for SyncGroupResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let throttle_time_ms = (v > 0).then(|| i32::read(reader)).transpose()?;
        let error_code = Error::new(i16::read(reader)?);
        let protocol_type = if v >= 5 {
            ReadCompactType::read_compact(reader)?
        } else {
            None
        };
        let protocol_name = if v >= 5 {
            ReadCompactType::read_compact(reader)?
        } else {
            None
        };

        // assignment
        let buf = if v < 4 {
            Vec::<u8>::read(reader)?
        } else {
            Vec::<u8>::read_compact(reader)?
        };
        let assignments = if buf.is_empty() {
            ConsumerGroupMemberAssignment {
                version: 0,
                topics: vec![],
                user_data: vec![],
            }
        } else {
            let mut assignment_reader = Cursor::new(buf);
            ConsumerGroupMemberAssignment::read(&mut assignment_reader)?
        };

        let tagged_fields = (v >= 4).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            protocol_type,
            protocol_name,
            assignments,
            tagged_fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::consumer_group::PartitionAssignment;

    #[test]
    fn request() {
        for (name, version, req, want) in [
            (
                "empty",
                0,
                SyncGroupRequest {
                    group_id: "foo".to_string(),
                    generation_id: 66051,
                    member_id: "baz".to_string(),
                    group_instance_id: None,
                    protocol_type: None,
                    protocol_name: None,
                    assignments: vec![],
                    tagged_fields: None,
                },
                [
                    0, 3, b'f', b'o', b'o', // Group ID
                    0x00, 0x01, 0x02, 0x03, // Generation ID
                    0, 3, b'b', b'a', b'z', // Member ID
                    0, 0, 0, 0, // no assignments
                ]
                .as_ref(),
            ),
            (
                "populated",
                0,
                SyncGroupRequest {
                    group_id: "foo".to_string(),
                    generation_id: 66051,
                    member_id: "baz".to_string(),
                    group_instance_id: None,
                    protocol_type: None,
                    protocol_name: None,
                    assignments: vec![SyncGroupRequestAssignment {
                        member_id: "baz".to_string(),
                        assignment: vec![b'f', b'o', b'o'],
                        tagged_fields: None,
                    }],
                    tagged_fields: None,
                },
                [
                    0, 3, b'f', b'o', b'o', // Group ID
                    0x00, 0x01, 0x02, 0x03, // Generation ID
                    0, 3, b'b', b'a', b'z', // Member ID
                    0, 0, 0, 1, // one assignment
                    0, 3, b'b', b'a', b'z', // Member ID
                    0, 0, 0, 3, b'f', b'o', b'o', // Member assignment
                ]
                .as_ref(),
            ),
            (
                "populated v3",
                3,
                SyncGroupRequest {
                    group_id: "foo".to_string(),
                    generation_id: 0x00010203,
                    member_id: "baz".to_string(),
                    group_instance_id: Some("gid".into()),
                    protocol_type: None,
                    protocol_name: None,
                    assignments: vec![SyncGroupRequestAssignment {
                        member_id: "baz".to_string(),
                        assignment: vec![b'f', b'o', b'o'],
                        tagged_fields: None,
                    }],
                    tagged_fields: None,
                },
                [
                    0, 3, b'f', b'o', b'o', // Group ID
                    0x00, 0x01, 0x02, 0x03, // Generation ID
                    0, 3, b'b', b'a', b'z', // Member ID
                    0, 3, b'g', b'i', b'd', // GroupInstance ID
                    0, 0, 0, 1, // one assignment
                    0, 3, b'b', b'a', b'z', // Member ID
                    0, 0, 0, 3, b'f', b'o', b'o', // Member assignment
                ]
                .as_ref(),
            ),
        ] {
            let mut buf = Cursor::new([0u8; 128]);
            req.write_versioned(&mut buf, ApiVersion(version)).unwrap();
            let len = buf.position() as usize;
            let data = &buf.get_ref().as_slice()[..len];
            assert_eq!(data, want, "{name}/{version}")
        }
    }

    #[test]
    fn response() {
        for (name, version, want, data) in [
            (
                "no err",
                0,
                SyncGroupResponse {
                    throttle_time_ms: None,
                    error_code: None,
                    protocol_type: None,
                    protocol_name: None,
                    assignments: ConsumerGroupMemberAssignment {
                        version: 0,
                        topics: vec![PartitionAssignment {
                            topic: "one".to_string(),
                            partitions: vec![0, 2, 4],
                        }],
                        user_data: vec![0x01, 0x02, 0x03],
                    },
                    tagged_fields: None,
                },
                [
                    0x00, 0x00, // No error
                    0, 0, 0, 34, // Member assignment data
                    0, 0, // Version
                    0, 0, 0, 1, // Topic array length
                    0, 3, b'o', b'n', b'e', // Topic one
                    0, 0, 0, 3, // Topic one, partition array length
                    0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, // 0, 2, 4
                    0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
                ]
                .as_ref(),
            ),
            (
                "error",
                0,
                SyncGroupResponse {
                    throttle_time_ms: None,
                    error_code: Some(Error::RebalanceInProgress),
                    protocol_type: None,
                    protocol_name: None,
                    assignments: ConsumerGroupMemberAssignment {
                        version: 0,
                        topics: vec![],
                        user_data: vec![],
                    },
                    tagged_fields: None,
                },
                [
                    0, 27, // ErrRebalanceInProgress
                    0, 0, 0, 0, // No member assignment data
                ]
                .as_ref(),
            ),
            (
                "no error",
                1,
                SyncGroupResponse {
                    throttle_time_ms: Some(100),
                    error_code: None,
                    protocol_type: None,
                    protocol_name: None,
                    assignments: ConsumerGroupMemberAssignment {
                        version: 0,
                        topics: vec![PartitionAssignment {
                            topic: "one".to_string(),
                            partitions: vec![0, 2, 4],
                        }],
                        user_data: vec![0x01, 0x02, 0x03],
                    },
                    tagged_fields: None,
                },
                [
                    0, 0, 0, 100, // ThrottleTimeMs
                    0x00, 0x00, // No error
                    0, 0, 0, 34, // Member assignment data
                    0, 0, // Version
                    0, 0, 0, 1, // Topic array length
                    0, 3, b'o', b'n', b'e', // Topic one
                    0, 0, 0, 3, // Topic one, partition array length
                    0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, // 0, 2, 4
                    0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let got = SyncGroupResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(got, want, "{name}/{version}");
        }
    }
}
