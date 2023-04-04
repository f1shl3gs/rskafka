use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    write_compact_versioned_array, write_versioned_array, ReadVersionedError, ReadVersionedType,
    RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::primitives::TaggedFields;
use crate::protocol::traits::{
    ReadCompactType, ReadError, ReadType, WriteCompactType, WriteError, WriteType,
};

// ConsumerGroupMemberMetadata holds the metadata for consumer group
// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolSubscription.json
#[derive(Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ConsumerGroupMemberMetadata {
    pub version: i16,
    pub topics: Vec<String>,
    pub user_data: Vec<u8>,
    pub owned_partitions: Vec<PartitionAssignment>,
}

impl<R: Read> ReadType<R> for ConsumerGroupMemberMetadata {
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let version = i16::read(reader)?;
        let mut topics = Vec::new();
        let len = i32::read(reader)?;
        for _i in 0..len {
            topics.push(String::read(reader)?);
        }

        let user_data = ReadType::read(reader)?;

        Ok(Self {
            version,
            topics,
            user_data,
            owned_partitions: vec![],
        })
    }
}

impl<W: Write> WriteType<W> for ConsumerGroupMemberMetadata {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        self.version.write(writer)?;

        let len = self.topics.len() as i32;
        len.write(writer)?;
        for s in &self.topics {
            s.write(writer)?;
        }

        self.user_data.write(writer)?;

        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PartitionAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

impl<R: Read> ReadType<R> for PartitionAssignment {
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let topic = String::read(reader)?;

        let len = i32::read(reader)?;
        let mut partitions = Vec::new();
        for _i in 0..len {
            let p = i32::read(reader)?;
            partitions.push(p);
        }

        Ok(Self { topic, partitions })
    }
}

impl<W: Write> WriteType<W> for PartitionAssignment {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        self.topic.write(writer)?;

        let len = self.partitions.len() as i32;
        len.write(writer)?;

        for p in &self.partitions {
            p.write(writer)?;
        }

        Ok(())
    }
}

// ConsumerGroupMemberAssignment holds the member assignment for a consume group
// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolAssignment.json
#[cfg_attr(test, derive(Debug, Eq, PartialEq))]
pub struct ConsumerGroupMemberAssignment {
    pub version: i16,
    pub topics: Vec<PartitionAssignment>,
    pub user_data: Vec<u8>,
}

impl<R: Read> ReadType<R> for ConsumerGroupMemberAssignment {
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let version = i16::read(reader)?;

        let len = i32::read(reader)?;
        let mut topics = Vec::new();
        for _i in 0..len {
            topics.push(PartitionAssignment::read(reader)?);
        }

        let user_data = ReadType::read(reader)?;

        Ok(Self {
            version,
            topics,
            user_data,
        })
    }
}

impl<W> WriteType<W> for ConsumerGroupMemberAssignment
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        self.version.write(writer)?;

        let len = self.topics.len() as i32;
        len.write(writer)?;

        for topic in &self.topics {
            topic.write(writer)?;
        }

        self.user_data.write(writer)?;

        Ok(())
    }
}

pub struct Assignment {
    /// The ID of the member to assign.
    ///
    /// String < 4
    /// CompactString >= 4
    pub member_id: String,

    /// The member assignment.
    ///
    /// Bytes < 4
    /// CompactBytes >= 4
    pub assignment: Vec<u8>,

    /// The tagged fields.
    ///
    /// Added in version 4.
    pub tagged_fields: Option<TaggedFields>,
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

pub struct SyncGroupRequest {
    /// The unique group identifier.
    ///
    /// String < 4
    /// CompactString >= 4
    pub group_id: String,

    /// The generation of the group.
    pub generation_id: i32,

    /// The member ID assigned by the group.
    ///
    /// String < 4
    /// CompactString >= 4
    pub member_id: String,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 3.
    /// NullableString == 3
    /// CompactNullableString >= 4
    pub group_instance_id: Option<String>,

    /// The group protocol type.
    ///
    /// Added in version 5.
    /// CompactNullableString >= 5
    pub protocol_type: Option<String>,

    /// The group proto name.
    ///
    /// Added in version 5.
    /// CompactNullableString >= 5
    pub protocol_name: Option<String>,

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
            self.group_id.write_compact(writer)?;
        }

        self.generation_id.write(writer)?;

        if v < 4 {
            self.member_id.write(writer)?;
        } else {
            self.member_id.write_compact(writer)?;
        }

        if v == 3 {
            self.group_instance_id.write(writer)?
        } else if v >= 4 {
            self.group_instance_id.write_compact(writer)?
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

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion::new(4);
}

pub struct SyncGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a
    /// quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1.
    pub throttle_time_ms: i32,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// The group protocol type.
    ///
    /// Added in version 5.
    pub protocol_type: Option<String>,

    /// The group protocol name.
    ///
    /// Added in version 5.
    pub protocol_name: Option<String>,

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

        let throttle_time_ms = if v >= 1 { i32::read(reader)? } else { 0 };

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

        let assignment = if v < 4 {
            ReadType::read(reader)?
        } else {
            ReadCompactType::read_compact(reader)?
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn consumer_group_member_metadata() {
        let want: [u8; 23] = [
            0, 0, // Version
            0, 0, 0, 2, // Topic array length
            0, 3, b'o', b'n', b'e', // Topic one
            0, 3, b't', b'w', b'o', // Topic two
            0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
        ];

        let meta = ConsumerGroupMemberMetadata {
            version: 0,
            topics: vec!["one".to_string(), "two".to_string()],
            user_data: vec![0x01, 0x02, 0x03],
            owned_partitions: vec![],
        };

        let mut writer = Vec::new();
        meta.write(&mut writer).unwrap();

        assert_eq!(writer, want);

        let mut buf = Cursor::new(want);
        let meta2 = ConsumerGroupMemberMetadata::read(&mut buf).unwrap();
        assert_eq!(meta, meta2);
    }

    #[test]
    fn consumer_group_member_assignment() {
        let assignment = ConsumerGroupMemberAssignment {
            version: 0,
            topics: vec![PartitionAssignment {
                topic: "one".to_string(),
                partitions: vec![0, 2, 4],
            }],
            user_data: vec![0x01, 0x02, 0x03],
        };

        let mut writer = Vec::new();
        assignment.write(&mut writer).unwrap();

        let want: [u8; 34] = [
            0x00, 0x00, // version
            0x00, 0x00, 0x00, 0x01, // topic array length
            0x00, 0x03, 0x6f, 0x6e, 0x65, // topic name
            0x00, 0x00, 0x00, 0x03, // partitions length
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
            0x04, // partition 0, 2, 4
            0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03, // userdata
        ];

        assert_eq!(writer, want);

        let mut reader = Cursor::new(want);
        let assignment2 = ConsumerGroupMemberAssignment::read(&mut reader).unwrap();

        assert_eq!(assignment, assignment2)
    }

    #[test]
    fn request_without_assignment() {
        let want = vec![
            0, 3, b'f', b'o', b'o', // Group ID
            0x00, 0x01, 0x02, 0x03, // Generation ID
            0, 3, b'b', b'a', b'z', // Member ID
            0, 0, 0, 0, // no assignments
        ];

        let req = SyncGroupRequest {
            group_id: "foo".to_string(),
            generation_id: 66051,
            member_id: "baz".to_string(),
            group_instance_id: None,
            protocol_type: None,
            protocol_name: None,
            assignments: vec![],
            tagged_fields: None,
        };

        let mut buf = Vec::new();
        req.write_versioned(&mut buf, ApiVersion::new(0)).unwrap();
        assert_eq!(buf, want);
    }

    #[test]
    fn request_with_one_assignment() {
        let want = vec![
            0, 3, b'f', b'o', b'o', // Group ID
            0x00, 0x01, 0x02, 0x03, // Generation ID
            0, 3, b'b', b'a', b'z', // Member ID
            0, 0, 0, 1, // one assignment
            0, 3, b'b', b'a', b'z', // Member ID
            0, 0, 0, 3, b'f', b'o', b'o', // Member assignment
        ];

        let assignment = Assignment {
            member_id: "baz".to_string(),
            assignment: "foo".into(),
            tagged_fields: None,
        };

        let req = SyncGroupRequest {
            group_id: "foo".to_string(),
            generation_id: 66051,
            member_id: "baz".to_string(),
            group_instance_id: None,
            protocol_type: None,
            protocol_name: None,
            assignments: vec![assignment],
            tagged_fields: None,
        };

        let mut buf = Vec::new();
        req.write_versioned(&mut buf, ApiVersion::new(0)).unwrap();
        assert_eq!(buf, want);
    }

    #[test]
    fn request_v3() {
        let req = SyncGroupRequest {
            group_id: "foo".to_string(),
            generation_id: 0x00010203,
            member_id: "baz".to_string(),
            group_instance_id: Some("gid".into()),
            protocol_type: None,
            protocol_name: None,
            assignments: vec![Assignment {
                member_id: "baz".to_string(),
                assignment: "foo".into(),
                tagged_fields: None,
            }],
            tagged_fields: None,
        };

        let want = vec![
            0, 3, b'f', b'o', b'o', // Group ID
            0x00, 0x01, 0x02, 0x03, // Generation ID
            0, 3, b'b', b'a', b'z', // Member ID
            0, 3, b'g', b'i', b'd', // GroupInstance ID
            0, 0, 0, 1, // one assignment
            0, 3, b'b', b'a', b'z', // Member ID
            0, 0, 0, 3, b'f', b'o', b'o', // Member assignment
        ];

        let mut buf = Vec::new();
        req.write_versioned(&mut buf, ApiVersion::new(3)).unwrap();
        assert_eq!(buf, want);
    }

    #[test]
    fn request_encode() {
        for (version, req, want) in [
            (
                4,
                SyncGroupRequest {
                    group_id: "group-id-1".to_string(),
                    generation_id: 10,
                    member_id: "member-id-1".to_string(),
                    group_instance_id: Some("group-instance-id".to_string()),
                    protocol_type: None,
                    protocol_name: None,
                    assignments: vec![Assignment {
                        member_id: "member-id-2".to_string(),
                        assignment: vec![0, 1, 2, 3, 4],
                        tagged_fields: None,
                    }],
                    tagged_fields: None,
                },
                vec![
                    0x0b, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x2d, 0x69, 0x64, 0x2d, 0x31, 0x00, 0x00,
                    0x00, 0x0a, 0x0c, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x2d, 0x69, 0x64, 0x2d,
                    0x31, 0x12, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x2d, 0x69, 0x6e, 0x73, 0x74, 0x61,
                    0x6e, 0x63, 0x65, 0x2d, 0x69, 0x64, 0x02, 0x0c, 0x6d, 0x65, 0x6d, 0x62, 0x65,
                    0x72, 0x2d, 0x69, 0x64, 0x2d, 0x32, 0x06, 0x00, 0x01, 0x02, 0x03, 0x04, 0x00,
                    0x00,
                ],
            ),
            (
                5,
                SyncGroupRequest {
                    group_id: "group-id-1".to_string(),
                    generation_id: 10,
                    member_id: "member-id-1".to_string(),
                    group_instance_id: Some("group-instance-id".into()),
                    protocol_type: Some("protocol-type".into()),
                    protocol_name: Some("protocol-name".into()),
                    assignments: vec![Assignment {
                        member_id: "member-id-2".to_string(),
                        assignment: vec![0, 1, 2, 3, 4],
                        tagged_fields: None,
                    }],
                    tagged_fields: None,
                },
                vec![
                    0x0b, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x2d, 0x69, 0x64, 0x2d, 0x31, 0x00, 0x00,
                    0x00, 0x0a, 0x0c, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x2d, 0x69, 0x64, 0x2d,
                    0x31, 0x12, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x2d, 0x69, 0x6e, 0x73, 0x74, 0x61,
                    0x6e, 0x63, 0x65, 0x2d, 0x69, 0x64, 0x0e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63,
                    0x6f, 0x6c, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x0e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
                    0x63, 0x6f, 0x6c, 0x2d, 0x6e, 0x61, 0x6d, 0x65, 0x02, 0x0c, 0x6d, 0x65, 0x6d,
                    0x62, 0x65, 0x72, 0x2d, 0x69, 0x64, 0x2d, 0x32, 0x06, 0x00, 0x01, 0x02, 0x03,
                    0x04, 0x00, 0x00,
                ],
            ),
        ] {
            let mut buf = Vec::new();
            req.write_versioned(&mut buf, ApiVersion::new(version))
                .unwrap();

            assert_eq!(buf, want);
        }
    }
}
