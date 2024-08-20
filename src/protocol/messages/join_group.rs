use std::io::{Cursor, Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::consumer_group::ConsumerGroupMemberMetadata;
use crate::protocol::messages::{
    read_versioned_array, write_versioned_array, ReadVersionedError, ReadVersionedType,
    RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::traits::{ReadType, WriteType};

/// The list of protocols that the member supports.
#[derive(Debug)]
pub struct JoinGroupProtocol {
    /// The protocol name.
    ///
    /// The value could be "range", "roundrobin" or "sticky",
    pub name: String,

    /// The protocol metadata
    pub metadata: Vec<u8>,
}

impl<W> WriteVersionedType<W> for JoinGroupProtocol
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        _version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        self.name.write(writer)?;
        self.metadata.write(writer)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct JoinGroupRequest {
    /// The group identifier
    pub group_id: String,

    /// the coordinator considers the consumer dead if it receives no heartbeat
    /// after this timeout in milliseconds.
    pub session_timeout_ms: i32,

    /// The maximum time in milliseconds that the coordinator will wait for
    /// each member to rejoin when rebalancing the group.
    ///
    /// Added in version 1.
    pub rebalance_timeout_ms: i32,

    /// The member id assigned by the group coordinator.
    pub member_id: String,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 5.
    pub group_instance_id: Option<String>,

    /// The unique name the for class of protocols implemented by the group we
    /// want to join.
    pub protocol_type: String,

    /// The list of protocols that the member supports.
    pub protocols: Vec<JoinGroupProtocol>,
}

impl<W: Write> WriteVersionedType<W> for JoinGroupRequest {
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        self.group_id.write(writer)?;
        self.session_timeout_ms.write(writer)?;

        if v >= 1 {
            self.rebalance_timeout_ms.write(writer)?;
        }

        self.member_id.write(writer)?;
        if v >= 5 {
            self.group_instance_id.write(writer)?;
        }

        self.protocol_type.write(writer)?;
        write_versioned_array(writer, version, Some(&self.protocols))?;

        Ok(())
    }
}

impl RequestBody for JoinGroupRequest {
    type ResponseBody = JoinGroupResponse;
    const API_KEY: ApiKey = ApiKey::JoinGroup;
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 5);
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(6);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct JoinGroupResponseMember {
    /// The group member ID.
    pub member_id: String,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 5.
    pub group_instance_id: Option<String>,

    /// the group member metadata.
    pub metadata: ConsumerGroupMemberMetadata,
}

impl<R> ReadVersionedType<R> for JoinGroupResponseMember
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let member_id = String::read(reader)?;

        let group_instance_id = if v >= 5 {
            Option::<String>::read(reader)?
        } else {
            None
        };

        let buf = Vec::<u8>::read(reader)?;
        let mut buf = Cursor::new(buf);
        let metadata = ReadType::read(&mut buf)?;

        Ok(Self {
            member_id,
            group_instance_id,
            metadata,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct JoinGroupResponse {
    /// The duration in milliseconds for which the request was throttled
    /// due to a quota violation, or zero if the request did not violate
    /// any quota.
    ///
    /// Added in version 2
    pub throttle_time_ms: Option<i32>,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// The generation ID of the group.
    pub generation_id: i32,

    /// The group protocol selected by the coordinator
    pub protocol_name: String,

    /// The leader of the group.
    pub leader: String,

    /// The member ID assigned by the group coordinator.
    pub member_id: String,

    /// The members of the group.
    pub members: Vec<JoinGroupResponseMember>,
}

impl<R> ReadVersionedType<R> for JoinGroupResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let throttle_time_ms = (v >= 2).then(|| i32::read(reader)).transpose()?;
        let error_code = Error::new(i16::read(reader)?);
        let generation_id = i32::read(reader)?;
        let protocol_name = String::read(reader)?;
        let leader = String::read(reader)?;
        let member_id = String::read(reader)?;
        let members = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_name,
            leader,
            member_id,
            members,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request() {
        for (name, version, req, want) in [
            (
                "no protocols",
                0,
                JoinGroupRequest {
                    group_id: "TestGroup".to_string(),
                    session_timeout_ms: 100,
                    rebalance_timeout_ms: 0,
                    member_id: "".to_string(),
                    group_instance_id: None,
                    protocol_type: "consumer".to_string(),
                    protocols: vec![],
                },
                [
                    0u8, 9, b'T', b'e', b's', b't', b'G', b'r', b'o', b'u', b'p', // Group ID
                    0, 0, 0, 100, // Session timeout
                    0, 0, // Member ID
                    0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', // Protocol Type
                    0, 0, 0, 0, // 0 protocol groups
                ]
                .as_ref(),
            ),
            (
                "one protocol",
                0,
                JoinGroupRequest {
                    group_id: "TestGroup".to_string(),
                    session_timeout_ms: 100,
                    rebalance_timeout_ms: 0,
                    member_id: "OneProtocol".to_string(),
                    group_instance_id: None,
                    protocol_type: "consumer".to_string(),
                    protocols: vec![JoinGroupProtocol {
                        name: "one".to_string(),
                        metadata: vec![0x01, 0x02, 0x03],
                    }],
                },
                [
                    0, 9, b'T', b'e', b's', b't', b'G', b'r', b'o', b'u', b'p', // Group ID
                    0, 0, 0, 100, // Session timeout
                    0, 11, b'O', b'n', b'e', b'P', b'r', b'o', b't', b'o', b'c', b'o',
                    b'l', // Member ID
                    0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', // Protocol Type
                    0, 0, 0, 1, // 1 group protocol
                    0, 3, b'o', b'n', b'e', // Protocol name
                    0, 0, 0, 3, 0x01, 0x02, 0x03, // protocol metadata
                ]
                .as_ref(),
            ),
            (
                "v1 one protocol",
                1,
                JoinGroupRequest {
                    group_id: "TestGroup".to_string(),
                    session_timeout_ms: 100,
                    rebalance_timeout_ms: 200,
                    member_id: "OneProtocol".to_string(),
                    group_instance_id: None,
                    protocol_type: "consumer".to_string(),
                    protocols: vec![JoinGroupProtocol {
                        name: "one".to_string(),
                        metadata: vec![0x01, 0x02, 0x03],
                    }],
                },
                [
                    0, 9, b'T', b'e', b's', b't', b'G', b'r', b'o', b'u', b'p', // Group ID
                    0, 0, 0, 100, // Session timeout
                    0, 0, 0, 200, // Rebalance timeout
                    0, 11, b'O', b'n', b'e', b'P', b'r', b'o', b't', b'o', b'c', b'o',
                    b'l', // Member ID
                    0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', // Protocol Type
                    0, 0, 0, 1, // 1 group protocol
                    0, 3, b'o', b'n', b'e', // Protocol name
                    0, 0, 0, 3, 0x01, 0x02, 0x03, // protocol metadata
                ]
                .as_ref(),
            ),
            (
                "v5",
                5,
                JoinGroupRequest {
                    group_id: "TestGroup".to_string(),
                    session_timeout_ms: 100,
                    rebalance_timeout_ms: 200,
                    member_id: "OneProtocol".to_string(),
                    group_instance_id: Some("gid".into()),
                    protocol_type: "consumer".to_string(),
                    protocols: vec![JoinGroupProtocol {
                        name: "one".to_string(),
                        metadata: vec![0x01, 0x02, 0x03],
                    }],
                },
                [
                    0, 9, b'T', b'e', b's', b't', b'G', b'r', b'o', b'u', b'p', // Group ID
                    0, 0, 0, 100, // Session timeout
                    0, 0, 0, 200, // Rebalance timeout
                    0, 11, b'O', b'n', b'e', b'P', b'r', b'o', b't', b'o', b'c', b'o',
                    b'l', // Member ID
                    0, 3, b'g', b'i', b'd', // GroupInstanceId
                    0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', // Protocol Type
                    0, 0, 0, 1, // 1 group protocol
                    0, 3, b'o', b'n', b'e', // Protocol name
                    0, 0, 0, 3, 0x01, 0x02, 0x03, // protocol metadata
                ]
                .as_ref(),
            ),
        ] {
            let mut cursor = Cursor::new([0u8; 128]);
            req.write_versioned(&mut cursor, ApiVersion(version))
                .unwrap();
            let len = cursor.position() as usize;
            let got = &cursor.get_ref()[..len];
            assert_eq!(got, want, "{name}/{version}");
        }
    }

    #[test]
    fn response() {
        for (name, version, want, data) in [
            (
                "no error",
                0,
                JoinGroupResponse {
                    throttle_time_ms: None,
                    error_code: None,
                    generation_id: 66051,
                    protocol_name: "protocol".to_string(),
                    leader: "foo".to_string(),
                    member_id: "bar".to_string(),
                    members: vec![],
                },
                [
                    0x00, 0x00, // No error
                    0x00, 0x01, 0x02, 0x03, // Generation ID
                    0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o',
                    b'l', // Protocol name chosen
                    0, 3, b'f', b'o', b'o', // Leader ID
                    0, 3, b'b', b'a', b'r', // Member ID
                    0, 0, 0, 0, // No member info
                ]
                .as_ref(),
            ),
            (
                "with error",
                0,
                JoinGroupResponse {
                    throttle_time_ms: None,
                    error_code: Some(Error::InconsistentGroupProtocol),
                    generation_id: 0,
                    protocol_name: "".to_string(),
                    leader: "".to_string(),
                    member_id: "".to_string(),
                    members: vec![],
                },
                [
                    0, 23, // Error: inconsistent group protocol
                    0x00, 0x00, 0x00, 0x00, // Generation ID
                    0, 0, // Protocol name chosen
                    0, 0, // Leader ID
                    0, 0, // Member ID
                    0, 0, 0, 0, // No member info
                ]
                .as_ref(),
            ),
            (
                "leader",
                0,
                JoinGroupResponse {
                    throttle_time_ms: None,
                    error_code: None,
                    generation_id: 66051,
                    protocol_name: "protocol".to_string(),
                    leader: "foo".to_string(),
                    member_id: "foo".to_string(),
                    members: vec![JoinGroupResponseMember {
                        member_id: "foo".to_string(),
                        group_instance_id: None,
                        metadata: ConsumerGroupMemberMetadata {
                            version: 0,
                            topics: vec!["one".into(), "two".into()],
                            user_data: vec![0x01, 0x02, 0x03],
                            owned_partitions: vec![],
                            generation_id: 0,
                            rack_id: None,
                        },
                    }],
                },
                [
                    0x00, 0x00, // No error
                    0x00, 0x01, 0x02, 0x03, // Generation ID
                    0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o',
                    b'l', // Protocol name chosen
                    0, 3, b'f', b'o', b'o', // Leader ID
                    0, 3, b'f', b'o', b'o', // Member ID == Leader ID
                    0, 0, 0, 1, // 1 member
                    0, 3, b'f', b'o', b'o', // Member ID
                    0, 0, 0, 23, // Member metadata
                    // members
                    0, 0, // Version
                    0, 0, 0, 2, // Topic array length
                    0, 3, b'o', b'n', b'e', // Topic one
                    0, 3, b't', b'w', b'o', // Topic two
                    0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
                ]
                .as_ref(),
            ),
            (
                "v1",
                1,
                JoinGroupResponse {
                    throttle_time_ms: None,
                    error_code: None,
                    generation_id: 66051,
                    protocol_name: "protocol".to_string(),
                    leader: "foo".to_string(),
                    member_id: "bar".to_string(),
                    members: vec![],
                },
                [
                    0x00, 0x00, // No error
                    0x00, 0x01, 0x02, 0x03, // Generation ID
                    0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o',
                    b'l', // Protocol name chosen
                    0, 3, b'f', b'o', b'o', // Leader ID
                    0, 3, b'b', b'a', b'r', // Member ID
                    0, 0, 0, 0, // No member info
                ]
                .as_ref(),
            ),
            (
                "v2",
                2,
                JoinGroupResponse {
                    throttle_time_ms: Some(100),
                    error_code: None,
                    generation_id: 66051,
                    protocol_name: "protocol".to_string(),
                    leader: "foo".to_string(),
                    member_id: "bar".to_string(),
                    members: vec![],
                },
                [
                    0, 0, 0, 100, 0x00, 0x00, // No error
                    0x00, 0x01, 0x02, 0x03, // Generation ID
                    0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o',
                    b'l', // Protocol name chosen
                    0, 3, b'f', b'o', b'o', // Leader ID
                    0, 3, b'b', b'a', b'r', // Member ID
                    0, 0, 0, 0, // No member info
                ]
                .as_ref(),
            ),
            (
                "v5",
                5,
                JoinGroupResponse {
                    throttle_time_ms: Some(100),
                    error_code: None,
                    generation_id: 0x00010203,
                    protocol_name: "protocol".to_string(),
                    leader: "foo".to_string(),
                    member_id: "bar".to_string(),
                    members: vec![JoinGroupResponseMember {
                        member_id: "mid".to_string(),
                        group_instance_id: Some("gid".into()),
                        metadata: ConsumerGroupMemberMetadata {
                            version: 0,
                            topics: vec!["one".into(), "two".into()],
                            user_data: vec![0x01, 0x02, 0x03],
                            owned_partitions: vec![],
                            generation_id: 0,
                            rack_id: None,
                        },
                    }],
                },
                [
                    0, 0, 0, 100, // ThrottleTimeMs
                    0x00, 0x00, // No error
                    0x00, 0x01, 0x02, 0x03, // Generation ID
                    0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o',
                    b'l', // Protocol name chosen
                    0, 3, b'f', b'o', b'o', // Leader ID
                    0, 3, b'b', b'a', b'r', // Member ID
                    0, 0, 0, 1, // One member info
                    0, 3, b'm', b'i', b'd', // memberId
                    0, 3, b'g', b'i', b'd', // GroupInstanceId
                    0, 0, 0, 23, // Metadata
                    0, 0, // Version
                    0, 0, 0, 2, // Topic array length
                    0, 3, b'o', b'n', b'e', // Topic one
                    0, 3, b't', b'w', b'o', // Topic two
                    0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let got = JoinGroupResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(got, want, "{name}/{version}");
        }
    }
}
