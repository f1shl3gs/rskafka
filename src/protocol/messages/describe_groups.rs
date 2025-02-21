use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
    read_versioned_array,
};
use crate::protocol::traits::{ReadType, WriteType};

#[derive(Debug)]
pub struct DescribeGroupsRequest {
    /// The names of the groups to describe.
    pub groups: Vec<String>,

    /// Whether to include authorized operations.
    ///
    /// Added in version 3
    pub include_authorized_operations: bool,
}

impl<W> WriteVersionedType<W> for DescribeGroupsRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;

        self.groups.write(writer)?;
        if v >= 3 {
            self.include_authorized_operations.write(writer)?
        }

        Ok(())
    }
}

impl RequestBody for DescribeGroupsRequest {
    type ResponseBody = DescribeGroupsResponse;
    const API_KEY: ApiKey = ApiKey::DescribeGroups;
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 4);
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(5);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct DescribeGroupsResponseGroupMember {
    /// The member ID assigned by the group coordinator.
    pub member_id: String,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 4.
    pub group_instance_id: Option<String>,

    /// The client ID used in the member's latest join group request.
    pub client_id: String,

    /// The client host.
    pub client_host: String,

    /// The metadata corresponding to the current group protocol in use.
    pub member_metadata: Vec<u8>,

    /// The current assignment provided by the group leader.
    pub member_assignment: Vec<u8>,
}

impl<R> ReadVersionedType<R> for DescribeGroupsResponseGroupMember
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v < 5);

        let member_id = String::read(reader)?;
        let group_instance_id = if v > 3 {
            Option::<String>::read(reader)?
        } else {
            None
        };
        let client_id = String::read(reader)?;
        let client_host = String::read(reader)?;
        let member_metadata = Vec::<u8>::read(reader)?;
        let member_assignment = Vec::<u8>::read(reader)?;

        Ok(DescribeGroupsResponseGroupMember {
            member_id,
            group_instance_id,
            client_id,
            client_host,
            member_metadata,
            member_assignment,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct DescribeGroupsResponseGroup {
    /// The describe error, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// The group ID string.
    pub group_id: String,

    /// The group state string, or the empty string.
    pub group_state: String,

    /// The group protocol type, or the empty string,
    pub protocol_type: String,

    /// The group protocol data, or the empty string.
    pub protocol_data: String,

    /// The group members.
    pub members: Vec<DescribeGroupsResponseGroupMember>,

    /// 32-bit bitfield to represent authorized operations for this group
    ///
    /// Added in version 3
    pub authorized_operations: Option<i32>,
}

impl<R> ReadVersionedType<R> for DescribeGroupsResponseGroup
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v < 5);

        let error_code = Error::new(i16::read(reader)?);
        let group_id = String::read(reader)?;
        let group_state = String::read(reader)?;
        let protocol_type = String::read(reader)?;
        let protocol_data = String::read(reader)?;
        let members = read_versioned_array(reader, version)?.unwrap_or_default();
        let authorized_operations = (v >= 3).then(|| i32::read(reader)).transpose()?;

        Ok(DescribeGroupsResponseGroup {
            error_code,
            group_id,
            group_state,
            protocol_type,
            protocol_data,
            members,
            authorized_operations,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct DescribeGroupsResponse {
    /// The duration in milliseconds for which the request was throttled due to
    /// a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1
    pub throttle_time_ms: Option<i32>,

    /// Each described group
    pub groups: Vec<DescribeGroupsResponseGroup>,
}

impl<R> ReadVersionedType<R> for DescribeGroupsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v < 5);

        let throttle_time_ms = (v > 0).then(|| i32::read(reader)).transpose()?;
        let groups = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(DescribeGroupsResponse {
            throttle_time_ms,
            groups,
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
                "no groups",
                0,
                DescribeGroupsRequest {
                    groups: vec![],
                    include_authorized_operations: false,
                },
                [0, 0, 0, 0].as_ref(),
            ),
            (
                "one group",
                0,
                DescribeGroupsRequest {
                    groups: vec!["foo".to_string()],
                    include_authorized_operations: false,
                },
                [
                    0, 0, 0, 1, // 1 group
                    0, 3, b'f', b'o', b'o', // group name: foo
                ]
                .as_ref(),
            ),
            (
                "two groups",
                0,
                DescribeGroupsRequest {
                    groups: vec!["foo".to_string(), "bar".to_string()],
                    include_authorized_operations: false,
                },
                [
                    0, 0, 0, 2, // 2 groups
                    0, 3, b'f', b'o', b'o', // group name: foo
                    0, 3, b'b', b'a', b'r', // group name: foo
                ]
                .as_ref(),
            ),
            (
                "no groups",
                3,
                DescribeGroupsRequest {
                    groups: vec![],
                    include_authorized_operations: false,
                },
                [0, 0, 0, 0, 0].as_ref(),
            ),
            (
                "one group",
                3,
                DescribeGroupsRequest {
                    groups: vec!["foo".to_string()],
                    include_authorized_operations: false,
                },
                [
                    0, 0, 0, 1, // 1 group
                    0, 3, b'f', b'o', b'o', // group name: foo
                    0,
                ]
                .as_ref(),
            ),
            (
                "two groups",
                3,
                DescribeGroupsRequest {
                    groups: vec!["foo".to_string(), "bar".to_string()],
                    include_authorized_operations: true,
                },
                [
                    0, 0, 0, 2, // 2 groups
                    0, 3, b'f', b'o', b'o', // group name: foo
                    0, 3, b'b', b'a', b'r', // group name: foo
                    1,
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
                "empty",
                0,
                DescribeGroupsResponse {
                    throttle_time_ms: None,
                    groups: vec![],
                },
                [
                    0, 0, 0, 0, // no groups
                ]
                .as_ref(),
            ),
            (
                "populated",
                0,
                DescribeGroupsResponse {
                    throttle_time_ms: None,
                    groups: vec![
                        DescribeGroupsResponseGroup {
                            error_code: None,
                            group_id: "foo".to_string(),
                            group_state: "bar".to_string(),
                            protocol_type: "consumer".to_string(),
                            protocol_data: "baz".to_string(),
                            members: vec![DescribeGroupsResponseGroupMember {
                                member_id: "id".to_string(),
                                group_instance_id: None,
                                client_id: "sarama".to_string(),
                                client_host: "localhost".to_string(),
                                member_metadata: vec![0x01, 0x02, 0x03],
                                member_assignment: vec![0x04, 0x05, 0x06],
                            }],
                            authorized_operations: None,
                        },
                        DescribeGroupsResponseGroup {
                            error_code: Some(Error::GroupAuthorizationFailed),
                            group_id: "".to_string(),
                            group_state: "".to_string(),
                            protocol_type: "".to_string(),
                            protocol_data: "".to_string(),
                            members: vec![],
                            authorized_operations: None,
                        },
                    ],
                },
                [
                    0, 0, 0, 2, // 2 groups
                    0, 0, // no error
                    0, 3, b'f', b'o', b'o', // Group ID
                    0, 3, b'b', b'a', b'r', // State
                    0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e',
                    b'r', // ConsumerProtocol type
                    0, 3, b'b', b'a', b'z', // Protocol name
                    0, 0, 0, 1, // 1 member
                    0, 2, b'i', b'd', // Member ID
                    0, 6, b's', b'a', b'r', b'a', b'm', b'a', // Client ID
                    0, 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', // Client Host
                    0, 0, 0, 3, 0x01, 0x02, 0x03, // MemberMetadata
                    0, 0, 0, 3, 0x04, 0x05, 0x06, // MemberAssignment
                    0, 30, // ErrGroupAuthorizationFailed
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ]
                .as_ref(),
            ),
            (
                "empty",
                3,
                DescribeGroupsResponse {
                    throttle_time_ms: Some(0),
                    groups: vec![],
                },
                [
                    0, 0, 0, 0, // throttle time 0
                    0, 0, 0, 0, // no groups
                ]
                .as_ref(),
            ),
            (
                "populated",
                3,
                DescribeGroupsResponse {
                    throttle_time_ms: Some(0),
                    groups: vec![
                        DescribeGroupsResponseGroup {
                            error_code: None,
                            group_id: "foo".to_string(),
                            group_state: "bar".to_string(),
                            protocol_type: "consumer".to_string(),
                            protocol_data: "baz".to_string(),
                            members: vec![DescribeGroupsResponseGroupMember {
                                member_id: "id".to_string(),
                                group_instance_id: None,
                                client_id: "sarama".to_string(),
                                client_host: "localhost".to_string(),
                                member_metadata: vec![0x01, 0x02, 0x03],
                                member_assignment: vec![0x04, 0x05, 0x06],
                            }],
                            authorized_operations: Some(0),
                        },
                        DescribeGroupsResponseGroup {
                            error_code: Some(Error::GroupAuthorizationFailed),
                            group_id: "".to_string(),
                            group_state: "".to_string(),
                            protocol_type: "".to_string(),
                            protocol_data: "".to_string(),
                            members: vec![],
                            authorized_operations: Some(0),
                        },
                    ],
                },
                [
                    0, 0, 0, 0, // throttle time 0
                    0, 0, 0, 2, // 2 groups
                    0, 0, // no error
                    0, 3, b'f', b'o', b'o', // Group ID
                    0, 3, b'b', b'a', b'r', // State
                    0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e',
                    b'r', // ConsumerProtocol type
                    0, 3, b'b', b'a', b'z', // Protocol name
                    0, 0, 0, 1, // 1 member
                    0, 2, b'i', b'd', // Member ID
                    0, 6, b's', b'a', b'r', b'a', b'm', b'a', // Client ID
                    0, 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', // Client Host
                    0, 0, 0, 3, 0x01, 0x02, 0x03, // MemberMetadata
                    0, 0, 0, 3, 0x04, 0x05, 0x06, // MemberAssignment
                    0, 0, 0, 0, // authorizedOperations 0
                    0, 30, // ErrGroupAuthorizationFailed
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // authorizedOperations 0
                ]
                .as_ref(),
            ),
            (
                "empty",
                4,
                DescribeGroupsResponse {
                    throttle_time_ms: Some(0),
                    groups: vec![],
                },
                [
                    0, 0, 0, 0, // throttle time 0
                    0, 0, 0, 0, // no groups
                ]
                .as_ref(),
            ),
            (
                "populated",
                4,
                DescribeGroupsResponse {
                    throttle_time_ms: Some(0),
                    groups: vec![
                        DescribeGroupsResponseGroup {
                            error_code: None,
                            group_id: "foo".to_string(),
                            group_state: "bar".to_string(),
                            protocol_type: "consumer".to_string(),
                            protocol_data: "baz".to_string(),
                            members: vec![DescribeGroupsResponseGroupMember {
                                member_id: "id".to_string(),
                                group_instance_id: Some("gid".to_string()),
                                client_id: "sarama".to_string(),
                                client_host: "localhost".to_string(),
                                member_metadata: vec![0x01, 0x02, 0x03],
                                member_assignment: vec![0x04, 0x05, 0x06],
                            }],
                            authorized_operations: Some(0),
                        },
                        DescribeGroupsResponseGroup {
                            error_code: Some(Error::GroupAuthorizationFailed),
                            group_id: "".to_string(),
                            group_state: "".to_string(),
                            protocol_type: "".to_string(),
                            protocol_data: "".to_string(),
                            members: vec![],
                            authorized_operations: Some(0),
                        },
                    ],
                },
                [
                    0, 0, 0, 0, // throttle time 0
                    0, 0, 0, 2, // 2 groups
                    0, 0, // no error
                    0, 3, b'f', b'o', b'o', // Group ID
                    0, 3, b'b', b'a', b'r', // State
                    0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e',
                    b'r', // ConsumerProtocol type
                    0, 3, b'b', b'a', b'z', // Protocol name
                    0, 0, 0, 1, // 1 member
                    0, 2, b'i', b'd', // Member ID
                    0, 3, b'g', b'i', b'd', // Group Instance ID
                    0, 6, b's', b'a', b'r', b'a', b'm', b'a', // Client ID
                    0, 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', // Client Host
                    0, 0, 0, 3, 0x01, 0x02, 0x03, // MemberMetadata
                    0, 0, 0, 3, 0x04, 0x05, 0x06, // MemberAssignment
                    0, 0, 0, 0, // authorizedOperations 0
                    0, 30, // ErrGroupAuthorizationFailed
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // authorizedOperations 0
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let got =
                DescribeGroupsResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(got, want, "{name}/{version}");
        }
    }
}
