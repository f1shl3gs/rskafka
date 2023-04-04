use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_versioned_array, ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError,
    WriteVersionedType,
};
use crate::protocol::traits::{ReadType, WriteType};

pub struct DescribeGroupsRequest {
    /// The names of the groups to describe.
    pub groups: Vec<String>,

    /// Whether to include authorized operations.
    ///
    /// Added in version 3.
    pub include_authorized_operations: bool,
}

impl RequestBody for DescribeGroupsRequest {
    type ResponseBody = DescribeGroupsResponse;

    const API_KEY: ApiKey = ApiKey::DescribeGroups;

    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 4);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion::new(5);
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
        assert!(v <= 4);

        self.groups.write(writer)?;

        if v >= 3 {
            self.include_authorized_operations.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct Member {
    /// The member ID assigned by the group coordinator.
    pub member_id: String,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 4
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

impl<R> ReadVersionedType<R> for Member
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        let member_id = String::read(reader)?;
        let group_instance_id = (v >= 4).then(|| ReadType::read(reader)).transpose()?;
        let client_id = String::read(reader)?;
        let client_host = String::read(reader)?;
        let member_metadata = ReadType::read(reader)?;
        let member_assignment = ReadType::read(reader)?;

        Ok(Self {
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
pub struct Group {
    /// The describe error, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// The group ID string.
    pub group_id: String,

    /// The group state string, or the empty string.
    pub group_state: String,

    /// The group protocol type, or the empty string.
    pub protocol_type: String,

    /// The group protocol data, or the empty string.
    pub protocol_data: String,

    /// The group members.
    pub members: Vec<Member>,

    /// 32-bit bitfield to represent authorized operations for this group.
    ///
    /// Added in version 3.
    pub authorized_operations: Option<i32>,
}

impl<R> ReadVersionedType<R> for Group
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        let error_code = Error::new(i16::read(reader)?);
        let group_id = String::read(reader)?;
        let group_state = String::read(reader)?;
        let protocol_type = String::read(reader)?;
        let protocol_data = String::read(reader)?;
        let members = read_versioned_array(reader, version)?.unwrap_or_default();
        let authorized_operations = (v >= 3).then(|| i32::read(reader)).transpose()?;

        Ok(Self {
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
pub struct DescribeGroupsResponse {
    /// Each described group.
    pub groups: Vec<Group>,
}

impl<R> ReadVersionedType<R> for DescribeGroupsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let groups = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(Self { groups })
    }
}
