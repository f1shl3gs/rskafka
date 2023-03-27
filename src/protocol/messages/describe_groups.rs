use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_versioned_array, ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError,
    WriteVersionedType,
};
use crate::protocol::primitives::{Array, Boolean, Bytes, Int16, Int32, NullableString, String_};
use crate::protocol::traits::{ReadType, WriteType};

pub struct DescribeGroupsRequest {
    /// The names of the groups to describe.
    pub groups: Array<String_>,

    /// Whether to include authorized operations.
    ///
    /// Added in version 3.
    pub include_authorized_operations: Option<Boolean>,
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

        if v >= 3 && self.include_authorized_operations.is_some() {
            match self.include_authorized_operations {
                Some(b) => b.write(writer)?,
                None => Boolean(false).write(writer)?,
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct Member {
    /// The member ID assigned by the group coordinator.
    pub member_id: String_,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 4
    pub group_instance_id: Option<NullableString>,

    /// The client ID used in the member's latest join group request.
    pub client_id: String_,

    /// The client host.
    pub client_host: String_,

    /// The metadata corresponding to the current group protocol in use.
    pub member_metadata: Bytes,

    /// The current assignment provided by the group leader.
    pub member_assignment: Bytes,
}

impl<R> ReadVersionedType<R> for Member
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        let member_id = String_::read(reader)?;
        let group_instance_id = (v >= 4).then(|| NullableString::read(reader)).transpose()?;
        let client_id = String_::read(reader)?;
        let client_host = String_::read(reader)?;
        let member_metadata = Bytes::read(reader)?;
        let member_assignment = Bytes::read(reader)?;

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
    pub group_id: String_,

    /// The group state string, or the empty string.
    pub group_state: String_,

    /// The group protocol type, or the empty string.
    pub protocol_type: String_,

    /// The group protocol data, or the empty string.
    pub protocol_data: String_,

    /// The group members.
    pub members: Vec<Member>,

    /// 32-bit bitfield to represent authorized operations for this group.
    ///
    /// Added in version 3.
    pub authorized_operations: Option<Int32>,
}

impl<R> ReadVersionedType<R> for Group
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        let error_code = Error::new(Int16::read(reader)?.0);
        let group_id = String_::read(reader)?;
        let group_state = String_::read(reader)?;
        let protocol_type = String_::read(reader)?;
        let protocol_data = String_::read(reader)?;
        let members = read_versioned_array(reader, version)?.unwrap_or_default();
        let authorized_operations = (v >= 3).then(|| Int32::read(reader)).transpose()?;

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
