use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_versioned_array, write_versioned_array, ReadVersionedError, ReadVersionedType,
    RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::traits::{ReadType, WriteType};

/// The list of protocols that the member supports.
#[derive(Clone, Debug)]
pub struct Protocol {
    /// The protocol name.
    ///
    /// The value could be "range", "roundrobin" or "sticky",
    /// maybe we should use enum for this field.
    pub name: String,

    /// The protocol metadata.
    pub metadata: Vec<u8>,
}

impl<W> WriteVersionedType<W> for Protocol
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

#[derive(Clone, Debug)]
pub struct JoinGroupRequest {
    /// The group identifier.
    pub group_id: String,

    /// The coordinator considers the consumer dead if it receives no heartbeat after
    /// this timeout in milliseconds.
    pub session_timeout_ms: i32,

    /// The maximum time in milliseconds that the coordinator will wait for each
    /// member to rejoin when rebalancing the group.
    ///
    /// Added in version 2.
    pub rebalance_timeout_ms: i32,

    /// The member id assigned by the group coordinator.
    pub member_id: String,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 5.
    pub group_instance_id: Option<String>,

    /// The unique name the for class of protocols implemented by the group we want to join.
    pub protocol_type: String,

    /// The list of protocols that the member supports.
    pub protocols: Vec<Protocol>,
}

impl RequestBody for JoinGroupRequest {
    type ResponseBody = JoinGroupResponse;

    const API_KEY: ApiKey = ApiKey::JoinGroup;

    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 5);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion::new(6);
}

impl<W> WriteVersionedType<W> for JoinGroupRequest
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

        self.group_id.write(writer)?;
        self.session_timeout_ms.write(writer)?;

        if v >= 2 {
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

#[derive(Debug)]
pub struct Member {
    /// The group member ID.
    pub member_id: String,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 5.
    pub group_instance_id: Option<String>,

    /// The group member metadata.
    pub metadata: Vec<u8>,
}

impl<R> ReadVersionedType<R> for Member
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 5);

        let member_id = String::read(reader)?;
        let group_instance_id = ReadType::read(reader)?;
        let metadata = ReadType::read(reader)?;

        Ok(Self {
            member_id,
            group_instance_id,
            metadata,
        })
    }
}

#[derive(Debug)]
pub struct JoinGroupResponse {
    /// The duration in milliseconds for which the request was throttled
    /// due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 2.
    pub throttle_time_ms: Option<i32>,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// The generation ID of the group.
    pub generation_id: i32,

    /// The group protocol selected by the coordinator.
    pub protocol_name: String,

    /// The leader of the group.
    pub leader: String,

    /// The member ID assigned by the group coordinator.
    pub member_id: String,

    pub members: Vec<Member>,
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
