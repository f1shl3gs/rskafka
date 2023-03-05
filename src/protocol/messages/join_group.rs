use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_versioned_array, write_versioned_array, ReadVersionedError, ReadVersionedType,
    RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::primitives::{Bytes, Int16, Int32, NullableString, String_};
use crate::protocol::traits::{ReadType, WriteType};

/// The list of protocols that the member supports.
pub struct Protocol {
    /// The protocol name.
    pub name: String_,

    /// The protocol metadata.
    pub metadata: Bytes,
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

pub struct JoinGroupRequest {
    /// The group identifier.
    pub group_id: String_,

    /// The coordinator considers the consumer dead if it receives no heartbeat after
    /// this timeout in milliseconds.
    pub session_timeout_ms: Int32,

    /// The maximum time in milliseconds that the coordinator will wait for each
    /// member to rejoin when rebalancing the group.
    ///
    /// Added in version 2.
    pub rebalance_timeout_ms: Int32,

    /// The member id assigned by the group coordinator.
    pub member_id: String_,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 5.
    pub group_instance_id: NullableString,

    /// The unique name the for class of protocols implemented by the group we want to join.
    pub protocol_type: String_,

    /// The list of protocols that the member supports.
    pub protocols: Vec<Protocol>,
}

impl RequestBody for JoinGroupRequest {
    type ResponseBody = JoinGroupResponse;

    const API_KEY: ApiKey = ApiKey::JoinGroup;

    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(0)), ApiVersion(Int16(5)));

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(6));
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
        let v = version.0 .0;
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

pub struct Member {
    /// The group member ID.
    pub member_id: String_,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 5.
    pub group_instance_id: Option<NullableString>,

    /// The group member metadata.
    pub metadata: Bytes,
}

impl<R> ReadVersionedType<R> for Member
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 5);

        let member_id = String_::read(reader)?;
        let group_instance_id = (v >= 5).then(|| NullableString::read(reader)).transpose()?;
        let metadata = Bytes::read(reader)?;

        Ok(Self {
            member_id,
            group_instance_id,
            metadata,
        })
    }
}

pub struct JoinGroupResponse {
    /// The duration in milliseconds for which the request was throttled
    /// due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 2.
    pub throttle_time_ms: Option<Int32>,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// The generation ID of the group.
    pub generation_id: Int32,

    /// The group protocol selected by the coordinator.
    pub protocol_name: String_,

    /// The leader of the group.
    pub leader: String_,

    /// The member ID assigned by the group coordinator.
    pub member_id: String_,

    pub members: Vec<Member>,
}

impl<R> ReadVersionedType<R> for JoinGroupResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 5);

        let throttle_time_ms = (v >= 2).then(|| Int32::read(reader)).transpose()?;
        let error_code = Error::new(Int16::read(reader)?.0);
        let generation_id = Int32::read(reader)?;
        let protocol_name = String_::read(reader)?;
        let leader = String_::read(reader)?;
        let member_id = String_::read(reader)?;
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
