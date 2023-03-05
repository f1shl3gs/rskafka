use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::primitives::{Int16, Int32, NullableString, String_};
use crate::protocol::traits::{ReadType, WriteType};

#[derive(Debug)]
pub struct HeartbeatRequest {
    /// The group id.
    pub group_id: String_,

    /// The generation of the group.
    pub generation_id: Int32,

    /// The member ID
    pub member_id: String_,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 3.
    pub group_instance_id: NullableString,
}

impl<W> WriteVersionedType<W> for HeartbeatRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 3);

        self.group_id.write(writer)?;
        self.generation_id.write(writer)?;
        self.member_id.write(writer)?;

        if v >= 3 {
            self.group_instance_id.write(writer)?;
        }

        Ok(())
    }
}

impl RequestBody for HeartbeatRequest {
    type ResponseBody = HeartbeatResponse;

    const API_KEY: ApiKey = ApiKey::Heartbeat;

    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(0)), ApiVersion(Int16(3)));

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(4));
}

#[derive(Debug)]
pub struct HeartbeatResponse {
    /// The duration in milliseconds for which the request was throttled due to a
    /// quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: Int32,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,
}

impl<R> ReadVersionedType<R> for HeartbeatResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 3);

        let throttle_time_ms = Int32::read(reader)?;
        let error_code = Error::new(Int16::read(reader)?.0);

        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }
}
