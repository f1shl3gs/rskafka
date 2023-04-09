use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::traits::{ReadType, WriteType};

#[derive(Debug)]
pub struct HeartbeatRequest<'a> {
    /// The group id.
    pub group_id: &'a str,

    /// The generation of the group.
    pub generation_id: i32,

    /// The member ID
    pub member_id: &'a str,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Added in version 3.
    pub group_instance_id: Option<String>,
}

impl<W> WriteVersionedType<W> for HeartbeatRequest<'_>
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
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

impl RequestBody for HeartbeatRequest<'_> {
    type ResponseBody = HeartbeatResponse;

    const API_KEY: ApiKey = ApiKey::Heartbeat;

    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 3);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion::new(4);
}

#[derive(Debug)]
pub struct HeartbeatResponse {
    /// The duration in milliseconds for which the request was throttled due to a
    /// quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,
}

impl<R> ReadVersionedType<R> for HeartbeatResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 3);

        let throttle_time_ms = i32::read(reader)?;
        let error_code = Error::new(i16::read(reader)?);

        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }
}
