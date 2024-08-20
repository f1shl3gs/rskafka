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
    /// The group id
    pub group_id: &'a str,

    /// The generation of the group.
    pub generation_id: i32,

    /// The member ID
    pub member_id: &'a str,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// support KIP-345
    pub group_instance_id: Option<&'a str>,
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
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(4);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
#[allow(missing_copy_implementations)]
pub struct HeartbeatResponse {
    /// The duration in milliseconds for which the request was throttled due
    /// to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1
    pub throttle_time_ms: Option<i32>,

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

        let throttle_time_ms = (v >= 1).then(|| i32::read(reader)).transpose()?;
        let error_code = Error::new(i16::read(reader)?);

        Ok(Self {
            throttle_time_ms,
            error_code,
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
                "v0",
                0,
                HeartbeatRequest {
                    group_id: "foo",
                    generation_id: 0x00010203,
                    member_id: "baz",
                    group_instance_id: None,
                },
                [
                    0, 3, b'f', b'o', b'o', // Group ID
                    0x00, 0x01, 0x02, 0x03, // Generation ID
                    0, 3, b'b', b'a', b'z', // Member ID
                ]
                .as_ref(),
            ),
            (
                "v3",
                3,
                HeartbeatRequest {
                    group_id: "foo",
                    generation_id: 0x00010203,
                    member_id: "baz",
                    group_instance_id: Some("gid".into()),
                },
                [
                    0, 3, b'f', b'o', b'o', // Group ID
                    0x00, 0x01, 0x02, 0x03, // Generation ID
                    0, 3, b'b', b'a', b'z', // Member ID
                    0, 3, b'g', b'i', b'd', // Group Instance ID
                ]
                .as_ref(),
            ),
            (
                "v3 - no instance",
                3,
                HeartbeatRequest {
                    group_id: "foo",
                    generation_id: 0x00010203,
                    member_id: "baz",
                    group_instance_id: None,
                },
                [
                    0, 3, b'f', b'o', b'o', // Group ID
                    0x00, 0x01, 0x02, 0x03, // Generation ID
                    0, 3, b'b', b'a', b'z', // Member ID
                    255, 255, // Group Instance ID
                ]
                .as_ref(),
            ),
        ] {
            let mut buf = Cursor::new([0u8; 128]);
            req.write_versioned(&mut buf, ApiVersion(version)).unwrap();
            let len = buf.position() as usize;
            let got = &buf.get_ref().as_slice()[..len];
            assert_eq!(got, want, "{name}/{version}");
        }
    }

    #[test]
    fn response() {
        for (name, version, want, data) in [
            (
                "v0 - no error",
                0,
                HeartbeatResponse {
                    throttle_time_ms: None,
                    error_code: None,
                },
                [0x00, 0x00].as_ref(),
            ),
            (
                "v1 - no error",
                1,
                HeartbeatResponse {
                    throttle_time_ms: Some(100),
                    error_code: None,
                },
                [0, 0, 0, 100, 0, 0].as_ref(),
            ),
            (
                "v1 - err",
                1,
                HeartbeatResponse {
                    throttle_time_ms: Some(100),
                    error_code: Some(Error::FencedInstanceId),
                },
                [0, 0, 0, 100, 0, 82].as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let resp = HeartbeatResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(resp, want, "{name}/{version}")
        }
    }
}
