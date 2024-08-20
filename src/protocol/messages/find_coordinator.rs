use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::primitives::TaggedFields;
use crate::protocol::traits::{ReadCompactType, ReadType, WriteCompactType, WriteType};

#[derive(Copy, Clone, Debug)]
pub enum CoordinatorType {
    Group,
    Transaction,
}

impl CoordinatorType {
    pub fn as_i8(&self) -> i8 {
        match self {
            CoordinatorType::Group => 0,
            CoordinatorType::Transaction => 1,
        }
    }
}

#[derive(Debug)]
pub struct FindCoordinatorRequest {
    /// The coordinator key.
    ///
    /// STRING < 3
    /// COMPACT_STRING >= 3
    pub key: String,

    /// The coordinator key type. (Group, transaction, etc.)
    pub key_type: CoordinatorType,

    /// The tagged_fields.
    ///
    /// Added in version 3.
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for FindCoordinatorRequest
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

        if v < 3 {
            self.key.write(writer)?;
        } else if v == 3 {
            self.key.write_compact(writer)?;
        } else {
            // removed in version 4
        }

        let key_type = self.key_type.as_i8();
        key_type.write(writer)?;

        if v >= 3 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

impl RequestBody for FindCoordinatorRequest {
    type ResponseBody = FindCoordinatorResponse;
    const API_KEY: ApiKey = ApiKey::FindCoordinator;
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(1, 3);
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(3);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct FindCoordinatorResponse {
    /// The duration in milliseconds for which the request was throttled due
    /// to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1
    pub throttle_time_ms: Option<i32>,

    /// The error code, or 0 if there was no error.
    ///
    /// Removed in version 4.
    pub error_code: Option<Error>,

    /// The error message, or null if there was no error.
    ///
    /// Added in version 1
    /// NULLABLE_STRING >= 1
    /// COMPACT_NULLABLE_STRING == 3
    /// Removed in version 4
    pub error_message: Option<String>,

    /// The node id
    ///
    /// Removed in version 4
    pub node_id: i32,

    /// The hostname
    ///
    /// STRING < 3
    /// COMPACT_STRING == 3
    /// Removed in version 4
    pub host: String,

    /// The port
    ///
    /// Removed in version 4
    pub port: i32,

    /// The tagged fields.
    ///
    /// Added in version 3
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for FindCoordinatorResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 3);

        let throttle_time_ms = (v >= 1).then(|| i32::read(reader)).transpose()?;
        let error_code = if v < 4 {
            Error::new(i16::read(reader)?)
        } else {
            None
        };

        let error_message = if v == 3 {
            ReadCompactType::read_compact(reader)?
        } else if v >= 1 {
            ReadType::read(reader)?
        } else {
            None
        };

        let node_id = i32::read(reader)?;
        let host = if v < 3 {
            String::read(reader)?
        } else if v == 3 {
            String::read_compact(reader)?
        } else {
            // V >= 4
            String::new()
        };

        let port = i32::read(reader)?;

        let tagged_fields = (v >= 3).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            node_id,
            host,
            port,
            tagged_fields,
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
                "group",
                1,
                FindCoordinatorRequest {
                    key: "group".to_string(),
                    key_type: CoordinatorType::Group,
                    tagged_fields: None,
                },
                [0, 5, b'g', b'r', b'o', b'u', b'p', 0].as_slice(),
            ),
            (
                "transaction",
                1,
                FindCoordinatorRequest {
                    key: "transactionid".to_string(),
                    key_type: CoordinatorType::Transaction,
                    tagged_fields: None,
                },
                [
                    0, 13, b't', b'r', b'a', b'n', b's', b'a', b'c', b't', b'i', b'o', b'n', b'i',
                    b'd', 1,
                ]
                .as_slice(),
            ),
        ] {
            let mut cursor = Cursor::new([0u8; 128]);
            req.write_versioned(&mut cursor, ApiVersion(version))
                .unwrap();
            let len = cursor.position() as usize;
            let got = &cursor.get_ref()[..len];
            assert_eq!(got, want, "{name}");
        }
    }

    #[test]
    fn response() {
        for (name, version, want, data) in [
            (
                "no error",
                0,
                FindCoordinatorResponse {
                    throttle_time_ms: None,
                    error_code: None,
                    error_message: None,
                    node_id: 7,
                    host: "host".to_string(),
                    port: 9092,
                    tagged_fields: None,
                },
                [
                    0, 0, // Err
                    0, 0, 0, 7, // Node ID
                    0, 4, b'h', b'o', b's', b't', // Host
                    0, 0, 35, 132, // Port
                ]
                .as_ref(),
            ),
            (
                "no error",
                1,
                FindCoordinatorResponse {
                    throttle_time_ms: Some(100),
                    error_code: None,
                    error_message: None,
                    node_id: 7,
                    host: "host".to_string(),
                    port: 9092,
                    tagged_fields: None,
                },
                [
                    0, 0, 0, 100, // ThrottleTime
                    0, 0, // Err
                    255, 255, // ErrMsg: empty
                    0, 0, 0, 7, // Coordinator.ID
                    0, 4, b'h', b'o', b's', b't', // Coordinator.Host
                    0, 0, 35, 132, // Coordinator.Port
                ]
                .as_ref(),
            ),
            (
                "error",
                0,
                FindCoordinatorResponse {
                    throttle_time_ms: None,
                    error_code: Some(Error::CoordinatorNotAvailable),
                    error_message: None,
                    node_id: -1,
                    host: "".to_string(),
                    port: -1,
                    tagged_fields: None,
                },
                [
                    0, 15, // Err
                    255, 255, 255, 255, // Coordinator.ID: -1
                    0, 0, // Coordinator.Host: ""
                    255, 255, 255, 255, // Coordinator.Port: -1
                ]
                .as_ref(),
            ),
            (
                "error",
                1,
                FindCoordinatorResponse {
                    throttle_time_ms: Some(100),
                    error_code: Some(Error::CoordinatorNotAvailable),
                    error_message: Some("kaboom".into()),
                    node_id: -1,
                    host: "".to_string(),
                    port: -1,
                    tagged_fields: None,
                },
                [
                    0, 0, 0, 100, // ThrottleTime
                    0, 15, // Err
                    0, 6, b'k', b'a', b'b', b'o', b'o', b'm', // ErrMsg
                    255, 255, 255, 255, // Coordinator.ID: -1
                    0, 0, // Coordinator.Host: ""
                    255, 255, 255, 255, // Coordinator.Port: -1
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let resp =
                FindCoordinatorResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(resp, want, "{name}/{version}")
        }
    }
}
