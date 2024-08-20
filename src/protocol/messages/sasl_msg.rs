use std::io::{Read, Write};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error as ApiError,
    primitives::TaggedFields,
    traits::{ReadCompactType, ReadType, WriteCompactType, WriteType},
};

#[derive(Debug)]
pub struct SaslHandshakeRequest {
    /// The SASL mechanism chosen by the client. e.g. PLAIN
    pub mechanism: String,
}

impl SaslHandshakeRequest {
    pub fn new(mechanism: &str) -> Self {
        Self {
            mechanism: mechanism.to_string(),
        }
    }
}

impl<R> ReadVersionedType<R> for SaslHandshakeRequest
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v == 1);
        Ok(Self {
            mechanism: String::read(reader)?,
        })
    }
}

impl<W> WriteVersionedType<W> for SaslHandshakeRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v == 1);
        self.mechanism.write(writer)?;
        Ok(())
    }
}

impl RequestBody for SaslHandshakeRequest {
    type ResponseBody = SaslHandshakeResponse;
    const API_KEY: ApiKey = ApiKey::SaslHandshake;
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(1, 1);
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(3);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct SaslHandshakeResponse {
    /// The error code, or 0 if there was no error.
    pub error_code: Option<ApiError>,

    /// The mechanisms enabled in the server.
    pub mechanisms: Vec<String>,
}

impl<R> ReadVersionedType<R> for SaslHandshakeResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v == 1);
        Ok(Self {
            error_code: ApiError::new(i16::read(reader)?),
            mechanisms: Vec::<String>::read(reader)?,
        })
    }
}

impl<W> WriteVersionedType<W> for SaslHandshakeResponse
where
    W: Write,
{
    fn write_versioned(
        &self,
        _writer: &mut W,
        _version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct SaslAuthenticateRequest {
    /// The SASL authentication bytes from the client, as defined by the SASL mechanism.
    ///
    /// The type changes to CompactBytes in version 2.
    pub auth_bytes: Vec<u8>,

    /// The tagged fields
    ///
    /// Added in version 2.
    pub tagged_fields: Option<TaggedFields>,
}

impl SaslAuthenticateRequest {
    pub fn new(auth_bytes: Vec<u8>) -> Self {
        Self {
            auth_bytes,
            tagged_fields: Some(TaggedFields::default()),
        }
    }
}

impl<R> ReadVersionedType<R> for SaslAuthenticateRequest
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 2);
        if v == 0 || v == 1 {
            Ok(Self::new(Vec::<u8>::read(reader)?))
        } else {
            Ok(Self {
                auth_bytes: Vec::<u8>::read_compact(reader)?,
                tagged_fields: Some(TaggedFields::read(reader)?),
            })
        }
    }
}

impl<W> WriteVersionedType<W> for SaslAuthenticateRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 2);
        if v == 0 || v == 1 {
            self.auth_bytes.write(writer)?;
        } else {
            self.auth_bytes.write_compact(writer)?;
            self.tagged_fields.write(writer)?;
        }
        Ok(())
    }
}

impl RequestBody for SaslAuthenticateRequest {
    type ResponseBody = SaslAuthenticateResponse;
    const API_KEY: ApiKey = ApiKey::SaslAuthenticate;
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 2);
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(2);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct SaslAuthenticateResponse {
    /// The error code, or 0 if there was no error.
    pub error_code: Option<ApiError>,

    /// The error message, or none if there was no error.
    ///
    /// Type changed to COMPACT_NULLABLE_STRING in version 2.
    pub error_message: Option<String>,

    /// The SASL authentication bytes from the server, as defined by the SASL mechanism.
    ///
    /// Type changed to CompactBytes in version 2.
    pub auth_bytes: Vec<u8>,

    /// The SASL authentication bytes from the server, as defined by the SASL mechanism.
    ///
    /// Added in version 1.
    pub session_lifetime_ms: Option<i64>,

    /// The tagged fields.
    ///
    /// Added in version 2.
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for SaslAuthenticateResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 2);
        if v == 0 {
            Ok(Self {
                error_code: ApiError::new(i16::read(reader)?),
                error_message: Option::<String>::read(reader)?,
                auth_bytes: Vec::<u8>::read(reader)?,
                session_lifetime_ms: None,
                tagged_fields: None,
            })
        } else if v == 1 {
            Ok(Self {
                error_code: ApiError::new(i16::read(reader)?),
                error_message: Option::<String>::read(reader)?,
                auth_bytes: Vec::<u8>::read(reader)?,
                session_lifetime_ms: Some(i64::read(reader)?),
                tagged_fields: None,
            })
        } else {
            Ok(Self {
                error_code: ApiError::new(i16::read(reader)?),
                error_message: Option::<String>::read_compact(reader)?,
                auth_bytes: Vec::<u8>::read_compact(reader)?,
                session_lifetime_ms: Some(i64::read(reader)?),
                tagged_fields: Some(TaggedFields::read(reader)?),
            })
        }
    }
}

impl<W> WriteVersionedType<W> for SaslAuthenticateResponse
where
    W: Write,
{
    fn write_versioned(
        &self,
        _writer: &mut W,
        _version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn handshake_request() {
        for (name, version, req, want) in [(
            "basic",
            1,
            SaslHandshakeRequest {
                mechanism: "foo".to_string(),
            },
            [
                0, 3, b'f', b'o', b'o', // Mechanism
            ]
            .as_ref(),
        )] {
            let mut buf = Cursor::new([0u8; 128]);
            req.write_versioned(&mut buf, ApiVersion(version)).unwrap();
            let len = buf.position() as usize;
            let got = &buf.get_ref().as_slice()[..len];
            assert_eq!(got, want, "{name}/{version}");
        }
    }

    #[test]
    fn handshake_response() {
        for (name, version, want, data) in [(
            "no error",
            1,
            SaslHandshakeResponse {
                error_code: None,
                mechanisms: vec!["foo".to_string()],
            },
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, b'f', b'o', b'o',
            ]
            .as_ref(),
        )] {
            let mut reader = Cursor::new(data);
            let resp =
                SaslHandshakeResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(resp, want, "{name}/{version}")
        }
    }

    #[test]
    fn auth_request() {
        for (name, version, req, want) in [
            (
                "basic",
                0,
                SaslAuthenticateRequest {
                    auth_bytes: vec![b'f', b'o', b'o'],
                    tagged_fields: None,
                },
                [0, 0, 0, 3, b'f', b'o', b'o'].as_ref(),
            ),
            (
                "basic",
                1,
                SaslAuthenticateRequest {
                    auth_bytes: vec![b'f', b'o', b'o'],
                    tagged_fields: None,
                },
                [0, 0, 0, 3, b'f', b'o', b'o'].as_ref(),
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
    fn auth_response() {
        for (name, version, want, data) in [
            (
                "error",
                0,
                SaslAuthenticateResponse {
                    error_code: Some(ApiError::SaslAuthenticationFailed),
                    error_message: Some("err".to_string()),
                    auth_bytes: vec![b'm', b's', b'g'],
                    session_lifetime_ms: None,
                    tagged_fields: None,
                },
                [0, 58, 0, 3, b'e', b'r', b'r', 0, 0, 0, 3, b'm', b's', b'g'].as_ref(),
            ),
            (
                "error",
                1,
                SaslAuthenticateResponse {
                    error_code: Some(ApiError::SaslAuthenticationFailed),
                    error_message: Some("err".to_string()),
                    auth_bytes: vec![b'm', b's', b'g'],
                    session_lifetime_ms: Some(1),
                    tagged_fields: None,
                },
                [
                    0, 58, 0, 3, b'e', b'r', b'r', 0, 0, 0, 3, b'm', b's', b'g', 0, 0, 0, 0, 0, 0,
                    0, 1,
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let resp =
                SaslAuthenticateResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(resp, want, "{name}/{version}")
        }
    }
}
