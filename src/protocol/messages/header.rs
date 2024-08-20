use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::ApiVersion,
    primitives::TaggedFields,
    traits::{ReadType, WriteType},
};

use super::{ReadVersionedError, ReadVersionedType, WriteVersionedError, WriteVersionedType};

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct RequestHeader {
    /// The API key of this request.
    pub request_api_key: ApiKey,

    /// The API version of this request.
    pub request_api_version: ApiVersion,

    /// The correlation ID of this request.
    pub correlation_id: i32,

    /// The client ID string.
    ///
    /// Added in version 1.
    pub client_id: Option<String>,

    /// The tagged fields.
    ///
    /// Added in version 2.
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for RequestHeader
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 2);

        Ok(Self {
            request_api_key: ApiKey::from(i16::read(reader)?),
            request_api_version: ApiVersion(i16::read(reader)?),
            correlation_id: i32::read(reader)?,
            client_id: if v >= 1 {
                Option::<String>::read(reader)?
            } else {
                None
            },
            tagged_fields: (v >= 2).then(|| TaggedFields::read(reader)).transpose()?,
        })
    }
}

impl<W> WriteVersionedType<W> for RequestHeader
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

        i16::from(self.request_api_key).write(writer)?;
        self.request_api_version.0.write(writer)?;
        self.correlation_id.write(writer)?;

        if v >= 1 {
            match self.client_id.as_ref() {
                Some(client_id) => {
                    client_id.write(writer)?;
                }
                None => {
                    Option::<String>::default().write(writer)?;
                }
            }
        }

        if v >= 2 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct ResponseHeader {
    /// The correlation ID of this response.
    pub correlation_id: i32,

    /// The tagged fields.
    ///
    /// Added in version 1.
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ResponseHeader
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 1);

        Ok(Self {
            correlation_id: i32::read(reader)?,
            tagged_fields: (v >= 1).then(|| TaggedFields::read(reader)).transpose()?,
        })
    }
}

// this is not technically required for production but helpful for testing
impl<W> WriteVersionedType<W> for ResponseHeader
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 1);

        self.correlation_id.write(writer)?;

        if v >= 1 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::messages::test_utils::test_roundtrip_versioned;

    use super::*;

    test_roundtrip_versioned!(
        RequestHeader,
        ApiVersion(0),
        ApiVersion(2),
        test_roundtrip_request_header
    );

    test_roundtrip_versioned!(
        ResponseHeader,
        ApiVersion(0),
        ApiVersion(1),
        test_roundtrip_response_header
    );
}
