use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::primitives::{
    CompactNullableString, CompactString, CompactStringRef, Int16, Int32, Int8, NullableString,
    String_, TaggedFields,
};
use crate::protocol::traits::{ReadType, WriteType};

pub enum CoordinatorType {
    Group,
    Transaction,
}

impl CoordinatorType {
    pub fn group() -> Self {
        Self::Group
    }

    pub fn transaction() -> Self {
        Self::Transaction
    }

    pub fn as_i8(&self) -> i8 {
        match self {
            Self::Group => 0,
            Self::Transaction => 1,
        }
    }
}

pub struct FindCoordinatorRequest {
    /// The coordinator key.
    ///
    /// Changed type to COMPACT_STRING in version 3.
    pub key: String_,

    /// The coordinator key type. (Group, transaction, etc.)
    pub key_type: CoordinatorType,

    /// The tagged fields.
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
            CompactStringRef(&self.key.0.as_str()).write(writer)?;
        } else {
            // removed in version 4
        }

        let key_type = Int8(self.key_type.as_i8());
        key_type.write(writer)?;

        if v >= 3 {
            match self.tagged_fields.as_ref() {
                Some(tagged_fields) => {
                    tagged_fields.write(writer)?;
                }
                None => TaggedFields::default().write(writer)?,
            }
        }

        Ok(())
    }
}

impl RequestBody for FindCoordinatorRequest {
    type ResponseBody = FindCoordinatorResponse;

    const API_KEY: ApiKey = ApiKey::FindCoordinator;

    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(1, 3);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion::new(3);
}

#[derive(Debug)]
pub struct FindCoordinatorResponse {
    /// The duration in milliseconds for which the request was throttled due to a
    /// quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: Int32,

    /// The error code, or 0 if there was no error.
    ///
    /// Removed in version 4
    pub error_code: Option<Error>,

    /// The error message, or null if there was no error.
    ///
    /// Added in version 1
    /// Change to COMPACT_NULLABLE_STRING in version 3
    /// Removed in version 4
    pub error_message: NullableString,

    /// The node id.
    ///
    /// Removed in version 4
    pub node_id: Int32,

    /// The host name.
    ///
    /// Changed to COMPACT_STRING in version 3.
    /// Removed in version 4
    pub host: String_,

    /// The port.
    ///
    /// Removed in version 4
    pub port: Int32,

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
        assert!(v > 0);
        assert!(v <= 4);

        let throttle_time_ms = Int32::read(reader)?;
        let error_code = if v < 4 {
            Error::new(Int16::read(reader)?.0)
        } else {
            None
        };
        let error_message = if v < 3 {
            NullableString::read(reader)?
        } else {
            let c = CompactNullableString::read(reader)?;
            NullableString(c.0)
        };

        let node_id = Int32::read(reader)?;
        let host = if v < 3 {
            String_::read(reader)?
        } else {
            let c = CompactString::read(reader)?;
            String_(c.0)
        };

        let port = Int32::read(reader)?;

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
