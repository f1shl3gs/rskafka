use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error as ApiError,
    messages::{
        read_compact_versioned_array, write_compact_versioned_array, write_versioned_array,
    },
    primitives::TaggedFields,
    traits::{ReadCompactType, ReadType, WriteCompactType, WriteType},
};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
    read_versioned_array,
};

#[cfg(test)]
use proptest::prelude::*;

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct ApiVersionsRequest {
    /// The name of the client.
    ///
    /// Added in version 3.
    pub client_software_name: Option<String>,

    /// The version of the client.
    ///
    /// Added in version 3.
    pub client_software_version: Option<String>,

    /// The tagged fields.
    ///
    /// Added in version 3.
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ApiVersionsRequest
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 3);

        Ok(Self {
            client_software_name: (v >= 3).then(|| String::read_compact(reader)).transpose()?,
            client_software_version: (v >= 3).then(|| String::read_compact(reader)).transpose()?,
            tagged_fields: (v >= 3).then(|| TaggedFields::read(reader)).transpose()?,
        })
    }
}

impl<W> WriteVersionedType<W> for ApiVersionsRequest
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

        if v >= 3 {
            match self.client_software_name.as_ref() {
                Some(client_software_name) => {
                    client_software_name.write_compact(writer)?;
                }
                None => {
                    String::default().write_compact(writer)?;
                }
            }

            match self.client_software_version.as_ref() {
                Some(client_software_version) => {
                    client_software_version.write_compact(writer)?;
                }
                None => {
                    String::default().write_compact(writer)?;
                }
            }

            self.tagged_fields.write(writer)?
        }

        Ok(())
    }
}

impl RequestBody for ApiVersionsRequest {
    type ResponseBody = ApiVersionsResponse;
    const API_KEY: ApiKey = ApiKey::ApiVersions;
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 3);
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(3);

    // It seems version 3 actually doesn't use tagged fields during response, at least not for Kafka 3.
    //
    // rdkafka also does this, see
    // https://github.com/edenhill/librdkafka/blob/2b76b65212e5efda213961d5f84e565038036270/src/rdkafka_broker.c#L1781-L1785
    const FIRST_TAGGED_FIELD_IN_RESPONSE_VERSION: ApiVersion = ApiVersion(i16::MAX);
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct ApiVersionsResponseApiKey {
    /// The API index.
    pub api_key: ApiKey,

    /// The minimum supported version, inclusive.
    pub min_version: ApiVersion,

    /// The maximum supported version, inclusive.
    pub max_version: ApiVersion,

    /// The tagged fields.
    ///
    /// Added in version 3
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ApiVersionsResponseApiKey
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 3);

        Ok(Self {
            api_key: i16::read(reader)?.into(),
            min_version: ApiVersion(i16::read(reader)?),
            max_version: ApiVersion(i16::read(reader)?),
            tagged_fields: (v >= 3).then(|| TaggedFields::read(reader)).transpose()?,
        })
    }
}

// this is not technically required for production but helpful for testing
impl<W> WriteVersionedType<W> for ApiVersionsResponseApiKey
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

        let api_key: i16 = self.api_key.into();
        api_key.write(writer)?;

        self.min_version.0.write(writer)?;
        self.max_version.0.write(writer)?;

        if v >= 3 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct ApiVersionsResponse {
    /// The top-level error code.
    #[cfg_attr(test, proptest(strategy = "any::<i16>().prop_map(ApiError::new)"))]
    pub error_code: Option<ApiError>,

    /// The APIs supported by the broker.
    // tell proptest to only generate small vectors, otherwise tests take forever
    #[cfg_attr(
        test,
        proptest(strategy = "prop::collection::vec(any::<ApiVersionsResponseApiKey>(), 0..2)")
    )]
    pub api_keys: Vec<ApiVersionsResponseApiKey>,

    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1
    pub throttle_time_ms: Option<i32>,

    /// The tagged fields.
    ///
    /// Added in version 3
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ApiVersionsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 3);

        let error_code = ApiError::new(i16::read(reader)?);
        let api_keys = if v >= 3 {
            read_compact_versioned_array(reader, version)?.unwrap_or_default()
        } else {
            read_versioned_array(reader, version)?.unwrap_or_default()
        };
        let throttle_time_ms = (v >= 1).then(|| i32::read(reader)).transpose()?;
        let tagged_fields = (v >= 3).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            tagged_fields,
        })
    }
}

// this is not technically required for production but helpful for testing
impl<W> WriteVersionedType<W> for ApiVersionsResponse
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

        let err_code = match self.error_code {
            Some(err) => err.code(),
            None => 0,
        };
        err_code.write(writer)?;

        if v >= 3 {
            write_compact_versioned_array(writer, version, Some(&self.api_keys))?;
        } else {
            write_versioned_array(writer, version, Some(&self.api_keys))?;
        }

        if v >= 1 {
            // defaults to "no throttle"
            self.throttle_time_ms.unwrap_or(0).write(writer)?;
        }

        if v >= 3 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::protocol::messages::test_utils::test_roundtrip_versioned;
    use crate::protocol::primitives::UnsignedVarint;

    test_roundtrip_versioned!(
        ApiVersionsRequest,
        ApiVersionsRequest::API_VERSION_RANGE.min(),
        ApiVersionsRequest::API_VERSION_RANGE.max(),
        test_roundtrip_api_versions_request
    );

    test_roundtrip_versioned!(
        ApiVersionsResponse,
        ApiVersionsRequest::API_VERSION_RANGE.min(),
        ApiVersionsRequest::API_VERSION_RANGE.max(),
        test_roundtrip_api_versions_response
    );

    #[test]
    fn request() {
        for (name, version, req, want) in [
            (
                "basic",
                0,
                ApiVersionsRequest {
                    client_software_name: None,
                    client_software_version: None,
                    tagged_fields: None,
                },
                [].as_ref(),
            ),
            (
                "basic",
                3,
                ApiVersionsRequest {
                    client_software_name: Some("sarama".to_string()),
                    client_software_version: Some("0.10.0".to_string()),
                    tagged_fields: None,
                },
                [
                    0x07, b's', b'a', b'r', b'a', b'm', b'a', // client software name
                    0x07, b'0', b'.', b'1', b'0', b'.', b'0', // client software version
                    0x00, // tagged fields
                ]
                .as_ref(),
            ),
        ] {
            let mut cursor = Cursor::new([0u8; 128]);
            req.write_versioned(&mut cursor, ApiVersion(version))
                .unwrap();
            let len = cursor.position() as usize;
            let got = &cursor.get_ref()[..len];
            assert_eq!(got, want, "{name}/{version}");
        }
    }

    #[test]
    fn response() {
        for (name, version, want, data) in [
            (
                "no error",
                0,
                ApiVersionsResponse {
                    error_code: None,
                    api_keys: vec![ApiVersionsResponseApiKey {
                        api_key: ApiKey::Metadata,
                        min_version: ApiVersion(0x02),
                        max_version: ApiVersion(0x01),
                        tagged_fields: None,
                    }],
                    throttle_time_ms: None,
                    tagged_fields: None,
                },
                [
                    0x00, 0x00, // error code
                    0x00, 0x00, 0x00, 0x01, // api keys length
                    0x00, 0x03, // api key Metadata
                    0x00, 0x02, // min version
                    0x00, 0x01, // max version
                ]
                .as_ref(),
            ),
            (
                "no error",
                3,
                ApiVersionsResponse {
                    error_code: None,
                    api_keys: vec![ApiVersionsResponseApiKey {
                        api_key: ApiKey::Metadata,
                        min_version: ApiVersion(0x02),
                        max_version: ApiVersion(0x01),
                        tagged_fields: Some(TaggedFields::default()),
                    }],
                    throttle_time_ms: Some(0),
                    tagged_fields: Some(TaggedFields(vec![(
                        UnsignedVarint(1),
                        vec![0, 0, 0, 0, 0, 0, 0, 0],
                    )])),
                },
                [
                    0x00, 0x00, // no error
                    0x02, // compact array length 1
                    0x00, 0x03, 0x00, 0x02, 0x00, 0x01, 0x00, // tagged fields
                    0x00, 0x00, 0x00, 0x00, // throttle time
                    0x01, 0x01, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, // tagged fields (empty SupportedFeatures)
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let got =
                ApiVersionsResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(want, got, "{name}/{version}");
        }
    }
}
