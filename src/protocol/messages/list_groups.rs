use std::io::{Read, Write};

use crate::protocol::api_key::ApiKey;
use crate::protocol::api_version::{ApiVersion, ApiVersionRange};
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_compact_versioned_array, read_versioned_array, ReadVersionedError, ReadVersionedType,
    RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::primitives::TaggedFields;
use crate::protocol::traits::{ReadCompactType, ReadType, WriteCompactType, WriteType};

#[derive(Debug)]
pub struct ListGroupsRequest {
    /// The states of the groups we want to list. If empty
    /// all groups are returned with their state.
    ///
    /// Added in version 4
    /// COMPACT_STRING_ARRAY >= 4
    pub states_filter: Vec<String>,

    /// The tagged fields
    ///
    /// Added in version 3
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for ListGroupsRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v < 5);

        if v > 3 {
            self.states_filter.write_compact(writer)?;
        }

        if v > 2 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

impl RequestBody for ListGroupsRequest {
    type ResponseBody = ListGroupsResponse;
    const API_KEY: ApiKey = ApiKey::ListGroups;
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 4);
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(3);
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ListGroupsResponseGroup {
    /// The group ID.
    pub group_id: String,

    /// The group protocol type.
    pub protocol_type: String,

    /// The group state name.
    ///
    /// Added in version 4
    /// COMPACT_STRING >= 4
    pub group_state: Option<String>,

    /// The tagged fields
    ///
    /// Added in version 3
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ListGroupsResponseGroup
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v < 5);

        let group_id = if v < 3 {
            String::read(reader)?
        } else {
            String::read_compact(reader)?
        };

        let protocol_type = if v < 3 {
            String::read(reader)?
        } else {
            String::read_compact(reader)?
        };

        let group_state = (v >= 4).then(|| String::read_compact(reader)).transpose()?;

        let tagged_fields = if v > 2 {
            Some(TaggedFields::read(reader)?)
        } else {
            None
        };

        Ok(ListGroupsResponseGroup {
            group_id,
            protocol_type,
            group_state,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ListGroupsResponse {
    /// The duration in milliseconds for which the request was throttled due to
    /// a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1
    pub throttle_time_ms: Option<i32>,

    /// The error code, or 0 if there was no error.
    pub error_code: Option<Error>,

    /// Each group in the response.
    pub groups: Vec<ListGroupsResponseGroup>,

    /// The tagged fields
    ///
    /// Added in version 3
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ListGroupsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v < 5);

        let throttle_time_ms = (v > 0).then(|| i32::read(reader)).transpose()?;
        let error_code = Error::new(i16::read(reader)?);
        let groups = if v < 3 {
            read_versioned_array(reader, version)?
        } else {
            read_compact_versioned_array(reader, version)?
        }
        .unwrap_or_default();
        let tagged_fields = (v > 2).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(ListGroupsResponse {
            throttle_time_ms,
            error_code,
            groups,
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
                "empty",
                0,
                ListGroupsRequest {
                    states_filter: vec![],
                    tagged_fields: None,
                },
                [].as_ref(),
            ),
            (
                "empty",
                1,
                ListGroupsRequest {
                    states_filter: vec![],
                    tagged_fields: None,
                },
                [].as_ref(),
            ),
            (
                "empty",
                2,
                ListGroupsRequest {
                    states_filter: vec![],
                    tagged_fields: None,
                },
                [].as_ref(),
            ),
            (
                "empty",
                3,
                ListGroupsRequest {
                    states_filter: vec![],
                    tagged_fields: None,
                },
                [
                    0u8, // empty tag buffer
                ]
                .as_ref(),
            ),
            (
                "empty",
                4,
                ListGroupsRequest {
                    states_filter: vec![],
                    tagged_fields: None,
                },
                [
                    1, // compact array length 0
                    0, // empty tag buffer
                ]
                .as_ref(),
            ),
            (
                "with empty state filter",
                4,
                ListGroupsRequest {
                    states_filter: vec!["Empty".to_string()],
                    tagged_fields: None,
                },
                [
                    2, // compact array length (1)
                    6, b'E', b'm', b'p', b't', b'y', // compact string
                    0,    // empty tag buffer
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
                ListGroupsResponse {
                    throttle_time_ms: None,
                    error_code: None,
                    groups: vec![],
                    tagged_fields: None,
                },
                [
                    0, 0, // no error
                    0, 0, 0, 0, // no groups
                ]
                .as_ref(),
            ),
            (
                "error",
                0,
                ListGroupsResponse {
                    throttle_time_ms: None,
                    error_code: Some(Error::ClusterAuthorizationFailed),
                    groups: vec![],
                    tagged_fields: None,
                },
                [
                    0, 31, // no error
                    0, 0, 0, 0, // ErrClusterAuthorizationFailed
                ]
                .as_ref(),
            ),
            (
                "no error with consumer",
                0,
                ListGroupsResponse {
                    throttle_time_ms: None,
                    error_code: None,
                    groups: vec![ListGroupsResponseGroup {
                        group_id: "foo".to_string(),
                        protocol_type: "consumer".to_string(),
                        group_state: None,
                        tagged_fields: None,
                    }],
                    tagged_fields: None,
                },
                [
                    0, 0, // no error
                    0, 0, 0, 1, // 1 group
                    0, 3, b'f', b'o', b'o', // group name
                    0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', // protocol type
                ]
                .as_ref(),
            ),
            (
                "no error",
                4,
                ListGroupsResponse {
                    throttle_time_ms: Some(0),
                    error_code: None,
                    groups: vec![ListGroupsResponseGroup {
                        group_id: "foo".to_string(),
                        protocol_type: "consumer".to_string(),
                        group_state: Some("Empty".to_string()),
                        tagged_fields: None,
                    }],
                    tagged_fields: Some(TaggedFields::default()),
                },
                [
                    0, 0, 0, 0, // no throttle time
                    0, 0, // no error
                    2, // compact array length (1)
                    4, b'f', b'o', b'o', // group name (compact string)
                    9, b'c', b'o', b'n', b's', b'u', b'm', b'e',
                    b'r', // protocol type (compact string)
                    6, b'E', b'm', b'p', b't', b'y', // state (compact string)
                    0,    // Empty tag buffer
                    0,    // Empty tag buffer
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let got = ListGroupsResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(got, want, "{name}/{version}");
        }
    }
}
