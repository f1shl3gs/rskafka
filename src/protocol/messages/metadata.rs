use std::io::{Read, Write};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::api_version::ApiVersionRange;
use crate::protocol::messages::{read_versioned_array, write_versioned_array};
use crate::protocol::{
    api_key::ApiKey,
    api_version::ApiVersion,
    error::Error,
    traits::{ReadType, WriteType},
};

#[derive(Debug)]
pub struct MetadataRequest {
    /// The topics to fetch metadata for
    ///
    /// Requests data for all topics if None
    pub topics: Option<Vec<MetadataRequestTopic>>,

    /// If this is true, the broker may auto-create topics that we requested
    /// which do not already exist, if it is configured to do so.
    ///
    /// Added in version 4
    pub allow_auto_topic_creation: Option<bool>,
}

impl RequestBody for MetadataRequest {
    type ResponseBody = MetadataResponse;

    const API_KEY: ApiKey = ApiKey::Metadata;

    /// At the time of writing this is the same subset supported by rdkafka
    const API_VERSION_RANGE: ApiVersionRange = ApiVersionRange::new(0, 4);

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(9);
}

impl<W> WriteVersionedType<W> for MetadataRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        if v < 4 && self.allow_auto_topic_creation.is_some() {
            return Err(WriteVersionedError::FieldNotAvailable {
                version,
                field: "allow_auto_topic_creation".to_string(),
            });
        }

        write_versioned_array(writer, version, self.topics.as_deref())?;
        if v >= 4 {
            match self.allow_auto_topic_creation {
                // The default behaviour is to allow topic creation
                None => true.write(writer)?,
                Some(b) => b.write(writer)?,
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MetadataRequestTopic {
    /// The topic name
    pub name: String,
}

impl<W> WriteVersionedType<W> for MetadataRequestTopic
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        assert!(version.0 <= 4);
        Ok(self.name.write(writer)?)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MetadataResponse {
    /// The duration in milliseconds for which the request was throttled due to
    /// a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 3
    pub throttle_time_ms: Option<i32>,

    /// Each broker in the response
    pub brokers: Vec<MetadataResponseBroker>,

    /// The cluster ID that responding broker belongs to.
    ///
    /// Added in version 2
    pub cluster_id: Option<String>,

    /// The ID of the controller broker.
    ///
    /// Added in version 1
    pub controller_id: Option<i32>,

    /// Each topic in the response
    pub topics: Vec<MetadataResponseTopic>,
}

impl<R> ReadVersionedType<R> for MetadataResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        let throttle_time_ms = (v >= 3).then(|| i32::read(reader)).transpose()?;
        let brokers = read_versioned_array(reader, version)?.unwrap_or_default();
        let cluster_id = if v >= 2 {
            Option::<String>::read(reader)?
        } else {
            None
        };
        let controller_id = (v >= 1).then(|| i32::read(reader)).transpose()?;
        let topics = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(Self {
            throttle_time_ms,
            brokers,
            topics,
            cluster_id,
            controller_id,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MetadataResponseBroker {
    /// The broker ID
    pub node_id: i32,
    /// The broker hostname
    pub host: String,
    /// The broker port
    pub port: i32,
    /// Added in version 1
    pub rack: Option<String>,
}

impl<R> ReadVersionedType<R> for MetadataResponseBroker
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        let node_id = i32::read(reader)?;
        let host = String::read(reader)?;
        let port = i32::read(reader)?;
        let rack = if v >= 1 {
            Option::<String>::read(reader)?
        } else {
            None
        };

        Ok(Self {
            node_id,
            host,
            port,
            rack,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MetadataResponseTopic {
    /// The topic error if any
    pub error: Option<Error>,
    /// The topic name
    pub name: String,
    /// True if the topic is internal
    pub is_internal: Option<bool>,
    /// Each partition in the topic
    pub partitions: Vec<MetadataResponsePartition>,
}

impl<R> ReadVersionedType<R> for MetadataResponseTopic
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        let error = Error::new(i16::read(reader)?);
        let name = String::read(reader)?;
        let is_internal = (v >= 1).then(|| bool::read(reader)).transpose()?;
        let partitions = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(Self {
            error,
            name,
            is_internal,
            partitions,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MetadataResponsePartition {
    /// The partition error if any
    pub error: Option<Error>,
    /// The partition index
    pub partition_index: i32,
    /// The ID of the leader broker
    pub leader_id: i32,
    /// The set of all nodes that host this partition
    pub replica_nodes: Vec<i32>,
    /// The set of all nodes that are in sync with the leader for this partition
    pub isr_nodes: Vec<i32>,
}

impl<R> ReadVersionedType<R> for MetadataResponsePartition
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0;
        assert!(v <= 4);

        Ok(Self {
            error: Error::new(i16::read(reader)?),
            partition_index: i32::read(reader)?,
            leader_id: i32::read(reader)?,
            replica_nodes: Vec::<i32>::read(reader)?,
            isr_nodes: Vec::<i32>::read(reader)?,
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
                "no topics",
                0,
                MetadataRequest {
                    topics: Some(vec![]),
                    allow_auto_topic_creation: None,
                },
                [0x00, 0x00, 0x00, 0x00].as_ref(),
            ),
            (
                "one topic",
                0,
                MetadataRequest {
                    topics: Some(vec![MetadataRequestTopic {
                        name: "topic1".to_string(),
                    }]),
                    allow_auto_topic_creation: None,
                },
                [
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x06, b't', b'o', b'p', b'i', b'c', b'1',
                ]
                .as_ref(),
            ),
            (
                "three topics",
                0,
                MetadataRequest {
                    topics: Some(vec![
                        MetadataRequestTopic {
                            name: "foo".to_string(),
                        },
                        MetadataRequestTopic {
                            name: "bar".to_string(),
                        },
                        MetadataRequestTopic {
                            name: "baz".to_string(),
                        },
                    ]),
                    allow_auto_topic_creation: None,
                },
                [
                    0x00, 0x00, 0x00, 0x03, // topic length
                    0x00, 0x03, b'f', b'o', b'o', // topic one
                    0x00, 0x03, b'b', b'a', b'r', // topic two
                    0x00, 0x03, b'b', b'a', b'z', // topic there
                ]
                .as_ref(),
            ),
            (
                "no topics",
                1,
                MetadataRequest {
                    topics: None,
                    allow_auto_topic_creation: None,
                },
                [0xff, 0xff, 0xff, 0xff].as_ref(),
            ),
            (
                "one topic",
                1,
                MetadataRequest {
                    topics: Some(vec![MetadataRequestTopic {
                        name: "topic1".to_string(),
                    }]),
                    allow_auto_topic_creation: None,
                },
                [
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x06, b't', b'o', b'p', b'i', b'c', b'1',
                ]
                .as_ref(),
            ),
            (
                "three topics",
                1,
                MetadataRequest {
                    topics: Some(vec![
                        MetadataRequestTopic {
                            name: "foo".to_string(),
                        },
                        MetadataRequestTopic {
                            name: "bar".to_string(),
                        },
                        MetadataRequestTopic {
                            name: "baz".to_string(),
                        },
                    ]),
                    allow_auto_topic_creation: None,
                },
                [
                    0x00, 0x00, 0x00, 0x03, 0x00, 0x03, b'f', b'o', b'o', 0x00, 0x03, b'b', b'a',
                    b'r', 0x00, 0x03, b'b', b'a', b'z',
                ]
                .as_ref(),
            ),
            (
                "no topics",
                2,
                MetadataRequest {
                    topics: None,
                    allow_auto_topic_creation: None,
                },
                [0xff, 0xff, 0xff, 0xff].as_ref(),
            ),
            (
                "one topic",
                2,
                MetadataRequest {
                    topics: Some(vec![MetadataRequestTopic {
                        name: "topic1".to_string(),
                    }]),
                    allow_auto_topic_creation: None,
                },
                [
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x06, b't', b'o', b'p', b'i', b'c', b'1',
                ]
                .as_ref(),
            ),
            (
                "three topics",
                2,
                MetadataRequest {
                    topics: Some(vec![
                        MetadataRequestTopic {
                            name: "foo".to_string(),
                        },
                        MetadataRequestTopic {
                            name: "bar".to_string(),
                        },
                        MetadataRequestTopic {
                            name: "baz".to_string(),
                        },
                    ]),
                    allow_auto_topic_creation: None,
                },
                [
                    0x00, 0x00, 0x00, 0x03, 0x00, 0x03, b'f', b'o', b'o', 0x00, 0x03, b'b', b'a',
                    b'r', 0x00, 0x03, b'b', b'a', b'z',
                ]
                .as_ref(),
            ),
            (
                "no topics",
                3,
                MetadataRequest {
                    topics: None,
                    allow_auto_topic_creation: None,
                },
                [0xff, 0xff, 0xff, 0xff].as_ref(),
            ),
            (
                "one topic",
                3,
                MetadataRequest {
                    topics: Some(vec![MetadataRequestTopic {
                        name: "topic1".to_string(),
                    }]),
                    allow_auto_topic_creation: None,
                },
                [
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x06, b't', b'o', b'p', b'i', b'c', b'1',
                ]
                .as_ref(),
            ),
            (
                "three topics",
                3,
                MetadataRequest {
                    topics: Some(vec![
                        MetadataRequestTopic {
                            name: "foo".to_string(),
                        },
                        MetadataRequestTopic {
                            name: "bar".to_string(),
                        },
                        MetadataRequestTopic {
                            name: "baz".to_string(),
                        },
                    ]),
                    allow_auto_topic_creation: None,
                },
                [
                    0x00, 0x00, 0x00, 0x03, 0x00, 0x03, b'f', b'o', b'o', 0x00, 0x03, b'b', b'a',
                    b'r', 0x00, 0x03, b'b', b'a', b'z',
                ]
                .as_ref(),
            ),
            (
                "no topics",
                4,
                MetadataRequest {
                    topics: None,
                    allow_auto_topic_creation: None,
                },
                [0xff, 0xff, 0xff, 0xff, 0x01].as_ref(),
            ),
            (
                "auto create topic",
                4,
                MetadataRequest {
                    topics: Some(vec![MetadataRequestTopic {
                        name: "topic1".to_string(),
                    }]),
                    allow_auto_topic_creation: Some(true),
                },
                [
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x06, b't', b'o', b'p', b'i', b'c', b'1', 0x01,
                ]
                .as_ref(),
            ),
            (
                "no auto create topic",
                4,
                MetadataRequest {
                    topics: Some(vec![MetadataRequestTopic {
                        name: "topic1".to_string(),
                    }]),
                    allow_auto_topic_creation: Some(false),
                },
                [
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x06, b't', b'o', b'p', b'i', b'c', b'1', 0x00,
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
                "empty",
                0,
                MetadataResponse {
                    throttle_time_ms: None,
                    brokers: vec![],
                    cluster_id: None,
                    controller_id: None,
                    topics: vec![],
                },
                [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00].as_ref(),
            ),
            (
                "with brokers, no topics",
                0,
                MetadataResponse {
                    throttle_time_ms: None,
                    brokers: vec![
                        MetadataResponseBroker {
                            node_id: 0xabff,
                            host: "localhost".to_string(),
                            port: 51,
                            rack: None,
                        },
                        MetadataResponseBroker {
                            node_id: 0x010203,
                            host: "google.com".to_string(),
                            port: 273,
                            rack: None,
                        },
                    ],
                    cluster_id: None,
                    controller_id: None,
                    topics: vec![],
                },
                [
                    0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0xab, 0xff, 0x00, 0x09, b'l', b'o', b'c',
                    b'a', b'l', b'h', b'o', b's', b't', 0x00, 0x00, 0x00, 0x33, 0x00, 0x01, 0x02,
                    0x03, 0x00, 0x0a, b'g', b'o', b'o', b'g', b'l', b'e', b'.', b'c', b'o', b'm',
                    0x00, 0x00, 0x01, 0x11, 0x00, 0x00, 0x00, 0x00,
                ]
                .as_ref(),
            ),
            (
                "topics, no brokers",
                0,
                MetadataResponse {
                    throttle_time_ms: None,
                    brokers: vec![],
                    cluster_id: None,
                    controller_id: None,
                    topics: vec![
                        MetadataResponseTopic {
                            error: None,
                            name: "foo".to_string(),
                            is_internal: None,
                            partitions: vec![MetadataResponsePartition {
                                error: Some(Error::InvalidFetchSize),
                                partition_index: 0x01,
                                leader_id: 0x07,
                                replica_nodes: vec![1, 2, 3],
                                isr_nodes: vec![],
                            }],
                        },
                        MetadataResponseTopic {
                            error: None,
                            name: "bar".to_string(),
                            is_internal: None,
                            partitions: vec![],
                        },
                    ],
                },
                [
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, b'f',
                    b'o', b'o', 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00,
                    0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
                    0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x03, b'b', b'a', b'r', 0x00, 0x00, 0x00, 0x00,
                ]
                .as_ref(),
            ),
            (
                "topics",
                1,
                MetadataResponse {
                    throttle_time_ms: None,
                    brokers: vec![
                        MetadataResponseBroker {
                            node_id: 0xabff,
                            host: "localhost".to_string(),
                            port: 51,
                            rack: Some("rack0".to_string()),
                        },
                        MetadataResponseBroker {
                            node_id: 0x010203,
                            host: "google.com".to_string(),
                            port: 273,
                            rack: Some("rack1".to_string()),
                        },
                    ],
                    cluster_id: None,
                    controller_id: Some(1),
                    topics: vec![],
                },
                [
                    0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0xab, 0xff, 0x00, 0x09, b'l', b'o', b'c',
                    b'a', b'l', b'h', b'o', b's', b't', 0x00, 0x00, 0x00, 0x33, 0x00, 0x05, b'r',
                    b'a', b'c', b'k', b'0', 0x00, 0x01, 0x02, 0x03, 0x00, 0x0a, b'g', b'o', b'o',
                    b'g', b'l', b'e', b'.', b'c', b'o', b'm', 0x00, 0x00, 0x01, 0x11, 0x00, 0x05,
                    b'r', b'a', b'c', b'k', b'1', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
                ]
                .as_ref(),
            ),
            (
                "topics",
                1,
                MetadataResponse {
                    throttle_time_ms: None,
                    brokers: vec![],
                    cluster_id: None,
                    controller_id: Some(4),
                    topics: vec![
                        MetadataResponseTopic {
                            error: None,
                            name: "foo".to_string(),
                            is_internal: Some(false),
                            partitions: vec![MetadataResponsePartition {
                                error: Some(Error::InvalidFetchSize),
                                partition_index: 1,
                                leader_id: 7,
                                replica_nodes: vec![1, 2, 3],
                                isr_nodes: vec![],
                            }],
                        },
                        MetadataResponseTopic {
                            error: None,
                            name: "bar".to_string(),
                            is_internal: Some(true),
                            partitions: vec![],
                        },
                    ],
                },
                [
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x02, 0x00,
                    0x00, 0x00, 0x03, b'f', b'o', b'o', 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04,
                    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x03, 0x00,
                    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x03, b'b', b'a', b'r', 0x01, 0x00, 0x00, 0x00,
                    0x00,
                ]
                .as_ref(),
            ),
            (
                "no topics, no brokers, throttle time and cluster id",
                3,
                MetadataResponse {
                    throttle_time_ms: Some(16),
                    brokers: vec![],
                    cluster_id: Some("clusterId".to_string()),
                    controller_id: Some(1),
                    topics: vec![],
                },
                [
                    0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, b'c', b'l', b'u',
                    b's', b't', b'e', b'r', b'I', b'd', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
                    0x00,
                ]
                .as_ref(),
            ),
        ] {
            let mut reader = Cursor::new(data);
            let resp = MetadataResponse::read_versioned(&mut reader, ApiVersion(version)).unwrap();
            assert_eq!(resp, want, "{name}/{version}")
        }
    }
}
