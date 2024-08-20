use std::io::{Read, Write};

use crate::protocol::traits::{ReadError, ReadType, WriteError, WriteType};

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct OwnedPartition {
    pub topic: String,
    pub partitions: Vec<i32>,
}

impl<R> ReadType<R> for OwnedPartition
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let topic = String::read(reader)?;

        let len = i32::read(reader)?;
        let mut partitions = Vec::with_capacity(len as usize);
        for _i in 0..len {
            let p = i32::read(reader)?;
            partitions.push(p);
        }

        Ok(Self { topic, partitions })
    }
}

impl<W> WriteType<W> for OwnedPartition
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        self.topic.write(writer)?;

        let len = i32::try_from(self.partitions.len())?;
        len.write(writer)?;

        for p in &self.partitions {
            p.write(writer)?;
        }

        Ok(())
    }
}

// ConsumerGroupMemberMetadata holds the metadata for consumer group
// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolSubscription.json
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ConsumerGroupMemberMetadata {
    pub version: i16,
    // Version >= 0
    pub topics: Vec<String>,
    // Version >= 0
    pub user_data: Vec<u8>,
    // Version >= 1
    pub owned_partitions: Vec<OwnedPartition>,
    // Version >= 2
    pub generation_id: i32,
    // Version >= 2
    pub rack_id: Option<String>,
}

impl<R> ReadType<R> for ConsumerGroupMemberMetadata
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let version = i16::read(reader)?;
        let len = i32::read(reader)?;
        let mut topics = Vec::with_capacity(len as usize);
        for _i in 0..len {
            topics.push(String::read(reader)?);
        }

        let user_data = ReadType::read(reader)?;

        let owned_partitions = if version >= 1 {
            let len = usize::try_from(i32::read(reader)?)?;
            let mut owned_partitions = Vec::with_capacity(len);
            for _i in 0..len {
                let op = OwnedPartition::read(reader)?;
                owned_partitions.push(op);
            }

            owned_partitions
        } else {
            vec![]
        };

        let generation_id = if version >= 2 { i32::read(reader)? } else { 0 };

        let rack_id = if version >= 3 {
            Option::<String>::read(reader)?
        } else {
            None
        };

        Ok(Self {
            version,
            topics,
            user_data,
            owned_partitions,
            generation_id,
            rack_id,
        })
    }
}

impl<W: Write> WriteType<W> for ConsumerGroupMemberMetadata {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        self.version.write(writer)?;

        let len = self.topics.len() as i32;
        len.write(writer)?;
        for s in &self.topics {
            s.write(writer)?;
        }

        self.user_data.write(writer)?;

        if self.version >= 1 {
            let len = i16::try_from(self.owned_partitions.len())?;
            len.write(writer)?;

            for op in &self.owned_partitions {
                op.write(writer)?;
            }
        }

        if self.version >= 2 {
            self.generation_id.write(writer)?;
        }

        if self.version >= 3 {
            self.rack_id.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct PartitionAssignment {
    /// The topic name
    pub topic: String,

    /// The partition array
    pub partitions: Vec<i32>,
}

impl<R> ReadType<R> for PartitionAssignment
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let topic = String::read(reader)?;

        let len = i32::read(reader)?;
        let mut partitions = Vec::with_capacity(len as usize);
        for _i in 0..len {
            let p = i32::read(reader)?;
            partitions.push(p);
        }

        Ok(Self { topic, partitions })
    }
}

impl<W> WriteType<W> for PartitionAssignment
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        self.topic.write(writer)?;

        let len = self.partitions.len() as i32;
        len.write(writer)?;

        for p in &self.partitions {
            p.write(writer)?;
        }

        Ok(())
    }
}

// ConsumerGroupMemberAssignment holds the member assignment for a consume group
// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolAssignment.json
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ConsumerGroupMemberAssignment {
    pub version: i16,
    pub topics: Vec<PartitionAssignment>,
    pub user_data: Vec<u8>,
}

impl<R> ReadType<R> for ConsumerGroupMemberAssignment
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let version = i16::read(reader)?;

        let len = usize::try_from(i32::read(reader)?)?;
        let mut topics = Vec::with_capacity(len);
        for _i in 0..len {
            topics.push(PartitionAssignment::read(reader)?);
        }

        let user_data = ReadType::read(reader)?;

        Ok(Self {
            version,
            topics,
            user_data,
        })
    }
}

impl<W> WriteType<W> for ConsumerGroupMemberAssignment
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        self.version.write(writer)?;

        let len = i32::try_from(self.topics.len())?;
        len.write(writer)?;

        for topic in &self.topics {
            topic.write(writer)?;
        }

        self.user_data.write(writer)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn consumer_group_member_metadata_v0() {
        let metadata = ConsumerGroupMemberMetadata {
            version: 0,
            topics: vec!["one".into(), "two".into()],
            user_data: vec![0x01, 0x02, 0x03],
            owned_partitions: vec![],
            generation_id: 0,
            rack_id: None,
        };
        let data = [
            0, 0, // Version
            0, 0, 0, 2, // Topic array length
            0, 3, b'o', b'n', b'e', // Topic one
            0, 3, b't', b'w', b'o', // Topic two
            0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
        ]
        .as_ref();

        let mut cursor = Cursor::new([0u8; 256]);
        metadata.write(&mut cursor).unwrap();
        let len = cursor.position() as usize;
        let buf = &cursor.get_ref().as_slice()[..len];
        assert_eq!(buf, data);

        cursor.set_position(0);
        let new = ConsumerGroupMemberMetadata::read(&mut cursor).unwrap();
        assert_eq!(new, metadata);
    }

    #[test]
    fn consumer_group_member_metadata_v1() {
        let v1 = [
            0, 1, // Version
            0, 0, 0, 2, // Topic array length
            0, 3, b'o', b'n', b'e', // Topic one
            0, 3, b't', b'w', b'o', // Topic two
            0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
            0, 0, 0, 0, // OwnedPartitions KIP-429
        ];
        let v1_bad = [
            0, 1, // Version
            0, 0, 0, 2, // Topic array length
            0, 3, b'o', b'n', b'e', // Topic one
            0, 3, b't', b'w', b'o', // Topic two
            0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
        ];

        let mut cursor = Cursor::new(v1);
        ConsumerGroupMemberMetadata::read(&mut cursor).unwrap();
        let mut cursor = Cursor::new(v1_bad);
        ConsumerGroupMemberMetadata::read(&mut cursor).expect_err("v1 bad");
    }

    #[test]
    fn consumer_group_member_metadata_v3() {
        let v3 = [
            0, 3, // Version
            0, 0, 0, 1, // Topic array length
            0, 3, b'o', b'n', b'e', // Topic one
            0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
            0, 0, 0, 0, // OwnedPartitions KIP-429
            0, 0, 0, 64, // GenerationID
            0, 4, b'r', b'a', b'c', b'k', // RackID
        ];
        let mut cursor = Cursor::new(v3);
        ConsumerGroupMemberMetadata::read(&mut cursor).unwrap();
    }

    #[test]
    fn consumer_group_member_assignment() {
        let want = ConsumerGroupMemberAssignment {
            version: 0,
            topics: vec![PartitionAssignment {
                topic: "one".to_string(),
                partitions: vec![0, 2, 4],
            }],
            user_data: vec![0x01, 0x02, 0x03],
        };

        let mut buf = Cursor::new([
            0, 0, // Version
            0, 0, 0, 1, // Topic array length
            0, 3, b'o', b'n', b'e', // Topic one
            0, 0, 0, 3, // Topic one, partition array length
            0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, // 0, 2, 4
            0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
        ]);
        let got = ConsumerGroupMemberAssignment::read(&mut buf).unwrap();
        assert_eq!(got, want)
    }
}
