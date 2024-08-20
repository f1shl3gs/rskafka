//! Primitive types.
//!
//! # References
//! - <https://kafka.apache.org/protocol#protocol_types>
//! - <https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-UnsignedVarints>

use std::io::{Cursor, Read, Write};

use super::{
    record::RecordBatch,
    traits::{ReadError, ReadType, WriteError, WriteType},
    vec_builder::VecBuilder,
};
use crate::protocol::traits::{ReadCompactType, WriteCompactType};

#[cfg(test)]
use proptest::prelude::*;

/// Represents a boolean
impl<R> ReadType<R> for bool
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        match buf[0] {
            0 => Ok(false),
            _ => Ok(true),
        }
    }
}

impl<W> WriteType<W> for bool
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match self {
            true => Ok(writer.write_all(&[1])?),
            false => Ok(writer.write_all(&[0])?),
        }
    }
}

/// Represents an integer between `-2^7` and `2^7-1` inclusive.
impl<R> ReadType<R> for i8
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(i8::from_be_bytes(buf))
    }
}

impl<W> WriteType<W> for i8
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let buf = self.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

/// Represents an integer between `-2^15` and `2^15-1` inclusive.
///
/// The values are encoded using two bytes in network byte order (big-endian).
impl<R> ReadType<R> for i16
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        Ok(i16::from_be_bytes(buf))
    }
}

impl<W> WriteType<W> for i16
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let buf = self.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

/// Represents an integer between `-2^31` and `2^31-1` inclusive.
///
/// The values are encoded using four bytes in network byte order (big-endian).
impl<R> ReadType<R> for i32
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }
}

impl<W> WriteType<W> for i32
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let buf = self.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

/// Represents an integer between `-2^63` and `2^63-1` inclusive.
///
/// The values are encoded using eight bytes in network byte order (big-endian).
impl<R> ReadType<R> for i64
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }
}

impl<W> WriteType<W> for i64
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let buf = self.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

fn read_unsigned_varint<R: Read>(reader: &mut R) -> Result<u64, ReadError> {
    let mut result = 0u64;
    let mut shift = 0;
    let mut buf = [0u8; 1];

    let mut success = false;
    // 10 is the max size of zig-zag encoded data.
    for i in 0..10 {
        if reader.read(&mut buf)? == 0 {
            if i == 0 {
                return Err(ReadError::IO(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Reached EOF",
                )));
            }

            break;
        }

        let b = buf[0];
        let msb_dropped = b & 0b0111_1111;
        result |= (msb_dropped as u64) << shift;
        shift += 7;

        if b & 0b1000_0000 == 0 || shift > (9 * 7) {
            success = b & 0b1000_0000 == 0;
            break;
        }
    }

    if success {
        Ok(result)
    } else {
        Err(ReadError::IO(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unterminated varint",
        )))
    }
}

fn write_unsigned_varint<W: Write>(writer: &mut W, mut value: u64) -> Result<(), WriteError> {
    let mut buf = [0u8; 10];

    for pos in 0..10 {
        if value < 0x80 {
            buf[pos] = value as u8;
            return writer
                .write_all(&buf[..pos + 1])
                .map_err(|err| WriteError::IO(err));
        } else {
            buf[pos] = ((value & 0x7F) | 0x80) as u8;
            value >>= 7;
        }
    }

    Ok(())
}

#[inline]
fn read_varint<R: Read>(reader: &mut R) -> Result<i64, ReadError> {
    let res = read_unsigned_varint(reader)?;
    Ok(((res >> 1) ^ (-((res & 1) as i64)) as u64) as i64)
}

#[inline]
fn write_varint<W: Write>(writer: &mut W, value: i64) -> Result<(), WriteError> {
    write_unsigned_varint(writer, ((value << 1) ^ (value >> 63)) as u64)
}

/// Represents an integer between `-2^31` and `2^31-1` inclusive.
///
/// Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Varint(pub i32);

impl<R> ReadType<R> for Varint
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let value = read_varint(reader)?;
        Ok(Self(i32::try_from(value)?))
    }
}

impl<W> WriteType<W> for Varint
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        write_varint(writer, self.0 as i64)
    }
}

/// Represents an integer between `-2^63` and `2^63-1` inclusive.
///
/// Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Varlong(pub i64);

impl<R> ReadType<R> for Varlong
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let value = read_varint(reader)?;
        Ok(Self(value))
    }
}

impl<W> WriteType<W> for Varlong
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        write_varint(writer, self.0)
    }
}

/// The UNSIGNED_VARINT type describes an unsigned variable length integer.
///
/// To serialize a number as a variable-length integer, you break it up into groups of 7 bits. The lowest 7 bits is
/// written out first, followed by the second-lowest, and so on.  Each time a group of 7 bits is written out, the high
/// bit (bit 8) is cleared if this group is the last one, and set if it is not.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct UnsignedVarint(pub u64);

impl<R> ReadType<R> for UnsignedVarint
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let value = read_unsigned_varint(reader)?;
        Ok(Self(value))
    }
}

impl<W> WriteType<W> for UnsignedVarint
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        write_unsigned_varint(writer, self.0)
    }
}

/// Represents a sequence of characters.
///
/// First the length N is given as an INT16. Then N bytes follow which are the UTF-8 encoding of the character
/// sequence. Length must not be negative.
impl<R> ReadType<R> for String
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = i16::read(reader)?;
        let len = usize::try_from(len).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut buf = VecBuilder::new(len);
        buf = buf.read_exact(reader)?;
        String::from_utf8(buf.into()).map_err(|e| ReadError::Malformed(Box::new(e)))
    }
}

/// Implement for COMPACT_STRING
impl<R> ReadCompactType<R> for String
where
    R: Read,
{
    fn read_compact(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?;

        match len.0 {
            0 => Err(ReadError::Malformed(
                "COMPACT_STRING must have non-zero length".into(),
            )),
            len => {
                let len = usize::try_from(len)? - 1;

                let mut buf = VecBuilder::new(len);
                buf = buf.read_exact(reader)?;

                String::from_utf8(buf.into()).map_err(|err| ReadError::Malformed(Box::new(err)))
            }
        }
    }
}

impl<W> WriteType<W> for String
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = i16::try_from(self.len()).map_err(WriteError::Overflow)?;
        len.write(writer)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

impl<W: Write> WriteCompactType<W> for String {
    fn write_compact(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = u64::try_from(self.len() + 1)?;
        UnsignedVarint(len).write(writer)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

/// This looks like kind of unnecessary, but it could avoid allocations.
impl<W: Write> WriteType<W> for &str {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = i16::try_from(self.len()).map_err(WriteError::Overflow)?;
        len.write(writer)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

/// COMPACT_NULLABLE_STRING
impl<R: Read> ReadCompactType<R> for Option<String> {
    fn read_compact(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?;

        match len.0 {
            0 => Ok(None),
            len => {
                let len = usize::try_from(len)? - 1;

                let mut buf = VecBuilder::new(len);
                buf = buf.read_exact(reader)?;

                let s = String::from_utf8(buf.into())
                    .map_err(|err| ReadError::Malformed(Box::new(err)))?;

                Ok(Some(s))
            }
        }
    }
}

/// NULLABLE_STRING
impl<R> ReadType<R> for Option<String>
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = i16::read(reader)?;
        match len {
            l if l < -1 => Err(ReadError::Malformed(
                format!("Invalid negative length for nullable string: {}", l).into(),
            )),
            -1 => Ok(None),
            l => {
                let len = usize::try_from(l)?;
                let mut buf = VecBuilder::new(len);
                buf = buf.read_exact(reader)?;
                let s =
                    String::from_utf8(buf.into()).map_err(|e| ReadError::Malformed(Box::new(e)))?;
                Ok(Some(s))
            }
        }
    }
}

/// NULLABLE_STRING
impl<W> WriteType<W> for Option<String>
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match self {
            Some(s) => s.write(writer),
            None => (-1i16).write(writer),
        }
    }
}

/// This looks like kind of unnecessary, but it could avoid allocations.
impl<W> WriteType<W> for Option<&str>
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match self {
            Some(s) => s.write(writer),
            None => (-1i16).write(writer),
        }
    }
}

impl<W> WriteCompactType<W> for Option<String>
where
    W: Write,
{
    fn write_compact(&self, writer: &mut W) -> Result<(), WriteError> {
        match self {
            Some(s) => {
                let len = u64::try_from(s.len() + 1)?;
                UnsignedVarint(len).write(writer)?;
                writer.write_all(s.as_bytes()).map_err(WriteError::IO)
            }
            None => UnsignedVarint(0).write(writer),
        }
    }
}

/// Represents a raw sequence of bytes.
///
/// First the length N is given as an INT32. Then N bytes follow.
impl<R> ReadType<R> for Vec<u8>
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = i32::read(reader)?;
        let len = usize::try_from(len)?;
        let mut buf = VecBuilder::new(len);
        buf = buf.read_exact(reader)?;
        Ok(buf.into())
    }
}

impl<W> WriteType<W> for Vec<u8>
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = i32::try_from(self.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
        len.write(writer)?;
        writer.write_all(&self)?;
        Ok(())
    }
}

/// Represents a raw sequence of bytes.
///
/// First the length N+1 is given as an UNSIGNED_VARINT.Then N bytes follow.
impl<R> ReadCompactType<R> for Vec<u8>
where
    R: Read,
{
    fn read_compact(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?;
        let len = usize::try_from(len.0)? - 1;
        let mut buf = VecBuilder::new(len);
        buf = buf.read_exact(reader)?;
        Ok(buf.into())
    }
}

impl<W> WriteCompactType<W> for Vec<u8>
where
    W: Write,
{
    fn write_compact(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = u64::try_from(self.len() + 1).map_err(WriteError::Overflow)?;
        UnsignedVarint(len).write(writer)?;
        writer.write_all(self)?;
        Ok(())
    }
}

/// Represents a raw sequence of bytes or null, aka NULLABLE_BYTES
///
/// For non-null values, first the length N is given as an INT32. Then N bytes follow. A null value is encoded with
/// length of -1 and there are no following bytes.
impl<R> ReadType<R> for Option<Vec<u8>>
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = i32::read(reader)?;
        match len {
            l if l < -1 => Err(ReadError::Malformed(
                format!("Invalid negative length for nullable bytes: {}", l).into(),
            )),
            -1 => Ok(None),
            l => {
                let len = usize::try_from(l)?;
                let mut buf = VecBuilder::new(len);
                buf = buf.read_exact(reader)?;
                Ok(Some(buf.into()))
            }
        }
    }
}

impl<W> WriteType<W> for Option<Vec<u8>>
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match &self {
            Some(s) => {
                let len = i32::try_from(s.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
                len.write(writer)?;
                writer.write_all(s)?;
                Ok(())
            }
            None => (-1i32).write(writer),
        }
    }
}

/// Represents a section containing optional tagged fields.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct TaggedFields(pub Vec<(UnsignedVarint, Vec<u8>)>);

impl<R> ReadType<R> for TaggedFields
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?;
        let len = usize::try_from(len.0).map_err(ReadError::Overflow)?;
        let mut res = VecBuilder::new(len);
        for _ in 0..len {
            let tag = UnsignedVarint::read(reader)?;

            let data_len = UnsignedVarint::read(reader)?;
            let data_len = usize::try_from(data_len.0).map_err(ReadError::Overflow)?;
            let mut data_builder = VecBuilder::new(data_len);
            data_builder = data_builder.read_exact(reader)?;

            res.push((tag, data_builder.into()));
        }
        Ok(Self(res.into()))
    }
}

impl<W> WriteType<W> for TaggedFields
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = u64::try_from(self.0.len()).map_err(WriteError::Overflow)?;
        UnsignedVarint(len).write(writer)?;

        for (tag, data) in &self.0 {
            tag.write(writer)?;
            let data_len = u64::try_from(data.len()).map_err(WriteError::Overflow)?;
            UnsignedVarint(data_len).write(writer)?;
            writer.write_all(data)?;
        }

        Ok(())
    }
}

impl<W: Write> WriteType<W> for Option<TaggedFields> {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match self {
            Some(tagged_fields) => tagged_fields.write(writer),
            None => {
                // just write a zero
                writer.write_all(&[0u8]).map_err(WriteError::from)
            }
        }
    }
}

impl<R> ReadType<R> for Vec<i32>
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = i32::read(reader)?;
        if len == -1 {
            Ok(vec![])
        } else {
            let len = usize::try_from(len)?;
            let mut res = VecBuilder::new(len);
            for _ in 0..len {
                res.push(i32::read(reader)?);
            }

            Ok(res.into())
        }
    }
}

impl<W> WriteType<W> for Vec<i32>
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = self.len();
        if len == 0 {
            (-1i32).write(writer)
        } else {
            let len = i32::try_from(len)?;
            len.write(writer)?;

            for item in self {
                item.write(writer)?;
            }

            Ok(())
        }
    }
}

impl<R> ReadCompactType<R> for Vec<i32>
where
    R: Read,
{
    fn read_compact(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?.0;
        if len == 0 {
            Ok(vec![])
        } else {
            let len = usize::try_from(len - 1).map_err(ReadError::Overflow)?;
            let mut builder = VecBuilder::new(len);
            for _ in 0..len {
                builder.push(i32::read(reader)?);
            }

            Ok(builder.into())
        }
    }
}

impl<W> WriteCompactType<W> for Vec<i32>
where
    W: Write,
{
    fn write_compact(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = self.len();
        if len == 0 {
            UnsignedVarint(0).write(writer)
        } else {
            let len = u64::try_from(len + 1)?;
            UnsignedVarint(len).write(writer)?;

            for item in self {
                item.write(writer)?;
            }

            Ok(())
        }
    }
}

impl<R> ReadType<R> for Vec<i64>
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = i32::read(reader)?;
        if len == -1 {
            Ok(vec![])
        } else {
            let len = usize::try_from(len)?;
            let mut res = VecBuilder::new(len);
            for _ in 0..len {
                res.push(i64::read(reader)?);
            }

            Ok(res.into())
        }
    }
}

impl<R> ReadType<R> for Vec<String>
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = i32::read(reader)?;
        if len == -1 {
            Ok(vec![])
        } else {
            let len = usize::try_from(len)?;
            let mut res = VecBuilder::new(len);
            for _ in 0..len {
                res.push(String::read(reader)?);
            }
            Ok(res.into())
        }
    }
}

impl<W> WriteType<W> for Vec<String>
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = i32::try_from(self.len())?;
        len.write(writer)?;

        for item in self {
            item.write(writer)?;
        }

        Ok(())
    }
}

impl<R> ReadCompactType<R> for Vec<String>
where
    R: Read,
{
    fn read_compact(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?.0;
        if len == 0 {
            Ok(vec![])
        } else {
            let len = usize::try_from(len - 1).map_err(ReadError::Overflow)?;
            let mut builder = VecBuilder::new(len);
            for _ in 0..len {
                builder.push(String::read_compact(reader)?);
            }

            Ok(builder.into())
        }
    }
}

impl<W> WriteCompactType<W> for Vec<String>
where
    W: Write,
{
    fn write_compact(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = u64::try_from(self.len() + 1).map_err(WriteError::Overflow)?;
        UnsignedVarint(len).write(writer)?;

        for item in self {
            item.write_compact(writer)?;
        }

        Ok(())
    }
}

/// Represents a sequence of Kafka records as NULLABLE_BYTES.
///
/// This primitive actually depends on the message version and evolved twice in [KIP-32] and [KIP-98]. We only support
/// the latest generation (message version 2).
///
/// It seems that during `Produce` this must contain exactly one batch, but during `Fetch` this can contain zero, one or
/// more batches -- however I could not find any documentation stating this behavior. [KIP-74] at least documents the
/// `Fetch` case, although it does not clearly state that record batches might be cut off half-way (this however is what
/// we see during integration tests w/ Apache Kafka).
///
/// [KIP-32]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message
/// [KIP-74]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-74%3A+Add+Fetch+Response+Size+Limit+in+Bytes
/// [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Records(
    // tell proptest to only generate small vectors, otherwise tests take forever
    #[cfg_attr(
        test,
        proptest(strategy = "prop::collection::vec(any::<RecordBatch>(), 0..2)")
    )]
    pub Vec<RecordBatch>,
);

impl<R> ReadType<R> for Records
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let buf = Option::<Vec<u8>>::read(reader)?.unwrap_or_default();
        let len = u64::try_from(buf.len())?;
        let mut buf = Cursor::new(buf);

        let mut batches = vec![];
        while buf.position() < len {
            let batch = match RecordBatch::read(&mut buf) {
                Ok(batch) => batch,
                Err(ReadError::IO(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Record batch got cut off, likely due to `FetchRequest::max_bytes`.
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
            };

            batches.push(batch);
        }

        Ok(Self(batches))
    }
}

impl<W> WriteType<W> for Records
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        // TODO: it would be nice if we could avoid the copy here by writing the records and then seeking back.
        let mut buf = vec![];
        for record in &self.0 {
            record.write(&mut buf)?;
        }
        Some(buf).write(writer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::protocol::{
        record::{ControlBatchOrRecords, RecordBatchCompression, RecordBatchTimestampType},
        test_utils::test_roundtrip,
    };

    use super::*;

    use crate::protocol::test_utils::test_compact_roundtrip;
    use assert_matches::assert_matches;

    #[test]
    fn varint() {
        for (num, data) in [
            (0, &[0u8].as_ref()),
            (1, &[0x2].as_ref()),
            (-1, &[0x1].as_ref()),
            (23, &[0x2e].as_ref()),
            (-23, &[0x2d].as_ref()),
            (253, &[0xfa, 3].as_ref()),
            (
                1234567890101112,
                &[0xf0, 0x8d, 0xd3, 0xc8, 0xa7, 0xb5, 0xb1, 0x04].as_ref(),
            ),
            (
                -1234567890101112,
                &[0xef, 0x8d, 0xd3, 0xc8, 0xa7, 0xb5, 0xb1, 0x04].as_ref(),
            ),
        ] {
            // decode
            let mut cursor = Cursor::new(data);
            let got = read_varint(&mut cursor).unwrap();
            assert_eq!(got, num);

            // encode
            let mut cursor = Cursor::new([0u8; 16]);
            write_varint(&mut cursor, got).unwrap();
            let len = cursor.position() as usize;
            assert_eq!(len, data.len());
            assert_eq!(&cursor.get_ref()[..len], data.as_ref());
        }
    }

    test_roundtrip!(bool, test_bool_roundtrip);

    #[test]
    fn test_boolean_decode() {
        assert!(!bool::read(&mut Cursor::new(vec![0])).unwrap());

        // When reading a boolean value, any non-zero value is considered true.
        for v in [1, 35, 255] {
            assert!(bool::read(&mut Cursor::new(vec![v])).unwrap());
        }
    }

    test_roundtrip!(i8, test_int8_roundtrip);

    test_roundtrip!(i16, test_int16_roundtrip);

    test_roundtrip!(i32, test_int32_roundtrip);

    test_roundtrip!(i64, test_int64_roundtrip);

    test_roundtrip!(Varint, test_varint_roundtrip);

    #[test]
    fn test_varint_special_values() {
        // Taken from https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints
        for v in [0, -1, 1, -2, 2147483647, -2147483648] {
            let mut data = vec![];
            Varint(v).write(&mut data).unwrap();

            let restored = Varint::read(&mut Cursor::new(data)).unwrap();
            assert_eq!(restored.0, v);
        }
    }

    #[test]
    fn test_varint_read_read_overflow() {
        // this should overflow a 64bit bytes varint
        let mut buf = Cursor::new(vec![0xffu8; 11]);

        let err = Varint::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
        assert_eq!(err.to_string(), "Cannot read data: Unterminated varint",);
    }

    #[test]
    fn test_varint_read_downcast_overflow() {
        // this should overflow when reading a 64bit varint and casting it down to 32bit
        let mut data = vec![0xffu8; 9];
        data.push(0x00);
        let mut buf = Cursor::new(data);

        let err = Varint::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::Overflow(_));
        assert_eq!(
            err.to_string(),
            "Overflow converting integer: out of range integral type conversion attempted",
        );
    }

    test_roundtrip!(Varlong, test_varlong_roundtrip);

    #[test]
    fn test_varlong_special_values() {
        // Taken from https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints + min/max
        for v in [0, -1, 1, -2, 2147483647, -2147483648, i64::MIN, i64::MAX] {
            let mut data = vec![];
            Varlong(v).write(&mut data).unwrap();

            let restored = Varlong::read(&mut Cursor::new(data)).unwrap();
            assert_eq!(restored.0, v);
        }
    }

    #[test]
    fn test_varlong_read_overflow() {
        let mut buf = Cursor::new(vec![0xffu8; 11]);

        let err = Varlong::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
        assert_eq!(err.to_string(), "Cannot read data: Unterminated varint",);
    }

    test_roundtrip!(UnsignedVarint, test_unsigned_varint_roundtrip);

    #[test]
    fn test_unsigned_varint_read_overflow() {
        let mut buf = Cursor::new(vec![0xffu8; 64 / 7 + 1]);

        let err = UnsignedVarint::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
        assert_eq!(err.to_string(), "Cannot read data: Unterminated varint",);
    }

    test_roundtrip!(String, test_string_roundtrip);

    test_roundtrip!(Vec<i32>, test_i32_array_roundtrip);
    test_compact_roundtrip!(Vec<i32>, i32_compact_array_roundtrip);

    #[test]
    fn test_string_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        i16::MAX.write(&mut buf).unwrap();
        buf.set_position(0);

        let err = String::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    test_roundtrip!(Option<String>, test_nullable_string_roundtrip);

    #[test]
    fn test_nullable_string_read_negative_length() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        (-2i16).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = Option::<String>::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::Malformed(_));
        assert_eq!(
            err.to_string(),
            "Malformed data: Invalid negative length for nullable string: -2",
        );
    }

    #[test]
    fn test_nullable_string_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        i16::MAX.write(&mut buf).unwrap();
        buf.set_position(0);

        let err = Option::<String>::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    test_compact_roundtrip!(String, test_compact_string_roundtrip);

    #[test]
    fn test_compact_string_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = String::read_compact(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    test_compact_roundtrip!(Option<String>, test_compact_nullable_string_roundtrip);

    #[test]
    fn test_compact_nullable_string_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = Option::<String>::read_compact(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    test_roundtrip!(Vec<u8>, test_bytes_roundtrip);

    test_roundtrip!(Option<Vec<u8>>, test_nullable_bytes_roundtrip);

    #[test]
    fn test_nullable_bytes_read_negative_length() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        (-2i32).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = Option::<Vec<u8>>::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::Malformed(_));
        assert_eq!(
            err.to_string(),
            "Malformed data: Invalid negative length for nullable bytes: -2",
        );
    }

    #[test]
    fn test_nullable_bytes_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        i32::MAX.write(&mut buf).unwrap();
        buf.set_position(0);

        let err = Option::<Vec<u8>>::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    test_roundtrip!(TaggedFields, test_tagged_fields_roundtrip);

    #[test]
    fn test_tagged_fields_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());

        // number of fields
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();

        // tag
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();

        // data length
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();

        buf.set_position(0);

        let err = TaggedFields::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    #[test]
    fn test_array_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        i32::MAX.write(&mut buf).unwrap();
        buf.set_position(0);

        let err = Vec::<Large>::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    #[test]
    fn test_compact_array_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = Vec::<Large>::read_compact(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    test_roundtrip!(Records, test_records_roundtrip);

    #[test]
    fn test_records_partial() {
        // Records might be partially returned when fetch requests are issued w/ size limits
        let batch_1 = record_batch(1);
        let batch_2 = record_batch(2);

        let mut buf = vec![];
        batch_1.write(&mut buf).unwrap();
        batch_2.write(&mut buf).unwrap();
        let inner = buf[..buf.len() - 1].to_vec();

        let mut buf = vec![];
        Some(inner).write(&mut buf).unwrap();

        let records = Records::read(&mut Cursor::new(buf)).unwrap();
        assert_eq!(records.0, vec![batch_1]);
    }

    fn record_batch(base_offset: i64) -> RecordBatch {
        RecordBatch {
            base_offset,
            partition_leader_epoch: 0,
            last_offset_delta: 0,
            first_timestamp: 0,
            max_timestamp: 0,
            producer_id: 0,
            producer_epoch: 0,
            base_sequence: 0,
            records: ControlBatchOrRecords::Records(vec![]),
            compression: RecordBatchCompression::NoCompression,
            is_transactional: false,
            timestamp_type: RecordBatchTimestampType::CreateTime,
        }
    }

    /// A rather large struct here to trigger OOM.
    #[derive(Debug)]
    struct Large {
        _inner: [u8; 1024],
    }

    impl<R> ReadType<R> for Large
    where
        R: Read,
    {
        fn read(reader: &mut R) -> Result<Self, ReadError> {
            i32::read(reader)?;
            unreachable!()
        }
    }

    impl<R: Read> ReadCompactType<R> for Large {
        fn read_compact(reader: &mut R) -> Result<Self, ReadError> {
            i32::read(reader)?;
            unreachable!()
        }
    }

    impl<R: Read> ReadType<R> for Vec<Large> {
        fn read(reader: &mut R) -> Result<Self, ReadError> {
            let len = i32::read(reader)?;
            if len == -1 {
                Ok(vec![])
            } else {
                let len = usize::try_from(len)?;
                let mut res = VecBuilder::new(len);
                for _ in 0..len {
                    res.push(Large::read(reader)?);
                }
                Ok(res.into())
            }
        }
    }

    impl<R: Read> ReadCompactType<R> for Vec<Large> {
        fn read_compact(reader: &mut R) -> Result<Self, ReadError> {
            let len = UnsignedVarint::read(reader)?.0;
            if len == 0 {
                Ok(vec![])
            } else {
                let len = usize::try_from(len - 1).map_err(ReadError::Overflow)?;
                let mut builder = VecBuilder::new(len);
                for _ in 0..len {
                    builder.push(Large::read_compact(reader)?);
                }

                Ok(builder.into())
            }
        }
    }
}
