//! Primitive types.
//!
//! # References
//! - <https://kafka.apache.org/protocol#protocol_types>
//! - <https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-UnsignedVarints>

use std::io::{Cursor, Read, Write};

use integer_encoding::{VarIntReader, VarIntWriter};

use crate::protocol::traits::{ReadCompactType, WriteCompactType};
#[cfg(test)]
use proptest::prelude::*;

use super::{
    record::RecordBatch,
    traits::{ReadError, ReadType, WriteError, WriteType},
    vec_builder::VecBuilder,
};

impl<R: Read> ReadType<R> for bool {
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        match buf[0] {
            0 => Ok(false),
            _ => Ok(true),
        }
    }
}

impl<W: Write> WriteType<W> for bool {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match self {
            true => Ok(writer.write_all(&[1])?),
            false => Ok(writer.write_all(&[0])?),
        }
    }
}

impl<R: Read> ReadType<R> for i8 {
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(i8::from_be_bytes(buf))
    }
}

impl<W: Write> WriteType<W> for i8 {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let buf = self.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

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

impl<R: Read> ReadType<R> for i32 {
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }
}

impl<W: Write> WriteType<W> for i32 {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let buf = self.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

impl<R: Read> ReadType<R> for i64 {
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }
}

impl<W: Write> WriteType<W> for i64 {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let buf = self.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
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
        // workaround for https://github.com/dermesser/integer-encoding-rs/issues/21
        // read 64bit and use a checked downcast instead
        let i: i64 = reader.read_varint()?;
        Ok(Self(i32::try_from(i)?))
    }
}

impl<W> WriteType<W> for Varint
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        writer.write_varint(self.0)?;
        Ok(())
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
        Ok(Self(reader.read_varint()?))
    }
}

impl<W> WriteType<W> for Varlong
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        writer.write_varint(self.0)?;
        Ok(())
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
        let mut buf = [0u8; 1];
        let mut res: u64 = 0;
        let mut shift = 0;
        loop {
            reader.read_exact(&mut buf)?;
            let c: u64 = buf[0].into();

            res |= (c & 0x7f) << shift;
            shift += 7;

            if (c & 0x80) == 0 {
                break;
            }
            if shift > 63 {
                return Err(ReadError::Malformed(
                    String::from("Overflow while reading unsigned varint").into(),
                ));
            }
        }

        Ok(Self(res))
    }
}

impl<W> WriteType<W> for UnsignedVarint
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let mut curr = self.0;
        loop {
            let mut c = u8::try_from(curr & 0x7f).map_err(WriteError::Overflow)?;
            curr >>= 7;
            if curr > 0 {
                c |= 0x80;
            }
            writer.write_all(&[c])?;

            if curr == 0 {
                break;
            }
        }
        Ok(())
    }
}

/// Represents a sequence of characters or null.
///
/// For non-null strings, first the length N is given as an INT16. Then N bytes follow which are the UTF-8 encoding of
/// the character sequence. A null value is encoded with length of -1 and there are no following bytes.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct NullableString(pub Option<String>);

impl<R> ReadType<R> for NullableString
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = i16::read(reader)?;
        match len {
            l if l < -1 => Err(ReadError::Malformed(
                format!("Invalid negative length for nullable string: {}", l).into(),
            )),
            -1 => Ok(Self(None)),
            l => {
                let len = usize::try_from(l)?;
                let mut buf = VecBuilder::new(len);
                buf = buf.read_exact(reader)?;
                let s =
                    String::from_utf8(buf.into()).map_err(|e| ReadError::Malformed(Box::new(e)))?;
                Ok(Self(Some(s)))
            }
        }
    }
}

impl<W> WriteType<W> for NullableString
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match &self.0 {
            Some(s) => {
                let l = i16::try_from(s.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
                l.write(writer)?;
                writer.write_all(s.as_bytes())?;
                Ok(())
            }
            None => (-1i16).write(writer),
        }
    }
}

// STRING
impl<R: Read> ReadType<R> for String {
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = i16::read(reader)?;
        let len = usize::try_from(len).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut buf = VecBuilder::new(len);
        buf = buf.read_exact(reader)?;
        let s = String::from_utf8(buf.into()).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        Ok(s)
    }
}

impl<W: Write> WriteType<W> for String {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = i16::try_from(self.len()).map_err(WriteError::Overflow)?;
        len.write(writer)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

// NULLABLE_STRING
impl<R: Read> ReadType<R> for Option<String> {
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

impl<W: Write> WriteType<W> for Option<String> {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match &self {
            Some(s) => {
                let l =
                    i16::try_from(s.len()).map_err(|err| WriteError::Malformed(Box::new(err)))?;
                l.write(writer)?;
                writer.write_all(s.as_bytes())?;
                Ok(())
            }
            None => (-1i16).write(writer),
        }
    }
}

// COMPACT_STRING
impl<R: Read> ReadCompactType<R> for String {
    fn read_compact(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?;

        match len.0 {
            0 => Err(ReadError::Malformed(
                "CompactString must have non-zero length".into(),
            )),
            len => {
                let len = usize::try_from(len)?;
                let len = len - 1;

                let mut buf = VecBuilder::new(len);
                buf = buf.read_exact(reader)?;

                let s =
                    String::from_utf8(buf.into()).map_err(|e| ReadError::Malformed(Box::new(e)))?;
                Ok(s)
            }
        }
    }
}

impl<W: Write> WriteCompactType<W> for String {
    fn write_compact(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = u64::try_from(self.len() + 1).map_err(WriteError::Overflow)?;
        UnsignedVarint(len).write(writer)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

// COMPACT_NULLABLE_STRING
impl<R: Read> ReadCompactType<R> for Option<String> {
    fn read_compact(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?;

        match len.0 {
            0 => Ok(None),
            len => {
                let len = usize::try_from(len)?;
                let len = len - 1;

                let mut buf = VecBuilder::new(len);
                buf = buf.read_exact(reader)?;

                let s =
                    String::from_utf8(buf.into()).map_err(|e| ReadError::Malformed(Box::new(e)))?;
                Ok(Some(s))
            }
        }
    }
}

impl<W: Write> WriteCompactType<W> for Option<String> {
    fn write_compact(&self, writer: &mut W) -> Result<(), WriteError> {
        match &self {
            Some(s) => {
                let len = u64::try_from(s.len() + 1).map_err(WriteError::Overflow)?;
                UnsignedVarint(len).write(writer)?;
                writer.write_all(s.as_bytes())?;
            }
            None => {
                UnsignedVarint(0).write(writer)?;
            }
        }
        Ok(())
    }
}

impl<R: Read> ReadType<R> for Option<Vec<u8>> {
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

impl<W: Write> WriteType<W> for Option<Vec<u8>> {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match self {
            Some(buf) => {
                let l = i32::try_from(buf.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
                l.write(writer)?;
                writer.write_all(buf)?;
                Ok(())
            }
            None => (-1i32).write(writer),
        }
    }
}

impl<R: Read> ReadType<R> for Vec<u8> {
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = i32::read(reader)?;
        match len {
            l if l < 0 => Err(ReadError::Malformed(
                format!("Invalid length for bytes: {}", l).into(),
            )),
            0 => Ok(vec![]),
            l => {
                let len = usize::try_from(l)?;
                let mut buf = VecBuilder::new(len);
                buf = buf.read_exact(reader)?;
                Ok(buf.into())
            }
        }
    }
}

impl<R: Read> ReadCompactType<R> for Vec<u8> {
    fn read_compact(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?;

        match len.0 {
            0 => Err(ReadError::Malformed(
                "CompactBytes must have non-zero length".into(),
            )),

            len => {
                let len = usize::try_from(len)?;
                let len = len - 1;

                let mut buf = VecBuilder::new(len);
                buf = buf.read_exact(reader)?;

                Ok(buf.into())
            }
        }
    }
}

impl<W: Write> WriteType<W> for Vec<u8> {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        if self.is_empty() {
            (0i32).write(writer)
        } else {
            let l =
                i32::try_from(self.len()).map_err(|err| WriteError::Malformed(Box::new(err)))?;
            l.write(writer)?;
            writer.write_all(&self)?;
            Ok(())
        }
    }
}

impl<W: Write> WriteCompactType<W> for Vec<u8> {
    fn write_compact(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = u64::try_from(self.len() + 1).map_err(WriteError::Overflow)?;
        UnsignedVarint(len).write(writer)?;
        writer.write_all(&self)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct CompactBytes(pub Vec<u8>);

impl<R> ReadType<R> for CompactBytes
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?;

        match len.0 {
            0 => Err(ReadError::Malformed(
                "CompactBytes must have non-zero length".into(),
            )),

            len => {
                let len = usize::try_from(len)?;
                let len = len - 1;

                let mut buf = VecBuilder::new(len);
                buf = buf.read_exact(reader)?;

                Ok(Self(buf.into()))
            }
        }
    }
}

impl<W> WriteType<W> for CompactBytes
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = u64::try_from(self.0.len() + 1).map_err(WriteError::Overflow)?;
        UnsignedVarint(len).write(writer)?;
        writer.write_all(&self.0)?;
        Ok(())
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
            None => TaggedFields::default().write(writer),
        }
    }
}

/// Represents a sequence of objects of a given type T.
///
/// Type T can be either a primitive type (e.g. STRING) or a structure. First, the length N is given as an INT32. Then
/// N instances of type T follow. A null array is represented with a length of -1. In protocol documentation an array
/// of T instances is referred to as `[T]`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Array<T>(pub Option<Vec<T>>);

impl<R: Read> ReadType<R> for Vec<String> {
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

// ARRAY<STRING>
impl<W> WriteType<W> for Vec<String>
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        if self.len() == 0 {
            (-1i32).write(writer)
        } else {
            let len = i32::try_from(self.len())?;
            len.write(writer)?;

            for elmt in self {
                elmt.write(writer)?;
            }

            Ok(())
        }
    }
}

impl<R: Read> ReadType<R> for Vec<i32> {
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

impl<W: Write> WriteType<W> for Vec<i32> {
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        if self.len() == 0 {
            (-1i32).write(writer)
        } else {
            let len = i32::try_from(self.len())?;
            len.write(writer)?;

            for elmt in self {
                elmt.write(writer)?;
            }

            Ok(())
        }
    }
}

impl<R> ReadCompactType<R> for Vec<String>
where
    R: Read,
{
    fn read_compact(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?.0;
        if len == 0 {
            return Ok(vec![]);
        }

        let len = usize::try_from(len - 1).map_err(ReadError::Overflow)?;
        let mut builder = VecBuilder::new(len);
        for _ in 0..len {
            builder.push(String::read_compact(reader)?);
        }

        Ok(builder.into())
    }
}

impl<W: Write> WriteCompactType<W> for Vec<String> {
    fn write_compact(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = self.len();
        if len == 0 {
            UnsignedVarint(0).write(writer)
        } else {
            let len = u64::try_from(self.len() + 1).map_err(WriteError::from)?;
            UnsignedVarint(len).write(writer)?;

            for elmt in self {
                elmt.write_compact(writer)?;
            }

            Ok(())
        }
    }
}

impl<W: Write> WriteCompactType<W> for Vec<i32> {
    fn write_compact(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = self.len();
        if len == 0 {
            UnsignedVarint(0).write(writer)
        } else {
            let len = u64::try_from(self.len() + 1).map_err(WriteError::from)?;
            UnsignedVarint(len).write(writer)?;

            for elmt in self {
                elmt.write(writer)?;
            }

            Ok(())
        }
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
        // TODO: it would be nice if we could avoid the copy here by writing the
        //   records and then seeking back.
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

    use assert_matches::assert_matches;

    test_roundtrip!(bool, test_bool_roundtrip);

    #[test]
    fn test_boolean_decode() {
        assert!(!bool::read(&mut Cursor::new(vec![0])).unwrap());

        // When reading a boolean value, any non-zero value is considered true.
        for v in [1, 35, 255] {
            assert!(bool::read(&mut Cursor::new(vec![v])).unwrap());
        }
    }

    test_roundtrip!(i16, test_i16_roundtrip);

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
        assert_matches!(err, ReadError::Malformed(_));
        assert_eq!(
            err.to_string(),
            "Malformed data: Overflow while reading unsigned varint",
        );
    }

    test_roundtrip!(String, test_string_roundtrip);

    #[test]
    fn test_string_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        i16::MAX.write(&mut buf).unwrap();
        buf.set_position(0);

        let err = String::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    test_roundtrip!(NullableString, test_nullable_string_roundtrip);

    #[test]
    fn test_nullable_string_read_negative_length() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        (-2i16).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = NullableString::read(&mut buf).unwrap_err();
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

        let err = NullableString::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    // test_roundtrip!(String, test_compact_string_roundtrip);

    #[test]
    fn test_compact_string_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = String::read_compact(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    // test_roundtrip!(CompactNullableString,test_compact_nullable_string_roundtrip);

    #[test]
    fn test_compact_nullable_string_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();
        buf.set_position(0);

        let result: Result<Option<String>, ReadError> = ReadCompactType::read_compact(&mut buf);
        assert_matches!(result.unwrap_err(), ReadError::IO(_));
    }

    /*
        test_roundtrip!(NullableBytes, test_nullable_bytes_roundtrip);

        #[test]
        fn test_nullable_bytes_read_negative_length() {
            let mut buf = Cursor::new(Vec::<u8>::new());
            (-2i32).write(&mut buf).unwrap();
            buf.set_position(0);

            let err = NullableBytes::read(&mut buf).unwrap_err();
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

            let err = NullableBytes::read(&mut buf).unwrap_err();
            assert_matches!(err, ReadError::IO(_));
        }
    */

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

    test_roundtrip!(Vec<i32>, test_array_roundtrip);

    test_roundtrip!(Vec<i32>, test_compact_array_roundtrip);

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
}
