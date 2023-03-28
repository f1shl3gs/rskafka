use std::io::{Read, Write};

use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ReadError {
    #[error("Cannot read data: {0}")]
    IO(#[from] std::io::Error),

    #[error("Overflow converting integer: {0}")]
    Overflow(#[from] std::num::TryFromIntError),

    #[error("Malformed data: {0}")]
    Malformed(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub trait ReadType<R>: Sized
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError>;
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum WriteError {
    #[error("Cannot write data: {0}")]
    IO(#[from] std::io::Error),

    #[error("Overflow converting integer: {0}")]
    Overflow(#[from] std::num::TryFromIntError),

    #[error("Malformed data: {0}")]
    Malformed(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub trait WriteType<W>: Sized
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError>;
}

pub trait WriteCompactType<W>: WriteType<W>
where
    W: Write,
{
    fn write_compact(&self, writer: &mut W) -> Result<(), WriteError>;
}

pub trait ReadCompactType<R>: ReadType<R>
where
    R: Read,
{
    fn read_compact(reader: &mut R) -> Result<Self, ReadError>;
}
