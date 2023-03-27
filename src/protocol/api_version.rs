use std::io::Read;

use crate::protocol::traits::{ReadError, ReadType};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[non_exhaustive]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct ApiVersion(pub i16);

impl ApiVersion {
    pub const fn new(value: i16) -> ApiVersion {
        Self(value)
    }
}

impl<R: Read> ReadType<R> for ApiVersion {
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let v = i16::read(reader)?;
        Ok(ApiVersion(v))
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ApiVersionRange {
    min: ApiVersion,
    max: ApiVersion,
}

impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ApiVersionRange {
    pub const fn new(min: i16, max: i16) -> Self {
        assert!(min <= max);

        Self {
            min: ApiVersion(min),
            max: ApiVersion(max),
        }
    }

    pub fn min(&self) -> ApiVersion {
        self.min
    }

    pub fn max(&self) -> ApiVersion {
        self.max
    }
}

impl std::fmt::Display for ApiVersionRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.min, self.max)
    }
}
