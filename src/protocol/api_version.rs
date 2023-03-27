use super::primitives::Int16;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct ApiVersion(pub Int16);

impl From<i16> for ApiVersion {
    fn from(value: i16) -> Self {
        ApiVersion(Int16(value))
    }
}

impl ApiVersion {
    pub const fn new(value: i16) -> ApiVersion {
        ApiVersion(Int16(value))
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ApiVersionRange {
    min: ApiVersion,
    max: ApiVersion,
}

impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0 .0)
    }
}

impl ApiVersionRange {
    pub const fn new(min: ApiVersion, max: ApiVersion) -> Self {
        assert!(min.0 .0 <= max.0 .0);

        Self { min, max }
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
