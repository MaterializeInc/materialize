// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A multi-dimensional array data type.

use std::cmp::Ordering;
use std::error::Error;
use std::{fmt, mem};

use mz_lowertest::MzReflect;
use mz_proto::{RustType, TryFromProtoError};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::row::DatumList;

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.array.rs"));

/// The maximum number of dimensions permitted in an array.
pub const MAX_ARRAY_DIMENSIONS: u8 = 6;

/// A variable-length multi-dimensional array.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Array<'a> {
    /// The elements in the array.
    pub(crate) elements: DatumList<'a>,
    /// The dimensions of the array.
    pub(crate) dims: ArrayDimensions<'a>,
}

impl<'a> Array<'a> {
    /// Returns the dimensions of the array.
    pub fn dims(&self) -> ArrayDimensions<'a> {
        self.dims
    }

    /// Returns the elements of the array.
    pub fn elements(&self) -> DatumList<'a> {
        self.elements
    }
}

/// The dimensions of an [`Array`].
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct ArrayDimensions<'a> {
    pub(crate) data: &'a [u8],
}

impl ArrayDimensions<'_> {
    /// Returns the number of dimensions in the array as a [`u8`].
    pub fn ndims(&self) -> u8 {
        let ndims = self.data.len() / (mem::size_of::<usize>() * 2);
        ndims.try_into().expect("ndims is known to fit in a u8")
    }

    /// Returns the number of the dimensions in the array as a [`usize`].
    pub fn len(&self) -> usize {
        self.ndims().into()
    }

    /// Reports whether the number of dimensions in the array is zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Ord for ArrayDimensions<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ndims().cmp(&other.ndims())
    }
}

impl PartialOrd for ArrayDimensions<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for ArrayDimensions<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_list().entries(*self).finish()
    }
}

impl<'a> IntoIterator for ArrayDimensions<'a> {
    type Item = ArrayDimension;
    type IntoIter = ArrayDimensionsIter<'a>;

    fn into_iter(self) -> ArrayDimensionsIter<'a> {
        ArrayDimensionsIter { data: self.data }
    }
}

/// An iterator over the dimensions in an [`ArrayDimensions`].
#[derive(Debug)]
pub struct ArrayDimensionsIter<'a> {
    data: &'a [u8],
}

impl Iterator for ArrayDimensionsIter<'_> {
    type Item = ArrayDimension;

    fn next(&mut self) -> Option<ArrayDimension> {
        if self.data.is_empty() {
            None
        } else {
            let sz = mem::size_of::<usize>();
            let lower_bound = isize::from_ne_bytes(self.data[..sz].try_into().unwrap());
            self.data = &self.data[sz..];
            let length = usize::from_ne_bytes(self.data[..sz].try_into().unwrap());
            self.data = &self.data[sz..];
            Some(ArrayDimension {
                lower_bound,
                length,
            })
        }
    }
}

/// The specification of one dimension of an [`Array`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ArrayDimension {
    /// The "logical" index at which this dimension begins. This value has no bearing on the
    /// physical layout of the data, only how users want to use its indices (which may be negative).
    pub lower_bound: isize,
    /// The number of elements in this array.
    pub length: usize,
}

impl ArrayDimension {
    /// Presents the "logical indices" of the array, i.e. those that are revealed to the user.
    ///
    /// # Panics
    /// - If the array contain more than [`isize::MAX`] elements (i.e. more than 9EB of data).
    pub fn dimension_bounds(&self) -> (isize, isize) {
        (
            self.lower_bound,
            self.lower_bound
                + isize::try_from(self.length).expect("fewer than isize::MAX elements")
                - 1,
        )
    }
}

/// An error that can occur when constructing an array.
#[derive(
    Arbitrary,
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    MzReflect,
)]
pub enum InvalidArrayError {
    /// The number of dimensions in the array exceeds [`MAX_ARRAY_DIMENSIONS]`.
    TooManyDimensions(usize),
    /// The number of array elements does not match the cardinality derived from
    /// its dimensions.
    WrongCardinality { actual: usize, expected: usize },
}

impl fmt::Display for InvalidArrayError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InvalidArrayError::TooManyDimensions(n) => write!(
                f,
                "number of array dimensions ({}) exceeds the maximum allowed ({})",
                n, MAX_ARRAY_DIMENSIONS
            ),
            InvalidArrayError::WrongCardinality { actual, expected } => write!(
                f,
                "number of array elements ({}) does not match declared cardinality ({})",
                actual, expected
            ),
        }
    }
}

impl Error for InvalidArrayError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl RustType<ProtoInvalidArrayError> for InvalidArrayError {
    fn into_proto(&self) -> ProtoInvalidArrayError {
        use proto_invalid_array_error::*;
        use Kind::*;
        let kind = match self {
            InvalidArrayError::TooManyDimensions(dims) => TooManyDimensions(dims.into_proto()),
            InvalidArrayError::WrongCardinality { actual, expected } => {
                WrongCardinality(ProtoWrongCardinality {
                    actual: actual.into_proto(),
                    expected: expected.into_proto(),
                })
            }
        };
        ProtoInvalidArrayError { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoInvalidArrayError) -> Result<Self, TryFromProtoError> {
        use proto_invalid_array_error::Kind::*;
        match proto.kind {
            Some(kind) => match kind {
                TooManyDimensions(dims) => Ok(InvalidArrayError::TooManyDimensions(
                    usize::from_proto(dims)?,
                )),
                WrongCardinality(v) => Ok(InvalidArrayError::WrongCardinality {
                    actual: usize::from_proto(v.actual)?,
                    expected: usize::from_proto(v.expected)?,
                }),
            },
            None => Err(TryFromProtoError::missing_field(
                "`ProtoInvalidArrayError::kind`",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[mz_ore::test]
        fn invalid_array_error_protobuf_roundtrip(expect in any::<InvalidArrayError>()) {
            let actual = protobuf_roundtrip::<_, ProtoInvalidArrayError>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
