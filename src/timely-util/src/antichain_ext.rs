// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Antichain utilities.

use std::fmt;
use std::slice;

use timely::PartialOrder;

/// An [`Antichain`] that stores at most one element.
///
/// [`Antichain`]: timely::progress::frontier::Antichain
#[derive(Clone, PartialEq, Eq)]
pub enum Max1Antichain<T> {
    /// An antichain with a single element.
    Singular(T),
    /// An antichain with no elements.
    Empty,
}

impl<T> Max1Antichain<T>
where
    T: PartialOrder,
{
    /// Returns true if any item in the antichain is strictly less than the
    /// argument.
    pub fn less_than(&self, element: &T) -> bool {
        match self {
            Max1Antichain::Singular(s) => s.less_than(element),
            Max1Antichain::Empty => false,
        }
    }

    /// Returns true if any item in the antichain is less than or equal to the
    /// argument.
    pub fn less_equal(&self, element: &T) -> bool {
        match self {
            Max1Antichain::Singular(s) => s.less_equal(element),
            Max1Antichain::Empty => false,
        }
    }

    /// Reveals the elements in the antichain.
    pub fn elements(&self) -> &[T] {
        match self {
            Max1Antichain::Singular(s) => slice::from_ref(s),
            Max1Antichain::Empty => &[],
        }
    }

    /// Converts the antichain to an option.
    ///
    /// Use with caution. The comparison semantics on the returned option do
    /// not match the this type's [`PartialOrder`] implementation.
    pub fn as_option(&self) -> Option<&T> {
        match self {
            Max1Antichain::Singular(s) => Some(s),
            Max1Antichain::Empty => None,
        }
    }
}

impl<T> fmt::Debug for Max1Antichain<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Max1Antichain::Empty => f.write_str("{}"),
            Max1Antichain::Singular(s) => {
                f.write_str("{")?;
                s.fmt(f)?;
                f.write_str("}")
            }
        }
    }
}

impl<T> PartialOrder for Max1Antichain<T>
where
    T: PartialOrder,
{
    fn less_equal(&self, other: &Self) -> bool {
        match (self, other) {
            (Max1Antichain::Singular(l), Max1Antichain::Singular(r)) => l.less_equal(r),
            (Max1Antichain::Singular(_), Max1Antichain::Empty) => true,
            (Max1Antichain::Empty, Max1Antichain::Singular(_)) => false,
            (Max1Antichain::Empty, Max1Antichain::Empty) => true,
        }
    }
}

impl<T> From<Option<T>> for Max1Antichain<T> {
    fn from(option: Option<T>) -> Max1Antichain<T> {
        match option {
            Some(t) => Max1Antichain::Singular(t),
            None => Max1Antichain::Empty,
        }
    }
}

#[cfg(test)]
mod tests {
    use timely::PartialOrder;

    use super::Max1Antichain;

    #[test]
    fn test_max1antichain_eq() {
        assert_eq!(Max1Antichain::<i64>::Empty, Max1Antichain::Empty);
        assert_eq!(Max1Antichain::Singular(1), Max1Antichain::Singular(1));
        assert_ne!(Max1Antichain::Singular(1), Max1Antichain::Singular(2));
        assert_ne!(Max1Antichain::Empty, Max1Antichain::Singular(2));
    }

    #[test]
    fn test_max1antichain_partial_order() {
        assert!(PartialOrder::less_equal(
            &Max1Antichain::<i64>::Empty,
            &Max1Antichain::Empty
        ));
        assert!(PartialOrder::less_equal(
            &Max1Antichain::Singular(1),
            &Max1Antichain::Empty
        ));
        assert!(PartialOrder::less_equal(
            &Max1Antichain::Singular(1),
            &Max1Antichain::Singular(1)
        ));
        assert!(PartialOrder::less_equal(
            &Max1Antichain::Singular(1),
            &Max1Antichain::Singular(2)
        ));
        assert!(!PartialOrder::less_equal(
            &Max1Antichain::Singular(2),
            &Max1Antichain::Singular(1)
        ));
        assert!(!PartialOrder::less_equal(
            &Max1Antichain::Empty,
            &Max1Antichain::Singular(1)
        ));
    }

    #[test]
    fn test_max1antichain_less_equal() {
        assert!(Max1Antichain::Singular(1).less_equal(&1));
        assert!(Max1Antichain::Singular(1).less_equal(&2));
        assert!(!Max1Antichain::Singular(2).less_equal(&1));
        assert!(!Max1Antichain::Empty.less_equal(&1));
    }

    #[test]
    fn test_max1antichain_less_than() {
        assert!(!Max1Antichain::Singular(1).less_than(&1));
        assert!(Max1Antichain::Singular(1).less_than(&2));
        assert!(!Max1Antichain::Singular(2).less_than(&1));
        assert!(!Max1Antichain::Empty.less_than(&1));
    }
}
