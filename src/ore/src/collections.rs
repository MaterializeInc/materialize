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

//! Collection utilities.

use std::fmt::Display;

/// Extension methods for collections.
pub trait CollectionExt<T>: Sized
where
    T: IntoIterator,
{
    /// Consumes the collection and returns its first element.
    ///
    /// This method panics if the collection does not have at least one element.
    fn into_first(self) -> T::Item;

    /// Consumes the collection and returns its last element.
    ///
    /// This method panics if the collection does not have at least one element.
    fn into_last(self) -> T::Item;

    /// Consumes the collection and returns its only element.
    ///
    /// This method panics if the collection does not have exactly one element.
    fn into_element(self) -> T::Item {
        self.expect_element("into_element called on collection with more than one element")
    }

    /// Consumes the collection and returns its only element.
    ///
    /// This method panics with the given error message if the collection does not have exactly one element.
    fn expect_element<Err: Display>(self, msg: Err) -> T::Item;
}

impl<T> CollectionExt<T> for T
where
    T: IntoIterator,
{
    fn into_first(self) -> T::Item {
        self.into_iter().next().unwrap()
    }

    fn into_last(self) -> T::Item {
        self.into_iter().last().unwrap()
    }

    fn expect_element<Err: Display>(self, msg: Err) -> T::Item {
        let mut iter = self.into_iter();
        match (iter.next(), iter.next()) {
            (Some(el), None) => el,
            _ => panic!("{}", msg),
        }
    }
}
