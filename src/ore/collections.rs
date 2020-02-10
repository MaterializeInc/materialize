// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Collection utilities.

/// Extension methods for collections.
pub trait CollectionExt<T>
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
    fn into_element(self) -> T::Item;
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

    fn into_element(self) -> T::Item {
        let mut iter = self.into_iter();
        match (iter.next(), iter.next()) {
            (Some(el), None) => el,
            _ => panic!("into_element called on collection with more than one element"),
        }
    }
}
