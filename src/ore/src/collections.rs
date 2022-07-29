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

use std::collections::btree_map::Entry as BEntry;
use std::collections::hash_map::Entry as HEntry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Display};

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

/// Extension methods for associative collections.
pub trait AssociativeExt<K, V> {
    /// Inserts a key and value, panicking with
    /// a given message if a true
    /// insert (as opposed to an update) cannot be done
    /// because the key already existed in the collection.
    fn expect_insert(&mut self, k: K, v: V, msg: &str);
    /// Inserts a key and value, panicking if a true
    /// insert (as opposed to an update) cannot be done
    /// because the key already existed in the collection.
    fn unwrap_insert(&mut self, k: K, v: V) {
        self.expect_insert(k, v, "called `unwrap_insert` for an already-existing key")
    }
}

impl<K, V> AssociativeExt<K, V> for HashMap<K, V>
where
    K: Eq + std::hash::Hash + Debug,
    V: Debug,
{
    fn expect_insert(&mut self, k: K, v: V, msg: &str) {
        match self.entry(k) {
            HEntry::Vacant(e) => {
                e.insert(v);
            }
            HEntry::Occupied(e) => {
                panic!(
                    "{} (key: {:?}, old value: {:?}, new value: {:?})",
                    msg,
                    e.key(),
                    e.get(),
                    v
                )
            }
        }
    }
}

impl<K, V> AssociativeExt<K, V> for BTreeMap<K, V>
where
    K: Ord + Debug,
    V: Debug,
{
    fn expect_insert(&mut self, k: K, v: V, msg: &str) {
        match self.entry(k) {
            BEntry::Vacant(e) => {
                e.insert(v);
            }
            BEntry::Occupied(e) => {
                panic!(
                    "{} (key: {:?}, old value: {:?}, new value: {:?})",
                    msg,
                    e.key(),
                    e.get(),
                    v
                )
            }
        }
    }
}
