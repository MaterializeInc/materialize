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

//! [`AddressMap`] implementation.

use std::collections::BTreeMap;
use std::marker::PhantomData;

/// A minimal interface wrapping [`BTreeMap`] that maps from `const` pointers to
/// `K` to values of type `V`.
///
/// This map is needed in order to ensure that structurally identical sub-terms
/// of the same expression can be assigned different values. This is important
/// for values that depend on the position of a sub-term in the encosing
/// expression (such as the pre- or post-order visitation index).
#[derive(Debug)]
pub struct AddressMap<'a, K, V> {
    map: BTreeMap<*const K, V>,
    phantom: PhantomData<&'a K>,
}

impl<'a, K, V: Clone> Clone for AddressMap<'a, K, V> {
    fn clone(&self) -> Self {
        match self {
            AddressMap { map, phantom } => AddressMap {
                map: map.clone(),
                phantom: phantom.clone(),
            },
        }
    }
}

impl<'a, K, V> Default for AddressMap<'a, K, V> {
    fn default() -> Self {
        Self {
            map: Default::default(),
            phantom: Default::default(),
        }
    }
}

impl<'a, K, V> AddressMap<'a, K, V> {
    /// Get a mutable reference to the vlaue associated with the address of `k`
    /// from the underlying map, inserting and returning the [`Default`] for `V`
    /// if the entry is absent.
    pub fn entry_or_default(&mut self, k: &'a K) -> &mut V
    where
        V: Default,
    {
        let k_ptr = std::ptr::from_ref(k);
        self.map.entry(k_ptr).or_default()
    }

    /// Get an immutable reference to the value associated with the address of
    /// `k` from the underlying map if an entry is present, or `None` otherwise.
    pub fn get(&'a self, k: &K) -> Option<&'a V> {
        let k_ptr = std::ptr::from_ref(k);
        self.map.get(&k_ptr)
    }
}

#[cfg(test)]
mod tests {
    use super::AddressMap;
    use std::collections::BTreeMap;
    use Expr::*;

    /// This test motivates the need for [`super::AddressMap`].
    ///
    /// Structurally identical `Expr` subtrees are considered equal by the
    /// `Equals` and `Ord` implementations, so they cannot have separate slots
    /// in an attribute container of type `BTreeMap<&Expr, Attribute>`. This
    /// means that we cannot use a `BTreeMap` as a sidecar container of
    /// attributes that are not determined by the expression structure.
    ///
    /// See [`test_address_address_map`] for an attribute container test that
    /// works as expected.
    #[mz_ore::test]
    pub fn test_address_reference_map() {
        // Initialize an attribute container that has some issues.
        let mut attrs = BTreeMap::default();

        // Create an expression `1 + 1` where `1` appears twice.
        let expr = Add(Const(1).into(), Const(1).into());

        // Save pre-order visitation index of expr and its subterms.
        *(attrs.entry(&expr).or_default()) = 0;
        if let Add(lhs, rhs) = &expr {
            // Since lhs and rhs are structurally identical expressions, the
            // `lhs: 1` entry is overwritten by `rhs: 2`!
            *(attrs.entry(&lhs).or_default()) = 1;
            *(attrs.entry(&rhs).or_default()) = 2;
        }

        // If we uncomment this line and make `expr` mutable the borrow checker
        // will (rightfully) complain because `attrs` is still alive.
        /*
        if let Add(lhs, rhs) = &mut expr {
            *lhs = Const(3).into();
            *rhs = Const(4).into();
        }
        */

        assert_eq!(attrs.get(&expr).cloned(), Some(0));
        if let Add(lhs, rhs) = &expr {
            assert_eq!(attrs.get(lhs.as_ref()).cloned(), Some(2)); // We want `1` here!
            assert_eq!(attrs.get(rhs.as_ref()).cloned(), Some(2));
        }
    }

    /// A test that ensures that [`super::AddressMap`] works as expected.
    ///
    /// See [`test_address_reference_map`] for an example that motivates why
    /// this helper struct is needed.
    #[mz_ore::test]
    pub fn test_address_address_map() {
        // Initialize an attribute container that works as expected.
        let mut attrs = AddressMap::default();

        // Create an expression `1 + 1` where `1` appears twice.
        let expr = Add(Const(1).into(), Const(1).into());

        // Save pre-order visitation index of expr and its subterms.
        *(attrs.entry_or_default(&expr)) = 0;
        if let Add(lhs, rhs) = &expr {
            // Even though lhs and rhs are structurally identical expressions,
            // the `lhs: 1` entry is not overwritten by `rhs: 2`!
            *(attrs.entry_or_default(&lhs)) = 1;
            *(attrs.entry_or_default(&rhs)) = 2;
        }

        // If we uncomment this line and make `expr` mutable the borrow checker
        // will (rightfully) complain because `attrs` is still alive.
        /*
        if let Add(lhs, rhs) = &mut expr {
            *lhs = Const(3).into();
            *rhs = Const(4).into();
        }
        */

        assert_eq!(attrs.get(&expr).cloned(), Some(0));
        if let Add(lhs, rhs) = &expr {
            assert_eq!(attrs.get(lhs.as_ref()).cloned(), Some(1));
            assert_eq!(attrs.get(rhs.as_ref()).cloned(), Some(2));
        }
    }

    #[derive(Eq, PartialEq, PartialOrd, Ord)]
    enum Expr {
        Const(i64),
        Add(Box<Expr>, Box<Expr>),
    }
}
