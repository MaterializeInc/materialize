// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Framework for modeling and computing derived attributes for QGM
//! graphs.
//!
//! A derived attribute is a value that is associated with a specific
//! QGM box and can be derived from a QGM graph and other derived
//! attributes.
//!
//! To implement a new attribute, define a new type to represent that
//! attribute and implement the [`Attribute`] and [`AttributeKey`]
//! for that type.
//!
//! Note that the current implementation does not support parameterized
//! [`Attribute`] instances, so `MyAttr` is OK, but `MyAttr(i32)` isn't.

use crate::query_model::model::{BoxId, Model};
use std::any::type_name;
use std::collections::HashSet;
use std::hash::Hash;
use std::marker::PhantomData;
use typemap_rev::{TypeMap, TypeMapKey};

/// A container for derived attributes associated with a specific QGM
/// box.
pub struct Attributes(TypeMap);

impl std::fmt::Debug for Attributes {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(()) // FIXME
    }
}

/// A public api for getting and setting attributes.
impl Attributes {
    pub(crate) fn new() -> Self {
        Self(TypeMap::new())
    }

    /// Return a value for the attribute `A`.
    ///
    /// # Panics
    ///
    /// Panics if the attribute `A` is not set.
    #[allow(dead_code)]
    pub(crate) fn get<A: 'static + AttributeKey>(&self) -> &A::Value
    where
        A::Value: std::fmt::Debug,
    {
        match self.0.get::<AsKey<A>>() {
            Some(value) => value,
            None => panic!("attribute {} not present", type_name::<A>()),
        }
    }

    /// Set attribute `A` to `value`.
    #[allow(dead_code)]
    pub(crate) fn set<A: 'static + AttributeKey>(&mut self, value: A::Value)
    where
        A::Value: std::fmt::Debug,
    {
        self.0.insert::<AsKey<A>>(value);
    }
}

/// A trait that defines the logic for deriving an attribute.
pub(crate) trait Attribute: std::fmt::Debug + 'static {
    /// A globally unique identifier for this attribute type.
    fn attr_id(&self) -> &'static str;

    /// A vector of attributes that need to  be derived before
    /// this attribute.
    fn requires(&self) -> Vec<Box<dyn Attribute>>;

    /// A function that derives at a specific [`BoxId`].
    fn derive(&self, model: &mut Model, box_id: BoxId);
}

/// A naive [`PartialEq`] implementation for [`Attribute`] trait objects that
/// differentiates two attributes based on their [`std::any::TypeId`].
impl PartialEq for dyn Attribute {
    fn eq(&self, other: &Self) -> bool {
        self.attr_id() == other.attr_id()
    }
}

/// An evidence that the [`PartialEq`] implementation for [`Attribute`] is an
/// equivalence relation.
impl Eq for dyn Attribute {}

/// A naive `Hash` for attributes that delegates to the associated
/// [`std::any::TypeId`].
impl Hash for dyn Attribute {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.attr_id().hash(state);
    }
}

/// A trait sets an attribute `Value` type.
pub(crate) trait AttributeKey: Attribute {
    type Value: Send + Sync;
}

/// Helper struct to derive a [`TypeMapKey`] from an [`Attribute`].
struct AsKey<A: AttributeKey>(PhantomData<A>);

/// Derive a [`TypeMapKey`] from an [`Attribute`].
impl<A: AttributeKey> TypeMapKey for AsKey<A> {
    type Value = A::Value;
}

/// A struct that represents an [`Attribute`] set that needs
/// to be present for some follow-up logic (most likely
/// transformation, but can also be pretty-printing or something
/// else).
pub(crate) struct RequiredAttributes {
    attributes: Vec<Box<dyn Attribute>>,
}

impl From<HashSet<Box<dyn Attribute>>> for RequiredAttributes {
    /// Completes the set attributes with transitive dependencies
    /// and wraps the result in a representation that is suitable
    /// for attribute derivation in a minimum number of passes.
    fn from(mut attributes: HashSet<Box<dyn Attribute>>) -> Self {
        // add missing dependencies required to derive this set of attributes
        transitive_closure(&mut attributes);
        // order transitive closure topologically based on dependency order
        let attributes = dependency_order(attributes);
        // wrap resulting vector a new RequiredAttributes instance
        RequiredAttributes { attributes }
    }
}

impl RequiredAttributes {
    /// Derive attributes for the entire model.
    ///
    /// The currently implementation assumes that all attributes
    /// can be derived in a single bottom up pass.
    pub(crate) fn derive(&self, model: &mut Model, root: BoxId) {
        if !self.attributes.is_empty() {
            let _ = model.try_visit_mut_pre_post_descendants(
                &mut |_, _| -> Result<(), ()> { Ok(()) },
                &mut |m, box_id| -> Result<(), ()> {
                    for attr in self.attributes.iter() {
                        attr.derive(m, *box_id);
                    }
                    Ok(())
                },
                root,
            );
        }
    }
}

/// Consumes a set of attributes and produces a topologically sorted
/// version of the elements in that set based on the dependency
/// information provided by the [`Attribute::requires`] results.
///
/// We use Kahn's algorithm[^1] to sort the input.
///
/// [^1]: <https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm>
fn dependency_order(attributes: HashSet<Box<dyn Attribute>>) -> Vec<Box<dyn Attribute>> {
    let mut rest = attributes.into_iter().collect::<Vec<_>>();
    let mut seen = HashSet::new() as HashSet<&'static str>;
    let mut sort = vec![] as Vec<Box<dyn Attribute>>;

    while !rest.is_empty() {
        let (tail, head) = rest.into_iter().partition::<Vec<_>, _>(|attr| {
            attr.requires()
                .into_iter()
                .filter(|req| !seen.contains(req.attr_id()))
                .next()
                .is_some()
        });
        rest = tail;
        seen.extend(head.iter().map(|attr| attr.attr_id()));
        sort.extend(head);
    }

    sort
}

/// Compute the transitive closure of the given set of attributes.
fn transitive_closure(attributes: &mut HashSet<Box<dyn Attribute>>) {
    let mut diff = requirements(&attributes);

    // iterate until no new attributes can be discovered
    while !diff.is_empty() {
        attributes.extend(diff);
        diff = requirements(&attributes);
    }
}

/// Compute the attributes required to derive the given set of `attributes` that are not
/// already in that set.
fn requirements(attributes: &HashSet<Box<dyn Attribute>>) -> HashSet<Box<dyn Attribute>> {
    attributes
        .iter()
        .flat_map(|a| a.requires())
        .filter(|a| !attributes.contains(a))
        .collect::<HashSet<_>>()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// Shorthand macros for defining test attributes.
    macro_rules! attr_def {
        // attribute with dependencies
        ($id:ident depending on $($deps:expr),*) => {
            #[derive(Debug, PartialEq, Eq, Clone, Hash)]
            struct $id;

            impl Attribute for $id {
                fn attr_id(&self) -> &'static str {
                    stringify!($id)
                }

                fn requires(&self) -> Vec<Box<(dyn Attribute)>> {
                    vec![$(Box::new($deps)),*]
                }

                fn derive(&self, _: &mut Model, _: BoxId) {}
            }
        };
        // attribute without dependencies
        ($id:ident) => {
            #[derive(Debug, PartialEq, Eq, Clone, Hash)]
            struct $id;

            impl Attribute for $id {
                fn attr_id(&self) -> &'static str {
                    stringify!($id)
                }

                fn requires(&self) -> Vec<Box<(dyn Attribute)>> {
                    vec![]
                }

                fn derive(&self, _: &mut Model, _: BoxId) {}
            }
        };
    }

    /// Shorthand macro for referencing test attributes.
    macro_rules! attr_ref {
        // attribute without dependencies
        ($id:ident) => {
            Box::new($id) as Box<dyn Attribute>
        };
    }

    // Define trivial attributes for testing purposes. The dependencies
    // ionduce the following graph (dependencies are always on the left):
    //
    // C1 --- B2 --- B1    E1
    //    ╲-- A2 --- A1
    //        D1 --╱
    attr_def!(A1 depending on A2, D1);
    attr_def!(A2 depending on C1);
    attr_def!(B1 depending on B2);
    attr_def!(B2 depending on C1);
    attr_def!(C1);
    attr_def!(D1);
    attr_def!(E1);

    #[test]
    fn test_eq() {
        assert_eq!(&attr_ref!(A1), &attr_ref!(A1));
        assert_ne!(&attr_ref!(A1), &attr_ref!(B1));
        assert_ne!(&attr_ref!(B1), &attr_ref!(C1));
    }

    #[test]
    fn requirements_ok() {
        let attrs1 = HashSet::from([attr_ref!(A1), attr_ref!(B1)]);
        let attrs2 = HashSet::from([attr_ref!(A2), attr_ref!(B2), attr_ref!(D1)]);
        assert_eq!(requirements(&attrs1), attrs2);
    }

    #[test]
    fn transitive_closure_ok() {
        let mut attrs1 = HashSet::from([attr_ref!(A1), attr_ref!(B1)]);
        let attrs2 = HashSet::from([
            attr_ref!(A1),
            attr_ref!(A2),
            attr_ref!(B1),
            attr_ref!(B2),
            attr_ref!(C1),
            attr_ref!(D1),
        ]);
        transitive_closure(&mut attrs1);
        assert_eq!(attrs1, attrs2);
    }

    #[test]
    fn dependency_ordered_ok() {
        let attrs = dependency_order(HashSet::from([
            attr_ref!(A1),
            attr_ref!(A2),
            attr_ref!(B1),
            attr_ref!(B2),
            attr_ref!(C1),
            attr_ref!(D1),
            attr_ref!(E1),
        ]));

        let index = attrs
            .iter()
            .enumerate()
            .map(|(i, a)| (a, i))
            .collect::<HashMap<_, _>>();

        // assert that the result is the right size and has the right elements
        assert_eq!(attrs.len(), 7);
        assert!(index.contains_key(&attr_ref!(A1)));
        assert!(index.contains_key(&attr_ref!(A2)));
        assert!(index.contains_key(&attr_ref!(B1)));
        assert!(index.contains_key(&attr_ref!(B2)));
        assert!(index.contains_key(&attr_ref!(C1)));
        assert!(index.contains_key(&attr_ref!(D1)));
        assert!(index.contains_key(&attr_ref!(E1)));

        // assert that the result is topologically sorted
        let a1 = index[&attr_ref!(A1)];
        let a2 = index[&attr_ref!(A2)];
        let b1 = index[&attr_ref!(B1)];
        let b2 = index[&attr_ref!(B2)];
        let c1 = index[&attr_ref!(C1)];
        let d1 = index[&attr_ref!(D1)];
        assert!(a1 > a2 && a1 > d1);
        assert!(a2 > c1);
        assert!(b1 > b2);
        assert!(b2 > c1);
    }
}
