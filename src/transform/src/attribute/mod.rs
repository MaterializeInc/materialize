// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Derived attributes framework and definitions.

use std::{collections::HashMap, marker::PhantomData};

use num_traits::FromPrimitive;
use typemap_rev::{TypeMap, TypeMapKey};

use mz_expr::{visit::Visitor, LocalId, MirRelationExpr};

// Attributes should have shortened paths when exported.
mod arity;
pub use arity::Arity;
mod non_negative;
pub use non_negative::NonNegative;
mod relation_type;
pub use relation_type::RelationType;
mod subtree_size;
pub use subtree_size::SubtreeSize;
mod unique_keys;
pub use unique_keys::UniqueKeys;

/// A common interface to be implemented by all derived attributes.
pub trait Attribute: Send + Sync {
    /// The domain of the attribute values.
    type Value: Clone + Eq + PartialEq;

    /// Derive an attribute.
    fn derive(&mut self, expr: &MirRelationExpr, deps: &DerivedAttributes);

    /// Schedule environment maintenance tasks.
    ///
    /// Deletate to [`Env::schedule_tasks`] if this attribute has an [`Env`] field.
    fn schedule_env_tasks(&mut self, _expr: &MirRelationExpr) {}

    /// Handle scheduled environment maintenance tasks.
    ///
    /// Delegate to [`Env::handle_tasks`] if this attribute has an [`Env`] field.
    fn handle_env_tasks(&mut self) {}

    /// The attribute ids of all other attributes that this attribute depends on.
    fn add_dependencies(builder: &mut RequiredAttributes)
    where
        Self: Sized;

    /// Attributes for each subexpression, visited in post-order
    fn get_results(&self) -> &Vec<Self::Value>;

    /// Mutable attributes for each subexpression, visited in post-order
    fn get_results_mut(&mut self) -> &mut Vec<Self::Value>;
}

#[allow(missing_debug_implementations)]
/// Converts an Attribute into a Key that can be looked up in [DerivedAttributes]
pub struct AsKey<A: Attribute>(PhantomData<A>);

impl<A: Attribute + 'static> TypeMapKey for AsKey<A> {
    type Value = A;
}

/// A map that keeps the computed `A` values for all `LocalId`
/// bindings in the current environment.
#[derive(Default)]
#[allow(missing_debug_implementations)]
pub struct Env<A: Attribute> {
    /// The [`HashMap`] backing this environment.
    env: HashMap<LocalId, A::Value>,
    // A stack of tasks to maintain the `env` map.
    env_tasks: Vec<EnvTask<A::Value>>,
}

impl<A: Attribute> Env<A> {
    pub(crate) fn get(&self, id: &LocalId) -> Option<&A::Value> {
        self.env.get(id)
    }
}

impl<A: Attribute> Env<A> {
    /// Schedules environment maintenance tasks.
    ///
    /// Should be called from a `Visitor<MirRelationExpr>::pre_visit` implementaion.
    pub fn schedule_tasks(&mut self, expr: &MirRelationExpr) {
        match expr {
            MirRelationExpr::Let { id, .. } => {
                // Add an Extend task to be handled in the post_visit the node children.
                self.env_tasks.push(EnvTask::Extend(id.clone()));
            }
            _ => {
                // Don not do anything with the environment in this node's children.
                self.env_tasks.push(EnvTask::NoOp);
            }
        }
    }

    /// Handles scheduled evinronment maintenance tasks.
    ///
    /// Should be called from a `Visitor<MirRelationExpr>::post_visit` implementaion.
    pub fn handle_tasks(&mut self, results: &Vec<A::Value>) {
        // Pop the env task for this element's children.
        let parent = self.env_tasks.pop();
        // This should be always be NoOp, as this is either the original value or the
        // terminal state of the state machine that Let children implement (see the
        // code at the end of this method).
        debug_assert!(parent.is_some() && parent.unwrap() == EnvTask::NoOp);

        // Handle EnvTask initiated by the parent.
        if let Some(env_task) = self.env_tasks.pop() {
            // Compute a task to represent the next state of the state machine associated with
            // this task.
            let env_task = match env_task {
                // An Extend indicates that the parent of the current node is a Let binding
                // and we are about to leave a Let binding value.
                EnvTask::Extend(id) => {
                    // Before descending to the next sibling (the Let binding body), do as follows:
                    // 1. Update the env map with the attribute derived for the Let binding value.
                    let result = results.last().expect("unexpected empty results vector");
                    let oldval = self.env.insert(id, result.clone());
                    // 2. Create a task to be handled after visiting the Let binding body.
                    match oldval {
                        Some(val) => EnvTask::Reset(id, val), // reset old value if present
                        None => EnvTask::Remove(id),          // remove key otherwise
                    }
                }
                // An Reset task indicates that we are about to leave the Let binding body
                // and the id of the Let parent shadowed another `id` in the environment.
                EnvTask::Reset(id, val) => {
                    // Before moving to the post_visit of the enclosing Let parent, do as follows:
                    // 1. Reset the entry in the env map with the shadowed value.
                    self.env.insert(id, val);
                    // 2. Create a NoOp task indicating that there is nothing more to be done.
                    EnvTask::NoOp
                }
                // An Remove task indicates that we are about to leave the Let binding body
                // and the id of the Let parent did not shadow another `id` in the environment.
                EnvTask::Remove(id) => {
                    // Before moving to the post_visit of the enclosing Let parent, do as follows:
                    // 1. Remove the value assciated with the given `id` from the environment.
                    self.env.remove(&id);
                    // 2. Create a NoOp task indicating that there is nothing more to be done.
                    EnvTask::NoOp
                }
                // A NoOp task indicates that we don't need to do anyting.
                EnvTask::NoOp => EnvTask::NoOp,
            };
            // Advance the state machine.
            self.env_tasks.push(env_task);
        };
    }
}

/// Models an environment maintenence task that needs to be executed
/// after visiting a [`MirRelationExpr`].
///
/// The [`Env::schedule_tasks`] hook installs such a task for each node,
/// and the [`Env::handle_tasks`] hook removes it.
///
/// In addition, the [`Env::handle_tasks`] looks at the task installed
/// by its parent and modifies it if needed, advancing it through the
/// following state machinge from left to right:
/// ```text
/// Set --- Reset ---- NoOp
///     \            /
///      -- Remove --
/// ```
#[derive(Eq, PartialEq, Debug)]
enum EnvTask<T> {
    /// Add the latest attribute to the environment under the given key.
    Extend(LocalId),
    /// Reset value of an environment entry.
    Reset(LocalId, T),
    /// Remove an entry from the environment.
    Remove(LocalId),
    /// Do not do anything.
    NoOp,
}

#[allow(missing_debug_implementations)]
#[derive(Default)]
/// Builds an [DerivedAttributes]
pub struct RequiredAttributes {
    attributes: TypeMap,
}

impl RequiredAttributes {
    /// Add an attribute that [DerivedAttributes] should derive
    pub fn require<T: TypeMapKey>(&mut self)
    where
        T::Value: Attribute + Default,
    {
        if !self.attributes.contains_key::<T>() {
            T::Value::add_dependencies(self);
            self.attributes.insert::<T>(T::Value::default());
        }
    }

    /// Consume `self`, producing an [DerivedAttributes]
    pub fn finish(self) -> DerivedAttributes {
        DerivedAttributes {
            attributes: Box::new(self.attributes),
            to_be_derived: Box::new(TypeMap::default()),
        }
    }
}

#[allow(missing_debug_implementations)]
/// Derives an attribute and any attribute it depends on.
///
/// Can only be constructed from a [RequiredAttributes].
pub struct DerivedAttributes {
    attributes: Box<TypeMap>,
    /// Holds attributes that have not yet been derived for a given
    /// subexpression.
    to_be_derived: Box<TypeMap>,
}

/// A topological sort of extant attributes.
///
/// The attribute assigned value 0 depends on no other attributes.
/// We expect the attributes to be assigned numbers in the range
/// 0..TOTAL_ATTRIBUTES with no gaps in between.
#[derive(FromPrimitive)]
enum AttributeId {
    SubtreeSize = 0,
    NonNegative = 1,
    Arity = 2,
    RelationType = 3,
    UniqueKeys = 4,
}

/// Should always be equal to the number of attributes
const TOTAL_ATTRIBUTES: usize = 5;

impl DerivedAttributes {
    fn pre_visit_attr<T: TypeMapKey>(&mut self, expr: &MirRelationExpr)
    where
        T::Value: Attribute,
    {
        if let Some(attr) = self.attributes.get_mut::<T>() {
            attr.schedule_env_tasks(expr)
        }
    }

    /// Does derivation work required upon entering a subexpression
    pub fn pre_visit(&mut self, expr: &MirRelationExpr) {
        for i in 0..TOTAL_ATTRIBUTES {
            match FromPrimitive::from_usize(i) {
                Some(AttributeId::SubtreeSize) => {
                    self.pre_visit_attr::<AsKey<SubtreeSize>>(expr);
                }
                Some(AttributeId::NonNegative) => {
                    self.pre_visit_attr::<AsKey<NonNegative>>(expr);
                }
                Some(AttributeId::RelationType) => {
                    self.pre_visit_attr::<AsKey<RelationType>>(expr);
                }
                Some(AttributeId::Arity) => {
                    self.pre_visit_attr::<AsKey<Arity>>(expr);
                }
                Some(AttributeId::UniqueKeys) => {
                    self.pre_visit_attr::<AsKey<UniqueKeys>>(expr);
                }
                None => {}
            }
        }
    }

    fn post_visit_attr<T: TypeMapKey>(&mut self, expr: &MirRelationExpr)
    where
        T::Value: Attribute,
    {
        if let Some(mut attr) = self.to_be_derived.remove::<T>() {
            attr.derive(expr, self);
            attr.handle_env_tasks();
            self.attributes.insert::<T>(attr);
        }
    }

    /// Does derivation work required upon exiting a subexpression
    pub fn post_visit(&mut self, expr: &MirRelationExpr) {
        // TODO
        std::mem::swap(&mut self.attributes, &mut self.to_be_derived);
        for i in 0..TOTAL_ATTRIBUTES {
            match FromPrimitive::from_usize(i) {
                Some(AttributeId::SubtreeSize) => {
                    self.post_visit_attr::<AsKey<SubtreeSize>>(expr);
                }
                Some(AttributeId::NonNegative) => {
                    self.post_visit_attr::<AsKey<NonNegative>>(expr);
                }
                Some(AttributeId::RelationType) => {
                    self.post_visit_attr::<AsKey<RelationType>>(expr);
                }
                Some(AttributeId::Arity) => {
                    self.post_visit_attr::<AsKey<Arity>>(expr);
                }
                Some(AttributeId::UniqueKeys) => {
                    self.post_visit_attr::<AsKey<UniqueKeys>>(expr);
                }
                None => {}
            }
        }
    }

    /// Get a reference to the attributes derived so far.
    pub fn get_results<T: TypeMapKey>(&self) -> &Vec<<<T as TypeMapKey>::Value as Attribute>::Value>
    where
        T::Value: Attribute,
    {
        self.attributes.get::<T>().unwrap().get_results()
    }

    /// Get a mutable reference to the attributes derived so far.
    ///
    /// Calling this and modifying the result risks messing up subsequent
    /// derivations.
    pub fn get_results_mut<T: TypeMapKey>(
        &mut self,
    ) -> &mut Vec<<<T as TypeMapKey>::Value as Attribute>::Value>
    where
        T::Value: Attribute,
    {
        self.attributes.get_mut::<T>().unwrap().get_results_mut()
    }

    fn trim_attr<T: TypeMapKey>(&mut self)
    where
        T::Value: Attribute,
    {
        self.attributes.get_mut::<T>().iter_mut().for_each(|a| {
            a.get_results_mut().pop();
        });
    }
    /// Call when the most recently exited node of the [MirRelationExpr]
    /// has been deleted.
    ///
    /// For each attribute, removes the last value derived.
    pub fn trim(&mut self) {
        for i in 0..TOTAL_ATTRIBUTES {
            match FromPrimitive::from_usize(i) {
                Some(AttributeId::SubtreeSize) => self.trim_attr::<AsKey<SubtreeSize>>(),
                Some(AttributeId::NonNegative) => self.trim_attr::<AsKey<NonNegative>>(),
                Some(AttributeId::RelationType) => self.trim_attr::<AsKey<RelationType>>(),
                Some(AttributeId::Arity) => self.trim_attr::<AsKey<Arity>>(),
                Some(AttributeId::UniqueKeys) => self.trim_attr::<AsKey<UniqueKeys>>(),
                None => {}
            }
        }
    }

    /// Consumes `self`, returning the inner map of attributes.
    pub fn take(self) -> TypeMap {
        *self.attributes
    }
}

impl Visitor<MirRelationExpr> for DerivedAttributes {
    fn pre_visit(&mut self, expr: &MirRelationExpr) {
        self.pre_visit(expr);
    }

    fn post_visit(&mut self, expr: &MirRelationExpr) {
        self.post_visit(expr);
    }
}
