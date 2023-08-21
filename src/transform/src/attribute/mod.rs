// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Derived attributes framework and definitions.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use mz_expr::explain::ExplainContext;
use mz_expr::visit::{Visit, Visitor};
use mz_expr::{LocalId, MirRelationExpr};
use mz_ore::stack::RecursionLimitError;
use mz_ore::str::{bracketed, separated};
use mz_repr::explain::{AnnotatedPlan, Attributes, ExplainConfig};
use num_traits::FromPrimitive;
use typemap_rev::{TypeMap, TypeMapKey};

// Attributes should have shortened paths when exported.
mod arity;
pub use arity::Arity;
pub mod cardinality;
pub use cardinality::Cardinality;
mod non_negative;
pub use non_negative::NonNegative;
mod relation_type;
pub use relation_type::RelationType;
mod subtree_size;
pub use subtree_size::SubtreeSize;
mod unique_keys;
pub use unique_keys::UniqueKeys;

/// A common interface to be implemented by all derived attributes.
pub trait Attribute: 'static + Default + Send + Sync {
    /// The domain of the attribute values.
    type Value: Clone + Eq + PartialEq;

    /// Derive an attribute.
    fn derive(&mut self, expr: &MirRelationExpr, deps: &DerivedAttributes);

    /// Schedule environment maintenance tasks.
    ///
    /// Delegate to [`Env::schedule_tasks`] if this attribute has an [`Env`] field.
    fn schedule_env_tasks(&mut self, _expr: &MirRelationExpr) {}

    /// Handle scheduled environment maintenance tasks.
    ///
    /// Delegate to [`Env::handle_tasks`] if this attribute has an [`Env`] field.
    fn handle_env_tasks(&mut self) {}

    /// The attribute ids of all other attributes that this attribute depends on.
    fn add_dependencies(builder: &mut RequiredAttributes)
    where
        Self: Sized;

    /// Attributes for each subexpression, visited in post-order.
    fn get_results(&self) -> &Vec<Self::Value>;

    /// Mutable attributes for each subexpression, visited in post-order.
    fn get_results_mut(&mut self) -> &mut Vec<Self::Value>;

    /// Consume self and return the attributes for each subexpression.
    fn take(self) -> Vec<Self::Value>;
}

#[allow(missing_debug_implementations)]
/// Converts an Attribute into a Key that can be looked up in [DerivedAttributes]
struct AsKey<A: Attribute>(PhantomData<A>);

impl<A: Attribute + 'static> TypeMapKey for AsKey<A> {
    type Value = A;
}

/// A map that keeps the computed `A` values for all `LocalId`
/// bindings in the current environment.
#[derive(Default)]
#[allow(missing_debug_implementations)]
pub struct Env<A: Attribute> {
    /// The [`BTreeMap`] backing this environment.
    env: BTreeMap<LocalId, A::Value>,
    // A stack of tasks to maintain the `env` map.
    env_tasks: Vec<EnvTask>,
}

impl<A: Attribute> Env<A> {
    pub(crate) fn get(&self, id: &LocalId) -> Option<&A::Value> {
        self.env.get(id)
    }
}

impl<A: Attribute> Env<A> {
    /// Schedules exactly one environment maintenance task associated with the
    /// given `expr`.
    ///
    /// This should be:
    /// 1. Called from a `Visitor<MirRelationExpr>::pre_visit` implementation.
    /// 2. A `Bind` task for `MirRelationExpr` variants that represent a change
    ///    to the environment (in other words, `Let*` variants).
    /// 3. A `Done` task for all other `MirRelationExpr` variants.
    pub fn schedule_tasks(&mut self, expr: &MirRelationExpr) {
        match expr {
            MirRelationExpr::Let { id, .. } => {
                // Add a `Bind` task for the `LocalId` bound by this node.
                self.env_tasks.push(EnvTask::bind_one(id));
            }
            MirRelationExpr::LetRec { ids, .. } => {
                // Add a `Bind` task for all `LocalId`s bound by this node. This
                // will result in attribute derivation corresponding to a single
                // sequential pass through all bindings (one iteration).
                self.env_tasks.push(EnvTask::bind_seq(ids));
            }
            _ => {
                // Do not do anything with the environment.
                self.env_tasks.push(EnvTask::Done);
            }
        }
    }

    /// Handles scheduled environment maintenance tasks.
    ///
    /// Should be called from a `Visitor<MirRelationExpr>::post_visit` implementation.
    pub fn handle_tasks(&mut self, results: &Vec<A::Value>) {
        // Pop the env task associated with the just visited node.
        let parent = self.env_tasks.pop();
        // This should always be a Done, as this is either the original value or
        // the terminal state of the state machine that should be reached after
        // all children of the current node have been visited, which should
        // always be the case if handle_tasks is called from post_visit.
        assert!(parent.is_some() && parent.unwrap() == EnvTask::Done);

        // Update the state machine associated with the parent.
        // This should always exist when post-visiting non-root nodes.
        if let Some(env_task) = self.env_tasks.pop() {
            // Compute a task to represent the next state of the state machine associated with
            // this task.
            let env_task = match env_task {
                // A Bind state indicates that the parent of the current node is
                // a Let or a LetRec, and we are about to leave a Let or LetRec
                // binding value.
                EnvTask::Bind(mut unbound, mut bound) => {
                    let id = unbound.pop().expect("non-empty unbound vector");
                    // Update the env map with the attribute value derived for
                    // the Let or LetRec binding value.
                    let result = results.last().expect("non-empty attribute results vector");
                    let oldval = self.env.insert(id.clone(), result.clone());
                    // No shadowing
                    assert!(oldval.is_none());
                    // Mark this LocalId as bound
                    bound.insert(id);

                    if unbound.is_empty() {
                        // The next sibling is a Let / LetRec body. Advance the
                        // state machine to a state where after the next sibling
                        // is visited all bound LocalIds associated with this
                        // state machine will be removed from the environment.
                        EnvTask::Unbind(bound)
                    } else {
                        // The next sibling is a LetRec value bound to the last
                        // element of `unbound`. Advance the state machine to a
                        // state where this LocalId will be bound after visiting
                        // that value.
                        EnvTask::Bind(unbound, bound)
                    }
                }
                // An Unbind state indicates that we are about to leave the Let or LetRec body.
                EnvTask::Unbind(ids) => {
                    // Remove values associated with the given `ids`.
                    for id in ids {
                        self.env.remove(&id);
                    }

                    // Advance to a Done task indicating that there is nothing
                    // more to be done.
                    EnvTask::Done
                }
                // A Done state indicates that we don't need to do anything.
                EnvTask::Done => EnvTask::Done,
            };
            // Advance the state machine.
            self.env_tasks.push(env_task);
        };
    }
}

/// Models an environment maintenance task that needs to be executed after
/// visiting a [`MirRelationExpr`]. Each task is a state of a finite state
/// machine.
///
/// The [`Env::schedule_tasks`] hook installs exactly one such task for each
/// subexpression, and the [`Env::handle_tasks`] hook removes it.
///
/// In addition, the [`Env::handle_tasks`] looks at the task installed by the
/// parent expression and modifies it if needed, advancing it through the
/// following states from left to right:
///
/// ```text
/// Bind([...,x], {...}) --> Bind([...], {...,x}) --> Unbind({...}) --> Done
/// ```
#[derive(Eq, PartialEq, Debug)]
enum EnvTask {
    /// Extend the environment by binding the last derived attribute under
    /// the key given the tail of the lhs Vec and move that key from the
    /// lhs Vec to the rhs BTreeSet.
    Bind(Vec<LocalId>, BTreeSet<LocalId>),
    /// Remove all LocalId entries in the given set from the environment.
    Unbind(BTreeSet<LocalId>),
    /// Do not do anything.
    Done,
}

impl EnvTask {
    /// Create an [`EnvTask::Bind`] state for a single binding.
    fn bind_one(id: &LocalId) -> Self {
        EnvTask::Bind(vec![id.clone()], BTreeSet::new())
    }

    /// Create an [`EnvTask::Bind`] state for a sequence of bindings.
    fn bind_seq(ids: &Vec<LocalId>) -> Self {
        EnvTask::Bind(ids.iter().rev().cloned().collect(), BTreeSet::new())
    }
}

#[allow(missing_debug_implementations)]
#[derive(Default)]
/// Builds an [DerivedAttributes]
pub struct RequiredAttributes {
    attributes: TypeMap,
}

impl RequiredAttributes {
    /// Add an attribute that [DerivedAttributes] should derive
    pub fn require<A: Attribute>(&mut self) {
        if !self.attributes.contains_key::<AsKey<A>>() {
            A::add_dependencies(self);
            self.attributes.insert::<AsKey<A>>(A::default());
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
/// Can be constructed either manually from an [RequiredAttributes] or directly
/// from an [ExplainConfig].
pub struct DerivedAttributes {
    attributes: Box<TypeMap>,
    /// Holds attributes that have not yet been derived for a given
    /// subexpression.
    to_be_derived: Box<TypeMap>,
}

impl From<&ExplainConfig> for DerivedAttributes {
    fn from(config: &ExplainConfig) -> DerivedAttributes {
        let mut builder = RequiredAttributes::default();
        if config.subtree_size {
            builder.require::<SubtreeSize>();
        }
        if config.non_negative {
            builder.require::<NonNegative>();
        }
        if config.types {
            builder.require::<RelationType>();
        }
        if config.arity {
            builder.require::<Arity>();
        }
        if config.keys {
            builder.require::<UniqueKeys>();
        }
        if config.cardinality {
            builder.require::<Cardinality>();
        }
        builder.finish()
    }
}

impl DerivedAttributes {
    /// Get a reference to the attributes derived so far.
    pub fn get_results<A: Attribute>(&self) -> &Vec<A::Value> {
        self.attributes.get::<AsKey<A>>().unwrap().get_results()
    }

    /// Get a mutable reference to the attributes derived so far.
    ///
    /// Calling this and modifying the result risks messing up subsequent
    /// derivations.
    pub fn get_results_mut<A: Attribute>(&mut self) -> &mut Vec<A::Value> {
        self.attributes
            .get_mut::<AsKey<A>>()
            .unwrap()
            .get_results_mut()
    }

    /// Extract the vector of attributes derived so far
    ///
    /// After this call, no further attributes of this type will be derived.
    pub fn remove_results<A: Attribute>(&mut self) -> Vec<A::Value> {
        self.attributes.remove::<AsKey<A>>().unwrap().take()
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
                Some(AttributeId::Cardinality) => self.trim_attr::<AsKey<Cardinality>>(),
                None => {}
            }
        }
    }
}

impl Visitor<MirRelationExpr> for DerivedAttributes {
    /// Does derivation work required upon entering a subexpression.
    fn pre_visit(&mut self, expr: &MirRelationExpr) {
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
                Some(AttributeId::Cardinality) => {
                    self.pre_visit_attr::<AsKey<Cardinality>>(expr);
                }
                None => {}
            }
        }
    }

    /// Does derivation work required upon exiting a subexpression.
    fn post_visit(&mut self, expr: &MirRelationExpr) {
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
                Some(AttributeId::Cardinality) => {
                    self.post_visit_attr::<AsKey<Cardinality>>(expr);
                }
                None => {}
            }
        }
    }
}

impl DerivedAttributes {
    fn pre_visit_attr<T: TypeMapKey>(&mut self, expr: &MirRelationExpr)
    where
        T::Value: Attribute,
    {
        if let Some(attr) = self.attributes.get_mut::<T>() {
            attr.schedule_env_tasks(expr)
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

    fn trim_attr<T: TypeMapKey>(&mut self)
    where
        T::Value: Attribute,
    {
        self.attributes.get_mut::<T>().iter_mut().for_each(|a| {
            a.get_results_mut().pop();
        });
    }
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
    Cardinality = 5,
}

/// Should always be equal to the number of attributes
const TOTAL_ATTRIBUTES: usize = 6;

/// Produce an [`AnnotatedPlan`] wrapping the given [`MirRelationExpr`] along
/// with [`Attributes`] derived from the given context configuration.
pub fn annotate_plan<'a>(
    plan: &'a MirRelationExpr,
    context: &'a ExplainContext,
) -> Result<AnnotatedPlan<'a, MirRelationExpr>, RecursionLimitError> {
    let mut annotations = BTreeMap::<&MirRelationExpr, Attributes>::default();
    let config = context.config;

    if config.requires_attributes() {
        // get the annotation keys
        let subtree_refs = plan.post_order_vec();
        // get the annotation values
        let mut attributes = DerivedAttributes::from(config);
        plan.visit(&mut attributes)?;

        if config.subtree_size {
            for (expr, attr) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<SubtreeSize>().into_iter(),
            ) {
                let attributes = annotations.entry(expr).or_default();
                attributes.subtree_size = Some(attr);
            }
        }
        if config.non_negative {
            for (expr, attr) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<NonNegative>().into_iter(),
            ) {
                let attrs = annotations.entry(expr).or_default();
                attrs.non_negative = Some(attr);
            }
        }

        if config.arity {
            for (expr, attr) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<Arity>().into_iter(),
            ) {
                let attrs = annotations.entry(expr).or_default();
                attrs.arity = Some(attr);
            }
        }

        if config.types {
            for (expr, types) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<RelationType>().into_iter(),
            ) {
                let humanized_columns = types
                    .into_iter()
                    .map(|c| context.humanizer.humanize_column_type(&c))
                    .collect::<Vec<_>>();
                let attr = bracketed("(", ")", separated(", ", humanized_columns)).to_string();
                let attrs = annotations.entry(expr).or_default();
                attrs.types = Some(attr);
            }
        }

        if config.keys {
            for (expr, keys) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<UniqueKeys>().into_iter(),
            ) {
                let formatted_keys = keys
                    .into_iter()
                    .map(|key_set| bracketed("[", "]", separated(", ", key_set)).to_string());
                let attr = bracketed("(", ")", separated(", ", formatted_keys)).to_string();
                let attrs = annotations.entry(expr).or_default();
                attrs.keys = Some(attr);
            }
        }

        if config.cardinality {
            for (expr, cardinality) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<Cardinality>().into_iter(),
            ) {
                let attr =
                    cardinality::HumanizedSymbolicExpression::new(&cardinality, context.humanizer)
                        .to_string();
                let attrs = annotations.entry(expr).or_default();
                attrs.cardinality = Some(attr);
            }
        }
    }

    Ok(AnnotatedPlan { plan, annotations })
}
