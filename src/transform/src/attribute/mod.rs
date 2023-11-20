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

use mz_expr::explain::ExplainContext;
use mz_expr::visit::{Visit, Visitor};
use mz_expr::{LocalId, MirRelationExpr};
use mz_ore::stack::RecursionLimitError;
use mz_repr::explain::{AnnotatedPlan, Attributes};

mod arity;
pub mod cardinality;
mod column_names;
mod non_negative;
mod relation_type;
mod subtree_size;
mod unique_keys;

// Attributes should have shortened paths when exported.
pub use arity::Arity;
pub use cardinality::Cardinality;
pub use column_names::ColumnNames;
pub use non_negative::NonNegative;
pub use relation_type::RelationType;
pub use subtree_size::SubtreeSize;
pub use unique_keys::UniqueKeys;

/// A common interface to be implemented by all derived attributes.
pub trait Attribute {
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
    fn add_dependencies(builder: &mut DerivedAttributesBuilder)
    where
        Self: Sized;

    /// Attributes for each subexpression, visited in post-order.
    fn get_results(&self) -> &Vec<Self::Value>;

    /// Mutable attributes for each subexpression, visited in post-order.
    fn get_results_mut(&mut self) -> &mut Vec<Self::Value>;

    /// Consume self and return the attributes for each subexpression.
    fn take(self) -> Vec<Self::Value>;
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
    pub(crate) fn empty() -> Self {
        Self {
            env: Default::default(),
            env_tasks: Default::default(),
        }
    }

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

/// A builder for `DerivedAttributes` instances.
#[allow(missing_debug_implementations)]
#[derive(Default)]
/// Builds an [DerivedAttributes]
pub struct DerivedAttributesBuilder<'c> {
    attributes: Box<AttributeStore<'c>>,
}

impl<'c> DerivedAttributesBuilder<'c> {
    /// Add an attribute that [DerivedAttributes] should derive
    pub fn require<A>(&mut self, attribute: A)
    where
        A: Attribute,
        AttributeStore<'c>: AttributeContainer<A>,
    {
        if !self.attributes.attribute_is_enabled() {
            A::add_dependencies(self);
            self.attributes.push_attribute(attribute);
        }
    }

    /// Consume `self`, producing an [DerivedAttributes]
    pub fn finish(self) -> DerivedAttributes<'c> {
        DerivedAttributes {
            store: self.attributes,
            buffer: Box::new(AttributeStore::default()),
        }
    }
}

#[allow(missing_debug_implementations)]
/// Derives an attribute and any attribute it depends on.
///
/// Can be constructed either manually from a [DerivedAttributesBuilder] or
/// directly from an [ExplainContext].
pub struct DerivedAttributes<'c> {
    /// Holds the attributes derived for all subexpressions so far.
    store: Box<AttributeStore<'c>>,
    /// A buffer to be used for the next subexpresion. For every subexpressions
    /// we swap the `store` and `buffer` state and then process items from
    /// `buffer` one by one, moving them back to `store`.
    buffer: Box<AttributeStore<'c>>,
}

impl<'c> From<&ExplainContext<'c>> for DerivedAttributes<'c> {
    fn from(context: &ExplainContext<'c>) -> DerivedAttributes<'c> {
        let mut builder = DerivedAttributesBuilder::default();
        if context.config.subtree_size {
            builder.require(SubtreeSize::default());
        }
        if context.config.non_negative {
            builder.require(NonNegative::default());
        }
        if context.config.types {
            builder.require(RelationType::default());
        }
        if context.config.arity {
            builder.require(Arity::default());
        }
        if context.config.keys {
            builder.require(UniqueKeys::default());
        }
        if context.config.cardinality {
            builder.require(Cardinality::default());
        }
        if context.config.column_names || context.config.humanized_exprs {
            builder.require(ColumnNames::new(context.humanizer));
        }
        builder.finish()
    }
}

impl<'c> DerivedAttributes<'c> {
    /// Get a reference to the attributes derived so far.
    pub fn get_results<'a, A>(&'a self) -> &Vec<A::Value>
    where
        A: Attribute + 'a,
        AttributeStore<'c>: AttributeContainer<A>,
    {
        self.store.get_attribute().unwrap().get_results()
    }

    /// Get a mutable reference to the attributes derived so far.
    ///
    /// Calling this and modifying the result risks messing up subsequent
    /// derivations.
    pub fn get_results_mut<'a, A>(&'a mut self) -> &mut Vec<A::Value>
    where
        A: Attribute + 'a,
        AttributeStore<'c>: AttributeContainer<A>,
    {
        self.store.get_mut_attribute().unwrap().get_results_mut()
    }

    /// Extract the vector of attributes derived so far
    ///
    /// After this call, no further attributes of this type will be derived.
    pub fn remove_results<A>(&mut self) -> Vec<A::Value>
    where
        A: Attribute,
        AttributeStore<'c>: AttributeContainer<A>,
    {
        self.store.take_attribute().unwrap().take()
    }

    /// Call when the most recently exited node of the [MirRelationExpr]
    /// has been deleted.
    ///
    /// For each attribute, removes the last value derived.
    pub fn trim(&mut self) {
        self.trim_attr::<SubtreeSize>();
        self.trim_attr::<NonNegative>();
        self.trim_attr::<RelationType>();
        self.trim_attr::<Arity>();
        self.trim_attr::<UniqueKeys>();
        self.trim_attr::<Cardinality>();
        self.trim_attr::<ColumnNames>();
    }
}

impl<'c> Visitor<MirRelationExpr> for DerivedAttributes<'c> {
    /// Does derivation work required upon entering a subexpression.
    fn pre_visit(&mut self, expr: &MirRelationExpr) {
        // The `pre_visit` methods must be called in dependency order!
        self.pre_visit::<SubtreeSize>(expr);
        self.pre_visit::<NonNegative>(expr);
        self.pre_visit::<RelationType>(expr);
        self.pre_visit::<Arity>(expr);
        self.pre_visit::<UniqueKeys>(expr);
        self.pre_visit::<Cardinality>(expr);
        self.pre_visit::<ColumnNames>(expr);
    }

    /// Does derivation work required upon exiting a subexpression.
    fn post_visit(&mut self, expr: &MirRelationExpr) {
        std::mem::swap(&mut self.store, &mut self.buffer);
        // The `post_visit` methods must be called in dependency order!
        self.post_visit::<SubtreeSize>(expr);
        self.post_visit::<NonNegative>(expr);
        self.post_visit::<RelationType>(expr);
        self.post_visit::<Arity>(expr);
        self.post_visit::<UniqueKeys>(expr);
        self.post_visit::<Cardinality>(expr);
        self.post_visit::<ColumnNames>(expr);
    }
}

impl<'c> DerivedAttributes<'c> {
    fn pre_visit<A>(&mut self, expr: &MirRelationExpr)
    where
        A: Attribute,
        AttributeStore<'c>: AttributeContainer<A>,
    {
        if let Some(attr) = self.store.get_mut_attribute() {
            attr.schedule_env_tasks(expr)
        }
    }

    fn post_visit<A>(&mut self, expr: &MirRelationExpr)
    where
        A: Attribute,
        AttributeStore<'c>: AttributeContainer<A>,
    {
        if let Some(mut attr) = self.buffer.take_attribute() {
            attr.derive(expr, self);
            attr.handle_env_tasks();
            self.store.push_attribute(attr);
        }
    }

    fn trim_attr<A>(&mut self)
    where
        A: Attribute,
        AttributeStore<'c>: AttributeContainer<A>,
    {
        if let Some(a) = self.store.get_mut_attribute() {
            a.get_results_mut().pop();
        };
    }
}

/// An API for manipulating the of type `A` an enclosing store.
pub trait AttributeContainer<A: Attribute> {
    /// Push the attribute to the store.
    fn push_attribute(&mut self, value: A);

    /// Take the attribute from the store, leaving its slot empty.
    fn take_attribute(&mut self) -> Option<A>;

    /// Check if the attribute is enabled.
    fn attribute_is_enabled(&self) -> bool;

    /// Get an immutable reference to the attribute.
    fn get_attribute(&self) -> Option<&A>;

    /// Get a mutable reference to the attribute.
    fn get_mut_attribute(&mut self) -> Option<&mut A>;
}

/// A store of attribute derivation algorithms.
///
/// Values of `Some(attribute)` in the individual fields represent attributes
/// that should be derived.
///
/// The definition order of the fields must respect dependencies (the first
/// attribute cannot have dependencies, the second can only depend on the first,
/// and so forth).
#[allow(missing_debug_implementations)]
#[derive(Default)]
pub struct AttributeStore<'c> {
    subtree_size: Option<SubtreeSize>,
    non_negative: Option<NonNegative>,
    arity: Option<Arity>,
    relation_type: Option<RelationType>,
    unique_keys: Option<UniqueKeys>,
    cardinality: Option<Cardinality>,
    column_names: Option<ColumnNames<'c>>,
}

macro_rules! attribute_store_container_for {
    ($field:expr) => {
        paste::paste! {
            impl<'c> AttributeContainer<[<$field:camel>]> for AttributeStore<'c> {
                fn push_attribute(&mut self, value: [<$field:camel>]) {
                    self.$field = Some(value);
                }

                fn take_attribute(&mut self) -> Option<[<$field:camel>]> {
                    std::mem::take(&mut self.$field)
                }

                fn attribute_is_enabled(&self) -> bool {
                    self.$field.is_some()
                }

                fn get_attribute(&self) -> Option<&[<$field:camel>]> {
                    self.$field.as_ref()
                }

                fn get_mut_attribute(&mut self) -> Option<&mut [<$field:camel>]> {
                    self.$field.as_mut()
                }
            }
        }
    };
}

attribute_store_container_for!(subtree_size);
attribute_store_container_for!(non_negative);
attribute_store_container_for!(arity);
attribute_store_container_for!(relation_type);
attribute_store_container_for!(unique_keys);
attribute_store_container_for!(cardinality);

impl<'a> AttributeContainer<ColumnNames<'a>> for AttributeStore<'a> {
    fn push_attribute(&mut self, value: ColumnNames<'a>) {
        self.column_names = Some(value);
    }

    fn take_attribute(&mut self) -> Option<ColumnNames<'a>> {
        std::mem::take(&mut self.column_names)
    }

    fn attribute_is_enabled(&self) -> bool {
        self.column_names.is_some()
    }

    fn get_attribute(&self) -> Option<&ColumnNames<'a>> {
        self.column_names.as_ref()
    }

    fn get_mut_attribute(&mut self) -> Option<&mut ColumnNames<'a>> {
        self.column_names.as_mut()
    }
}

/// Produce an [`AnnotatedPlan`] wrapping the given [`MirRelationExpr`] along
/// with [`Attributes`] derived from the given context configuration.
pub fn annotate_plan<'a>(
    plan: &'a MirRelationExpr,
    context: &'a ExplainContext,
) -> Result<AnnotatedPlan<'a, MirRelationExpr>, RecursionLimitError> {
    let mut annotations = BTreeMap::<&MirRelationExpr, Attributes>::default();
    let config = context.config;

    // We want to annotate the plan with attributes in the following cases:
    // 1. An attribute was explicitly requested in the ExplainConfig.
    // 2. Humanized expressions were requested in the ExplainConfig (in which
    //    case we need to derive the ColumnNames attribute).
    if config.requires_attributes() || config.humanized_exprs {
        // get the annotation keys
        let subtree_refs = plan.post_order_vec();
        // get the annotation values
        let mut attributes = DerivedAttributes::from(context);
        plan.visit(&mut attributes)?;

        if config.subtree_size {
            for (expr, subtree_size) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<SubtreeSize>().into_iter(),
            ) {
                let attributes = annotations.entry(expr).or_default();
                attributes.subtree_size = Some(subtree_size);
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
            for (expr, arity) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<Arity>().into_iter(),
            ) {
                let attrs = annotations.entry(expr).or_default();
                attrs.arity = Some(arity);
            }
        }

        if config.types {
            for (expr, types) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<RelationType>().into_iter(),
            ) {
                let attrs = annotations.entry(expr).or_default();
                attrs.types = Some(types);
            }
        }

        if config.keys {
            for (expr, keys) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<UniqueKeys>().into_iter(),
            ) {
                let attrs = annotations.entry(expr).or_default();
                attrs.keys = Some(keys);
            }
        }

        if config.cardinality {
            for (expr, cardinality) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<Cardinality>().into_iter(),
            ) {
                let cardinality =
                    cardinality::HumanizedSymbolicExpression::new(&cardinality, context.humanizer)
                        .to_string();
                let attrs = annotations.entry(expr).or_default();
                attrs.cardinality = Some(cardinality);
            }
        }

        if config.column_names || config.humanized_exprs {
            for (expr, column_names) in std::iter::zip(
                subtree_refs.iter(),
                attributes.remove_results::<ColumnNames>().into_iter(),
            ) {
                let attrs = annotations.entry(expr).or_default();
                attrs.column_names = Some(column_names);
            }
        }
    }

    Ok(AnnotatedPlan { plan, annotations })
}
