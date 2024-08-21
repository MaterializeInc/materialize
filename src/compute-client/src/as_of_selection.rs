// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for selecting as-ofs of compute dataflows during system initialization.
//!
//! The functionality implemented here is invoked by the coordinator during its bootstrap process.
//! Ideally, it would be part of the controller and transparent to the coordinator, but that's
//! difficult to reconcile with the current controller API. For now, we still make the coordinator
//! worry about as-of selection but keep the implementation in a compute crate because it really is
//! a compute implementation concern.
//!
//! The as-of selection process takes a list of `DataflowDescription`s, determines compatible
//! as-ofs for the compute collections they export, and augments the `DataflowDescription`s with
//! these as-ofs.
//!
//! For each compute collection, the as-of selection process keeps an `AsOfBounds` instance that
//! tracks a lower and an upper bound for the as-of the collection may get assigned. Throughout the
//! process, a collection's `AsOfBounds` get repeatedly refined, by increasing the lower bound and
//! decreasing the upper bound. The final upper bound is then used for the collection as-of. Using
//! the upper bound maximizes the chances of compute reconciliation being effective, and minimizes
//! the amount of historical data that must be read from the dataflow sources.
//!
//! Refinement of `AsOfBounds` is performed by applying `Constraint`s to collections. A
//! `Constraint` specifies which bound should be refined to which frontier. A `Constraint` may be
//! "hard" or "soft", which determines how failure to apply it is handled. Failing to apply a hard
//! constraint is treated as an error, failing to apply a soft constraint is not. If a constraint
//! fails to apply, the respective `AsOfBounds` are refined as much as possible (to a single
//! frontier) and marked as "sealed". Subsequent constraint applications against the sealed bounds
//! are no-ops. This is done to avoid log noise from repeated constraint application failures.
//!
//! Note that failing to apply a hard constraint does not abort the as-of selection process for the
//! affected collection. Instead the failure is handled gracefully by logging an error and
//! assigning the collection a best-effort as-of. This is done, rather than panicking or returning
//! an error and letting the coordinator panic, to ensure the availability of the system. Ideally,
//! we would instead mark the affected dataflow as failed/poisoned, but such a mechanism doesn't
//! currently exist.
//!
//! The as-of selection process applies constraints in order of importance, because once a
//! constraint application fails, the respective `AsOfBounds` are sealed and later applications
//! won't have any effect. This means hard constraints must be applied before soft constraints, and
//! more desirable soft constraints should be applied before less desirable ones.
//!
//! # `AsOfBounds` Invariants
//!
//! Correctness requires two invariants of `AsOfBounds` of dependent collections:
//!
//!  (1) The lower bound of a collection is >= the lower bound of each of its inputs.
//!  (2) The upper bound of a collection is >= the upper bound of each of its inputs.
//!
//! Each step of the as-of selection process needs to ensure that these invariants are upheld once
//! it completes. The expectation is that each step (a) performs local changes to either the
//! `lower` _or_ the `upper` bounds of some collections and (b) invokes the appropriate
//! `propagate_bounds_*` method to restore the invariant broken by (a).
//!
//! For steps that behave as described in (a), we can prove that (b) will always succeed in
//! applying the bounds propagation constraints:
//!
//!     Let `A` and `B` be any pair of collections where `A` is an input of `B`.
//!     Before (a), both invariants are upheld, i.e. `A.lower <= B.lower` and `A.upper <= B.upper`.
//!
//!     Case 1: (a) increases `A.lower` and/or `B.lower` to `A.lower'` and `B.lower'`
//!         Invariant (1) might be broken, need to prove that it can be restored.
//!         Case 1.a: `A.lower' <= B.lower'`
//!             Invariant (1) is still upheld without propagation.
//!         Case 1.b: `A.lower' > B.lower'`
//!             A collection's lower bound can only be increased up to its upper bound.
//!             Therefore, and from invariant (2): `A.lower' <= A.upper <= B.upper`
//!             Therefore, propagation can set `B.lower' = A.lower'`, restoring invariant (1).
//!     Case 2: (a) decreases `A.upper` and/or `B.upper`
//!         Invariant (2) might be broken, need to prove that it can be restored.
//!         The proof is equivalent to Case 1.
//!
//! Because the bounds propagation is guaranteed to succeed, provided each step of the as-of
//! selection process only modifies either lower or upper bounds, the `propagate_bounds_*` methods
//! always apply hard constraints, even when they are invoked during a soft-constraint step.
//! Failing to apply a constraint while propagating bounds is a bug.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

use differential_dataflow::lattice::Lattice;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_ore::collections::CollectionExt;
use mz_ore::soft_panic_or_log;
use mz_repr::{GlobalId, TimestampManipulation};
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{info, warn};

/// Runs as-of selection for the given dataflows.
///
/// Assigns the selected as-of to the provided dataflow descriptions and returns a set of
/// `ReadHold`s that must not be dropped nor downgraded until the dataflows have been installed
/// with the compute controller.
pub fn run<T: TimestampManipulation>(
    dataflows: &mut [DataflowDescription<Plan<T>, (), T>],
    read_policies: &BTreeMap<GlobalId, ReadPolicy<T>>,
    storage_collections: &dyn StorageCollections<Timestamp = T>,
    current_time: T,
) -> BTreeMap<GlobalId, ReadHold<T>> {
    // Get read holds for the storage inputs of the dataflows.
    // This ensures that storage frontiers don't advance past the selected as-ofs.
    let mut storage_read_holds = BTreeMap::new();
    for dataflow in &*dataflows {
        for id in dataflow.source_imports.keys() {
            if !storage_read_holds.contains_key(id) {
                let read_hold = storage_collections
                    .acquire_read_holds(vec![*id])
                    .expect("storage collection exists")
                    .into_element();
                storage_read_holds.insert(*id, read_hold);
            }
        }
    }

    let mut ctx = Context::new(dataflows, storage_collections, read_policies, current_time);

    // Dataflows that sink into a storage collection that has advanced to the empty frontier don't
    // need to be installed at all. So we can apply an optimization where we prune them here and
    // assign them an empty as-of at the end.
    ctx.prune_sealed_persist_sinks();

    // Apply hard constraints from upstream and downstream storage collections.
    ctx.apply_upstream_storage_constraints(&storage_read_holds);
    ctx.apply_downstream_storage_constraints();

    // At this point all collections have as-of bounds that reflect what is required for
    // correctness. The current state isn't very usable though. In particular, most of the upper
    // bounds are likely to be the empty frontier, so if we'd select as-ofs on this basis, the
    // resulting dataflows would never hydrate. Instead we'll apply a number of soft constraints to
    // end up in a better place.

    // Constrain collection as-ofs to times that are currently available in the inputs. This
    // ensures that dataflows can immediately start hydrating. It also ensures that dataflows don't
    // get an empty as-of, except when they exclusively depend on constant collections.
    ctx.apply_warmup_constraints();

    // Constrain as-ofs of indexes according to their read policies.
    ctx.apply_index_read_policy_constraints();

    // Constrain as-ofs of indexes to the current time. This ensures that indexes are immediately
    // readable.
    ctx.apply_index_current_time_constraints();

    // Apply the derived as-of bounds to the dataflows.
    for dataflow in dataflows {
        // `AsOfBounds` are shared between the exports of a dataflow, so looking at just the first
        // export is sufficient.
        let first_export = dataflow.export_ids().next();
        let as_of = first_export.map_or(Antichain::new(), |id| ctx.best_as_of(id));
        dataflow.as_of = Some(as_of);
    }

    storage_read_holds
}

/// Bounds for possible as-of values of a dataflow.
#[derive(Debug)]
struct AsOfBounds<T> {
    lower: Antichain<T>,
    upper: Antichain<T>,
    /// Whether these bounds can still change.
    sealed: bool,
}

impl<T: Clone> AsOfBounds<T> {
    /// Creates an `AsOfBounds` that only allows the given `frontier`.
    fn single(frontier: Antichain<T>) -> Self {
        Self {
            lower: frontier.clone(),
            upper: frontier,
            sealed: false,
        }
    }

    /// Get the bound of the given type.
    fn get(&self, type_: BoundType) -> &Antichain<T> {
        match type_ {
            BoundType::Lower => &self.lower,
            BoundType::Upper => &self.upper,
        }
    }
}

impl<T: Timestamp> Default for AsOfBounds<T> {
    fn default() -> Self {
        Self {
            lower: Antichain::from_elem(T::minimum()),
            upper: Antichain::new(),
            sealed: false,
        }
    }
}

impl<T: fmt::Debug> fmt::Display for AsOfBounds<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{:?} .. {:?}]",
            self.lower.elements(),
            self.upper.elements()
        )
    }
}

/// Types of bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BoundType {
    Lower,
    Upper,
}

impl fmt::Display for BoundType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lower => f.write_str("lower"),
            Self::Upper => f.write_str("upper"),
        }
    }
}

/// Types of constraints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConstraintType {
    /// Hard constraints are applied to enforce correctness properties, and failing to apply them is
    /// an error.
    Hard,
    /// Soft constraints are applied to improve performance or UX, and failing to apply them is
    /// undesirable but not an error.
    Soft,
}

/// A constraint that can be applied to the `AsOfBounds` of a collection.
#[derive(Debug)]
struct Constraint<'a, T> {
    type_: ConstraintType,
    /// Which bound this constraint applies to.
    bound_type: BoundType,
    /// The frontier by which the bound should be constrained.
    frontier: &'a Antichain<T>,
    /// A short description of the reason for applying this constraint.
    ///
    /// Used only for logging.
    reason: &'a str,
}

impl<T: Timestamp> Constraint<'_, T> {
    /// Applies this constraint to the given bounds.
    ///
    /// Returns a bool indicating whether the given bounds were changed as a result.
    ///
    /// Applying a constraint can fail, if the constraint frontier is incompatible with the
    /// existing bounds. In this case, the constraint still gets partially applied by moving one of
    /// the bounds up/down to the other, depending on the `bound_type`.
    ///
    /// Applying a constraint to sealed bounds is a no-op.
    fn apply(&self, bounds: &mut AsOfBounds<T>) -> Result<bool, bool> {
        if bounds.sealed {
            return Ok(false);
        }

        match self.bound_type {
            BoundType::Lower => {
                if PartialOrder::less_than(&bounds.upper, self.frontier) {
                    bounds.sealed = true;
                    if PartialOrder::less_than(&bounds.lower, &bounds.upper) {
                        bounds.lower.clone_from(&bounds.upper);
                        Err(true)
                    } else {
                        Err(false)
                    }
                } else if PartialOrder::less_equal(self.frontier, &bounds.lower) {
                    Ok(false)
                } else {
                    bounds.lower.clone_from(self.frontier);
                    Ok(true)
                }
            }
            BoundType::Upper => {
                if PartialOrder::less_than(self.frontier, &bounds.lower) {
                    bounds.sealed = true;
                    if PartialOrder::less_than(&bounds.lower, &bounds.upper) {
                        bounds.upper.clone_from(&bounds.lower);
                        Err(true)
                    } else {
                        Err(false)
                    }
                } else if PartialOrder::less_equal(&bounds.upper, self.frontier) {
                    Ok(false)
                } else {
                    bounds.upper.clone_from(self.frontier);
                    Ok(true)
                }
            }
        }
    }
}

/// State tracked for a compute collection during as-of selection.
struct Collection<'a, T> {
    storage_inputs: Vec<GlobalId>,
    compute_inputs: Vec<GlobalId>,
    read_policy: Option<&'a ReadPolicy<T>>,
    /// The currently known as-of bounds.
    ///
    /// Shared between collections exported by the same dataflow.
    bounds: Rc<RefCell<AsOfBounds<T>>>,
    /// Whether this collection is an index.
    is_index: bool,
}

/// The as-of selection context.
struct Context<'a, T> {
    collections: BTreeMap<GlobalId, Collection<'a, T>>,
    storage_collections: &'a dyn StorageCollections<Timestamp = T>,
    current_time: T,
}

impl<'a, T: TimestampManipulation> Context<'a, T> {
    /// Initializes an as-of selection context for the given `dataflows`.
    fn new(
        dataflows: &[DataflowDescription<Plan<T>, (), T>],
        storage_collections: &'a dyn StorageCollections<Timestamp = T>,
        read_policies: &'a BTreeMap<GlobalId, ReadPolicy<T>>,
        current_time: T,
    ) -> Self {
        // Construct initial collection state for each dataflow export. Dataflows might have their
        // as-ofs already fixed, which we need to take into account when constructing `AsOfBounds`.
        let mut collections = BTreeMap::new();
        for dataflow in dataflows {
            let storage_inputs: Vec<_> = dataflow.source_imports.keys().copied().collect();
            let compute_inputs: Vec<_> = dataflow.index_imports.keys().copied().collect();

            let bounds = match dataflow.as_of.clone() {
                Some(frontier) => AsOfBounds::single(frontier),
                None => AsOfBounds::default(),
            };
            let bounds = Rc::new(RefCell::new(bounds));

            for id in dataflow.export_ids() {
                let collection = Collection {
                    storage_inputs: storage_inputs.clone(),
                    compute_inputs: compute_inputs.clone(),
                    read_policy: read_policies.get(&id),
                    bounds: Rc::clone(&bounds),
                    is_index: dataflow.index_exports.contains_key(&id),
                };
                collections.insert(id, collection);
            }
        }

        Self {
            collections,
            storage_collections,
            current_time,
        }
    }

    /// Returns the state of the identified collection.
    ///
    /// # Panics
    ///
    /// Panics if the identified collection doesn't exist.
    fn expect_collection(&self, id: GlobalId) -> &Collection<T> {
        self.collections
            .get(&id)
            .unwrap_or_else(|| panic!("collection missing: {id}"))
    }

    /// Applies the given as-of constraint to the identified collection.
    ///
    /// Returns whether the collection's as-of bounds where changed as a result.
    fn apply_constraint(&self, id: GlobalId, constraint: Constraint<T>) -> bool {
        let collection = self.expect_collection(id);
        let mut bounds = collection.bounds.borrow_mut();
        match constraint.apply(&mut bounds) {
            Ok(changed) => {
                if changed {
                    info!(%id, %bounds, reason = %constraint.reason, "applied as-of constraint");
                }
                changed
            }
            Err(changed) => {
                match constraint.type_ {
                    ConstraintType::Hard => {
                        soft_panic_or_log!(
                            "failed to apply hard as-of constraint \
                             (id={id}, bounds={bounds}, constraint={constraint:?})"
                        );
                    }
                    ConstraintType::Soft => {
                        warn!(%id, %bounds, ?constraint, "failed to apply soft as-of constraint");
                    }
                }
                changed
            }
        }
    }

    /// Apply as-of constraints imposed by the frontiers of upstream storage collections.
    ///
    /// A collection's as-of _must_ be >= the read frontier of each of its (transitive) storage
    /// inputs.
    ///
    /// Failing to apply this constraint to a collection is an error. The affected dataflow will
    /// not be able to hydrate successfully.
    fn apply_upstream_storage_constraints(
        &self,
        storage_read_holds: &BTreeMap<GlobalId, ReadHold<T>>,
    ) {
        // Apply direct constraints from storage inputs.
        for (id, collection) in &self.collections {
            for input_id in &collection.storage_inputs {
                let read_hold = &storage_read_holds[input_id];
                let constraint = Constraint {
                    type_: ConstraintType::Hard,
                    bound_type: BoundType::Lower,
                    frontier: read_hold.since(),
                    reason: &format!("storage input {input_id} read frontier"),
                };
                self.apply_constraint(*id, constraint);
            }
        }

        // Propagate constraints downstream, restoring `AsOfBounds` invariant (1).
        self.propagate_bounds_downstream(BoundType::Lower);
    }

    /// Apply as-of constraints imposed by the frontiers of downstream storage collections.
    ///
    /// A collection's as-of _must_ be <= the join of the read frontier and the write frontier of
    /// the storage collection it exports to, if any.
    ///
    /// We need to consider the read frontier in addition to the write frontier because storage
    /// collections are commonly initialyzed with a write frontier of `[0]`, even when they start
    /// producing output at some later time. The read frontier is always initialized to the first
    /// time the collection will produce valid output for, so it constrains the times that need to
    /// be produced by dependencies of newly initialized storage collections.
    ///
    /// Failing to apply this constraint to a collection is an error. The storage collection it
    /// exports to will have times visible to readers skipped in its output, violating correctness.
    fn apply_downstream_storage_constraints(&self) {
        // Apply direct constraints from storage exports.
        for id in self.collections.keys() {
            let Ok(frontiers) = self.storage_collections.collection_frontiers(*id) else {
                continue;
            };
            let upper = frontiers.read_capabilities.join(&frontiers.write_frontier);
            let constraint = Constraint {
                type_: ConstraintType::Hard,
                bound_type: BoundType::Upper,
                frontier: &upper,
                reason: &format!("storage export {id} write frontier"),
            };
            self.apply_constraint(*id, constraint);
        }

        // Propagate constraints upstream, restoring `AsOfBounds` invariant (2).
        self.propagate_bounds_upstream(BoundType::Upper);
    }

    /// Apply as-of constraints to ensure collections can hydrate immediately.
    ///
    /// A collection's as-of _should_ be < the write frontier of each of its (transitive) storage
    /// inputs.
    ///
    /// Failing to apply this constraint is not an error. The affected dataflow will not be able to
    /// hydrate immediately, but it will be able to hydrate once its inputs have sufficiently
    /// advanced.
    fn apply_warmup_constraints(&self) {
        // Apply direct constraints from storage inputs.
        for (id, collection) in &self.collections {
            for input_id in &collection.storage_inputs {
                let frontiers = self
                    .storage_collections
                    .collection_frontiers(*input_id)
                    .expect("storage collection exists");
                let upper = step_back_frontier(&frontiers.write_frontier);
                let constraint = Constraint {
                    type_: ConstraintType::Soft,
                    bound_type: BoundType::Upper,
                    frontier: &upper,
                    reason: &format!("storage input {input_id} warmup frontier"),
                };
                self.apply_constraint(*id, constraint);
            }
        }

        // Propagate constraints downstream. This transparently restores any violations of
        // `AsOfBounds` invariant (2) that might be introduced by the propagation.
        self.propagate_bounds_downstream(BoundType::Upper);
    }

    /// Apply as-of constraints to ensure indexes contain historical data as requested by their
    /// associated read policies.
    ///
    /// An index's as-of _should_ be <= the frontier determined by its read policy applied to its
    /// write frontier.
    ///
    /// Failing to apply this constraint is not an error. The affected index will not contain
    /// historical times for its entire compaction window initially, but will do so once sufficient
    /// time has passed.
    fn apply_index_read_policy_constraints(&self) {
        // For the write frontier of an index, we'll use the least write frontier of its
        // (transitive) storage inputs. This is an upper bound for the write frontier the index
        // could have had before the restart. For indexes without storage inputs we use the current
        // time.

        // Collect write frontiers from storage inputs.
        let mut write_frontiers = BTreeMap::new();
        for (id, collection) in &self.collections {
            let storage_frontiers = self
                .storage_collections
                .collections_frontiers(collection.storage_inputs.clone())
                .expect("storage collections exist");

            let mut write_frontier = Antichain::new();
            for frontiers in storage_frontiers {
                write_frontier.extend(frontiers.write_frontier);
            }

            write_frontiers.insert(*id, write_frontier);
        }

        // Propagate write frontiers through compute inputs.
        fixpoint(|changed| {
            for (id, collection) in &self.collections {
                let write_frontier = write_frontiers.get_mut(id).expect("inserted above");
                for input_id in &collection.compute_inputs {
                    let input_collection = self.expect_collection(*input_id);
                    let bounds = input_collection.bounds.borrow();
                    *changed |= write_frontier.extend(bounds.upper.iter().cloned());
                }
            }
        });

        // Apply the read policy constraint to indexes.
        for (id, collection) in &self.collections {
            if let (true, Some(read_policy)) = (collection.is_index, &collection.read_policy) {
                let mut write_frontier = write_frontiers.remove(id).expect("inserted above");
                if write_frontier.is_empty() {
                    write_frontier = Antichain::from_elem(self.current_time.clone());
                }
                let upper = read_policy.frontier(write_frontier.borrow());
                let constraint = Constraint {
                    type_: ConstraintType::Soft,
                    bound_type: BoundType::Upper,
                    frontier: &upper,
                    reason: &format!(
                        "read policy applied to write frontier {:?}",
                        write_frontier.elements()
                    ),
                };
                self.apply_constraint(*id, constraint);
            }
        }

        // Restore `AsOfBounds` invariant (2).
        self.propagate_bounds_upstream(BoundType::Upper);
    }

    /// Apply as-of constraints to ensure indexes are immediately readable.
    ///
    /// An index's as-of _should_ be <= the current time.
    ///
    /// Failing to apply this constraint is not an error. The affected index will not be readable
    /// immediately, but will be readable once sufficient time has passed.
    fn apply_index_current_time_constraints(&self) {
        // Apply the current time constraint to indexes.
        let upper = Antichain::from_elem(self.current_time.clone());
        for (id, collection) in &self.collections {
            if collection.is_index {
                let constraint = Constraint {
                    type_: ConstraintType::Soft,
                    bound_type: BoundType::Upper,
                    frontier: &upper,
                    reason: "index current time",
                };
                self.apply_constraint(*id, constraint);
            }
        }

        // Restore `AsOfBounds` invariant (2).
        self.propagate_bounds_upstream(BoundType::Upper);
    }

    /// Propagate as-of bounds through the dependency graph, in downstream direction.
    fn propagate_bounds_downstream(&self, bound_type: BoundType) {
        // We don't want to rely on a correspondence between `GlobalId` order and dependency order,
        // so we use a fixpoint loop here.
        fixpoint(|changed| {
            self.propagate_bounds_downstream_inner(bound_type, changed);

            // Propagating `upper` bounds downstream might break `AsOfBounds` invariant (2), so we
            // need to restore it.
            if bound_type == BoundType::Upper {
                self.propagate_bounds_upstream_inner(BoundType::Upper, changed);
            }
        });
    }

    fn propagate_bounds_downstream_inner(&self, bound_type: BoundType, changed: &mut bool) {
        for (id, collection) in &self.collections {
            for input_id in &collection.compute_inputs {
                let input_collection = self.expect_collection(*input_id);
                let bounds = input_collection.bounds.borrow();
                let constraint = Constraint {
                    type_: ConstraintType::Hard,
                    bound_type,
                    frontier: bounds.get(bound_type),
                    reason: &format!("upstream {input_id} {bound_type} as-of bound"),
                };
                *changed |= self.apply_constraint(*id, constraint);
            }
        }
    }

    /// Propagate as-of bounds through the dependency graph, in upstream direction.
    fn propagate_bounds_upstream(&self, bound_type: BoundType) {
        // We don't want to rely on a correspondence between `GlobalId` order and dependency order,
        // so we use a fixpoint loop here.
        fixpoint(|changed| {
            self.propagate_bounds_upstream_inner(bound_type, changed);

            // Propagating `lower` bounds upstream might break `AsOfBounds` invariant (1), so we
            // need to restore it.
            if bound_type == BoundType::Lower {
                self.propagate_bounds_downstream_inner(BoundType::Lower, changed);
            }
        });
    }

    fn propagate_bounds_upstream_inner(&self, bound_type: BoundType, changed: &mut bool) {
        for (id, collection) in self.collections.iter().rev() {
            let bounds = collection.bounds.borrow();
            for input_id in &collection.compute_inputs {
                let constraint = Constraint {
                    type_: ConstraintType::Hard,
                    bound_type,
                    frontier: bounds.get(bound_type),
                    reason: &format!("downstream {id} {bound_type} as-of bound"),
                };
                *changed |= self.apply_constraint(*input_id, constraint);
            }
        }
    }

    /// Selects the "best" as-of for the identified collection, based on its currently known
    /// bounds.
    ///
    /// We simply use the upper bound here, to maximize the chances of compute reconciliation
    /// succeeding. Choosing the latest possible as-of also minimizes the amount of work the
    /// dataflow has to spend processing historical data from its sources.
    fn best_as_of(&self, id: GlobalId) -> Antichain<T> {
        if let Some(collection) = self.collections.get(&id) {
            let bounds = collection.bounds.borrow();
            bounds.upper.clone()
        } else {
            Antichain::new()
        }
    }

    /// Removes collections that sink into sealed persist shards from the context.
    ///
    /// The dataflows of these collections will get an empty default as-of assigned at the end of
    /// the as-of selection process, ensuring that they won't get installed unnecessarily.
    ///
    /// Note that it is valid to remove these collections from consideration because they don't
    /// impose as-of constraints on other compute collections.
    fn prune_sealed_persist_sinks(&mut self) {
        self.collections.retain(|id, _| {
            self.storage_collections
                .collection_frontiers(*id)
                .map_or(true, |f| !f.write_frontier.is_empty())
        });
    }
}

/// Runs `step` in a loop until it stops reporting changes.
fn fixpoint(mut step: impl FnMut(&mut bool)) {
    loop {
        let mut changed = false;
        step(&mut changed);
        if !changed {
            break;
        }
    }
}

/// Step back the given frontier.
///
/// This method is saturating: If the frontier contains `T::minimum()` times, these are kept
/// unchanged.
fn step_back_frontier<T: TimestampManipulation>(frontier: &Antichain<T>) -> Antichain<T> {
    frontier
        .iter()
        .map(|t| t.step_back().unwrap_or(T::minimum()))
        .collect()
}
