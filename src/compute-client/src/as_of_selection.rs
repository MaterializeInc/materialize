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
//! | Let `A` and `B` be any pair of collections where `A` is an input of `B`.
//! | Before (a), both invariants are upheld, i.e. `A.lower <= B.lower` and `A.upper <= B.upper`.
//! |
//! | Case 1: (a) increases `A.lower` and/or `B.lower` to `A.lower'` and `B.lower'`
//! |     Invariant (1) might be broken, need to prove that it can be restored.
//! |     Case 1.a: `A.lower' <= B.lower'`
//! |         Invariant (1) is still upheld without propagation.
//! |     Case 1.b: `A.lower' > B.lower'`
//! |         A collection's lower bound can only be increased up to its upper bound.
//! |         Therefore, and from invariant (2): `A.lower' <= A.upper <= B.upper`
//! |         Therefore, propagation can set `B.lower' = A.lower'`, restoring invariant (1).
//! | Case 2: (a) decreases `A.upper` and/or `B.upper`
//! |     Invariant (2) might be broken, need to prove that it can be restored.
//! |     The proof is equivalent to Case 1.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;

use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_ore::collections::CollectionExt;
use mz_ore::soft_panic_or_log;
use mz_repr::{GlobalId, TimestampManipulation};
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
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
    read_only_mode: bool,
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

    // During 0dt upgrades, it can happen that the leader environment drops storage collections,
    // making the read-only environment observe inconsistent read frontiers (database-issues#8836).
    // To avoid hard-constraint failures, we prune all collections depending on these dropped
    // storage collections.
    if read_only_mode {
        ctx.prune_dropped_collections();
    }

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
    //
    // Allowing dataflows to hydrate immediately is desirable. It allows them to complete the most
    // resource-intensive phase of their lifecycle as early as possible. Ideally we want this phase
    // to occur during a 0dt upgrade, where we still have the option to roll back if a cluster
    // doesn't come up successfully. For dataflows with a refresh schedule, hydrating early also
    // ensures that there isn't a large output delay when the refresh time is reached.
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
        let as_of = first_export.map_or_else(Antichain::new, |id| ctx.best_as_of(id));
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
    fn expect_collection(&self, id: GlobalId) -> &Collection<'_, T> {
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
                        info!(%id, %bounds, ?constraint, "failed to apply soft as-of constraint");
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
    /// A collection's as-of _must_ be < the write frontier of the storage collection it exports to
    /// (if any) if it is non-empty, and <= the storage collection's read frontier otherwise.
    ///
    /// Rationale:
    ///
    /// * A collection's as-of must be <= the write frontier of its dependent storage collection,
    ///   because we need to pick up computing the contents of storage collections where we left
    ///   off previously, to avoid skipped times observable in the durable output.
    /// * Some dataflows feeding into storage collections (specifically: continual tasks) need to
    ///   be able to observe input changes at times they write to the output. If we selected the
    ///   as-of to be equal to the write frontier of the output storage collection, we wouldn't be
    ///   able to produce the correct output at that frontier. Thus the selected as-of must be
    ///   strictly less than the write frontier.
    /// * As an exception to the above, if the output storage collection is empty (i.e. its write
    ///   frontier is <= its read frontier), we need to allow the as-of to be equal to the read
    ///   frontier. This is correct in the sense that it mirrors the timestamp selection behavior
    ///   of the sequencer when it created the collection. Chances are that the sequencer chose the
    ///   initial as-of (and therefore the initial read frontier of the storage collection) as the
    ///   smallest possible time that can still be read from the collection inputs, so forcing the
    ///   upper bound any lower than that read frontier would produce a hard constraint violation.
    ///
    /// Failing to apply this constraint to a collection is an error. The storage collection it
    /// exports to may have times visible to readers skipped in its output, violating correctness.
    fn apply_downstream_storage_constraints(&self) {
        // Apply direct constraints from storage exports.
        for id in self.collections.keys() {
            let Ok(frontiers) = self.storage_collections.collection_frontiers(*id) else {
                continue;
            };

            let collection_empty =
                PartialOrder::less_equal(&frontiers.write_frontier, &frontiers.read_capabilities);
            let upper = if collection_empty {
                frontiers.read_capabilities
            } else {
                Antichain::from_iter(
                    frontiers
                        .write_frontier
                        .iter()
                        .map(|t| t.step_back().unwrap_or_else(T::minimum)),
                )
            };

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
                let mut write_frontier = write_frontiers.remove(id).expect("inserted above");
                for input_id in &collection.compute_inputs {
                    let frontier = &write_frontiers[input_id];
                    *changed |= write_frontier.extend(frontier.iter().cloned());
                }
                write_frontiers.insert(*id, write_frontier);
            }
        });

        // Apply the warmup constraint.
        for (id, write_frontier) in write_frontiers {
            let upper = step_back_frontier(&write_frontier);
            let constraint = Constraint {
                type_: ConstraintType::Soft,
                bound_type: BoundType::Upper,
                frontier: &upper,
                reason: &format!(
                    "warmup frontier derived from storage write frontier {:?}",
                    write_frontier.elements()
                ),
            };
            self.apply_constraint(id, constraint);
        }

        // Restore `AsOfBounds` invariant (2).
        self.propagate_bounds_upstream(BoundType::Upper);
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
                let mut write_frontier = write_frontiers.remove(id).expect("inserted above");
                for input_id in &collection.compute_inputs {
                    let frontier = &write_frontiers[input_id];
                    *changed |= write_frontier.extend(frontier.iter().cloned());
                }
                write_frontiers.insert(*id, write_frontier);
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
        // Propagating `lower` bounds downstream restores `AsOfBounds` invariant (1) and must
        // therefore always succeed.
        let constraint_type = match bound_type {
            BoundType::Lower => ConstraintType::Hard,
            BoundType::Upper => ConstraintType::Soft,
        };

        // We don't want to rely on a correspondence between `GlobalId` order and dependency order,
        // so we use a fixpoint loop here.
        fixpoint(|changed| {
            self.propagate_bounds_downstream_inner(bound_type, constraint_type, changed);

            // Propagating `upper` bounds downstream might break `AsOfBounds` invariant (2), so we
            // need to restore it.
            if bound_type == BoundType::Upper {
                self.propagate_bounds_upstream_inner(
                    BoundType::Upper,
                    ConstraintType::Hard,
                    changed,
                );
            }
        });
    }

    fn propagate_bounds_downstream_inner(
        &self,
        bound_type: BoundType,
        constraint_type: ConstraintType,
        changed: &mut bool,
    ) {
        for (id, collection) in &self.collections {
            for input_id in &collection.compute_inputs {
                let input_collection = self.expect_collection(*input_id);
                let bounds = input_collection.bounds.borrow();
                let constraint = Constraint {
                    type_: constraint_type,
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
        // Propagating `upper` bounds upstream restores `AsOfBounds` invariant (2) and must
        // therefore always succeed.
        let constraint_type = match bound_type {
            BoundType::Lower => ConstraintType::Soft,
            BoundType::Upper => ConstraintType::Hard,
        };

        // We don't want to rely on a correspondence between `GlobalId` order and dependency order,
        // so we use a fixpoint loop here.
        fixpoint(|changed| {
            self.propagate_bounds_upstream_inner(bound_type, constraint_type, changed);

            // Propagating `lower` bounds upstream might break `AsOfBounds` invariant (1), so we
            // need to restore it.
            if bound_type == BoundType::Lower {
                self.propagate_bounds_downstream_inner(
                    BoundType::Lower,
                    ConstraintType::Hard,
                    changed,
                );
            }
        });
    }

    fn propagate_bounds_upstream_inner(
        &self,
        bound_type: BoundType,
        constraint_type: ConstraintType,
        changed: &mut bool,
    ) {
        for (id, collection) in self.collections.iter().rev() {
            let bounds = collection.bounds.borrow();
            for input_id in &collection.compute_inputs {
                let constraint = Constraint {
                    type_: constraint_type,
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

    /// Removes collections depending on storage collections with empty read frontiers.
    ///
    /// The dataflows of these collections will get an empty default as-of assigned at the end of
    /// the as-of selection process, ensuring that they won't get installed.
    ///
    /// This exists only to work around database-issues#8836.
    fn prune_dropped_collections(&mut self) {
        // Remove collections with dropped storage inputs.
        let mut pruned = BTreeSet::new();
        self.collections.retain(|id, c| {
            let input_dropped = c.storage_inputs.iter().any(|id| {
                let frontiers = self
                    .storage_collections
                    .collection_frontiers(*id)
                    .expect("storage collection exists");
                frontiers.read_capabilities.is_empty()
            });

            if input_dropped {
                pruned.insert(*id);
                false
            } else {
                true
            }
        });

        warn!(?pruned, "pruned dependants of dropped storage collections");

        // Remove (transitive) dependants of pruned collections.
        while !pruned.is_empty() {
            let pruned_inputs = std::mem::take(&mut pruned);

            self.collections.retain(|id, c| {
                if c.compute_inputs.iter().any(|id| pruned_inputs.contains(id)) {
                    pruned.insert(*id);
                    false
                } else {
                    true
                }
            });

            warn!(?pruned, "pruned collections with pruned inputs");
        }
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
        .map(|t| t.step_back().unwrap_or_else(T::minimum))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use async_trait::async_trait;
    use differential_dataflow::lattice::Lattice;
    use futures::future::BoxFuture;
    use futures::stream::BoxStream;
    use mz_compute_types::dataflows::{IndexDesc, IndexImport};
    use mz_compute_types::sinks::ComputeSinkConnection;
    use mz_compute_types::sinks::ComputeSinkDesc;
    use mz_compute_types::sinks::MaterializedViewSinkConnection;
    use mz_compute_types::sources::SourceInstanceArguments;
    use mz_compute_types::sources::SourceInstanceDesc;
    use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
    use mz_persist_types::{Codec64, ShardId};
    use mz_repr::Timestamp;
    use mz_repr::{RelationDesc, RelationVersion, Row, SqlRelationType};
    use mz_storage_client::client::TimestamplessUpdateBuilder;
    use mz_storage_client::controller::{CollectionDescription, StorageMetadata, StorageTxn};
    use mz_storage_client::storage_collections::{CollectionFrontiers, SnapshotCursor};
    use mz_storage_types::StorageDiff;
    use mz_storage_types::connections::inline::InlinedConnection;
    use mz_storage_types::controller::{CollectionMetadata, StorageError};
    use mz_storage_types::errors::CollectionMissing;
    use mz_storage_types::parameters::StorageParameters;
    use mz_storage_types::sources::{GenericSourceConnection, SourceDesc};
    use mz_storage_types::sources::{SourceData, SourceExportDataConfig};
    use mz_storage_types::time_dependence::{TimeDependence, TimeDependenceError};
    use timely::progress::Timestamp as TimelyTimestamp;

    use super::*;

    const SEALED: u64 = 0x5ea1ed;

    fn ts_to_frontier(ts: u64) -> Antichain<Timestamp> {
        if ts == SEALED {
            Antichain::new()
        } else {
            Antichain::from_elem(ts.into())
        }
    }

    #[derive(Debug)]
    struct StorageFrontiers(BTreeMap<GlobalId, (Antichain<Timestamp>, Antichain<Timestamp>)>);

    #[async_trait]
    impl StorageCollections for StorageFrontiers {
        type Timestamp = Timestamp;

        async fn initialize_state(
            &self,
            _txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
            _init_ids: BTreeSet<GlobalId>,
        ) -> Result<(), StorageError<Self::Timestamp>> {
            unimplemented!()
        }

        fn update_parameters(&self, _config_params: StorageParameters) {
            unimplemented!()
        }

        fn collection_metadata(
            &self,
            _id: GlobalId,
        ) -> Result<CollectionMetadata, CollectionMissing> {
            unimplemented!()
        }

        fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)> {
            unimplemented!()
        }

        fn collections_frontiers(
            &self,
            ids: Vec<GlobalId>,
        ) -> Result<Vec<CollectionFrontiers<Self::Timestamp>>, CollectionMissing> {
            let mut frontiers = Vec::with_capacity(ids.len());
            for id in ids {
                let (read, write) = self.0.get(&id).ok_or(CollectionMissing(id))?;
                frontiers.push(CollectionFrontiers {
                    id,
                    write_frontier: write.clone(),
                    implied_capability: read.clone(),
                    read_capabilities: read.clone(),
                })
            }
            Ok(frontiers)
        }

        fn active_collection_frontiers(&self) -> Vec<CollectionFrontiers<Self::Timestamp>> {
            unimplemented!()
        }

        fn check_exists(&self, _id: GlobalId) -> Result<(), StorageError<Self::Timestamp>> {
            unimplemented!()
        }

        async fn snapshot_stats(
            &self,
            _id: GlobalId,
            _as_of: Antichain<Self::Timestamp>,
        ) -> Result<SnapshotStats, StorageError<Self::Timestamp>> {
            unimplemented!()
        }

        async fn snapshot_parts_stats(
            &self,
            _id: GlobalId,
            _as_of: Antichain<Self::Timestamp>,
        ) -> BoxFuture<'static, Result<SnapshotPartsStats, StorageError<Self::Timestamp>>> {
            unimplemented!()
        }

        fn snapshot(
            &self,
            _id: GlobalId,
            _as_of: Self::Timestamp,
        ) -> BoxFuture<'static, Result<Vec<(Row, StorageDiff)>, StorageError<Self::Timestamp>>>
        {
            unimplemented!()
        }

        async fn snapshot_latest(
            &self,
            _id: GlobalId,
        ) -> Result<Vec<Row>, StorageError<Self::Timestamp>> {
            unimplemented!()
        }

        fn snapshot_cursor(
            &self,
            _id: GlobalId,
            _as_of: Self::Timestamp,
        ) -> BoxFuture<
            'static,
            Result<SnapshotCursor<Self::Timestamp>, StorageError<Self::Timestamp>>,
        >
        where
            Self::Timestamp: TimelyTimestamp + Lattice + Codec64,
        {
            unimplemented!()
        }

        fn snapshot_and_stream(
            &self,
            _id: GlobalId,
            _as_of: Self::Timestamp,
        ) -> BoxFuture<
            'static,
            Result<
                BoxStream<'static, (SourceData, Self::Timestamp, StorageDiff)>,
                StorageError<Self::Timestamp>,
            >,
        > {
            unimplemented!()
        }

        /// Create a [`TimestamplessUpdateBuilder`] that can be used to stage
        /// updates for the provided [`GlobalId`].
        fn create_update_builder(
            &self,
            _id: GlobalId,
        ) -> BoxFuture<
            'static,
            Result<
                TimestamplessUpdateBuilder<SourceData, (), Self::Timestamp, StorageDiff>,
                StorageError<Self::Timestamp>,
            >,
        >
        where
            Self::Timestamp: Lattice + Codec64,
        {
            unimplemented!()
        }

        async fn prepare_state(
            &self,
            _txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
            _ids_to_add: BTreeSet<GlobalId>,
            _ids_to_drop: BTreeSet<GlobalId>,
            _ids_to_register: BTreeMap<GlobalId, ShardId>,
        ) -> Result<(), StorageError<Self::Timestamp>> {
            unimplemented!()
        }

        async fn create_collections_for_bootstrap(
            &self,
            _storage_metadata: &StorageMetadata,
            _register_ts: Option<Self::Timestamp>,
            _collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
            _migrated_storage_collections: &BTreeSet<GlobalId>,
        ) -> Result<(), StorageError<Self::Timestamp>> {
            unimplemented!()
        }

        async fn alter_ingestion_source_desc(
            &self,
            _ingestion_id: GlobalId,
            _source_desc: SourceDesc,
        ) -> Result<(), StorageError<Self::Timestamp>> {
            unimplemented!()
        }

        async fn alter_ingestion_export_data_configs(
            &self,
            _source_exports: BTreeMap<GlobalId, SourceExportDataConfig>,
        ) -> Result<(), StorageError<Self::Timestamp>> {
            unimplemented!()
        }

        async fn alter_ingestion_connections(
            &self,
            _source_connections: BTreeMap<GlobalId, GenericSourceConnection<InlinedConnection>>,
        ) -> Result<(), StorageError<Self::Timestamp>> {
            unimplemented!()
        }

        async fn alter_table_desc(
            &self,
            _existing_collection: GlobalId,
            _new_collection: GlobalId,
            _new_desc: RelationDesc,
            _expected_version: RelationVersion,
        ) -> Result<(), StorageError<Self::Timestamp>> {
            unimplemented!()
        }

        fn drop_collections_unvalidated(
            &self,
            _storage_metadata: &StorageMetadata,
            _identifiers: Vec<GlobalId>,
        ) {
            unimplemented!()
        }

        fn set_read_policies(&self, _policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>) {
            unimplemented!()
        }

        fn acquire_read_holds(
            &self,
            desired_holds: Vec<GlobalId>,
        ) -> Result<Vec<ReadHold<Self::Timestamp>>, CollectionMissing> {
            let mut holds = Vec::with_capacity(desired_holds.len());
            for id in desired_holds {
                let (read, _write) = self.0.get(&id).ok_or(CollectionMissing(id))?;
                let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
                holds.push(ReadHold::with_channel(id, read.clone(), tx));
            }
            Ok(holds)
        }

        fn determine_time_dependence(
            &self,
            _id: GlobalId,
        ) -> Result<Option<TimeDependence>, TimeDependenceError> {
            unimplemented!()
        }

        fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
            unimplemented!()
        }
    }

    fn dataflow(
        export_id: &str,
        input_ids: &[&str],
        storage_ids: &BTreeSet<&str>,
    ) -> DataflowDescription<Plan> {
        let source_imports = input_ids
            .iter()
            .filter(|s| storage_ids.contains(*s))
            .map(|s| {
                let id = s.parse().unwrap();
                let desc = SourceInstanceDesc {
                    arguments: SourceInstanceArguments {
                        operators: Default::default(),
                    },
                    storage_metadata: Default::default(),
                    typ: SqlRelationType::empty(),
                };
                (id, (desc, Default::default(), Default::default()))
            })
            .collect();
        let index_imports = input_ids
            .iter()
            .filter(|s| !storage_ids.contains(*s))
            .map(|s| {
                let id = s.parse().unwrap();
                let import = IndexImport {
                    desc: IndexDesc {
                        on_id: GlobalId::Transient(0),
                        key: Default::default(),
                    },
                    typ: SqlRelationType::empty(),
                    monotonic: Default::default(),
                };
                (id, import)
            })
            .collect();
        let index_exports = std::iter::once(export_id)
            .filter(|s| !storage_ids.contains(*s))
            .map(|sid| {
                let id = sid.parse().unwrap();
                let desc = IndexDesc {
                    on_id: GlobalId::Transient(0),
                    key: Default::default(),
                };
                let typ = SqlRelationType::empty();
                (id, (desc, typ))
            })
            .collect();
        let sink_exports = std::iter::once(export_id)
            .filter(|s| storage_ids.contains(*s))
            .map(|sid| {
                let id = sid.parse().unwrap();
                let desc = ComputeSinkDesc {
                    from: GlobalId::Transient(0),
                    from_desc: RelationDesc::empty(),
                    connection: ComputeSinkConnection::MaterializedView(
                        MaterializedViewSinkConnection {
                            value_desc: RelationDesc::empty(),
                            storage_metadata: Default::default(),
                        },
                    ),
                    with_snapshot: Default::default(),
                    up_to: Default::default(),
                    non_null_assertions: Default::default(),
                    refresh_schedule: Default::default(),
                };
                (id, desc)
            })
            .collect();

        DataflowDescription {
            source_imports,
            index_imports,
            objects_to_build: Default::default(),
            index_exports,
            sink_exports,
            as_of: None,
            until: Default::default(),
            initial_storage_as_of: Default::default(),
            refresh_schedule: Default::default(),
            debug_name: Default::default(),
            time_dependence: None,
        }
    }

    macro_rules! testcase {
        ($name:ident, {
            storage: { $( $storage_id:literal: ($read:expr, $write:expr), )* },
            dataflows: [ $( $export_id:literal <- $inputs:expr => $as_of:expr, )* ],
            current_time: $current_time:literal,
            $( read_policies: { $( $policy_id:literal: $policy:expr, )* }, )?
            $( read_only: $read_only:expr, )?
        }) => {
            #[mz_ore::test]
            fn $name() {
                let storage_ids = [$( $storage_id, )*].into();

                let storage_frontiers = StorageFrontiers(BTreeMap::from([
                    $(
                        (
                            $storage_id.parse().unwrap(),
                            (ts_to_frontier($read), ts_to_frontier($write)),
                        ),
                    )*
                ]));

                let mut dataflows = [
                    $(
                        dataflow($export_id, &$inputs, &storage_ids),
                    )*
                ];

                let read_policies = BTreeMap::from([
                    $($( ($policy_id.parse().unwrap(), $policy), )*)?
                ]);

                #[allow(unused_variables)]
                let read_only = false;
                $( let read_only = $read_only; )?

                super::run(
                    &mut dataflows,
                    &read_policies,
                    &storage_frontiers,
                    $current_time.into(),
                    read_only,
                );

                let actual_as_ofs: Vec<_> = dataflows
                    .into_iter()
                    .map(|d| d.as_of.unwrap())
                    .collect();
                let expected_as_ofs = [ $( ts_to_frontier($as_of), )* ];

                assert_eq!(actual_as_ofs, expected_as_ofs);
            }
        };
    }

    testcase!(upstream_storage_constraints, {
        storage: {
            "s1": (10, 20),
            "s2": (20, 30),
        },
        dataflows: [
            "u1" <- ["s1"]       => 10,
            "u2" <- ["s2"]       => 20,
            "u3" <- ["s1", "s2"] => 20,
            "u4" <- ["u1", "u2"] => 20,
        ],
        current_time: 0,
    });

    testcase!(downstream_storage_constraints, {
        storage: {
            "s1": (10, 20),
            "u3": (10, 15),
            "u4": (10, 13),
        },
        dataflows: [
            "u1" <- ["s1"] => 19,
            "u2" <- ["s1"] => 12,
            "u3" <- ["u2"] => 14,
            "u4" <- ["u2"] => 12,
        ],
        current_time: 100,
    });

    testcase!(warmup_constraints, {
        storage: {
            "s1": (10, 20),
            "s2": (10, 30),
            "s3": (10, 40),
            "s4": (10, 50),
        },
        dataflows: [
            "u1" <- ["s1"]       => 19,
            "u2" <- ["s2"]       => 19,
            "u3" <- ["s3"]       => 39,
            "u4" <- ["s4"]       => 39,
            "u5" <- ["u1", "u2"] => 19,
            "u6" <- ["u3", "u4"] => 39,
        ],
        current_time: 100,
    });

    testcase!(index_read_policy_constraints, {
        storage: {
            "s1": (10, 20),
            "u6": (10, 18),
        },
        dataflows: [
            "u1" <- ["s1"] => 15,
            "u2" <- ["s1"] => 10,
            "u3" <- ["s1"] => 13,
            "u4" <- ["s1"] => 10,
            "u5" <- []     => 95,
            "u6" <- ["s1"] => 17,
        ],
        current_time: 100,
        read_policies: {
            "u1": ReadPolicy::lag_writes_by(5.into(), 1.into()),
            "u2": ReadPolicy::lag_writes_by(15.into(), 1.into()),
            "u3": ReadPolicy::ValidFrom(Antichain::from_elem(13.into())),
            "u4": ReadPolicy::ValidFrom(Antichain::from_elem(5.into())),
            "u5": ReadPolicy::lag_writes_by(5.into(), 1.into()),
            "u6": ReadPolicy::ValidFrom(Antichain::from_elem(13.into())),
        },
    });

    testcase!(index_current_time_constraints, {
        storage: {
            "s1": (10, 20),
            "s2": (20, 30),
            "u4": (10, 12),
            "u5": (10, 18),
        },
        dataflows: [
            "u1" <- ["s1"] => 15,
            "u2" <- ["s2"] => 20,
            "u3" <- ["s1"] => 11,
            "u4" <- ["u3"] => 11,
            "u5" <- ["s1"] => 17,
            "u6" <- []     => 15,
        ],
        current_time: 15,
    });

    testcase!(sealed_storage_sink, {
        storage: {
            "s1": (10, 20),
            "u1": (10, SEALED),
        },
        dataflows: [
            "u1" <- ["s1"] => SEALED,
        ],
        current_time: 100,
    });

    testcase!(read_only_dropped_storage_inputs, {
        storage: {
            "s1": (10, 20),
            "s2": (SEALED, SEALED),
            "u4": (10, 20),
        },
        dataflows: [
            "u1" <- ["s1"] => 15,
            "u2" <- ["s2"] => SEALED,
            "u3" <- ["s1", "s2"] => SEALED,
            "u4" <- ["u2"] => SEALED,
        ],
        current_time: 15,
        read_only: true,
    });

    // Regression test for database-issues#9273.
    testcase!(github_9273, {
        storage: {
            "s1": (10, 20),
            "u3": (14, 15),
        },
        dataflows: [
            "u1" <- ["s1"] => 14,
            "u2" <- ["u1"] => 19,
            "u3" <- ["u1"] => 14,
        ],
        current_time: 100,
        read_policies: {
            "u1": ReadPolicy::lag_writes_by(1.into(), 1.into()),
            "u2": ReadPolicy::lag_writes_by(1.into(), 1.into()),
        },
    });
}
