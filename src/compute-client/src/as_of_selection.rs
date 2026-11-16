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

use mz_compute_types::dataflows::LirDataflowDescription;
use mz_ore::collections::CollectionExt;
use mz_ore::soft_panic_or_log;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use timely::PartialOrder;
use timely::progress::Antichain;
use tracing::{info, warn};

/// Runs as-of selection for the given dataflows.
///
/// Assigns the selected as-of to the provided dataflow descriptions and returns a set of
/// `ReadHold`s that must not be dropped nor downgraded until the dataflows have been installed
/// with the compute controller.
///
/// `pending_replacements` are the exports of replacement materialized views that are not applied
/// yet. The downstream storage constraint is soft for them.
pub fn run(
    dataflows: &mut [LirDataflowDescription],
    read_policies: &BTreeMap<GlobalId, ReadPolicy>,
    storage_collections: &dyn StorageCollections,
    current_time: Timestamp,
    read_only_mode: bool,
    pending_replacements: &BTreeSet<GlobalId>,
) -> BTreeMap<GlobalId, ReadHold> {
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

    // Apply constraints from upstream and downstream storage collections.
    ctx.apply_upstream_storage_constraints(&storage_read_holds);
    ctx.apply_downstream_storage_constraints(pending_replacements);

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
struct AsOfBounds {
    lower: Antichain<Timestamp>,
    upper: Antichain<Timestamp>,
    /// Whether these bounds can still change.
    sealed: bool,
}

impl AsOfBounds {
    /// Creates an `AsOfBounds` that only allows the given `frontier`.
    fn single(frontier: Antichain<Timestamp>) -> Self {
        Self {
            lower: frontier.clone(),
            upper: frontier,
            sealed: false,
        }
    }

    /// Get the bound of the given type.
    fn get(&self, type_: BoundType) -> &Antichain<Timestamp> {
        match type_ {
            BoundType::Lower => &self.lower,
            BoundType::Upper => &self.upper,
        }
    }
}

impl Default for AsOfBounds {
    fn default() -> Self {
        Self {
            lower: Antichain::from_elem(Timestamp::MIN),
            upper: Antichain::new(),
            sealed: false,
        }
    }
}

impl fmt::Display for AsOfBounds {
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
struct Constraint<'a> {
    type_: ConstraintType,
    /// Which bound this constraint applies to.
    bound_type: BoundType,
    /// The frontier by which the bound should be constrained.
    frontier: &'a Antichain<Timestamp>,
    /// A short description of the reason for applying this constraint.
    ///
    /// Used only for logging.
    reason: &'a str,
}

impl Constraint<'_> {
    /// Applies this constraint to the given bounds.
    ///
    /// Returns a bool indicating whether the given bounds were changed as a result.
    ///
    /// Applying a constraint can fail, if the constraint frontier is incompatible with the
    /// existing bounds. In this case, the constraint still gets partially applied by moving one of
    /// the bounds up/down to the other, depending on the `bound_type`.
    ///
    /// Applying a constraint to sealed bounds is a no-op.
    fn apply(&self, bounds: &mut AsOfBounds) -> Result<bool, bool> {
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
struct Collection<'a> {
    storage_inputs: Vec<GlobalId>,
    compute_inputs: Vec<GlobalId>,
    read_policy: Option<&'a ReadPolicy>,
    /// The currently known as-of bounds.
    ///
    /// Shared between collections exported by the same dataflow.
    bounds: Rc<RefCell<AsOfBounds>>,
    /// Whether this collection is an index.
    is_index: bool,
}

/// The frontier up to which the inputs of a collection count as complete.
struct InputFrontier {
    frontier: Antichain<Timestamp>,
    /// The storage write frontier that `frontier` stands in for, if the inputs are not readable
    /// at the collection's lower as-of bound.
    substituted_for: Option<Antichain<Timestamp>>,
}

impl InputFrontier {
    /// Describes the substitution, if any, for constraint reasons.
    fn substitution_note(&self) -> String {
        match &self.substituted_for {
            Some(write_frontier) => format!(
                " (current time, substituted for storage write frontier {:?} at which the inputs \
                 are not readable)",
                write_frontier.elements()
            ),
            None => String::new(),
        }
    }
}

/// The as-of selection context.
struct Context<'a> {
    collections: BTreeMap<GlobalId, Collection<'a>>,
    storage_collections: &'a dyn StorageCollections,
    current_time: Timestamp,
}

impl<'a> Context<'a> {
    /// Initializes an as-of selection context for the given `dataflows`.
    fn new(
        dataflows: &[LirDataflowDescription],
        storage_collections: &'a dyn StorageCollections,
        read_policies: &'a BTreeMap<GlobalId, ReadPolicy>,
        current_time: Timestamp,
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
    fn expect_collection(&self, id: GlobalId) -> &Collection<'_> {
        self.collections
            .get(&id)
            .unwrap_or_else(|| panic!("collection missing: {id}"))
    }

    /// Applies the given as-of constraint to the identified collection.
    ///
    /// Returns whether the collection's as-of bounds where changed as a result.
    fn apply_constraint(&self, id: GlobalId, constraint: Constraint) -> bool {
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
        storage_read_holds: &BTreeMap<GlobalId, ReadHold>,
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
    /// The exception is `pending_replacements`, for which the constraint is soft.
    fn apply_downstream_storage_constraints(&self, pending_replacements: &BTreeSet<GlobalId>) {
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
                step_back_frontier(&frontiers.write_frontier)
            };

            // A pending replacement reports its target's write frontier but doesn't write to it.
            // Its inputs might not be readable at that frontier, for example when they were
            // created after the target last wrote, so its as-of can be beyond it. Applying the
            // replacement waits for the target's write frontier to reach the replacement's, which
            // starts at the as-of. That wait, not this constraint, prevents skipped times.
            let type_ = if pending_replacements.contains(id) {
                ConstraintType::Soft
            } else {
                ConstraintType::Hard
            };
            let constraint = Constraint {
                type_,
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
        // Apply the warmup constraint.
        for (id, input) in self.input_frontiers() {
            let upper = step_back_frontier(&input.frontier);
            let constraint = Constraint {
                type_: ConstraintType::Soft,
                bound_type: BoundType::Upper,
                frontier: &upper,
                reason: &format!(
                    "warmup frontier derived from storage write frontier {:?}{}",
                    input.frontier.elements(),
                    input.substitution_note(),
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
        // For the write frontier of an index, we'll use the frontier of its inputs. This is an
        // upper bound for the write frontier the index could have had before the restart. For
        // indexes without storage inputs we use the current time.
        let mut input_frontiers = self.input_frontiers();

        // Apply the read policy constraint to indexes.
        for (id, collection) in &self.collections {
            if let (true, Some(read_policy)) = (collection.is_index, &collection.read_policy) {
                let input = input_frontiers.remove(id).expect("inserted above");
                let mut write_frontier = input.frontier.clone();
                if write_frontier.is_empty() {
                    write_frontier = Antichain::from_elem(self.current_time.clone());
                }
                let upper = read_policy.frontier(write_frontier.borrow());
                let constraint = Constraint {
                    type_: ConstraintType::Soft,
                    bound_type: BoundType::Upper,
                    frontier: &upper,
                    reason: &format!(
                        "read policy applied to write frontier {:?}{}",
                        write_frontier.elements(),
                        input.substitution_note(),
                    ),
                };
                self.apply_constraint(*id, constraint);
            }
        }

        // Restore `AsOfBounds` invariant (2).
        self.propagate_bounds_upstream(BoundType::Upper);
    }

    /// Returns, for each collection, the meet of the write frontiers of its transitive storage
    /// inputs.
    fn transitive_storage_write_frontiers(&self) -> BTreeMap<GlobalId, Antichain<Timestamp>> {
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

        write_frontiers
    }

    /// Returns, for each collection, the frontier up to which its inputs count as complete when
    /// deriving soft upper bounds.
    ///
    /// The lower as-of bounds must be final when this is called.
    fn input_frontiers(&self) -> BTreeMap<GlobalId, InputFrontier> {
        // NOTE: An input without readable data at the lower bound, for example an MV that never
        // ran, holds the write frontier at or below that bound. The collection can't hydrate at
        // any as-of we could select until that input advances. Constraining the upper bound by
        // that write frontier would seal the as-of at the lower bound, and invariant (2) would
        // then drag every compute input back to it, making them replay history for no benefit.
        // Instead we treat the inputs as complete up to the current time. The empty frontier
        // would not do: it removes the warmup bound, and a dataflow no other constraint bounds,
        // like a metric sink, would get an empty as-of and never be installed.
        let after_current_time = self
            .current_time
            .checked_add(1)
            .map_or_else(Antichain::new, Antichain::from_elem);
        self.transitive_storage_write_frontiers()
            .into_iter()
            .map(|(id, write_frontier)| {
                let bounds = self.expect_collection(id).bounds.borrow();
                let input = if PartialOrder::less_equal(&write_frontier, &bounds.lower) {
                    InputFrontier {
                        frontier: after_current_time.clone(),
                        substituted_for: Some(write_frontier),
                    }
                } else {
                    InputFrontier {
                        frontier: write_frontier,
                        substituted_for: None,
                    }
                };
                (id, input)
            })
            .collect()
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
    fn best_as_of(&self, id: GlobalId) -> Antichain<Timestamp> {
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
fn step_back_frontier(frontier: &Antichain<Timestamp>) -> Antichain<Timestamp> {
    frontier
        .iter()
        .map(|t| t.step_back().unwrap_or(Timestamp::MIN))
        .collect()
}

#[cfg(test)]
mod tests;
