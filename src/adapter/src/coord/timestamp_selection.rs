// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for selecting timestamps for various operations on collections.

use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use constraints::Constraints;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_adapter_types::dyncfgs::CONSTRAINT_BASED_TIMESTAMP_SELECTION;
use mz_adapter_types::timestamp_selection::ConstraintBasedTimestampSelection;
use mz_compute_types::ComputeInstanceId;
use mz_ore::cast::CastLossy;
use mz_ore::soft_assert_eq_or_log;
use mz_repr::{GlobalId, Timestamp, TimestampManipulation};
use mz_sql::plan::QueryWhen;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::IsolationLevel;
use mz_storage_types::sources::Timeline;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::{Level, event};

use crate::AdapterError;
use crate::catalog::CatalogState;
use crate::coord::Coordinator;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::read_policy::ReadHolds;
use crate::coord::timeline::TimelineContext;
use crate::session::Session;

/// The timeline and timestamp context of a read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimestampContext<T> {
    /// Read is executed in a specific timeline with a specific timestamp.
    TimelineTimestamp {
        timeline: Timeline,
        /// The timestamp that was chosen for a read. This can differ from the
        /// `oracle_ts` when collections are not readable at the (linearized)
        /// timestamp for the oracle. In those cases (when the chosen timestamp
        /// is further ahead than the oracle timestamp) we have to delay
        /// returning peek results until the timestamp oracle is also
        /// sufficiently advanced.
        chosen_ts: T,
        /// The timestamp that would have been chosen for the read by the
        /// (linearized) timestamp oracle). In most cases this will be picked as
        /// the `chosen_ts`.
        oracle_ts: Option<T>,
    },
    /// Read is executed without a timeline or timestamp.
    NoTimestamp,
}

impl<T: TimestampManipulation> TimestampContext<T> {
    /// Creates a `TimestampContext` from a timestamp and `TimelineContext`.
    pub fn from_timeline_context(
        chosen_ts: T,
        oracle_ts: Option<T>,
        transaction_timeline: Option<Timeline>,
        timeline_context: &TimelineContext,
    ) -> TimestampContext<T> {
        match timeline_context {
            TimelineContext::TimelineDependent(timeline) => {
                if let Some(transaction_timeline) = transaction_timeline {
                    assert_eq!(timeline, &transaction_timeline);
                }
                Self::TimelineTimestamp {
                    timeline: timeline.clone(),
                    chosen_ts,
                    oracle_ts,
                }
            }
            TimelineContext::TimestampDependent => {
                // We default to the `Timeline::EpochMilliseconds` timeline if one doesn't exist.
                Self::TimelineTimestamp {
                    timeline: transaction_timeline.unwrap_or(Timeline::EpochMilliseconds),
                    chosen_ts,
                    oracle_ts,
                }
            }
            TimelineContext::TimestampIndependent => Self::NoTimestamp,
        }
    }

    /// The timeline belonging to this context, if one exists.
    pub fn timeline(&self) -> Option<&Timeline> {
        self.timeline_timestamp().map(|tt| tt.0)
    }

    /// The timestamp belonging to this context, if one exists.
    pub fn timestamp(&self) -> Option<&T> {
        self.timeline_timestamp().map(|tt| tt.1)
    }

    /// The timeline and timestamp belonging to this context, if one exists.
    pub fn timeline_timestamp(&self) -> Option<(&Timeline, &T)> {
        match self {
            Self::TimelineTimestamp {
                timeline,
                chosen_ts,
                ..
            } => Some((timeline, chosen_ts)),
            Self::NoTimestamp => None,
        }
    }

    /// The timestamp belonging to this context, or a sensible default if one does not exists.
    pub fn timestamp_or_default(&self) -> T {
        match self {
            Self::TimelineTimestamp { chosen_ts, .. } => chosen_ts.clone(),
            // Anything without a timestamp is given the maximum possible timestamp to indicate
            // that they have been closed up until the end of time. This allows us to SUBSCRIBE to
            // static views.
            Self::NoTimestamp => T::maximum(),
        }
    }

    /// Whether or not the context contains a timestamp.
    pub fn contains_timestamp(&self) -> bool {
        self.timestamp().is_some()
    }

    /// Converts this `TimestampContext` to an `Antichain`.
    pub fn antichain(&self) -> Antichain<T> {
        Antichain::from_elem(self.timestamp_or_default())
    }
}

#[async_trait(?Send)]
impl TimestampProvider for Coordinator {
    /// Reports a collection's current read frontier.
    fn compute_read_frontier(
        &self,
        instance: ComputeInstanceId,
        id: GlobalId,
    ) -> Antichain<Timestamp> {
        self.controller
            .compute
            .collection_frontiers(id, Some(instance))
            .expect("id does not exist")
            .read_frontier
    }

    /// Reports a collection's current write frontier.
    fn compute_write_frontier(
        &self,
        instance: ComputeInstanceId,
        id: GlobalId,
    ) -> Antichain<Timestamp> {
        self.controller
            .compute
            .collection_frontiers(id, Some(instance))
            .expect("id does not exist")
            .write_frontier
    }

    fn storage_frontiers(
        &self,
        ids: Vec<GlobalId>,
    ) -> Vec<(GlobalId, Antichain<Timestamp>, Antichain<Timestamp>)> {
        self.controller
            .storage
            .collections_frontiers(ids)
            .expect("missing collections")
    }

    fn acquire_read_holds(&self, id_bundle: &CollectionIdBundle) -> ReadHolds<Timestamp> {
        self.acquire_read_holds(id_bundle)
    }

    fn catalog_state(&self) -> &CatalogState {
        self.catalog().state()
    }
}

/// A timestamp determination, which includes the timestamp, constraints, and session oracle read
/// timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTimestampDetermination<T> {
    pub timestamp: T,
    pub constraints: Option<Constraints>,
    pub session_oracle_read_ts: Option<T>,
}

#[async_trait(?Send)]
pub trait TimestampProvider {
    fn compute_read_frontier(
        &self,
        instance: ComputeInstanceId,
        id: GlobalId,
    ) -> Antichain<Timestamp>;
    fn compute_write_frontier(
        &self,
        instance: ComputeInstanceId,
        id: GlobalId,
    ) -> Antichain<Timestamp>;

    /// Returns the implied capability (since) and write frontier (upper) for
    /// the specified storage collections.
    fn storage_frontiers(
        &self,
        ids: Vec<GlobalId>,
    ) -> Vec<(GlobalId, Antichain<Timestamp>, Antichain<Timestamp>)>;

    fn catalog_state(&self) -> &CatalogState;

    fn get_timeline(timeline_context: &TimelineContext) -> Option<Timeline> {
        let timeline = match timeline_context {
            TimelineContext::TimelineDependent(timeline) => Some(timeline.clone()),
            // We default to the `Timeline::EpochMilliseconds` timeline if one doesn't exist.
            TimelineContext::TimestampDependent => Some(Timeline::EpochMilliseconds),
            TimelineContext::TimestampIndependent => None,
        };

        timeline
    }

    /// Returns true if-and-only-if the given configuration needs a linearized
    /// read timestamp from a timestamp oracle.
    ///
    /// This assumes that the query happens in the context of a timeline. If
    /// there is no timeline, we cannot and don't have to get a linearized read
    /// timestamp.
    fn needs_linearized_read_ts(isolation_level: &IsolationLevel, when: &QueryWhen) -> bool {
        // When we're in the context of a timeline (assumption) and one of these
        // scenarios hold, we need to use a linearized read timestamp:
        // - The isolation level is Strict Serializable and the `when` allows us to use the
        //   the timestamp oracle (ex: queries with no AS OF).
        // - The `when` requires us to use the timestamp oracle (ex: read-then-write queries).
        when.must_advance_to_timeline_ts()
            || (when.can_advance_to_timeline_ts()
                && matches!(
                    isolation_level,
                    IsolationLevel::StrictSerializable | IsolationLevel::StrongSessionSerializable
                ))
    }

    /// Determines the timestamp for a query using the classical logic (as opposed to constraint-based).
    fn determine_timestamp_classical(
        session: &Session,
        read_holds: &ReadHolds<Timestamp>,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        oracle_read_ts: Option<Timestamp>,
        compute_instance: ComputeInstanceId,
        real_time_recency_ts: Option<Timestamp>,
        isolation_level: &IsolationLevel,
        timeline: &Option<Timeline>,
        largest_not_in_advance_of_upper: Timestamp,
        since: &Antichain<Timestamp>,
    ) -> Result<RawTimestampDetermination<Timestamp>, AdapterError> {
        let mut session_oracle_read_ts = None;
        // Each involved trace has a validity interval `[since, upper)`.
        // The contents of a trace are only guaranteed to be correct when
        // accumulated at a time greater or equal to `since`, and they
        // are only guaranteed to be currently present for times not
        // greater or equal to `upper`.
        //
        // The plan is to first determine a timestamp, based on the requested
        // timestamp policy, and then determine if it can be satisfied using
        // the compacted arrangements we have at hand. It remains unresolved
        // what to do if it cannot be satisfied (perhaps the query should use
        // a larger timestamp and block, perhaps the user should intervene).

        {
            // TODO: We currently split out getting the oracle timestamp because
            // it's a potentially expensive call, but a call that can be done in an
            // async task. TimestampProvider is not Send (nor Sync), so we cannot do
            // the call to `determine_timestamp_for` (including the oracle call) on
            // an async task. If/when TimestampProvider can become Send, we can fold
            // the call to the TimestampOracle back into this function.
            //
            // We assert here that the logic that determines the oracle timestamp
            // matches our expectations.

            if timeline.is_some() && Self::needs_linearized_read_ts(isolation_level, when) {
                assert!(
                    oracle_read_ts.is_some(),
                    "should get a timestamp from the oracle for linearized timeline {:?} but didn't",
                    timeline
                );
            }
        }

        // Initialize candidate to the minimum correct time.
        let mut candidate = Timestamp::minimum();

        if let Some(ts) = when.advance_to_timestamp() {
            candidate.join_assign(&ts);
        }

        if when.advance_to_since() {
            // Note: This `advance_by` is a no-op if the given frontier is `[]`.
            candidate.advance_by(since.borrow());
        }

        // If we've acquired a read timestamp from the timestamp oracle, use it
        // as the new lower bound for the candidate.
        // In Strong Session Serializable, we ignore the oracle timestamp for now, unless we need
        // to use it.
        if let Some(timestamp) = &oracle_read_ts {
            if isolation_level != &IsolationLevel::StrongSessionSerializable
                || when.must_advance_to_timeline_ts()
            {
                candidate.join_assign(timestamp);
            }
        }

        // We advance to the upper in the following scenarios:
        // - The isolation level is Serializable and the `when` allows us to advance to upper (ex:
        //   queries with no AS OF). We avoid using the upper in Strict Serializable to prevent
        //   reading source data that is being written to in the future.
        // - The isolation level is Strict Serializable but there is no timelines and the `when`
        //   allows us to advance to upper.
        if when.can_advance_to_upper()
            && (isolation_level == &IsolationLevel::Serializable || timeline.is_none())
        {
            candidate.join_assign(&largest_not_in_advance_of_upper);
        }

        if let Some(real_time_recency_ts) = real_time_recency_ts {
            if !(session.vars().real_time_recency()
                && isolation_level == &IsolationLevel::StrictSerializable)
            {
                // Erring on the side of caution, lets bail out here.
                // This should never happen in practice, as the real time recency timestamp should
                // only be supplied when real time recency is enabled.
                coord_bail!(
                    "real time recency timestamp should only be supplied when real time recency \
                            is enabled and the isolation level is strict serializable"
                );
            }
            candidate.join_assign(&real_time_recency_ts);
        }

        if isolation_level == &IsolationLevel::StrongSessionSerializable {
            if let Some(timeline) = &timeline {
                if let Some(oracle) = session.get_timestamp_oracle(timeline) {
                    let session_ts = oracle.read_ts();
                    candidate.join_assign(&session_ts);
                    session_oracle_read_ts = Some(session_ts);
                }
            }

            // When advancing the read timestamp under Strong Session Serializable, there is a
            // trade-off to make between freshness and latency. We can choose a timestamp close the
            // `upper`, but then later queries might block if the `upper` is too far into the
            // future. We can chose a timestamp close to the current time, but then we may not be
            // getting results that are as fresh as possible. As a heuristic, we choose the minimum
            // of now and the upper, where we use the global timestamp oracle read timestamp as a
            // proxy for now. If upper > now, then we choose now and prevent blocking future
            // queries. If upper < now, then we choose the upper and prevent blocking the current
            // query.
            if when.can_advance_to_upper() && when.can_advance_to_timeline_ts() {
                let mut advance_to = largest_not_in_advance_of_upper;
                if let Some(oracle_read_ts) = oracle_read_ts {
                    advance_to = std::cmp::min(advance_to, oracle_read_ts);
                }
                candidate.join_assign(&advance_to);
            }
        }

        // If the timestamp is greater or equal to some element in `since` we are
        // assured that the answer will be correct.
        //
        // It's ok for this timestamp to be larger than the current timestamp of
        // the timestamp oracle. For Strict Serializable queries, the Coord will
        // linearize the query by holding back the result until the timestamp
        // oracle catches up.
        let timestamp = if since.less_equal(&candidate) {
            event!(
                Level::DEBUG,
                conn_id = format!("{}", session.conn_id()),
                since = format!("{since:?}"),
                largest_not_in_advance_of_upper = format!("{largest_not_in_advance_of_upper}"),
                timestamp = format!("{candidate}")
            );
            candidate
        } else {
            // This can happen not just when the query has AS OF, but also when the passed in
            // `since` is `[]`.
            coord_bail!(generate_timestamp_not_valid_error_msg(
                id_bundle,
                compute_instance,
                read_holds,
                candidate
            ));
        };
        Ok(RawTimestampDetermination {
            timestamp,
            constraints: None,
            session_oracle_read_ts,
        })
    }

    /// Uses constraints and preferences to determine a timestamp for a query.
    /// Returns the determined timestamp, the constraints that were applied, and
    /// session_oracle_read_ts.
    fn determine_timestamp_via_constraints(
        session: &Session,
        read_holds: &ReadHolds<Timestamp>,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        oracle_read_ts: Option<Timestamp>,
        compute_instance: ComputeInstanceId,
        real_time_recency_ts: Option<Timestamp>,
        isolation_level: &IsolationLevel,
        timeline: &Option<Timeline>,
        largest_not_in_advance_of_upper: Timestamp,
    ) -> Result<RawTimestampDetermination<Timestamp>, AdapterError> {
        use constraints::{Constraints, Preference, Reason};

        let mut session_oracle_read_ts = None;
        // We start by establishing the hard constraints that must be applied to timestamp determination.
        // These constraints are derived from the input arguments, and properties of the collections involved.
        // TODO: Many of the constraints are expressed obliquely, and could be made more direct.
        let constraints = {
            // Constraints we will populate through a sequence of opinions.
            let mut constraints = Constraints::default();

            // First, we have validity constraints from the `id_bundle` argument which indicates
            // which collections we are reading from.
            // TODO: Refine the detail about which identifiers are binding and which are not.
            // TODO(dov): It's not entirely clear to me that there ever would be a non
            // binding constraint introduced by the `id_bundle`. We should revisit this.
            let since = read_holds.least_valid_read();
            let storage = id_bundle
                .storage_ids
                .iter()
                .cloned()
                .collect::<Vec<GlobalId>>();
            if !storage.is_empty() {
                constraints
                    .lower
                    .push((since.clone(), Reason::StorageInput(storage)));
            }
            let compute = id_bundle
                .compute_ids
                .iter()
                .flat_map(|(key, ids)| ids.iter().map(|id| (*key, *id)))
                .collect::<Vec<(ComputeInstanceId, GlobalId)>>();
            if !compute.is_empty() {
                constraints
                    .lower
                    .push((since.clone(), Reason::ComputeInput(compute)));
            }

            // The query's `when` may indicates a specific timestamp we must advance to, or a specific value we must use.
            if let Some(ts) = when.advance_to_timestamp() {
                constraints
                    .lower
                    .push((Antichain::from_elem(ts), Reason::QueryAsOf));
                // If the query is at a specific timestamp, we must introduce an upper bound as well.
                if when.constrains_upper() {
                    constraints
                        .upper
                        .push((Antichain::from_elem(ts), Reason::QueryAsOf));
                }
            }

            // The specification of an `oracle_read_ts` may indicates that we must advance to it,
            // except in one isolation mode, or if `when` does not indicate that we should.
            // At the moment, only `QueryWhen::FreshestTableWrite` indicates that we should.
            // TODO: Should this just depend on the isolation level?
            if let Some(timestamp) = &oracle_read_ts {
                if isolation_level != &IsolationLevel::StrongSessionSerializable
                    || when.must_advance_to_timeline_ts()
                {
                    // When specification of an `oracle_read_ts` is required, we must advance to it.
                    // If it's not present, lets bail out.
                    constraints.lower.push((
                        Antichain::from_elem(*timestamp),
                        Reason::IsolationLevel(*isolation_level),
                    ));
                }
            }

            // If a real time recency timestamp is supplied, we must advance to it.
            if let Some(real_time_recency_ts) = real_time_recency_ts {
                assert!(
                    session.vars().real_time_recency()
                        && isolation_level == &IsolationLevel::StrictSerializable,
                    "real time recency timestamp should only be supplied when real time recency \
                                is enabled and the isolation level is strict serializable"
                );
                constraints.lower.push((
                    Antichain::from_elem(real_time_recency_ts),
                    Reason::RealTimeRecency,
                ));
            }

            // If we are operating in Strong Session Serializable, we use an alternate timestamp lower bound.
            if isolation_level == &IsolationLevel::StrongSessionSerializable {
                if let Some(timeline) = &timeline {
                    if let Some(oracle) = session.get_timestamp_oracle(timeline) {
                        let session_ts = oracle.read_ts();
                        constraints.lower.push((
                            Antichain::from_elem(session_ts),
                            Reason::IsolationLevel(*isolation_level),
                        ));
                        session_oracle_read_ts = Some(session_ts);
                    }

                    // When advancing the read timestamp under Strong Session Serializable, there is a
                    // trade-off to make between freshness and latency. We can choose a timestamp close the
                    // `upper`, but then later queries might block if the `upper` is too far into the
                    // future. We can chose a timestamp close to the current time, but then we may not be
                    // getting results that are as fresh as possible. As a heuristic, we choose the minimum
                    // of now and the upper, where we use the global timestamp oracle read timestamp as a
                    // proxy for now. If upper > now, then we choose now and prevent blocking future
                    // queries. If upper < now, then we choose the upper and prevent blocking the current
                    // query.
                    if when.can_advance_to_upper() && when.can_advance_to_timeline_ts() {
                        let mut advance_to = largest_not_in_advance_of_upper;
                        if let Some(oracle_read_ts) = oracle_read_ts {
                            advance_to = std::cmp::min(advance_to, oracle_read_ts);
                        }
                        constraints.lower.push((
                            Antichain::from_elem(advance_to),
                            Reason::IsolationLevel(*isolation_level),
                        ));
                    }
                }
            }

            constraints.minimize();
            constraints
        };

        // Next we establish the preferences that we would like to apply to timestamp determination.
        // Generally, we want to choose the freshest timestamp possible, although there are exceptions
        // when we either want a maximally *stale* timestamp, or we want to protect other queries from
        // a recklessly advanced timestamp.
        let preferences = {
            // Counter-intuitively, the only `when` that allows `can_advance_to_upper` is `Immediately`,
            // and not `FreshestTableWrite`. This is because `FreshestTableWrite` instead imposes a lower
            // bound through the `oracle_read_ts`, and then requires the stalest valid timestamp.

            if when.can_advance_to_upper()
                && (isolation_level == &IsolationLevel::Serializable || timeline.is_none())
            {
                Preference::FreshestAvailable
            } else {
                Preference::StalestValid
            }

            // TODO: `StrongSessionSerializable` has a different set of preferences that starts to tease
            // out the trade-off between freshness and responsiveness. I think we don't yet know enough
            // to properly frame these preferences, though they are clearly aimed at the right concerns.
        };

        // Determine a candidate based on constraints and preferences.
        let constraint_candidate = {
            let mut candidate = Timestamp::minimum();
            // Note: These `advance_by` calls are no-ops if the given frontier is `[]`.
            candidate.advance_by(constraints.lower_bound().borrow());
            // If we have a preference to be the freshest available, advance to the minimum
            // of the upper bound constraints and the `largest_not_in_advance_of_upper`.
            if let Preference::FreshestAvailable = preferences {
                let mut upper_bound = constraints.upper_bound();
                upper_bound.insert(largest_not_in_advance_of_upper);
                candidate.advance_by(upper_bound.borrow());
            }
            // If the candidate is strictly outside the constraints, we didn't have a viable
            // timestamp. This can happen e.g. when the query has AS OF, or when the lower bound is
            // `[]`.
            if !constraints.lower_bound().less_equal(&candidate)
                || constraints.upper_bound().less_than(&candidate)
            {
                // TODO: Generate a better error msg, which includes all the constraints.
                coord_bail!(generate_timestamp_not_valid_error_msg(
                    id_bundle,
                    compute_instance,
                    read_holds,
                    candidate
                ));
            } else {
                candidate
            }
        };

        Ok(RawTimestampDetermination {
            timestamp: constraint_candidate,
            constraints: Some(constraints),
            session_oracle_read_ts,
        })
    }

    /// Determines the timestamp for a query.
    ///
    /// Timestamp determination may fail due to the restricted validity of
    /// traces. Each has a `since` and `upper` frontier, and are only valid
    /// after `since` and sure to be available not after `upper`.
    ///
    /// The timeline that `id_bundle` belongs to is also returned, if one exists.
    fn determine_timestamp_for(
        &self,
        session: &Session,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        compute_instance: ComputeInstanceId,
        timeline_context: &TimelineContext,
        oracle_read_ts: Option<Timestamp>,
        real_time_recency_ts: Option<Timestamp>,
        isolation_level: &IsolationLevel,
        constraint_based: &ConstraintBasedTimestampSelection,
    ) -> Result<(TimestampDetermination<Timestamp>, ReadHolds<Timestamp>), AdapterError> {
        // First, we acquire read holds that will ensure the queried collections
        // stay queryable at the chosen timestamp.
        let read_holds = self.acquire_read_holds(id_bundle);

        let upper = self.least_valid_write(id_bundle);

        Self::determine_timestamp_for_inner(
            session,
            id_bundle,
            when,
            compute_instance,
            timeline_context,
            oracle_read_ts,
            real_time_recency_ts,
            isolation_level,
            constraint_based,
            read_holds,
            upper,
        )
    }

    /// Same as determine_timestamp_for, but read_holds and least_valid_write are already passed in.
    fn determine_timestamp_for_inner(
        session: &Session,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        compute_instance: ComputeInstanceId,
        timeline_context: &TimelineContext,
        oracle_read_ts: Option<Timestamp>,
        real_time_recency_ts: Option<Timestamp>,
        isolation_level: &IsolationLevel,
        constraint_based: &ConstraintBasedTimestampSelection,
        read_holds: ReadHolds<Timestamp>,
        upper: Antichain<Timestamp>,
    ) -> Result<(TimestampDetermination<Timestamp>, ReadHolds<Timestamp>), AdapterError> {
        let timeline = Self::get_timeline(timeline_context);
        let largest_not_in_advance_of_upper = Coordinator::largest_not_in_advance_of_upper(&upper);
        let since = read_holds.least_valid_read();

        // If the `since` is empty, then timestamp determination would fail. Let's return a more
        // specific error in this case: Empty `since` frontiers happen here when collections were
        // dropped concurrently with sequencing the query.
        if since.is_empty() {
            // Figure out what made the since frontier empty.
            let mut unreadable_collections = Vec::new();
            for (coll_id, hold) in read_holds.storage_holds {
                if hold.since().is_empty() {
                    unreadable_collections.push(coll_id);
                }
            }
            for ((_instance_id, coll_id), hold) in read_holds.compute_holds {
                if hold.since().is_empty() {
                    unreadable_collections.push(coll_id);
                }
            }
            return Err(AdapterError::CollectionUnreadable {
                id: unreadable_collections.into_iter().join(", "),
            });
        }

        let raw_determination = match constraint_based {
            ConstraintBasedTimestampSelection::Disabled => Self::determine_timestamp_classical(
                session,
                &read_holds,
                id_bundle,
                when,
                oracle_read_ts,
                compute_instance,
                real_time_recency_ts,
                isolation_level,
                &timeline,
                largest_not_in_advance_of_upper,
                &since,
            )?,
            ConstraintBasedTimestampSelection::Enabled => {
                Self::determine_timestamp_via_constraints(
                    session,
                    &read_holds,
                    id_bundle,
                    when,
                    oracle_read_ts,
                    compute_instance,
                    real_time_recency_ts,
                    isolation_level,
                    &timeline,
                    largest_not_in_advance_of_upper,
                )?
            }
            ConstraintBasedTimestampSelection::Verify => {
                let classical_determination = Self::determine_timestamp_classical(
                    session,
                    &read_holds,
                    id_bundle,
                    when,
                    oracle_read_ts,
                    compute_instance,
                    real_time_recency_ts,
                    isolation_level,
                    &timeline,
                    largest_not_in_advance_of_upper,
                    &since,
                );

                let constraint_determination = Self::determine_timestamp_via_constraints(
                    session,
                    &read_holds,
                    id_bundle,
                    when,
                    oracle_read_ts,
                    compute_instance,
                    real_time_recency_ts,
                    isolation_level,
                    &timeline,
                    largest_not_in_advance_of_upper,
                );

                match (classical_determination, constraint_determination) {
                    (Ok(classical_determination), Ok(constraint_determination)) => {
                        soft_assert_eq_or_log!(
                            classical_determination.timestamp,
                            constraint_determination.timestamp,
                            "timestamp determination mismatch"
                        );
                        if classical_determination.timestamp != constraint_determination.timestamp {
                            tracing::info!(
                                "timestamp constrains: {:?}",
                                constraint_determination.constraints
                            );
                        }
                        RawTimestampDetermination {
                            timestamp: classical_determination.timestamp,
                            constraints: constraint_determination.constraints,
                            session_oracle_read_ts: classical_determination.session_oracle_read_ts,
                        }
                    }
                    (Err(classical_determination_err), Err(_constraint_determination_err)) => {
                        // This is ok: The errors don't have to exactly match.
                        return Err(classical_determination_err);
                    }
                    (Ok(classical_determination), Err(constraint_determination_err)) => {
                        event!(
                            Level::ERROR,
                            classical = ?classical_determination,
                            constraint_based = ?constraint_determination_err,
                            "classical timestamp determination succeeded, but constraint-based failed"
                        );
                        RawTimestampDetermination {
                            timestamp: classical_determination.timestamp,
                            constraints: classical_determination.constraints,
                            session_oracle_read_ts: classical_determination.session_oracle_read_ts,
                        }
                    }
                    (Err(classical_determination_err), Ok(constraint_determination)) => {
                        event!(
                            Level::ERROR,
                            classical = ?classical_determination_err,
                            constraint_based = ?constraint_determination,
                            "classical timestamp determination failed, but constraint-based succeeded"
                        );
                        return Err(classical_determination_err);
                    }
                }
            }
        };

        let timestamp_context = TimestampContext::from_timeline_context(
            raw_determination.timestamp,
            oracle_read_ts,
            timeline,
            timeline_context,
        );

        let determination = TimestampDetermination {
            timestamp_context,
            since,
            upper,
            largest_not_in_advance_of_upper,
            oracle_read_ts,
            session_oracle_read_ts: raw_determination.session_oracle_read_ts,
            real_time_recency_ts,
            constraints: raw_determination.constraints,
        };

        Ok((determination, read_holds))
    }

    /// Acquires [ReadHolds], for the given `id_bundle` at the earliest possible
    /// times.
    fn acquire_read_holds(&self, id_bundle: &CollectionIdBundle) -> ReadHolds<mz_repr::Timestamp>;

    /// The smallest common valid write frontier among the specified collections.
    ///
    /// Times that are not greater or equal to this frontier are complete for all collections
    /// identified as arguments.
    fn least_valid_write(&self, id_bundle: &CollectionIdBundle) -> Antichain<mz_repr::Timestamp> {
        let mut upper = Antichain::new();
        {
            for (_id, _since, collection_upper) in
                self.storage_frontiers(id_bundle.storage_ids.iter().cloned().collect_vec())
            {
                upper.extend(collection_upper);
            }
        }
        {
            for (instance, compute_ids) in &id_bundle.compute_ids {
                for id in compute_ids.iter() {
                    upper.extend(self.compute_write_frontier(*instance, *id).into_iter());
                }
            }
        }
        upper
    }

    /// Returns `least_valid_write` - 1, i.e., each time in `least_valid_write` stepped back in a
    /// saturating way.
    fn greatest_available_read(&self, id_bundle: &CollectionIdBundle) -> Antichain<Timestamp> {
        let mut frontier = Antichain::new();
        for t in self.least_valid_write(id_bundle) {
            frontier.insert(t.step_back().unwrap_or(t));
        }
        frontier
    }
}

fn generate_timestamp_not_valid_error_msg(
    id_bundle: &CollectionIdBundle,
    compute_instance: ComputeInstanceId,
    read_holds: &ReadHolds<mz_repr::Timestamp>,
    candidate: mz_repr::Timestamp,
) -> String {
    let mut invalid = Vec::new();

    if let Some(compute_ids) = id_bundle.compute_ids.get(&compute_instance) {
        for id in compute_ids {
            let since = read_holds.since(id);
            if !since.less_equal(&candidate) {
                invalid.push((*id, since));
            }
        }
    }

    for id in id_bundle.storage_ids.iter() {
        let since = read_holds.since(id);
        if !since.less_equal(&candidate) {
            invalid.push((*id, since));
        }
    }

    format!(
        "Timestamp ({}) is not valid for all inputs: {:?}",
        candidate, invalid,
    )
}

impl Coordinator {
    pub(crate) async fn oracle_read_ts(
        &self,
        session: &Session,
        timeline_ctx: &TimelineContext,
        when: &QueryWhen,
    ) -> Option<Timestamp> {
        let isolation_level = session.vars().transaction_isolation().clone();
        let timeline = Coordinator::get_timeline(timeline_ctx);
        let needs_linearized_read_ts =
            Coordinator::needs_linearized_read_ts(&isolation_level, when);

        let oracle_read_ts = match timeline {
            Some(timeline) if needs_linearized_read_ts => {
                let timestamp_oracle = self.get_timestamp_oracle(&timeline);
                Some(timestamp_oracle.read_ts().await)
            }
            Some(_) | None => None,
        };

        oracle_read_ts
    }

    /// Determines the timestamp for a query, acquires read holds that ensure the
    /// query remains executable at that time, and returns those.
    /// The caller is responsible for eventually dropping those read holds.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) fn determine_timestamp(
        &self,
        session: &Session,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        compute_instance: ComputeInstanceId,
        timeline_context: &TimelineContext,
        oracle_read_ts: Option<Timestamp>,
        real_time_recency_ts: Option<mz_repr::Timestamp>,
    ) -> Result<
        (
            TimestampDetermination<mz_repr::Timestamp>,
            ReadHolds<mz_repr::Timestamp>,
        ),
        AdapterError,
    > {
        let constraint_based = ConstraintBasedTimestampSelection::from_str(
            &CONSTRAINT_BASED_TIMESTAMP_SELECTION
                .get(self.catalog_state().system_config().dyncfgs()),
        );

        let isolation_level = session.vars().transaction_isolation();
        let (det, read_holds) = self.determine_timestamp_for(
            session,
            id_bundle,
            when,
            compute_instance,
            timeline_context,
            oracle_read_ts,
            real_time_recency_ts,
            isolation_level,
            &constraint_based,
        )?;
        self.metrics
            .determine_timestamp
            .with_label_values(&[
                match det.respond_immediately() {
                    true => "true",
                    false => "false",
                },
                isolation_level.as_str(),
                &compute_instance.to_string(),
                constraint_based.as_str(),
            ])
            .inc();
        if !det.respond_immediately()
            && isolation_level == &IsolationLevel::StrictSerializable
            && real_time_recency_ts.is_none()
        {
            // Note down the difference between StrictSerializable and Serializable into a metric.
            if let Some(strict) = det.timestamp_context.timestamp() {
                let (serializable_det, _tmp_read_holds) = self.determine_timestamp_for(
                    session,
                    id_bundle,
                    when,
                    compute_instance,
                    timeline_context,
                    oracle_read_ts,
                    real_time_recency_ts,
                    &IsolationLevel::Serializable,
                    &constraint_based,
                )?;

                if let Some(serializable) = serializable_det.timestamp_context.timestamp() {
                    self.metrics
                        .timestamp_difference_for_strict_serializable_ms
                        .with_label_values(&[
                            compute_instance.to_string().as_ref(),
                            constraint_based.as_str(),
                        ])
                        .observe(f64::cast_lossy(u64::from(
                            strict.saturating_sub(*serializable),
                        )));
                }
            }
        }
        Ok((det, read_holds))
    }

    /// The largest timestamp not greater or equal to an element of `upper`.
    ///
    /// If no such timestamp exists, for example because `upper` contains only the
    /// minimal timestamp, the return value is `Timestamp::minimum()`.
    pub(crate) fn largest_not_in_advance_of_upper(
        upper: &Antichain<mz_repr::Timestamp>,
    ) -> mz_repr::Timestamp {
        // We peek at the largest element not in advance of `upper`, which
        // involves a subtraction. If `upper` contains a zero timestamp there
        // is no "prior" answer, and we do not want to peek at it as it risks
        // hanging awaiting the response to data that may never arrive.
        if let Some(upper) = upper.as_option() {
            upper.step_back().unwrap_or_else(Timestamp::minimum)
        } else {
            // A complete trace can be read in its final form with this time.
            //
            // This should only happen for literals that have no sources or sources that
            // are known to have completed (non-tailed files for example).
            Timestamp::MAX
        }
    }
}

/// Information used when determining the timestamp for a query.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TimestampDetermination<T> {
    /// The chosen timestamp context from `determine_timestamp`.
    pub timestamp_context: TimestampContext<T>,
    /// The read frontier of all involved sources.
    pub since: Antichain<T>,
    /// The write frontier of all involved sources.
    pub upper: Antichain<T>,
    /// The largest timestamp not in advance of upper.
    pub largest_not_in_advance_of_upper: T,
    /// The value of the timeline's oracle timestamp, if used.
    pub oracle_read_ts: Option<T>,
    /// The value of the session local timestamp's oracle timestamp, if used.
    pub session_oracle_read_ts: Option<T>,
    /// The value of the real time recency timestamp, if used.
    pub real_time_recency_ts: Option<T>,
    /// The constraints used by the constraint based solver.
    /// See the [`constraints`] module for more information.
    pub constraints: Option<Constraints>,
}

impl<T: TimestampManipulation> TimestampDetermination<T> {
    pub fn respond_immediately(&self) -> bool {
        match &self.timestamp_context {
            TimestampContext::TimelineTimestamp { chosen_ts, .. } => {
                !self.upper.less_equal(chosen_ts)
            }
            TimestampContext::NoTimestamp => true,
        }
    }
}

/// Information used when determining the timestamp for a query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimestampExplanation<T> {
    /// The chosen timestamp from `determine_timestamp`.
    pub determination: TimestampDetermination<T>,
    /// Details about each source.
    pub sources: Vec<TimestampSource<T>>,
    /// Wall time of first statement executed in this transaction
    pub session_wall_time: DateTime<Utc>,
    /// Cached value of determination.respond_immediately()
    pub respond_immediately: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimestampSource<T> {
    pub name: String,
    pub read_frontier: Vec<T>,
    pub write_frontier: Vec<T>,
}

pub trait DisplayableInTimeline {
    fn fmt(&self, timeline: Option<&Timeline>, f: &mut fmt::Formatter) -> fmt::Result;
    fn display<'a>(&'a self, timeline: Option<&'a Timeline>) -> DisplayInTimeline<'a, Self> {
        DisplayInTimeline { t: self, timeline }
    }
}

impl DisplayableInTimeline for mz_repr::Timestamp {
    fn fmt(&self, timeline: Option<&Timeline>, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(Timeline::EpochMilliseconds) = timeline {
            let ts_ms: u64 = self.into();
            if let Ok(ts_ms) = i64::try_from(ts_ms) {
                if let Some(ndt) = DateTime::from_timestamp_millis(ts_ms) {
                    return write!(f, "{:13} ({})", self, ndt.format("%Y-%m-%d %H:%M:%S%.3f"));
                }
            }
        }
        write!(f, "{:13}", self)
    }
}

pub struct DisplayInTimeline<'a, T: ?Sized> {
    t: &'a T,
    timeline: Option<&'a Timeline>,
}
impl<'a, T> fmt::Display for DisplayInTimeline<'a, T>
where
    T: DisplayableInTimeline,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.t.fmt(self.timeline, f)
    }
}

impl<'a, T> fmt::Debug for DisplayInTimeline<'a, T>
where
    T: DisplayableInTimeline,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self, f)
    }
}

impl<T: fmt::Display + fmt::Debug + DisplayableInTimeline + TimestampManipulation> fmt::Display
    for TimestampExplanation<T>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let timeline = self.determination.timestamp_context.timeline();
        writeln!(
            f,
            "                query timestamp: {}",
            self.determination
                .timestamp_context
                .timestamp_or_default()
                .display(timeline)
        )?;
        if let Some(oracle_read_ts) = &self.determination.oracle_read_ts {
            writeln!(
                f,
                "          oracle read timestamp: {}",
                oracle_read_ts.display(timeline)
            )?;
        }
        if let Some(session_oracle_read_ts) = &self.determination.session_oracle_read_ts {
            writeln!(
                f,
                "  session oracle read timestamp: {}",
                session_oracle_read_ts.display(timeline)
            )?;
        }
        if let Some(real_time_recency_ts) = &self.determination.real_time_recency_ts {
            writeln!(
                f,
                "    real time recency timestamp: {}",
                real_time_recency_ts.display(timeline)
            )?;
        }
        writeln!(
            f,
            "largest not in advance of upper: {}",
            self.determination
                .largest_not_in_advance_of_upper
                .display(timeline),
        )?;
        writeln!(
            f,
            "                          upper:{:?}",
            self.determination
                .upper
                .iter()
                .map(|t| t.display(timeline))
                .collect::<Vec<_>>()
        )?;
        writeln!(
            f,
            "                          since:{:?}",
            self.determination
                .since
                .iter()
                .map(|t| t.display(timeline))
                .collect::<Vec<_>>()
        )?;
        writeln!(
            f,
            "        can respond immediately: {}",
            self.respond_immediately
        )?;
        writeln!(f, "                       timeline: {:?}", &timeline)?;
        writeln!(
            f,
            "              session wall time: {:13} ({})",
            self.session_wall_time.timestamp_millis(),
            self.session_wall_time.format("%Y-%m-%d %H:%M:%S%.3f"),
        )?;

        for source in &self.sources {
            writeln!(f, "")?;
            writeln!(f, "source {}:", source.name)?;
            writeln!(
                f,
                "                  read frontier:{:?}",
                source
                    .read_frontier
                    .iter()
                    .map(|t| t.display(timeline))
                    .collect::<Vec<_>>()
            )?;
            writeln!(
                f,
                "                 write frontier:{:?}",
                source
                    .write_frontier
                    .iter()
                    .map(|t| t.display(timeline))
                    .collect::<Vec<_>>()
            )?;
        }

        if let Some(constraints) = &self.determination.constraints {
            writeln!(f, "")?;
            writeln!(f, "binding constraints:")?;
            write!(f, "{}", constraints.display(timeline))?;
        }

        Ok(())
    }
}

/// Types and logic in support of a constraint-based approach to timestamp determination.
mod constraints {

    use core::fmt;
    use std::fmt::Debug;

    use differential_dataflow::lattice::Lattice;
    use mz_storage_types::sources::Timeline;
    use serde::{Deserialize, Serialize};
    use timely::progress::{Antichain, Timestamp};

    use mz_compute_types::ComputeInstanceId;
    use mz_repr::GlobalId;
    use mz_sql::session::vars::IsolationLevel;

    use super::DisplayableInTimeline;

    /// Constraints expressed on the timestamp of a query.
    ///
    /// The constraints are expressed on the minimum and maximum values,
    /// resulting in a (possibly empty) interval of valid timestamps.
    ///
    /// The constraints may be redundant, in the interest of providing
    /// more complete explanations, but they may also be minimized at
    /// any point without altering their behavior by removing redundant
    /// constraints.
    ///
    /// When combined with a `Preference` one can determine an
    /// ideal timestamp to use.
    #[derive(Default, Serialize, Deserialize, Clone)]
    pub struct Constraints {
        /// Timestamps and reasons that impose an inclusive lower bound.
        pub lower: Vec<(Antichain<mz_repr::Timestamp>, Reason)>,
        /// Timestamps and reasons that impose an inclusive upper bound.
        pub upper: Vec<(Antichain<mz_repr::Timestamp>, Reason)>,
    }

    impl DisplayableInTimeline for Constraints {
        fn fmt(&self, timeline: Option<&Timeline>, f: &mut fmt::Formatter) -> fmt::Result {
            if !self.lower.is_empty() {
                writeln!(f, "lower:")?;
                for (ts, reason) in &self.lower {
                    let ts = ts.iter().map(|t| t.display(timeline)).collect::<Vec<_>>();
                    writeln!(f, "  ({:?}): {:?}", reason, ts)?;
                }
            }
            if !self.upper.is_empty() {
                writeln!(f, "upper:")?;
                for (ts, reason) in &self.upper {
                    let ts = ts.iter().map(|t| t.display(timeline)).collect::<Vec<_>>();
                    writeln!(f, "  ({:?}): {:?}", reason, ts)?;
                }
            }
            Ok(())
        }
    }

    impl Debug for Constraints {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            self.display(None).fmt(f)?;
            Ok(())
        }
    }

    impl Constraints {
        /// Remove constraints that are dominated by other constraints.
        ///
        /// This removes redundant constraints, without removing constraints
        /// that are "tight" in the sense that the interval would be
        /// meaningfully different without them.
        /// For example, two constraints at the same
        /// time will both be retained, in the interest of full information.
        /// But a lower bound constraint at time `t` will be removed if there is a
        /// constraint at time `t + 1` (or any larger time).
        pub fn minimize(&mut self) {
            // Establish the upper bound of lower constraints.
            let lower_frontier = self.lower_bound();
            // Retain constraints that intersect `lower_frontier`.
            self.lower.retain(|(anti, _)| {
                anti.iter()
                    .any(|time| lower_frontier.elements().contains(time))
            });

            // Establish the lower bound of upper constraints.
            let upper_frontier = self.upper_bound();
            // Retain constraints that intersect `upper_frontier`.
            self.upper.retain(|(anti, _)| {
                anti.iter()
                    .any(|time| upper_frontier.elements().contains(time))
            });
        }

        /// An antichain equal to the least upper bound of lower bounds.
        pub fn lower_bound(&self) -> Antichain<mz_repr::Timestamp> {
            let mut lower = Antichain::from_elem(mz_repr::Timestamp::minimum());
            for (anti, _) in self.lower.iter() {
                lower = lower.join(anti);
            }
            lower
        }
        /// An antichain equal to the greatest lower bound of upper bounds.
        pub fn upper_bound(&self) -> Antichain<mz_repr::Timestamp> {
            self.upper
                .iter()
                .flat_map(|(anti, _)| anti.iter())
                .cloned()
                .collect()
        }
    }

    /// An explanation of reasons for a timestamp constraint.
    #[derive(Serialize, Deserialize, Clone)]
    pub enum Reason {
        /// A compute input at a compute instance.
        /// This is something like an index or view
        /// that is mantained by compute.
        ComputeInput(Vec<(ComputeInstanceId, GlobalId)>),
        /// A storage input.
        StorageInput(Vec<GlobalId>),
        /// A specified isolation level and the timestamp it requires.
        IsolationLevel(IsolationLevel),
        /// Real-time recency may constrains the timestamp from below.
        RealTimeRecency,
        /// The query expressed its own constraint on the timestamp.
        QueryAsOf,
    }

    impl Debug for Reason {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match self {
                Reason::ComputeInput(ids) => write_split_ids(f, "ComputeInput", ids),
                Reason::StorageInput(ids) => write_split_ids(f, "StorageInput", ids),
                Reason::IsolationLevel(level) => {
                    write!(f, "IsolationLevel({:?})", level)
                }
                Reason::RealTimeRecency => {
                    write!(f, "RealTimeRecency")
                }
                Reason::QueryAsOf => {
                    write!(f, "QueryAsOf")
                }
            }
        }
    }

    //TODO: This is a bit of a hack to make the debug output of constraints more readable.
    //We should probably have a more structured way to do this.
    fn write_split_ids<T: Debug>(f: &mut fmt::Formatter, label: &str, ids: &[T]) -> fmt::Result {
        let (ids, rest) = if ids.len() > 10 {
            ids.split_at(10)
        } else {
            let rest: &[T] = &[];
            (ids, rest)
        };
        if rest.is_empty() {
            write!(f, "{}({:?})", label, ids)
        } else {
            write!(f, "{}({:?}, ... {} more)", label, ids, rest.len())
        }
    }

    /// Given an interval [read, write) of timestamp options,
    /// this expresses a preference for either end of the spectrum.
    pub enum Preference {
        /// Prefer the greatest timestamp immediately available.
        ///
        /// This considers the immediate inputs to a query and
        /// selects the greatest timestamp not greater or equal
        /// to any of their write frontiers.
        ///
        /// The preference only relates to immediate query inputs,
        /// but it could be extended to transitive inputs as well.
        /// For example, one could imagine prefering the freshest
        /// data known to be ingested into Materialize, under the
        /// premise that those answers should soon become available,
        /// and may be more fresh than the immediate inputs.
        FreshestAvailable,
        /// Prefer the least valid timeastamp.
        ///
        /// This is useful when one has no expressed freshness
        /// constraints, and wants to minimally impact others.
        /// For example, `AS OF AT LEAST <time>`.
        StalestValid,
    }
}
