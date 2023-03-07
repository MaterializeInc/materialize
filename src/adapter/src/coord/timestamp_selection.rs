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

use chrono::NaiveDateTime;
use differential_dataflow::lattice::Lattice;
use mz_sql::vars::IsolationLevel;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::{event, Level};

use mz_compute_client::controller::ComputeInstanceId;
use mz_expr::MirScalarExpr;
use mz_repr::explain::ExprHumanizer;
use mz_repr::{RowArena, ScalarType, Timestamp, TimestampManipulation};
use mz_sql::plan::QueryWhen;
use mz_storage_client::types::sources::Timeline;

use crate::coord::dataflows::{prep_scalar_expr, ExprPrepStyle};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::timeline::TimelineContext;
use crate::coord::Coordinator;
use crate::session::Session;
use crate::AdapterError;

/// The timeline and timestamp context of a read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimestampContext<T> {
    /// Read is executed in a specific timeline with a specific timestamp.
    TimelineTimestamp(Timeline, T),
    /// Read is execute without a timeline or timestamp.
    NoTimestamp,
}

impl<T: TimestampManipulation> TimestampContext<T> {
    /// Creates a `TimestampContext` from a timestamp and `TimelineContext`.
    pub fn from_timeline_context(
        ts: T,
        transaction_timeline: Option<Timeline>,
        timeline_context: TimelineContext,
    ) -> TimestampContext<T> {
        match timeline_context {
            TimelineContext::TimelineDependent(timeline) => {
                if let Some(transaction_timeline) = transaction_timeline {
                    assert_eq!(timeline, transaction_timeline);
                }
                Self::TimelineTimestamp(timeline, ts)
            }
            TimelineContext::TimestampDependent => {
                // We default to the `Timeline::EpochMilliseconds` timeline if one doesn't exist.
                Self::TimelineTimestamp(
                    transaction_timeline.unwrap_or(Timeline::EpochMilliseconds),
                    ts,
                )
            }
            TimelineContext::TimestampIndependent => Self::NoTimestamp,
        }
    }

    /// The timeline belonging to this context, if one exists.
    pub fn timeline(&self) -> Option<&Timeline> {
        match self {
            Self::TimelineTimestamp(timeline, _) => Some(timeline),
            Self::NoTimestamp => None,
        }
    }

    /// The timestamp belonging to this context, if one exists.
    pub fn timestamp(&self) -> Option<&T> {
        match self {
            Self::TimelineTimestamp(_, ts) => Some(ts),
            Self::NoTimestamp => None,
        }
    }

    /// The timestamp belonging to this context, or a sensible default if one does not exists.
    pub fn timestamp_or_default(&self) -> T {
        match self {
            Self::TimelineTimestamp(_, ts) => ts.clone(),
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

impl Coordinator {
    /// Determines the timestamp for a query.
    ///
    /// Timestamp determination may fail due to the restricted validity of
    /// traces. Each has a `since` and `upper` frontier, and are only valid
    /// after `since` and sure to be available not after `upper`.
    ///
    /// The timeline that `id_bundle` belongs to is also returned, if one exists.
    pub(crate) fn determine_timestamp(
        &self,
        session: &Session,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        compute_instance: ComputeInstanceId,
        timeline_context: TimelineContext,
        real_time_recency_ts: Option<mz_repr::Timestamp>,
    ) -> Result<TimestampDetermination<mz_repr::Timestamp>, AdapterError> {
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

        let since = self.least_valid_read(id_bundle);

        // Initialize candidate to the minimum correct time.
        let mut candidate = Timestamp::minimum();

        if let Some(timestamp) = when.advance_to_timestamp() {
            let ts = self.evaluate_when(timestamp, session)?;
            candidate.join_assign(&ts);
        }

        let isolation_level = session.vars().transaction_isolation();
        let timeline = match &timeline_context {
            TimelineContext::TimelineDependent(timeline) => Some(timeline.clone()),
            // We default to the `Timeline::EpochMilliseconds` timeline if one doesn't exist.
            TimelineContext::TimestampDependent => Some(Timeline::EpochMilliseconds),
            TimelineContext::TimestampIndependent => None,
        };

        let upper = self.least_valid_write(id_bundle);
        let largest_not_in_advance_of_upper = self.largest_not_in_advance_of_upper(&upper);
        let mut oracle_read_ts = None;

        if when.advance_to_since() {
            candidate.advance_by(since.borrow());
        }

        // In order to use a timestamp oracle, we must be in the context of some timeline. In that
        // context we would use the timestamp oracle in the following scenarios:
        // - The isolation level is Strict Serializable and the `when` allows us to use the
        //   the timestamp oracle (ex: queries with no AS OF).
        // - The `when` requires us to use the timestamp oracle (ex: read-then-write queries).
        if let Some(timeline) = &timeline {
            if when.must_advance_to_timeline_ts()
                || (when.can_advance_to_timeline_ts()
                    && isolation_level == &IsolationLevel::StrictSerializable)
            {
                let timestamp_oracle = self.get_timestamp_oracle(timeline);
                oracle_read_ts = Some(timestamp_oracle.read_ts());
                candidate.join_assign(&oracle_read_ts.expect("known to be `Some`"));
            }
        }

        // We advance to the upper in the following scenarios:
        // - The isolation level is Serializable and the `when` allows us to advance to upper (ex:
        //   queries with no AS OF). We avoid using the upper in Strict Serializable to prevent
        //   reading source data that is being written to in the future.
        // - The isolation level is Strict Serializable but there is no timelines and the `when`
        //   allows us to advance to upper.
        // - The `when` requires us to advance to the upper (ex: read-then-write queries).
        if when.must_advance_to_upper()
            || (when.can_advance_to_upper()
                && (isolation_level == &IsolationLevel::Serializable || timeline.is_none()))
        {
            candidate.join_assign(&largest_not_in_advance_of_upper);
        }

        if let Some(real_time_recency_ts) = real_time_recency_ts {
            assert!(
                session.vars().real_time_recency()
                    && isolation_level == &IsolationLevel::StrictSerializable,
                "real time recency timestamp should only be supplied when real time recency \
                    is enabled and the isolation level is strict serializable"
            );
            candidate.join_assign(&real_time_recency_ts);
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
            coord_bail!(self.generate_timestamp_not_valid_error_msg(
                id_bundle,
                compute_instance,
                candidate
            ));
        };

        let timestamp_context =
            TimestampContext::from_timeline_context(timestamp, timeline, timeline_context);
        let det = TimestampDetermination {
            timestamp_context,
            since,
            upper,
            largest_not_in_advance_of_upper,
            oracle_read_ts,
        };
        self.metrics
            .determine_timestamp
            .with_label_values(&[
                match det.respond_immediately() {
                    true => "true",
                    false => "false",
                },
                isolation_level.as_str(),
                &compute_instance.to_string(),
            ])
            .inc();
        Ok(det)
    }

    /// The smallest common valid read frontier among the specified collections.
    pub(crate) fn least_valid_read(
        &self,
        id_bundle: &CollectionIdBundle,
    ) -> Antichain<mz_repr::Timestamp> {
        let mut since = Antichain::from_elem(Timestamp::minimum());
        {
            let storage = &self.controller.storage;
            for id in id_bundle.storage_ids.iter() {
                since.join_assign(
                    &storage
                        .collection(*id)
                        .expect("id does not exist")
                        .implied_capability,
                )
            }
        }
        {
            for (instance, compute_ids) in &id_bundle.compute_ids {
                for id in compute_ids.iter() {
                    let collection = self
                        .controller
                        .compute
                        .collection(*instance, *id)
                        .expect("id does not exist");
                    since.join_assign(collection.read_capability())
                }
            }
        }
        since
    }

    /// The smallest common valid write frontier among the specified collections.
    ///
    /// Times that are not greater or equal to this frontier are complete for all collections
    /// identified as arguments.
    pub(crate) fn least_valid_write(
        &self,
        id_bundle: &CollectionIdBundle,
    ) -> Antichain<mz_repr::Timestamp> {
        let mut since = Antichain::new();
        {
            for id in id_bundle.storage_ids.iter() {
                since.extend(
                    self.controller
                        .storage
                        .collection(*id)
                        .expect("id does not exist")
                        .write_frontier
                        .iter()
                        .cloned(),
                );
            }
        }
        {
            for (instance, compute_ids) in &id_bundle.compute_ids {
                for id in compute_ids.iter() {
                    let collection = self
                        .controller
                        .compute
                        .collection(*instance, *id)
                        .expect("id does not exist");
                    since.extend(collection.write_frontier().iter().cloned());
                }
            }
        }
        since
    }

    /// The largest element not in advance of any object in the collection.
    ///
    /// Times that are not greater to this frontier are complete for all collections
    /// identified as arguments.
    pub(crate) fn largest_not_in_advance_of_upper(
        &self,
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

    pub(crate) fn evaluate_when(
        &self,
        mut timestamp: MirScalarExpr,
        session: &Session,
    ) -> Result<mz_repr::Timestamp, AdapterError> {
        let temp_storage = RowArena::new();
        prep_scalar_expr(
            self.catalog.state(),
            &mut timestamp,
            ExprPrepStyle::AsOfUpTo,
        )?;
        let evaled = timestamp.eval(&[], &temp_storage)?;
        if evaled.is_null() {
            coord_bail!("can't use {} as a mz_timestamp for AS OF or UP TO", evaled);
        }
        let ty = timestamp.typ(&[]);
        Ok(match ty.scalar_type {
            ScalarType::MzTimestamp => evaled.unwrap_mz_timestamp(),
            ScalarType::Numeric { .. } => {
                let n = evaled.unwrap_numeric().0;
                n.try_into()?
            }
            ScalarType::Int16 => i64::from(evaled.unwrap_int16()).try_into()?,
            ScalarType::Int32 => i64::from(evaled.unwrap_int32()).try_into()?,
            ScalarType::Int64 => evaled.unwrap_int64().try_into()?,
            ScalarType::UInt16 => u64::from(evaled.unwrap_uint16()).into(),
            ScalarType::UInt32 => u64::from(evaled.unwrap_uint32()).into(),
            ScalarType::UInt64 => evaled.unwrap_uint64().into(),
            ScalarType::TimestampTz => evaled.unwrap_timestamptz().timestamp_millis().try_into()?,
            ScalarType::Timestamp => evaled.unwrap_timestamp().timestamp_millis().try_into()?,
            _ => coord_bail!(
                "can't use {} as a mz_timestamp for AS OF or UP TO",
                self.catalog.for_session(session).humanize_column_type(&ty)
            ),
        })
    }

    fn generate_timestamp_not_valid_error_msg(
        &self,
        id_bundle: &CollectionIdBundle,
        compute_instance: ComputeInstanceId,
        candidate: mz_repr::Timestamp,
    ) -> String {
        let invalid_indexes =
            if let Some(compute_ids) = id_bundle.compute_ids.get(&compute_instance) {
                compute_ids
                    .iter()
                    .filter_map(|id| {
                        let since = self
                            .controller
                            .compute
                            .collection(compute_instance, *id)
                            .expect("id does not exist")
                            .read_frontier()
                            .to_owned();
                        if since.less_equal(&candidate) {
                            None
                        } else {
                            Some(since)
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };
        let invalid_sources = id_bundle.storage_ids.iter().filter_map(|id| {
            let since = self
                .controller
                .storage
                .collection(*id)
                .expect("id does not exist")
                .read_capabilities
                .frontier()
                .to_owned();
            if since.less_equal(&candidate) {
                None
            } else {
                Some(since)
            }
        });
        let invalid = invalid_indexes
            .into_iter()
            .chain(invalid_sources)
            .collect::<Vec<_>>();
        format!(
            "Timestamp ({}) is not valid for all inputs: {:?}",
            candidate, invalid,
        )
    }
}

/// Information used when determining the timestamp for a query.
#[derive(Serialize, Deserialize)]
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
}

impl<T: TimestampManipulation> TimestampDetermination<T> {
    pub fn respond_immediately(&self) -> bool {
        match &self.timestamp_context {
            TimestampContext::TimelineTimestamp(_, timestamp) => !self.upper.less_equal(timestamp),
            TimestampContext::NoTimestamp => true,
        }
    }
}

/// Information used when determining the timestamp for a query.
#[derive(Serialize, Deserialize)]
pub struct TimestampExplanation<T> {
    /// The chosen timestamp from `determine_timestamp`.
    pub determination: TimestampDetermination<T>,
    /// Details about each source.
    pub sources: Vec<TimestampSource<T>>,
}

#[derive(Serialize, Deserialize)]
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
                if let Some(ndt) = NaiveDateTime::from_timestamp_millis(ts_ms) {
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
            self.determination.respond_immediately()
        )?;
        writeln!(f, "                       timeline: {:?}", &timeline)?;

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
        Ok(())
    }
}
