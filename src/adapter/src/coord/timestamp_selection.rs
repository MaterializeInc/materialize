// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for selecting timestamps for various operations on collections.

use differential_dataflow::lattice::Lattice;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};

use mz_compute_client::controller::ComputeInstanceId;
use mz_repr::explain_new::ExprHumanizer;
use mz_repr::{RowArena, ScalarType, Timestamp};
use mz_sql::plan::QueryWhen;
use mz_stash::Append;

use crate::coord::dataflows::{prep_scalar_expr, ExprPrepStyle};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::{CoordTimestamp, Coordinator};
use crate::session::{vars, Session};
use crate::AdapterError;

impl<S: Append + 'static> Coordinator<S> {
    /// Determines the timestamp for a query.
    ///
    /// Timestamp determination may fail due to the restricted validity of
    /// traces. Each has a `since` and `upper` frontier, and are only valid
    /// after `since` and sure to be available not after `upper`.
    ///
    /// The set of storage and compute IDs used when determining the timestamp
    /// are also returned.
    pub(crate) fn determine_timestamp(
        &mut self,
        session: &Session,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        compute_instance: ComputeInstanceId,
    ) -> Result<Timestamp, AdapterError> {
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

        let since = self.least_valid_read(&id_bundle);

        // Initialize candidate to the minimum correct time.
        let mut candidate = Timestamp::minimum();

        if let Some(mut timestamp) = when.advance_to_timestamp() {
            let temp_storage = RowArena::new();
            prep_scalar_expr(self.catalog.state(), &mut timestamp, ExprPrepStyle::AsOf)?;
            let evaled = timestamp.eval(&[], &temp_storage)?;
            if evaled.is_null() {
                coord_bail!("can't use {} as a timestamp for AS OF", evaled);
            }
            let ty = timestamp.typ(&[]);
            let ts = match ty.scalar_type {
                ScalarType::Numeric { .. } => {
                    let n = evaled.unwrap_numeric().0;
                    u64::try_from(n)?
                }
                ScalarType::Int16 => evaled.unwrap_int16().try_into()?,
                ScalarType::Int32 => evaled.unwrap_int32().try_into()?,
                ScalarType::Int64 => evaled.unwrap_int64().try_into()?,
                ScalarType::UInt16 => evaled.unwrap_uint16().into(),
                ScalarType::UInt32 => evaled.unwrap_uint32().into(),
                ScalarType::UInt64 => evaled.unwrap_uint64(),
                ScalarType::TimestampTz => {
                    evaled.unwrap_timestamptz().timestamp_millis().try_into()?
                }
                ScalarType::Timestamp => evaled.unwrap_timestamp().timestamp_millis().try_into()?,
                _ => coord_bail!(
                    "can't use {} as a timestamp for AS OF",
                    self.catalog.for_session(session).humanize_column_type(&ty)
                ),
            };
            candidate.join_assign(&ts);
        }

        let isolation_level = session.vars().transaction_isolation();
        let timeline = self.validate_timeline(id_bundle.iter())?;
        let use_timestamp_oracle = isolation_level == &vars::IsolationLevel::StrictSerializable
            && timeline.is_some()
            && when.advance_to_global_ts();

        if use_timestamp_oracle {
            let timeline = timeline.expect("checked that timeline exists above");
            let timestamp_oracle = self.get_timestamp_oracle_mut(&timeline);
            candidate.join_assign(&timestamp_oracle.read_ts());
        } else {
            if when.advance_to_since() {
                candidate.advance_by(since.borrow());
            }
            if when.advance_to_upper() {
                let upper = self.largest_not_in_advance_of_upper(&id_bundle);
                candidate.join_assign(&upper);
            }
        }

        if use_timestamp_oracle && when == &QueryWhen::Immediately {
            assert!(
                since.less_equal(&candidate),
                "the strict serializable isolation level guarantees that the timestamp chosen \
                ({candidate}) is greater than or equal to since ({:?}) via read holds",
                since
            )
        }

        // If the timestamp is greater or equal to some element in `since` we are
        // assured that the answer will be correct.
        if since.less_equal(&candidate) {
            Ok(candidate)
        } else {
            let invalid_indexes =
                if let Some(compute_ids) = id_bundle.compute_ids.get(&compute_instance) {
                    compute_ids
                        .iter()
                        .filter_map(|id| {
                            let since = self
                                .controller
                                .compute(compute_instance)
                                .unwrap()
                                .collection(*id)
                                .unwrap()
                                .read_capabilities
                                .frontier()
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
                    .storage()
                    .collection(*id)
                    .unwrap()
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
            coord_bail!(
                "Timestamp ({}) is not valid for all inputs: {:?}",
                candidate,
                invalid
            );
        }
    }

    /// The smallest common valid read frontier among the specified collections.
    pub(crate) fn least_valid_read(
        &self,
        id_bundle: &CollectionIdBundle,
    ) -> Antichain<mz_repr::Timestamp> {
        let mut since = Antichain::from_elem(Timestamp::minimum());
        {
            let storage = self.controller.storage();
            for id in id_bundle.storage_ids.iter() {
                since.join_assign(&storage.collection(*id).unwrap().implied_capability)
            }
        }
        {
            for (instance, compute_ids) in &id_bundle.compute_ids {
                let compute = self.controller.compute(*instance).unwrap();
                for id in compute_ids.iter() {
                    since.join_assign(&compute.collection(*id).unwrap().implied_capability)
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
            let storage = self.controller.storage();
            for id in id_bundle.storage_ids.iter() {
                since.extend(
                    storage
                        .collection(*id)
                        .unwrap()
                        .write_frontier
                        .frontier()
                        .iter()
                        .cloned(),
                );
            }
        }
        {
            for (instance, compute_ids) in &id_bundle.compute_ids {
                let compute = self.controller.compute(*instance).unwrap();
                for id in compute_ids.iter() {
                    since.extend(
                        compute
                            .collection(*id)
                            .unwrap()
                            .write_frontier
                            .frontier()
                            .iter()
                            .cloned(),
                    );
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
        id_bundle: &CollectionIdBundle,
    ) -> mz_repr::Timestamp {
        let upper = self.least_valid_write(&id_bundle);

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
