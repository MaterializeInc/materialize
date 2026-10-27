// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_controller_types::ClusterId;
use mz_ore::instrument;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::plan::QueryWhen;
use mz_sql::session::metadata::SessionMetadata;

use crate::coord::Coordinator;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::timeline::{TimelineContext, timedomain_for};
use crate::coord::timestamp_selection::{TimestampContext, TimestampDetermination};
use crate::error::AdapterError;
use crate::session::{RequireLinearization, Session, TransactionOps, TransactionStatus};

impl Coordinator {
    /// Determines the query timestamp and acquires read holds on dependent sources
    /// if necessary.
    #[instrument]
    pub(super) fn sequence_peek_timestamp(
        &mut self,
        session: &mut Session,
        when: &QueryWhen,
        cluster_id: ClusterId,
        timeline_context: TimelineContext,
        oracle_read_ts: Option<Timestamp>,
        source_bundle: &CollectionIdBundle,
        source_ids: &BTreeSet<GlobalId>,
        real_time_recency_ts: Option<Timestamp>,
        requires_linearization: RequireLinearization,
    ) -> Result<TimestampDetermination, AdapterError> {
        let in_immediate_multi_stmt_txn = session.transaction().in_immediate_multi_stmt_txn(when);
        let timedomain_bundle;

        // Fetch or generate a timestamp for this query and what the read holds would be if we need to set
        // them.
        let (determination, read_holds) = match session.get_transaction_timestamp_determination() {
            // Use the transaction's timestamp if it exists and this isn't an AS OF query.
            Some(
                determination @ TimestampDetermination {
                    timestamp_context: TimestampContext::TimelineTimestamp { .. },
                    ..
                },
            ) if in_immediate_multi_stmt_txn => (determination, None),
            _ => {
                let determine_bundle = if in_immediate_multi_stmt_txn {
                    // In a transaction, determine a timestamp that will be valid for anything in
                    // any schema referenced by the first query.
                    timedomain_bundle = timedomain_for(
                        self.catalog(),
                        &self.index_oracle(cluster_id),
                        source_ids,
                        &timeline_context,
                        session.conn_id(),
                        cluster_id,
                    )?;

                    &timedomain_bundle
                } else {
                    // If not in a transaction, use the source.
                    source_bundle
                };
                let (determination, read_holds) = self.determine_timestamp(
                    session,
                    determine_bundle,
                    when,
                    cluster_id,
                    &timeline_context,
                    oracle_read_ts,
                    real_time_recency_ts,
                )?;
                // We only need read holds if the read depends on a timestamp.
                let read_holds = match determination.timestamp_context.timestamp() {
                    Some(_ts) => Some(read_holds),
                    None => {
                        // We don't need the read holds and shouldn't add them
                        // to the txn.
                        //
                        // TODO: Handle this within determine_timestamp.
                        drop(read_holds);
                        None
                    }
                };
                (determination, read_holds)
            }
        };

        // Always either verify the current statement ids are within the existing
        // transaction's read hold set (timedomain), or create the read holds if this is the
        // first statement in a transaction (or this is a single statement transaction).
        // This must happen even if this is an `AS OF` query as well. There are steps after
        // this that happen off thread, so no matter the kind of statement or transaction,
        // we must acquire read holds here so they are held until the off-thread work
        // returns to the coordinator.

        if let Some(txn_reads) = self.txn_read_holds.get(session.conn_id()) {
            // Find referenced ids not in the read hold. A reference could be caused by a
            // user specifying an object in a different schema than the first query. An
            // index could be caused by a CREATE INDEX after the transaction started.
            let allowed_id_bundle = txn_reads.id_bundle();

            // We don't need the read holds that determine_timestamp acquired
            // for us.
            drop(read_holds);

            let outside = source_bundle.difference(&allowed_id_bundle);
            // Queries without a timestamp and timeline can belong to any existing timedomain.
            if determination.timestamp_context.contains_timestamp() && !outside.is_empty() {
                let valid_names =
                    allowed_id_bundle.resolve_names(self.catalog(), session.conn_id());
                let invalid_names = outside.resolve_names(self.catalog(), session.conn_id());
                return Err(AdapterError::RelationOutsideTimeDomain {
                    relations: invalid_names,
                    names: valid_names,
                });
            }
        } else if let Some(read_holds) = read_holds {
            self.store_transaction_read_holds(session.conn_id().clone(), read_holds);
        }

        // TODO: Checking for only `InTransaction` and not `Implied` (also `Started`?) seems
        // arbitrary and we don't recall why we did it (possibly an error!). Change this to always
        // set the transaction ops. Decide and document what our policy should be on AS OF queries.
        // Maybe they shouldn't be allowed in transactions at all because it's hard to explain
        // what's going on there. This should probably get a small design document.

        // We only track the peeks in the session if the query doesn't use AS
        // OF or we're inside an explicit transaction. The latter case is
        // necessary to support PG's `BEGIN` semantics, whose behavior can
        // depend on whether or not reads have occurred in the txn.
        let mut transaction_determination = determination.clone();
        if when.is_transactional() {
            session.add_transaction_ops(TransactionOps::Peeks {
                determination: transaction_determination,
                cluster_id,
                requires_linearization,
            })?;
        } else if matches!(session.transaction(), &TransactionStatus::InTransaction(_)) {
            // If the query uses AS OF, then ignore the timestamp.
            transaction_determination.timestamp_context = TimestampContext::NoTimestamp;
            session.add_transaction_ops(TransactionOps::Peeks {
                determination: transaction_determination,
                cluster_id,
                requires_linearization,
            })?;
        };

        Ok(determination)
    }
}
