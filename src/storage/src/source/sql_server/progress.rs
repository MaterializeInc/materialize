// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A "non-critical" operator that tracks the progress of a [`SqlServerSourceConnection`].
//!
//! The operator does the following:
//!
//! * At some cadence `timestamp_interval` will probe the source for the max
//!   [`Lsn`], emit the upstream known offset, and update `SourceStatistics`.
//! * Listen to a provided [`futures::Stream`] of resume uppers, which represents
//!   the durably committed uppers of the subsources/exports associated with
//!   this source. As the source makes progress this operator does two things:
//!     1. If [`CDC_CLEANUP_CHANGE_TABLE`] is enabled, will delete entries from
//!        each capture instance's change table that _all_ of its exports have
//!        ingested.
//!     2. Update each export's `SourceStatistics` to notify listeners of its new
//!        "committed LSN".
//!
//! [`SqlServerSourceConnection`]: mz_storage_types::sources::SqlServerSourceConnection

use std::collections::BTreeMap;

use futures::StreamExt;
use mz_ore::future::InTask;
use mz_repr::GlobalId;
use mz_sql_server_util::cdc::Lsn;
use mz_sql_server_util::inspect::{get_latest_restore_history_id, get_max_lsn};
use mz_storage_types::connections::SqlServerConnectionDetails;
use mz_storage_types::dyncfgs::SQL_SERVER_SOURCE_VALIDATE_RESTORE_HISTORY;
use mz_storage_types::sources::SqlServerSourceExtras;
use mz_storage_types::sources::sql_server::{
    CDC_CLEANUP_CHANGE_TABLE, CDC_CLEANUP_CHANGE_TABLE_MAX_DELETES,
};
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::vec::Map;
use timely::dataflow::{Scope, StreamVec};
use timely::progress::Antichain;

use crate::source::sql_server::{ReplicationError, SourceOutputInfo, TransientError};
use crate::source::types::{Probe, ResumeUppers};
use crate::source::{RawSourceCreationConfig, probe};

/// Used as a partition ID to determine the worker that is responsible for
/// handling progress.
static PROGRESS_WORKER: &str = "progress";

pub(crate) fn render<'scope>(
    scope: Scope<'scope, Lsn>,
    config: RawSourceCreationConfig,
    connection: SqlServerConnectionDetails,
    outputs: BTreeMap<GlobalId, SourceOutputInfo>,
    committed_uppers: impl futures::Stream<Item = ResumeUppers<Lsn>> + 'static,
    extras: SqlServerSourceExtras,
) -> (
    StreamVec<'scope, Lsn, ReplicationError>,
    StreamVec<'scope, Lsn, Probe<Lsn>>,
    PressOnDropButton,
) {
    let op_name = format!("SqlServerProgress({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let (probe_output, probe_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    let (button, transient_errors) = builder.build_fallible::<TransientError, _>(move |caps| {
        Box::pin(async move {
            let [probe_cap]: &mut [_; 1] = caps.try_into().unwrap();

            let emit_probe = |cap, probe: Probe<Lsn>| {
                probe_output.give(cap, probe);
            };

            // Only a single worker is responsible for processing progress.
            if !config.responsible_for(PROGRESS_WORKER) {
                // Emit 0 to mark this worker as having started up correctly.
                for stat in config.statistics.values() {
                    stat.set_offset_known(0);
                    stat.set_offset_committed(0);
                }
                return Ok(());
            }

            // Retrieve the latest upstream LSN eagerly to ensure the lag calculation
            // (offset_known - offset_committed) is non-negative. Statistics represents these as
            // uint8, which would cause the calculation to underflow for the brief period between
            // setting offset_committed here and offset_known further below.
            let conn_config = connection
                .resolve_config(
                    &config.config.connection_context.secrets_reader,
                    &config.config,
                    InTask::Yes,
                )
                .await?;
            let mut client = mz_sql_server_util::Client::connect(conn_config).await?;
            // increment here to match known_offset calculation below
            let next_upstream_lsn: Lsn = get_max_lsn(&mut client).await?.increment();

            // Seed `offset_committed` from the resumption LSN, or if not set, from the
            // upstream's current max LSN. Otherwise, it stays at the default 0 until the
            // initial snapshot durably commits and the first resume upper arrives, which
            // for a large snapshot can be a long time. During that window the ingestion-lag
            // calculation subtracts 0 from the (large) upstream LSN and reports an
            // enormous, bogus lag.
            //
            // This defaults to upstream's current max offset instead of `initial_lsn` because
            // `initial_lsn` can be ahead of the value returned by `sys.fn_cdc_get_max_lsn`. This
            // is a very obscure edge case where a user has a CDC enabled table, creates a new one
            // and configures a source for at least the second table immediately after, without
            // performing any DML operations.
            let seeded_lsn = outputs
                .values()
                // resume_lsn_or will panic if info resume_upper is empty
                .map(|info| info.resume_lsn_or(next_upstream_lsn))
                .min()
                .unwrap_or(next_upstream_lsn);

            for stat in config.statistics.values() {
                stat.set_offset_known(next_upstream_lsn.abbreviate());
                stat.set_offset_committed(seeded_lsn.abbreviate());
            }


            // Terminate the progress probes if a restore has happened. Replication operator will
            // emit a definite error at the max LSN, but we also have to terminate the RLU probes
            // to ensure that the error propogates to downstream consumers, otherwise it will
            // wait in reclock as the server LSN will always be less than the LSN of the definite
            // error.
            let current_restore_history_id = get_latest_restore_history_id(&mut client).await?;
            if current_restore_history_id != extras.restore_history_id
                && SQL_SERVER_SOURCE_VALIDATE_RESTORE_HISTORY.get(config.config.config_set()) {
                tracing::warn!("Restore detected, exiting");
                return Ok(());
             }


            let timestamp_interval = config.timestamp_interval;
            let mut probe_ticker = probe::Ticker::new(move || timestamp_interval, config.now_fn);

            // Offset that is measured from the upstream SQL Server instance. Tracked to detect an offset that moves backwards.
            let mut prev_offset_known: Option<Lsn> = None;

            // This stream of "resume uppers" tracks the Lsn each subsource/export has durably
            // committed, and thus we can notify the upstream that the change tables can be
            // cleaned up.
            let mut committed_uppers = std::pin::pin!(committed_uppers);
            let cleanup_change_table =
                CDC_CLEANUP_CHANGE_TABLE.handle(config.config.config_set());
            let cleanup_max_deletes =
                CDC_CLEANUP_CHANGE_TABLE_MAX_DELETES
                    .handle(config.config.config_set());
            // Each capture instance's exports, and the low water mark its change table was last
            // cleaned up to. Replication resumes each capture instance from the least resume
            // upper of its exports (`resume_lsns` in `replication.rs`), so its change table can
            // be cleaned up to the meet of those exports alone.
            let mut capture_instances: BTreeMap<_, (Vec<GlobalId>, Option<Lsn>)> =
                BTreeMap::new();
            for (id, info) in outputs {
                capture_instances.entry(info.capture_instance).or_default().0.push(id);
            }

            loop {
                tokio::select! {
                    probe_ts = probe_ticker.tick() => {
                        let max_lsn: Lsn = get_max_lsn(&mut client).await?;
                        // We have to return max_lsn + 1 in the probe so that the downstream consumers of
                        // the probe view the actual max lsn as fully committed and all data at that LSN
                        // as no longer subject to change. If we don't increment the LSN before emitting
                        // the probe then data will not be queryable in the tables produced by the Source.
                        let known_lsn = max_lsn.increment();
                        for stat in config.statistics.values() {
                            stat.set_offset_known(known_lsn.abbreviate());
                        }


                        // The DB should never go backwards, but it's good to know if it does.
                        let prev_known_lsn = match prev_offset_known {
                            None => {
                                prev_offset_known = Some(known_lsn);
                                known_lsn
                            },
                            Some(prev) => prev,
                        };
                        if known_lsn < prev_known_lsn {
                            tracing::warn!(
                                "upstream SQL Server went backwards \
                                 in time, current LSN: {known_lsn}, \
                                 last known {prev_known_lsn}",
                            );
                            continue;
                        }
                        let probe = Probe {
                            probe_ts,
                            upstream_frontier: Antichain::from_elem(known_lsn),
                        };
                        emit_probe(&probe_cap[0], probe);
                        prev_offset_known = Some(known_lsn);
                    },
                    Some(uppers) = committed_uppers.next() => {
                        // Never regress below the seeded resumption LSN. During the initial
                        // snapshot an export's resume upper sits at the minimum, which would
                        // otherwise drag its committed offset back to 0 and reintroduce the bogus
                        // lag.
                        for (id, upper) in &uppers.exports {
                            let stat = config.statistics.get(id);
                            if let (Some(lsn), Some(stat)) = (upper.as_option(), stat) {
                                let lsn = std::cmp::max(*lsn, seeded_lsn);
                                stat.set_offset_committed(lsn.abbreviate());
                            }
                        }

                        // If enabled, tell the upstream SQL Server instance to
                        // cleanup the underlying change table.
                        if cleanup_change_table.get() {
                            for (instance, (exports, cleaned)) in capture_instances.iter_mut() {
                                let Some(low_water_mark) = committed_lsn(exports, &uppers.exports)
                                else {
                                    continue;
                                };
                                // `uppers` changes whenever any export's upper moves, usually
                                // leaving this capture instance's low water mark unchanged. A
                                // failed cleanup is retried once the low water mark moves.
                                if *cleaned == Some(low_water_mark) {
                                    continue;
                                }
                                *cleaned = Some(low_water_mark);
                                // TODO(sql_server3): The number of rows that got cleaned
                                // up should be present in informational notices sent back
                                // from the upstream, but the tiberius crate does not
                                // expose these.
                                let cleanup_result =
                                    mz_sql_server_util::inspect::cleanup_change_table(
                                        &mut client,
                                        instance,
                                        &low_water_mark,
                                        cleanup_max_deletes.get(),
                                    ).await;
                                // TODO(sql_server2): Track this in a more user observable way.
                                if let Err(err) = cleanup_result {
                                    tracing::warn!(?err, %instance, "cleanup of change table failed!");
                                }
                            }
                        }
                    }
                };
            }
        })
    });

    let error_stream = transient_errors.map(ReplicationError::Transient);

    (error_stream, probe_stream, button.press_on_drop())
}

/// The [`Lsn`] that every export in `exports` has durably committed through.
///
/// `None` while some export has not committed beyond the ingestion's as_of, and once every
/// export's upper is empty, which happens when the source is dropped.
fn committed_lsn(exports: &[GlobalId], uppers: &BTreeMap<GlobalId, Antichain<Lsn>>) -> Option<Lsn> {
    let mut meet = None;
    for id in exports {
        if let Some(lsn) = uppers.get(id)?.as_option() {
            meet = Some(meet.map_or(*lsn, |meet: Lsn| meet.min(*lsn)));
        }
    }
    meet
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lsn(block_id: u32) -> Lsn {
        Lsn {
            vlf_id: 1,
            block_id,
            record_id: 0,
        }
    }

    #[mz_ore::test]
    fn committed_lsn_is_meet_of_exports() {
        let (a, b, closed) = (GlobalId::User(1), GlobalId::User(2), GlobalId::User(3));
        let uppers = BTreeMap::from([
            (a, Antichain::from_elem(lsn(5))),
            (b, Antichain::from_elem(lsn(3))),
            (closed, Antichain::new()),
        ]);
        assert_eq!(committed_lsn(&[a, b], &uppers), Some(lsn(3)));
        assert_eq!(
            committed_lsn(&[a, closed], &uppers),
            Some(lsn(5)),
            "an empty upper does not constrain the meet",
        );
        assert_eq!(
            committed_lsn(&[closed], &uppers),
            None,
            "every upper is empty"
        );
        assert_eq!(
            committed_lsn(&[a, GlobalId::User(4)], &uppers),
            None,
            "an export has not committed beyond the as_of",
        );
    }
}
