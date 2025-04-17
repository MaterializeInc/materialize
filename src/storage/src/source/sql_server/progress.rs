// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A "non-critical" operator that tracks the progress of a [`SqlServerSource`].
//!
//! The operator does the following:
//!
//! * At some cadence [`OFFSET_KNOWN_INTERVAL`] will probe the source for the max
//!   [`Lsn`] and emit a [`ProgressStatisticsUpdate`] to notify listeners of a
//!   new "known LSN".
//! * Listen to a provided [`futures::Stream`] of resume uppers, which represents
//!   the durably committed upper for _all_ of the subsources/exports associated
//!   with this source. As the source makes progress this operator does two
//!   things:
//!     1. If [`CDC_CLEANUP_CHANGE_TABLE`] is enabled, will delete entries from
//!        the upstream change table that we've already ingested.
//!     2. Emit a [`ProgressStatisticsUpdate`] to notify listeners of a new
//!        "committed LSN".
//!
//! [`SqlServerSource`]: mz_storage_types::sources::SqlServerSource

use std::collections::{BTreeMap, BTreeSet};

use futures::StreamExt;
use mz_ore::future::InTask;
use mz_repr::GlobalId;
use mz_sql_server_util::cdc::Lsn;
use mz_storage_types::connections::SqlServerConnectionDetails;
use mz_storage_types::sources::sql_server::{
    CDC_CLEANUP_CHANGE_TABLE, CDC_CLEANUP_CHANGE_TABLE_MAX_DELETES, OFFSET_KNOWN_INTERVAL,
};
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream as TimelyStream};
use timely::progress::Antichain;

use crate::source::sql_server::{ReplicationError, SourceOutputInfo, TransientError};
use crate::source::types::ProgressStatisticsUpdate;
use crate::source::{RawSourceCreationConfig, probe};

/// Used as a partition ID to determine the worker that is responsible for
/// handling progress.
static PROGRESS_WORKER: &str = "progress";

pub(crate) fn render<G: Scope<Timestamp = Lsn>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: SqlServerConnectionDetails,
    outputs: BTreeMap<GlobalId, SourceOutputInfo>,
    resume_uppers: impl futures::Stream<Item = Antichain<Lsn>> + 'static,
) -> (
    TimelyStream<G, ProgressStatisticsUpdate>,
    TimelyStream<G, ReplicationError>,
    PressOnDropButton,
) {
    let op_name = format!("SqlServerProgress({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let (stats_output, stats_stream) = builder.new_output();

    let (button, transient_errors) = builder.build_fallible::<TransientError, _>(move |caps| {
        Box::pin(async move {
            let [stats_cap]: &mut [_; 1] = caps.try_into().unwrap();

            // Small helper closure.
            let emit_stats = |cap, known: u64, committed: u64| {
                let update = ProgressStatisticsUpdate::SteadyState {
                    offset_known: known,
                    offset_committed: committed,
                };
                tracing::debug!(?config.id, %known, %committed, "steadystate progress");
                stats_output.give(cap, update);
            };

            // Only a single worker is responsible for processing progress.
            if !config.responsible_for(PROGRESS_WORKER) {
                // Emit 0 to mark this worker as having started up correctly.
                emit_stats(&stats_cap[0], 0, 0);
            }

            let conn_config = connection
                .resolve_config(
                    &config.config.connection_context.secrets_reader,
                    &config.config,
                    InTask::Yes,
                )
                .await?;
            let (mut client, conn) = mz_sql_server_util::Client::connect(conn_config).await?;
            // TODO(sql_server1): Move the connection into its own future.
            mz_ore::task::spawn(|| "sql_server-progress-conn", async move { conn.await });

            let probe_interval = OFFSET_KNOWN_INTERVAL.handle(config.config.config_set());
            let mut probe_ticker = probe::Ticker::new(|| probe_interval.get(), config.now_fn);

            // Offset that is measured from the upstream SQL Server instance.
            let mut prev_offset_known: Option<Lsn> = None;
            // Offset that we have observed from the `resume_uppers` stream.
            let mut prev_offset_committed: Option<Lsn> = None;

            // This stream of "resume uppers" tracks all of the Lsn's that we have durably
            // committed for all subsources/exports and thus we can notify the upstream that the
            // change tables can be cleaned up.
            let mut resume_uppers = std::pin::pin!(resume_uppers);
            let cleanup_change_table = CDC_CLEANUP_CHANGE_TABLE.handle(config.config.config_set());
            let cleanup_max_deletes = CDC_CLEANUP_CHANGE_TABLE_MAX_DELETES.handle(config.config.config_set());
            let capture_instances: BTreeSet<_> = outputs.into_values().map(|info| info.capture_instance).collect();

            loop {
                tokio::select! {
                    // TODO(sql_server2): Emit probes here.
                    _probe_ts = probe_ticker.tick() => {
                        let known_lsn = mz_sql_server_util::inspect::get_max_lsn(&mut client).await?;

                        // The DB should never go backwards, but it's good to know if it does.
                        let prev_known_lsn = match prev_offset_known {
                            None => {
                                prev_offset_known = Some(known_lsn);
                                known_lsn
                            },
                            Some(prev) => prev,
                        };
                        if known_lsn < prev_known_lsn {
                            mz_ore::soft_panic_or_log!(
                                "upstream SQL Server went backwards in time, current LSN: {known_lsn}, last known {prev_known_lsn}",
                            );
                            continue;
                        }

                        // Update any listeners with our most recently known LSN.
                        if let Some(prev_commit_lsn) = prev_offset_committed {
                            let known_lsn_abrv = known_lsn.abbreviate();
                            let commit_lsn_abrv = prev_commit_lsn.abbreviate();
                            emit_stats(&stats_cap[0], known_lsn_abrv, commit_lsn_abrv);
                        }
                        prev_offset_known = Some(known_lsn);
                    },
                    Some(resume_upper) = resume_uppers.next() => {
                        let Some(resume_upper) = resume_upper.as_option() else {
                            mz_ore::soft_panic_or_log!("empty resume upper? {resume_upper:?}");
                            continue;
                        };

                        // If enabled, tell the upstream SQL Server instance to
                        // cleanup the underlying change table.
                        if cleanup_change_table.get() {
                            for instance in &capture_instances {
                                // TODO(sql_server3): The number of rows that got cleaned
                                // up should be present in informational notices sent back
                                // from the upstream, but the tiberius crate does not
                                // expose these.
                                let cleanup_result = mz_sql_server_util::inspect::cleanup_change_table(
                                    &mut client,
                                    instance,
                                    resume_upper,
                                    cleanup_max_deletes.get(),
                                ).await;
                                // TODO(sql_server2): Track this in a more user observable way.
                                if let Err(err) = cleanup_result {
                                    tracing::warn!(?err, %instance, "cleanup of change table failed!");
                                }
                            }
                        }

                        // Update any listeners with our most recently committed LSN.
                        if let Some(prev_known_lsn) = prev_offset_known {
                            let known_lsn_abrv = prev_known_lsn.abbreviate();
                            let commit_lsn_abrv = resume_upper.abbreviate();
                            emit_stats(&stats_cap[0], known_lsn_abrv, commit_lsn_abrv);
                        }
                        prev_offset_committed = Some(*resume_upper);
                    }
                };
            }
        })
    });

    let error_stream = transient_errors.map(ReplicationError::Transient);

    (stats_stream, error_stream, button.press_on_drop())
}
