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
//!   [`Lsn`], emit the upstream known offset, and update `SourceStatistics`.
//! * Listen to a provided [`futures::Stream`] of resume uppers, which represents
//!   the durably committed upper for _all_ of the subsources/exports associated
//!   with this source. As the source makes progress this operator does two
//!   things:
//!     1. If [`CDC_CLEANUP_CHANGE_TABLE`] is enabled, will delete entries from
//!        the upstream change table that we've already ingested.
//!     2. Update `SourceStatistics` to notify listeners of a new
//!        "committed LSN".
//!
//! [`SqlServerSource`]: mz_storage_types::sources::SqlServerSource

use std::collections::{BTreeMap, BTreeSet};

use futures::StreamExt;
use mz_ore::future::InTask;
use mz_repr::GlobalId;
use mz_sql_server_util::cdc::Lsn;
use mz_sql_server_util::inspect::get_latest_restore_history_id;
use mz_storage_types::connections::SqlServerConnectionDetails;
use mz_storage_types::sources::SqlServerSourceExtras;
use mz_storage_types::sources::sql_server::{
    CDC_CLEANUP_CHANGE_TABLE, CDC_CLEANUP_CHANGE_TABLE_MAX_DELETES, OFFSET_KNOWN_INTERVAL,
};
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream as TimelyStream};
use timely::progress::Antichain;

use crate::source::sql_server::{ReplicationError, SourceOutputInfo, TransientError};
use crate::source::types::Probe;
use crate::source::{RawSourceCreationConfig, probe};

/// Used as a partition ID to determine the worker that is responsible for
/// handling progress.
static PROGRESS_WORKER: &str = "progress";

pub(crate) fn render<G: Scope<Timestamp = Lsn>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: SqlServerConnectionDetails,
    outputs: BTreeMap<GlobalId, SourceOutputInfo>,
    committed_uppers: impl futures::Stream<Item = Antichain<Lsn>> + 'static,
    extras: SqlServerSourceExtras,
) -> (
    TimelyStream<G, ReplicationError>,
    TimelyStream<G, Probe<Lsn>>,
    PressOnDropButton,
) {
    let op_name = format!("SqlServerProgress({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let (probe_output, probe_stream) = builder.new_output();

    let (button, transient_errors) = builder.build_fallible::<TransientError, _>(move |caps| {
        Box::pin(async move {
            let [probe_cap]: &mut [_; 1] = caps.try_into().unwrap();

            let source_statistics = config.source_statistics().clone();
            let emit_probe = |cap, probe: Probe<Lsn>| {
                probe_output.give(cap, probe);
            };

            // Only a single worker is responsible for processing progress.
            if !config.responsible_for(PROGRESS_WORKER) {
                // Emit 0 to mark this worker as having started up correctly.
                source_statistics.set_offset_known(0);
                source_statistics.set_offset_committed(0);
            }
            let conn_config = connection
                .resolve_config(
                    &config.config.connection_context.secrets_reader,
                    &config.config,
                    InTask::Yes,
                )
                .await?;
            let mut client = mz_sql_server_util::Client::connect(conn_config).await?;


            // Terminate the progress probes if a restore has happened. Replication operator will
            // emit a definite error at the max LSN, but we also have to terminate the RLU probes
            // to ensure that the error propogates to downstream consumers, otherwise it will
            // wait in reclock as the server LSN will always be less than the LSN of the definite
            // error.
            let current_restore_history_id = get_latest_restore_history_id(&mut client).await?;
            if current_restore_history_id != extras.restore_history_id {
                tracing::error!("Restore happened, exiting");
                return Ok(());
             }


            let probe_interval = OFFSET_KNOWN_INTERVAL.handle(config.config.config_set());
            let mut probe_ticker = probe::Ticker::new(|| probe_interval.get(), config.now_fn);

            // Offset that is measured from the upstream SQL Server instance. Tracked to detect an offset that moves backwards.
            let mut prev_offset_known: Option<Lsn> = None;

            // This stream of "resume uppers" tracks all of the Lsn's that we have durably
            // committed for all subsources/exports and thus we can notify the upstream that the
            // change tables can be cleaned up.
            let mut committed_uppers = std::pin::pin!(committed_uppers);
            let cleanup_change_table = CDC_CLEANUP_CHANGE_TABLE.handle(config.config.config_set());
            let cleanup_max_deletes = CDC_CLEANUP_CHANGE_TABLE_MAX_DELETES.handle(config.config.config_set());
            let capture_instances: BTreeSet<_> = outputs.into_values().map(|info| info.capture_instance).collect();

            loop {
                tokio::select! {
                    probe_ts = probe_ticker.tick() => {
                        let max_lsn: Lsn = mz_sql_server_util::inspect::get_max_lsn(&mut client).await?;
                        // We have to return max_lsn + 1 in the probe so that the downstream consumers of
                        // the probe view the actual max lsn as fully committed and all data at that LSN
                        // as no longer subject to change. If we don't increment the LSN before emitting
                        // the probe then data will not be queryable in the tables produced by the Source.
                        let known_lsn = max_lsn.increment();
                        source_statistics.set_offset_known(known_lsn.abbreviate());


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
                        let probe = Probe { probe_ts, upstream_frontier: Antichain::from_elem(known_lsn) };
                        emit_probe(&probe_cap[0], probe);
                        prev_offset_known = Some(known_lsn);
                    },
                    Some(committed_upper) = committed_uppers.next() => {
                        let Some(committed_upper) = committed_upper.as_option() else {
                            // It's possible that the source has been dropped, in which case this can
                            // observe an empty upper. This operator should continue to loop until
                            // the drop dataflow propagates.
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
                                    committed_upper,
                                    cleanup_max_deletes.get(),
                                ).await;
                                // TODO(sql_server2): Track this in a more user observable way.
                                if let Err(err) = cleanup_result {
                                    tracing::warn!(?err, %instance, "cleanup of change table failed!");
                                }
                            }
                        }
                        source_statistics.set_offset_committed(committed_upper.abbreviate());
                    }
                };
            }
        })
    });

    let error_stream = transient_errors.map(ReplicationError::Transient);

    (error_stream, probe_stream, button.press_on_drop())
}
