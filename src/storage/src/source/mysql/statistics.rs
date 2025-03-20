// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the statistics collection of the [`MySqlSourceConnection`] ingestion dataflow.

use std::cell::{Cell, RefCell};

use futures::StreamExt;
use mz_storage_types::dyncfgs::MYSQL_OFFSET_KNOWN_INTERVAL;
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use mz_mysql_util::query_sys_var;
use mz_ore::future::InTask;
use mz_storage_types::sources::mysql::{gtid_set_frontier, GtidPartition, GtidState};
use mz_storage_types::sources::MySqlSourceConnection;
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};

use crate::source::types::{Probe, ProgressStatisticsUpdate};
use crate::source::{probe, RawSourceCreationConfig};

use super::{ReplicationError, TransientError};

static STATISTICS: &str = "statistics";

/// Renders the statistics dataflow.
pub(crate) fn render<G: Scope<Timestamp = GtidPartition>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: MySqlSourceConnection,
    resume_uppers: impl futures::Stream<Item = Antichain<GtidPartition>> + 'static,
) -> (
    Stream<G, ProgressStatisticsUpdate>,
    Stream<G, ReplicationError>,
    Stream<G, Probe<GtidPartition>>,
    PressOnDropButton,
) {
    let op_name = format!("MySqlStatistics({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let (stats_output, stats_stream) = builder.new_output();
    let (probe_output, probe_stream) = builder.new_output();

    // TODO: Add additional metrics

    let (button, transient_errors) = builder.build_fallible::<TransientError, _>(move |caps| {
        Box::pin(async move {
            let worker_id = config.worker_id;
            let [stats_cap, probe_cap]: &mut [_; 2] = caps.try_into().unwrap();

            // Only run the replication reader on the worker responsible for it.
            if !config.responsible_for(STATISTICS) {
                // Emit 0, to mark this worker as having started up correctly.
                stats_output.give(
                    &stats_cap[0],
                    ProgressStatisticsUpdate::SteadyState {
                        offset_known: 0,
                        offset_committed: 0,
                    },
                );
                return Ok(());
            }

            let connection_config = connection
                .connection
                .config(
                    &config.config.connection_context.secrets_reader,
                    &config.config,
                    InTask::Yes,
                )
                .await?;

            let mut stats_conn = connection_config
                .connect(
                    &format!("timely-{worker_id} MySQL replication statistics"),
                    &config.config.connection_context.ssh_tunnel_manager,
                )
                .await?;

            tokio::pin!(resume_uppers);

            let prev_offset_known = Cell::new(None);
            let prev_offset_committed = Cell::new(None);
            let stats_output = RefCell::new(stats_output);

            let mut probe_ticker = probe::Ticker::new(
                || MYSQL_OFFSET_KNOWN_INTERVAL.get(config.config.config_set()),
                config.now_fn,
            );
            let probe_loop = async {
                loop {
                    let probe_ts = probe_ticker.tick().await;

                    let gtid_executed =
                        query_sys_var(&mut stats_conn, "global.gtid_executed").await?;
                    // We don't translate this into a definite error like in snapshotting, but we
                    // will restart the source.
                    let upstream_frontier =
                        gtid_set_frontier(&gtid_executed).map_err(TransientError::from)?;

                    let offset_known = aggregate_mysql_frontier(&upstream_frontier);
                    if let Some(offset_committed) = prev_offset_committed.get() {
                        stats_output.borrow_mut().give(
                            &stats_cap[0],
                            ProgressStatisticsUpdate::SteadyState {
                                offset_known,
                                offset_committed,
                            },
                        );
                    }
                    prev_offset_known.set(Some(offset_known));

                    probe_output.give(
                        &probe_cap[0],
                        Probe {
                            probe_ts,
                            upstream_frontier,
                        },
                    );
                }
            };
            let commit_loop = async {
                while let Some(committed_frontier) = resume_uppers.next().await {
                    let offset_committed = aggregate_mysql_frontier(&committed_frontier);
                    if let Some(offset_known) = prev_offset_known.get() {
                        stats_output.borrow_mut().give(
                            &stats_cap[0],
                            ProgressStatisticsUpdate::SteadyState {
                                offset_known,
                                offset_committed,
                            },
                        );
                    }
                    prev_offset_committed.set(Some(offset_committed));
                }
            };

            futures::future::join(probe_loop, commit_loop).await.0
        })
    });

    (
        stats_stream,
        transient_errors.map(ReplicationError::from),
        probe_stream,
        button.press_on_drop(),
    )
}

/// Aggregate a mysql frontier into single number representing the
/// _number of transactions_ it represents.
fn aggregate_mysql_frontier(frontier: &Antichain<GtidPartition>) -> u64 {
    let mut progress_stat = 0;
    for ts in frontier.iter() {
        if let Some(_uuid) = ts.interval().singleton() {
            // We assume source id's don't disappear once they appear.
            let ts = match ts.timestamp() {
                GtidState::Absent => 0,
                // Txid's in mysql start at 1, so we subtract 1 from the _frontier_
                // to get the _number of transactions_.
                GtidState::Active(id) => id.get().saturating_sub(1),
            };
            progress_stat += ts;
        }
    }
    progress_stat
}
