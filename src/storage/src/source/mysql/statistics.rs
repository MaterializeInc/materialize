// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the statistics collection of the [`MySqlSourceConnection`] ingestion dataflow.

use futures::StreamExt;
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use mz_mysql_util::query_sys_var;
use mz_storage_types::sources::mysql::{gtid_set_frontier, GtidPartition, GtidState};
use mz_storage_types::sources::MySqlSourceConnection;
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};

use crate::source::types::ProgressStatisticsUpdate;
use crate::source::RawSourceCreationConfig;

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
    PressOnDropButton,
) {
    let op_name = format!("MySqlStatistics({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let (mut stats_output, stats_stream) = builder.new_output();

    // TODO: Add additional metrics

    let (button, transient_errors) = builder.build_fallible::<TransientError, _>(move |caps| {
        Box::pin(async move {
            let worker_id = config.worker_id;
            let [stats_cap]: &mut [_; 1] = caps.try_into().unwrap();

            // Only run the replication reader on the worker responsible for it.
            if !config.responsible_for(STATISTICS) {
                // Emit 0, to mark this worker as having started up correctly.
                stats_output
                    .give(
                        &stats_cap[0],
                        ProgressStatisticsUpdate::SteadyState {
                            offset_known: 0,
                            offset_committed: 0,
                        },
                    )
                    .await;
                return Ok(());
            }

            let connection_config = connection
                .connection
                .config(
                    &*config.config.connection_context.secrets_reader,
                    &config.config,
                )
                .await?;

            let mut stats_conn = connection_config
                .connect(
                    &format!("timely-{worker_id} MySQL replication statistics"),
                    &config.config.connection_context.ssh_tunnel_manager,
                )
                .await?;

            tokio::pin!(resume_uppers);

            let mut offset_known = None;
            let mut offset_committed = None;

            let upstream_stream = async_stream::stream!({
                // TODO(guswynn): make configurable (at the same time as the pg one).
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
                loop {
                    interval.tick().await;

                    let gtid_executed =
                        query_sys_var(&mut stats_conn, "global.gtid_executed").await?;
                    match gtid_set_frontier(&gtid_executed) {
                        Ok(frontier) => yield Ok(frontier),
                        Err(err) => {
                            // We don't translate this into a definite error like in snapshotting, but we will
                            // restart the source.
                            yield Err::<_, TransientError>(err.into())
                        }
                    }
                }
            });
            tokio::pin!(upstream_stream);

            loop {
                tokio::select! {
                    Some(upstream_frontier) = upstream_stream.next() => {
                        offset_known = Some(aggregate_mysql_frontier(upstream_frontier?));

                    },
                    Some(committed_frontier) = resume_uppers.next() => {
                        offset_committed = Some(aggregate_mysql_frontier(committed_frontier));
                    },
                    else => break
                };

                if let (Some(offset_known), Some(offset_committed)) =
                    (offset_known, offset_committed)
                {
                    stats_output
                        .give(
                            &stats_cap[0],
                            ProgressStatisticsUpdate::SteadyState {
                                offset_known,
                                offset_committed,
                            },
                        )
                        .await;
                }
            }

            Ok(())
        })
    });

    (
        stats_stream,
        transient_errors.map(ReplicationError::from),
        button.press_on_drop(),
    )
}

/// Aggregate a mysql frontier into single number representing the
/// _number of transactions_ it represents.
fn aggregate_mysql_frontier(frontier: Antichain<GtidPartition>) -> u64 {
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
